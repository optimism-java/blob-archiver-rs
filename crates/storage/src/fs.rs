use std::path::PathBuf;

use crate::storage::{BackfillProcesses, BACKFILL_LOCK};
use crate::{BlobData, LockFile, Storage, StorageReader, StorageWriter};
use async_trait::async_trait;
use eth2::types::Hash256;
use eyre::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::log::info;

pub struct FSStorage {
    pub dir: PathBuf,
}

impl FSStorage {
    pub async fn new(dir: PathBuf) -> Result<Self> {
        let storage = Self { dir };

        let res = storage.read_backfill_processes().await;

        if let Err(e) = res {
            if let Some(io_error) = e.downcast_ref::<std::io::Error>() {
                if io_error.kind() != std::io::ErrorKind::NotFound {
                    return Err(e);
                } else {
                    storage
                        .write_backfill_processes(&BackfillProcesses::default())
                        .await?;
                }
            } else {
                return Err(e);
            }
        }

        let res = storage.read_lock_file().await;
        if let Err(e) = res {
            if let Some(io_error) = e.downcast_ref::<std::io::Error>() {
                if io_error.kind() != std::io::ErrorKind::NotFound {
                    return Err(e);
                } else {
                    storage.write_lock_file(&LockFile::default()).await?;
                }
            } else {
                return Err(e);
            }
        }

        Ok(storage)
    }
}

#[async_trait]
impl Storage for FSStorage {}

#[async_trait]
impl StorageReader for FSStorage {
    async fn read_blob_data(&self, hash: &Hash256) -> Result<BlobData> {
        let path = self.dir.join(format!("{:x}", hash));
        let mut file = tokio::fs::File::open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(serde_json::from_slice(&data)?)
    }

    async fn exists(&self, hash: &Hash256) -> bool {
        self.dir.join(format!("{:x}", hash)).exists()
    }

    async fn read_lock_file(&self) -> Result<LockFile> {
        let path = self.dir.join("lockfile");
        let mut file = tokio::fs::File::open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(serde_json::from_slice(&data)?)
    }

    async fn read_backfill_processes(&self) -> Result<BackfillProcesses> {
        BACKFILL_LOCK.lock();
        let path = self.dir.join("backfill_processes");
        let mut file = tokio::fs::File::open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        Ok(serde_json::from_slice(&data)?)
    }
}

#[async_trait]
impl StorageWriter for FSStorage {
    async fn write_blob_data(&mut self, blob_data: &BlobData) -> Result<()> {
        let path = self
            .dir
            .join(format!("{:x}", blob_data.header.beacon_block_hash));
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&serde_json::to_vec(blob_data)?).await?;
        Ok(())
    }

    async fn write_lock_file(&self, lock_file: &LockFile) -> Result<()> {
        let path = self.dir.join("lockfile");
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&serde_json::to_vec(lock_file)?).await?;
        info!("lock file {:#?}", lock_file);
        Ok(())
    }

    async fn write_backfill_processes(&self, backfill_process: &BackfillProcesses) -> Result<()> {
        BACKFILL_LOCK.lock();
        let path = self.dir.join("backfill_processes");
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&serde_json::to_vec(backfill_process)?)
            .await?;
        Ok(())
    }
}

pub struct TestFSStorage {
    pub fs_storage: FSStorage,
    pub write_fail_count: usize,
}

impl TestFSStorage {
    pub async fn new(fs_storage: FSStorage) -> Result<Self> {
        Ok(Self {
            fs_storage,
            write_fail_count: 0,
        })
    }

    pub async fn write_fail_times(&mut self, times: usize) {
        self.write_fail_count = times;
    }
}

#[async_trait]
impl StorageReader for TestFSStorage {
    async fn read_blob_data(&self, hash: &Hash256) -> Result<BlobData> {
        self.fs_storage.read_blob_data(hash).await
    }

    async fn exists(&self, hash: &Hash256) -> bool {
        self.fs_storage.exists(hash).await
    }

    async fn read_lock_file(&self) -> Result<LockFile> {
        self.fs_storage.read_lock_file().await
    }

    async fn read_backfill_processes(&self) -> Result<BackfillProcesses> {
        self.fs_storage.read_backfill_processes().await
    }
}

#[async_trait]
impl StorageWriter for TestFSStorage {
    async fn write_blob_data(&mut self, blob_data: &BlobData) -> Result<()> {
        if self.write_fail_count > 0 {
            self.write_fail_count -= 1;
            return Err(eyre::Error::msg("write fail"));
        }
        self.fs_storage.write_blob_data(blob_data).await
    }

    async fn write_lock_file(&self, lock_file: &LockFile) -> Result<()> {
        self.fs_storage.write_lock_file(lock_file).await
    }

    async fn write_backfill_processes(&self, backfill_process: &BackfillProcesses) -> Result<()> {
        self.fs_storage
            .write_backfill_processes(backfill_process)
            .await
    }
}

#[async_trait]
impl Storage for TestFSStorage {}

#[cfg(test)]
mod tests {
    use tokio::io;

    use crate::storage::create_test_blob_data;

    use super::*;

    #[tokio::test]
    async fn test_fs_storage() {
        let mut storage = FSStorage::new(PathBuf::from("test_fs_storage"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(&storage.dir).await.unwrap();
        let blob_data = create_test_blob_data();
        assert!(storage
            .read_blob_data(&blob_data.header.beacon_block_hash)
            .await
            .is_err_and(|e| e.downcast_ref::<io::Error>().is_some()));
        storage.write_blob_data(&blob_data).await.unwrap();
        assert_eq!(
            storage
                .read_blob_data(&blob_data.header.beacon_block_hash)
                .await
                .unwrap(),
            blob_data
        );
        assert_eq!(storage.read_lock_file().await.unwrap(), LockFile::default());
        assert_eq!(
            storage.read_backfill_processes().await.unwrap(),
            BackfillProcesses::default()
        );
        clean_dir(&storage.dir);
    }

    #[tokio::test]
    async fn test_fs_storage_exists() {
        let mut storage = FSStorage::new(PathBuf::from("test_fs_storage_exists"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(&storage.dir).await.unwrap();
        let blob_data = create_test_blob_data();
        assert!(!storage.exists(&blob_data.header.beacon_block_hash).await);
        storage.write_blob_data(&blob_data).await.unwrap();
        assert!(storage.exists(&blob_data.header.beacon_block_hash).await);
        clean_dir(&storage.dir);
    }

    fn clean_dir(dir: &PathBuf) {
        if dir.exists() {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
