use std::path::PathBuf;

use async_trait::async_trait;
use eth2::types::Hash256;
use eyre::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::storage::{BackfillProcesses, BACKFILL_LOCK};
use crate::{BlobData, LockFile, Storage, StorageReader, StorageWriter};

pub struct FSStorage {
    pub dir: PathBuf,
}

impl FSStorage {
    pub async fn new(dir: PathBuf) -> Result<Self> {
        Ok(Self { dir })
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
    async fn write_blob_data(&self, blob_data: &BlobData) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use tokio::io;

    use crate::storage::{
        create_test_blob_data, create_test_lock_file, create_test_test_backfill_processes,
    };

    use super::*;

    #[tokio::test]
    async fn test_fs_storage() {
        let storage = FSStorage::new(PathBuf::from("test_dir")).await.unwrap();
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
        let lock_file = create_test_lock_file();
        assert!(storage
            .read_lock_file()
            .await
            .is_err_and(|e| e.downcast_ref::<io::Error>().is_some()));
        storage.write_lock_file(&lock_file).await.unwrap();
        assert_eq!(storage.read_lock_file().await.unwrap(), lock_file);
        let test_backfill_processes = create_test_test_backfill_processes();
        assert!(storage
            .read_backfill_processes()
            .await
            .is_err_and(|e| e.downcast_ref::<io::Error>().is_some()));
        storage
            .write_backfill_processes(&test_backfill_processes)
            .await
            .unwrap();
        assert_eq!(
            storage.read_backfill_processes().await.unwrap(),
            test_backfill_processes
        );
        clean_dir(&storage.dir);
    }

    #[tokio::test]
    async fn test_fs_storage_exists() {
        let storage = FSStorage::new(PathBuf::from("test_dir")).await.unwrap();
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
