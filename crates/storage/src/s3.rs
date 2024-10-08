use std::path::Path;
use std::time::Duration;

use crate::storage::{
    BackfillProcesses, BlobData, LockFile, Storage, StorageReader, StorageWriter,
};
use async_trait::async_trait;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use eth2::types::Hash256;
use eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing::log::trace;

use crate::storage::BACKFILL_LOCK;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub path: String,

    pub compression: bool,
}

pub struct S3Storage {
    pub client: Client,
    pub bucket: String,
    pub path: String,
    pub compression: bool,
}

impl S3Storage {
    pub async fn new(config: S3Config) -> Result<Self> {
        let env_config = aws_config::from_env().load().await;
        let sdk_config = aws_sdk_s3::config::Builder::from(&env_config)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(15))
                    .operation_timeout(Duration::from_secs(30))
                    .build(),
            )
            .retry_config(RetryConfig::standard())
            .endpoint_url(config.endpoint.as_str())
            .force_path_style(true)
            .build();
        let client = Client::from_conf(sdk_config);

        let storage = Self {
            client,
            bucket: config.bucket,
            path: config.path,
            compression: config.compression,
        };

        let res = storage.read_backfill_processes().await;
        if let Err(e) = res {
            if let Some(io_error) = e.downcast_ref::<SdkError<GetObjectError>>() {
                if !matches!(io_error, SdkError::ServiceError(err) if err.err().is_no_such_key()) {
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
            if let Some(io_error) = e.downcast_ref::<SdkError<GetObjectError>>() {
                if !matches!(io_error, SdkError::ServiceError(err) if err.err().is_no_such_key()) {
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
impl Storage for S3Storage {}

#[async_trait]
impl StorageReader for S3Storage {
    async fn read_blob_data(&self, hash: &Hash256) -> Result<BlobData> {
        let blob_path = Path::new(&self.path).join(format!("{:x}", hash));
        let blob_res = self
            .client
            .get_object()
            .bucket(self.bucket.as_str())
            .key(blob_path.to_str().ok_or(eyre::eyre!("Invalid blob path"))?)
            .send()
            .await?;

        let blob_data_bytes = blob_res.body.collect().await?.to_vec();

        let blob_data: BlobData = if blob_res.content_encoding.filter(|x| x == "gzip").is_some() {
            let gzip_decoder = flate2::read::GzDecoder::new(blob_data_bytes.as_slice());
            serde_json::from_reader(gzip_decoder)?
        } else {
            serde_json::from_slice(blob_data_bytes.as_slice())?
        };

        Ok(blob_data)
    }

    async fn exists(&self, hash: &Hash256) -> bool {
        let blob_path = Path::new(&self.path).join(format!("{:x}", hash));
        if let Some(path) = blob_path.to_str() {
            self.client
                .head_object()
                .bucket(self.bucket.as_str())
                .key(path)
                .send()
                .await
                .is_ok()
        } else {
            false
        }
    }

    async fn read_lock_file(&self) -> Result<LockFile> {
        let lock_file_path = Path::new(&self.path).join("lockfile");
        let lock_file_res = self
            .client
            .get_object()
            .bucket(self.bucket.as_str())
            .key(
                lock_file_path
                    .to_str()
                    .ok_or(eyre::eyre!("Invalid lock file path"))?,
            )
            .send()
            .await?;

        let lock_file_bytes = lock_file_res.body.collect().await?.to_vec();
        let lock_file: LockFile = serde_json::from_slice(lock_file_bytes.as_slice())?;

        Ok(lock_file)
    }

    async fn read_backfill_processes(&self) -> Result<BackfillProcesses> {
        BACKFILL_LOCK.lock();
        let backfill_process_path = Path::new(&self.path).join("backfill_processes");
        let backfill_process_res = self
            .client
            .get_object()
            .bucket(self.bucket.as_str())
            .key(
                backfill_process_path
                    .to_str()
                    .ok_or(eyre::eyre!("Invalid backfill processes path"))?,
            )
            .send()
            .await?;

        let backfill_process_bytes = backfill_process_res.body.collect().await?.to_vec();
        let backfill_processes: BackfillProcesses =
            serde_json::from_slice(backfill_process_bytes.as_slice())?;

        Ok(backfill_processes)
    }
}

#[async_trait]
impl StorageWriter for S3Storage {
    async fn write_blob_data(&mut self, blob_data: &BlobData) -> Result<()> {
        let blob_path =
            Path::new(&self.path).join(format!("{:x}", blob_data.header.beacon_block_hash));
        let blob_data_bytes = if self.compression {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            serde_json::to_writer(&mut encoder, blob_data)?;
            encoder.finish()?
        } else {
            serde_json::to_vec(blob_data)?
        };

        let mut put_object_request = self
            .client
            .put_object()
            .bucket(self.bucket.as_str())
            .key(blob_path.to_str().ok_or(eyre::eyre!("Invalid blob path"))?)
            .content_type("application/json")
            .body(ByteStream::from(blob_data_bytes));

        if self.compression {
            put_object_request = put_object_request.content_encoding("gzip");
        }

        let _ = put_object_request.send().await?;
        trace!("Wrote blob data to S3: {:?}", blob_path);
        Ok(())
    }

    async fn write_lock_file(&self, lock_file: &LockFile) -> Result<()> {
        let lock_file_bytes = serde_json::to_vec(lock_file)?;
        let lock_file_path = Path::new(&self.path).join("lockfile");

        let _ = self
            .client
            .put_object()
            .bucket(self.bucket.as_str())
            .key(
                lock_file_path
                    .to_str()
                    .ok_or(eyre::eyre!("Invalid lock file path"))?,
            )
            .content_type("application/json")
            .body(ByteStream::from(lock_file_bytes))
            .send()
            .await?;

        trace!("Wrote lock file to S3: {:?}", lock_file_path);
        Ok(())
    }

    async fn write_backfill_processes(&self, backfill_processes: &BackfillProcesses) -> Result<()> {
        BACKFILL_LOCK.lock();
        let backfill_process_bytes = serde_json::to_vec(backfill_processes)?;
        let backfill_process_path = Path::new(&self.path).join("backfill_processes");

        let _ = self
            .client
            .put_object()
            .bucket(self.bucket.as_str())
            .key(
                backfill_process_path
                    .to_str()
                    .ok_or(eyre::eyre!("Invalid backfill processes path"))?,
            )
            .content_type("application/json")
            .body(ByteStream::from(backfill_process_bytes))
            .send()
            .await?;

        info!(
            "Wrote backfill processes to S3: {:?}",
            backfill_process_path
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::Duration;

    use aws_config::retry::RetryConfig;
    use aws_config::timeout::TimeoutConfig;
    use aws_sdk_s3::types::error::NoSuchKey;
    use aws_sdk_s3::Client;
    use testcontainers_modules::localstack::LocalStack;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

    use crate::s3::{S3Config, S3Storage};
    use crate::storage::{create_test_blob_data, LockFile};
    use crate::storage::{StorageReader, StorageWriter};

    #[tokio::test]
    async fn test_write_read_blob_data() {
        let (mut storage, _container) = setup(false).await;

        let blob_data = create_test_blob_data();
        let hash = blob_data.header.beacon_block_hash;
        assert!(storage
            .read_blob_data(&hash)
            .await
            .is_err_and(|e| e.root_cause().downcast_ref::<NoSuchKey>().is_some()));
        storage.write_blob_data(&blob_data).await.unwrap();

        let actual_blob_data = storage.read_blob_data(&hash).await.unwrap();
        assert_eq!(actual_blob_data.header.beacon_block_hash, hash);
        assert_eq!(actual_blob_data.blob_sidecars.data.len(), 0);
    }

    #[tokio::test]
    async fn test_write_read_lock_file() {
        let (storage, _container) = setup(false).await;

        assert_eq!(storage.read_lock_file().await.unwrap(), LockFile::default());
    }

    #[tokio::test]
    async fn test_write_read_blob_data_compressed() {
        let (mut storage, _container) = setup(true).await;

        let blob_data = create_test_blob_data();
        let hash = blob_data.header.beacon_block_hash;
        assert!(storage
            .read_blob_data(&hash)
            .await
            .is_err_and(|e| e.root_cause().downcast_ref::<NoSuchKey>().is_some()));
        storage.write_blob_data(&blob_data).await.unwrap();

        let actual_blob_data = storage.read_blob_data(&hash).await.unwrap();
        assert_eq!(actual_blob_data.header.beacon_block_hash, hash);
        assert_eq!(actual_blob_data.blob_sidecars.data.len(), 0);
    }

    async fn setup(compression: bool) -> (S3Storage, ContainerAsync<LocalStack>) {
        let request = LocalStack::default().with_env_var("SERVICES", "s3");
        let container = request.start().await.unwrap();

        let host_ip = container.get_host().await.unwrap();
        let host_port = container.get_host_port_ipv4(4566).await.unwrap();

        env::set_var("AWS_ACCESS_KEY_ID", "test");
        env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        env::set_var("AWS_REGION", "us-east-1");

        let config = S3Config {
            endpoint: format!("http://{}:{}", host_ip, host_port),
            bucket: "test-bucket".to_string(),
            path: "blobs".to_string(),
            compression,
        };

        let env_config = aws_config::from_env().load().await;
        let sdk_config = aws_sdk_s3::config::Builder::from(&env_config)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(15))
                    .operation_timeout(Duration::from_secs(30))
                    .build(),
            )
            .retry_config(RetryConfig::standard())
            .endpoint_url(config.clone().endpoint.as_str())
            .force_path_style(true)
            .build();
        let client = Client::from_conf(sdk_config);

        client
            .create_bucket()
            .bucket("test-bucket")
            .send()
            .await
            .unwrap();

        let storage = S3Storage::new(config.clone()).await.unwrap();
        (storage, container)
    }
}
