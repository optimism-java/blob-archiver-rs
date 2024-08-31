use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use again::RetryPolicy;
use blob_archiver_beacon::beacon_client;
use blob_archiver_beacon::beacon_client::BeaconClientEth2;
use blob_archiver_storage::fs::FSStorage;
use blob_archiver_storage::s3::{S3Config, S3Storage};
use blob_archiver_storage::storage;
use blob_archiver_storage::storage::{Storage, StorageType};
use clap::Parser;
use ctrlc::set_handler;
use eth2::types::{BlockId, Hash256};
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use eyre::eyre;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::log::error;
use tracing::Level;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;

use crate::archiver::{Archiver, Config, STARTUP_FETCH_BLOB_MAXIMUM_RETRIES};

mod api;
mod archiver;

static INIT: std::sync::Once = std::sync::Once::new();

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();

    let config: Config = args.to_config();
    init_logging(
        config.log_config.verbose,
        config.log_config.log_dir.clone(),
        config
            .log_config
            .log_rotation
            .clone()
            .map(|s| to_rotation(s.as_str())),
    );
    let beacon_client = BeaconNodeHttpClient::new(
        SensitiveUrl::from_str(config.beacon_config.beacon_endpoint.as_str()).unwrap(),
        Timeouts::set_all(config.beacon_config.beacon_client_timeout),
    );
    let storage: Arc<Mutex<dyn Storage>> = if config.storage_config.storage_type == StorageType::FS
    {
        Arc::new(Mutex::new(
            FSStorage::new(config.storage_config.fs_dir.clone().unwrap())
                .await
                .unwrap(),
        ))
    } else {
        Arc::new(Mutex::new(
            S3Storage::new(config.storage_config.s3_config.clone().unwrap())
                .await
                .unwrap(),
        ))
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    set_handler(move || {
        tracing::info!("shutting down");
        shutdown_tx
            .send(true)
            .expect("could not send shutdown signal");
    })
    .expect("could not register shutdown handler");

    let beacon_client_eth2 = BeaconClientEth2 { beacon_client };
    let archiver = Archiver::new(
        Arc::new(Mutex::new(beacon_client_eth2)),
        storage,
        config,
        shutdown_rx,
    );

    let retry_policy = RetryPolicy::exponential(Duration::from_millis(250))
        .with_jitter(true)
        .with_max_delay(Duration::from_secs(10))
        .with_max_retries(STARTUP_FETCH_BLOB_MAXIMUM_RETRIES);
    let res = retry_policy
        .retry(|| archiver.persist_blobs_for_block(BlockId::Head, false))
        .await;

    match res {
        Err(e) => {
            error!("failed to seed archiver with initial block: {:#?}", e);
            std::process::exit(1);
        }
        Ok(Some((curr, _))) => {
            archiver.wait_obtain_storage_lock().await;
            archiver.track_latest_block().await;
            tokio::spawn(async move {
                archiver.backfill_blobs(curr).await;
            });
        }
        Ok(None) => {
            error!("Error fetching blobs for block");
            std::process::exit(1);
        }
    };
}

fn init_logging(verbose: u8, log_dir: Option<PathBuf>, rotation: Option<Rotation>) {
    INIT.call_once(|| {
        setup_tracing(verbose, log_dir, rotation).expect("Failed to setup tracing");
    });
}

#[allow(dead_code)]
pub fn setup_tracing(
    verbose: u8,
    log_dir: Option<PathBuf>,
    rotation: Option<Rotation>,
) -> eyre::Result<()> {
    if let Some(log_dir) = log_dir {
        fs::create_dir_all(&log_dir)
            .map_err(|e| eyre::eyre!("Failed to create log directory: {}", e))?;

        let file_appender = RollingFileAppender::new(
            rotation.unwrap_or(Rotation::DAILY),
            log_dir,
            "blob-archiver.log",
        );
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        let file_layer = fmt::layer()
            .with_writer(non_blocking.with_max_level(match verbose {
                0 => Level::ERROR,
                1 => Level::WARN,
                2 => Level::INFO,
                3 => Level::DEBUG,
                _ => Level::TRACE,
            }))
            .with_ansi(false);

        let subscriber =
            tracing_subscriber::registry()
                .with(file_layer)
                .with(
                    fmt::layer().with_writer(std::io::stdout.with_max_level(match verbose {
                        0 => Level::ERROR,
                        1 => Level::WARN,
                        2 => Level::INFO,
                        3 => Level::DEBUG,
                        _ => Level::TRACE,
                    })),
                );
        tracing::subscriber::set_global_default(subscriber).map_err(|e| eyre!(e))?;
        Ok(())
    } else {
        let subscriber = tracing_subscriber::registry().with(fmt::layer().with_writer(
            std::io::stdout.with_max_level(match verbose {
                0 => Level::ERROR,
                1 => Level::WARN,
                2 => Level::INFO,
                3 => Level::DEBUG,
                _ => Level::TRACE,
            }),
        ));
        tracing::subscriber::set_global_default(subscriber).map_err(|e| eyre!(e))?;
        Ok(())
    }
}

#[derive(Parser, Serialize)]
pub struct CliArgs {
    #[clap(short = 'v', long, action = clap::ArgAction::Count, default_value = "2")]
    verbose: u8,

    #[clap(long)]
    log_dir: Option<String>,

    #[clap(long, help = "Log rotation values: DAILY, HOURLY, MINUTELY, NEVER")]
    log_rotation: Option<String>,

    #[clap(long, required = true)]
    beacon_endpoint: String,

    #[clap(long, default_value = "10")]
    beacon_client_timeout: u64,

    #[clap(long, default_value = "6")]
    poll_interval: u64,

    #[clap(long, default_value = "0.0.0.0:8000")]
    listen_addr: String,

    #[clap(long, required = true)]
    origin_block: String,

    #[clap(long, default_value = "s3")]
    storage_type: String,

    #[clap(long)]
    s3_endpoint: Option<String>,

    #[clap(long)]
    s3_bucket: Option<String>,

    #[clap(long)]
    s3_path: Option<String>,

    #[clap(long, default_value = "false")]
    s3_compress: Option<bool>,
    #[clap(long)]
    fs_dir: Option<String>,
}

impl CliArgs {
    fn to_config(&self) -> Config {
        let log_config = LogConfig {
            log_dir: self.log_dir.as_ref().map(PathBuf::from),
            log_rotation: self.log_rotation.clone(),
            verbose: self.verbose,
        };

        let beacon_config = beacon_client::Config {
            beacon_endpoint: self.beacon_endpoint.clone(),
            beacon_client_timeout: Duration::from_secs(self.beacon_client_timeout),
        };

        let storage_type = StorageType::from_str(self.storage_type.as_str()).unwrap();

        let s3_config = if storage_type == StorageType::S3 {
            Some(S3Config {
                endpoint: self.s3_endpoint.clone().unwrap(),
                bucket: self.s3_bucket.clone().unwrap(),
                path: self.s3_path.clone().unwrap(),
                compression: self.s3_compress.unwrap(),
            })
        } else {
            None
        };

        let fs_dir = self.fs_dir.as_ref().map(PathBuf::from);

        let storage_config = storage::Config {
            storage_type,
            s3_config,
            fs_dir,
        };

        let mut padded_hex = self
            .origin_block
            .strip_prefix("0x")
            .unwrap_or(&self.origin_block)
            .to_string();
        padded_hex = format!("{:0<64}", padded_hex);
        let origin_block = Hash256::from_slice(&hex::decode(padded_hex).expect("Invalid hex"));

        Config {
            poll_interval: Duration::from_secs(self.poll_interval),
            listen_addr: self.listen_addr.clone(),
            origin_block,
            beacon_config,
            storage_config,
            log_config,
        }
    }
}

#[allow(dead_code)]
fn to_rotation(s: &str) -> Rotation {
    match s {
        "DAILY" => Rotation::DAILY,
        "HOURLY" => Rotation::HOURLY,
        "MINUTELY" => Rotation::MINUTELY,
        _ => Rotation::NEVER,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use eth2::types::Hash256;

    use blob_archiver_storage::s3::S3Config;

    use crate::storage::StorageType;
    use crate::CliArgs;

    #[test]
    fn test_cli_args_to_config_s3_storage() {
        let cli_args = CliArgs {
            verbose: 3,
            log_dir: Some("logs".to_string()),
            log_rotation: Some("DAILY".to_string()),
            beacon_endpoint: "http://localhost:5052".to_string(),
            beacon_client_timeout: 10,
            poll_interval: 6,
            listen_addr: "0.0.0.0:8000".to_string(),
            origin_block: "0x1234".to_string(),
            storage_type: "s3".to_string(),
            s3_endpoint: Some("https://s3.amazonaws.com".to_string()),
            s3_bucket: Some("my-bucket".to_string()),
            s3_path: Some("my-path".to_string()),
            s3_compress: Some(true),
            fs_dir: None,
        };

        let config = cli_args.to_config();

        assert_eq!(config.poll_interval, Duration::from_secs(6));
        assert_eq!(config.listen_addr, "0.0.0.0:8000");
        assert_eq!(
            config.origin_block,
            Hash256::from_slice(
                &hex::decode("1234000000000000000000000000000000000000000000000000000000000000")
                    .unwrap()
            )
        );
        assert_eq!(
            config.beacon_config.beacon_endpoint,
            "http://localhost:5052"
        );
        assert_eq!(
            config.beacon_config.beacon_client_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(config.storage_config.storage_type, StorageType::S3);
        assert_eq!(
            config.storage_config.s3_config,
            Some(S3Config {
                endpoint: "https://s3.amazonaws.com".to_string(),
                bucket: "my-bucket".to_string(),
                path: "my-path".to_string(),
                compression: true,
            })
        );
        assert_eq!(config.storage_config.fs_dir, None);
    }

    #[test]
    fn test_cli_args_to_config_fs_storage() {
        let cli_args = CliArgs {
            verbose: 3,
            log_dir: Some("logs".to_string()),
            log_rotation: Some("DAILY".to_string()),
            beacon_endpoint: "http://localhost:5052".to_string(),
            beacon_client_timeout: 10,
            poll_interval: 6,
            listen_addr: "0.0.0.0:8000".to_string(),
            origin_block: "0xabcd".to_string(),
            storage_type: "fs".to_string(),
            s3_endpoint: None,
            s3_bucket: None,
            s3_path: None,
            s3_compress: None,
            fs_dir: Some("/path/to/storage".to_string()),
        };

        let config = cli_args.to_config();

        assert_eq!(config.poll_interval, Duration::from_secs(6));
        assert_eq!(config.listen_addr, "0.0.0.0:8000");
        assert_eq!(
            config.origin_block,
            Hash256::from_slice(
                &hex::decode("abcd000000000000000000000000000000000000000000000000000000000000")
                    .unwrap()
            )
        );
        assert_eq!(
            config.beacon_config.beacon_endpoint,
            "http://localhost:5052"
        );
        assert_eq!(
            config.beacon_config.beacon_client_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(config.storage_config.storage_type, StorageType::FS);
        assert_eq!(config.storage_config.s3_config, None);
        assert_eq!(
            config.storage_config.fs_dir,
            Some(PathBuf::from("/path/to/storage"))
        );
    }

    #[test]
    fn test_cli_args_to_config_origin_block_padding() {
        let cli_args = CliArgs {
            verbose: 3,
            log_dir: Some("logs".to_string()),
            log_rotation: Some("DAILY".to_string()),
            beacon_endpoint: "http://localhost:5052".to_string(),
            beacon_client_timeout: 10,
            poll_interval: 6,
            listen_addr: "0.0.0.0:8000".to_string(),
            origin_block: "0x1".to_string(),
            storage_type: "fs".to_string(),
            s3_endpoint: None,
            s3_bucket: None,
            s3_path: None,
            s3_compress: None,
            fs_dir: Some("/path/to/storage".to_string()),
        };

        let config = cli_args.to_config();

        assert_eq!(
            config.origin_block,
            Hash256::from_slice(
                &hex::decode("1000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_cli_args_to_config_origin_block_without_prefix() {
        let cli_args = CliArgs {
            verbose: 3,
            log_dir: Some("logs".to_string()),
            log_rotation: Some("DAILY".to_string()),
            beacon_endpoint: "http://localhost:5052".to_string(),
            beacon_client_timeout: 10,
            poll_interval: 6,
            listen_addr: "0.0.0.0:8000".to_string(),
            origin_block: "ff".to_string(),
            storage_type: "fs".to_string(),
            s3_endpoint: None,
            s3_bucket: None,
            s3_path: None,
            s3_compress: None,
            fs_dir: Some("/path/to/storage".to_string()),
        };

        let config = cli_args.to_config();

        assert_eq!(
            config.origin_block,
            Hash256::from_slice(
                &hex::decode("ff00000000000000000000000000000000000000000000000000000000000000")
                    .unwrap()
            )
        );
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct LogConfig {
    log_dir: Option<PathBuf>,
    log_rotation: Option<String>,
    verbose: u8,
}
