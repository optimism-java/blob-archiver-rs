use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ctrlc::set_handler;
use eth2::types::Hash256;
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use eyre::eyre;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::Level;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;

use blob_archiver_beacon::beacon_client;
use blob_archiver_beacon::beacon_client::BeaconClientEth2;
use blob_archiver_storage::fs::FSStorage;
use blob_archiver_storage::s3::{S3Config, S3Storage};
use blob_archiver_storage::storage;
use blob_archiver_storage::storage::{Storage, StorageType};

mod api;

#[allow(dead_code)]
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
    let api = api::Api::new(Arc::new(Mutex::new(beacon_client_eth2)), storage.clone());
    let addr: std::net::SocketAddr = config.listen_addr.parse().expect("Invalid listen address");

    let (_, server) = warp::serve(api.routes()).bind_with_graceful_shutdown(addr, async move {
        shutdown_rx.clone().changed().await.ok();
    });
    server.await;
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
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct CliArgs {
    #[clap(long, env = "VERBOSE", default_value = "2")]
    verbose: u8,

    #[clap(long, env = "LOG_DIR")]
    log_dir: Option<String>,

    #[clap(long, env = "LOG_ROTATION", help = "Log rotation values: DAILY, HOURLY, MINUTELY, NEVER")]
    log_rotation: Option<String>,

    #[clap(long, env = "BEACON_ENDPOINT", required = true)]
    beacon_endpoint: String,

    #[clap(long, env = "BEACON_CLIENT_TIMEOUT", default_value = "10")]
    beacon_client_timeout: u64,

    #[clap(long, env = "POLL_INTERVAL", default_value = "6")]
    poll_interval: u64,

    #[clap(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8000")]
    listen_addr: String,

    #[clap(long, env = "ORIGIN_BLOCK", required = true)]
    origin_block: String,

    #[clap(long, env = "STORAGE_TYPE", default_value = "s3")]
    storage_type: String,

    #[clap(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    #[clap(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,

    #[clap(long, env = "S3_PATH")]
    s3_path: Option<String>,

    #[clap(long, env = "S3_COMPRESS", default_value = "false")]
    s3_compress: Option<bool>,
    #[clap(long, env = "FS_DIR")]
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

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct LogConfig {
    log_dir: Option<PathBuf>,
    log_rotation: Option<String>,
    verbose: u8,
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    pub poll_interval: Duration,

    pub listen_addr: String,

    pub origin_block: Hash256,

    pub beacon_config: beacon_client::Config,

    pub storage_config: storage::Config,

    pub log_config: LogConfig,
}
