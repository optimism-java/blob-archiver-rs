use crate::archiver::{Archiver, Config, STARTUP_FETCH_BLOB_MAXIMUM_RETRIES};
use again::RetryPolicy;
use blob_archiver_beacon::beacon_client::BeaconClientEth2;
use blob_archiver_beacon::blob_test_helper;
use blob_archiver_storage::fs::FSStorage;
use clap::Parser;
use eth2::types::BlockId;
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::log::error;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

mod api;
mod archiver;

static INIT: std::sync::Once = std::sync::Once::new();

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    println!("{}", args.verbose);
    init_logging(0, None, None);
    let beacon_client = BeaconNodeHttpClient::new(
        SensitiveUrl::from_str("https://ethereum-beacon-api.publicnode.com").unwrap(),
        Timeouts::set_all(Duration::from_secs(30)),
    );
    let storage = FSStorage::new(PathBuf::from("test_dir")).await.unwrap();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let beacon_client_eth2 = BeaconClientEth2 { beacon_client };
    let config = Config {
        poll_interval: Duration::from_secs(5),
        listen_addr: "".to_string(),
        origin_block: *blob_test_helper::ORIGIN_BLOCK,
        beacon_config: Default::default(),
        storage_config: Default::default(),
    };
    let archiver = Archiver::new(
        Arc::new(Mutex::new(beacon_client_eth2)),
        Arc::new(Mutex::new(storage)),
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
    let filter = match verbose {
        0 => EnvFilter::new("error"),
        1 => EnvFilter::new("warn"),
        2 => EnvFilter::new("info"),
        3 => EnvFilter::new("debug"),
        _ => EnvFilter::new("trace"),
    };

    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(filter);

    if let Some(log_dir) = log_dir {
        fs::create_dir_all(&log_dir)
            .map_err(|e| eyre::eyre!("Failed to create log directory: {}", e))?;

        let file_appender = RollingFileAppender::new(
            rotation.unwrap_or(Rotation::DAILY),
            log_dir,
            "blob-archiver.log",
        );
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        let file_layer = fmt::layer().with_writer(non_blocking).with_ansi(false);

        subscriber
            .with(file_layer)
            .with(fmt::layer().with_writer(std::io::stdout))
            .try_init()?;
    } else {
        subscriber
            .with(fmt::layer().with_writer(std::io::stdout))
            .try_init()?;
    }

    Ok(())
}

#[derive(Parser, Serialize)]
pub struct CliArgs {
    #[clap(short='v', long, action = clap::ArgAction::Count, default_value = "3")]
    verbose: u8,

    #[clap(long, default_value = "logs")]
    log_dir: String,

    #[clap(long, default_value = "DAILY")]
    log_rotation: String,

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
