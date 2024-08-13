use crate::archiver::{Archiver, Config, STARTUP_FETCH_BLOB_MAXIMUM_RETRIES};
use again::RetryPolicy;
use blob_archiver_beacon::beacon_client::BeaconClientEth2;
use blob_archiver_beacon::blob_test_helper;
use blob_archiver_storage::fs::FSStorage;
use eth2::types::BlockId;
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::log::error;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod api;
mod archiver;

static INIT: std::sync::Once = std::sync::Once::new();

#[tokio::main]
async fn main() {
    setup_tracing();
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

fn setup_tracing() {
    INIT.call_once(|| {
        tracing_subscriber::registry().with(fmt::layer()).init();
    });
}