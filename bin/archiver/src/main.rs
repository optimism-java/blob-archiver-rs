use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::archiver::{Archiver, Config};
use blob_archiver_beacon::beacon_client::BeaconClientEth2;
use blob_archiver_beacon::blob_test_helper;
use blob_archiver_storage::fs::FSStorage;
use eth2::types::BlockId;
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};
use tokio::sync::Mutex;

mod archiver;

#[tokio::main]
async fn main() {
    let beacon_client = BeaconNodeHttpClient::new(
        SensitiveUrl::from_str("https://ethereum-beacon-api.publicnode.com").unwrap(),
        Timeouts::set_all(Duration::from_secs(30)),
    );
    let storage = FSStorage::new(PathBuf::from("test_dir")).await.unwrap();
    let (_, shutdown_rx) = tokio::sync::watch::channel(false);
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

    let block_id = BlockId::Head;

    archiver
        .persist_blobs_for_block(block_id, false)
        .await
        .expect("TODO: panic message");
}
