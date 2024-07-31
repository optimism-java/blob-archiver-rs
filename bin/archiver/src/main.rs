use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::archiver::Archiver;
use blob_archiver_storage::fs::FSStorage;
use eth2::types::BlockId;
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};

mod archiver;

#[tokio::main]
async fn main() {
    let beacon_client = BeaconNodeHttpClient::new(
        SensitiveUrl::from_str("https://ethereum-beacon-api.publicnode.com").unwrap(),
        Timeouts::set_all(Duration::from_secs(30)),
    );
    let storage = FSStorage::new(PathBuf::from("test_dir")).await.unwrap();
    let (_, shutdown_rx) = tokio::sync::watch::channel(false);
    let archiver = Archiver::new(beacon_client, Arc::new(storage), shutdown_rx);

    let block_id = BlockId::Head;

    archiver
        .persist_blobs_for_block(block_id, false)
        .await
        .expect("TODO: panic message");
}
