use std::str::FromStr;
use std::time::Duration;

use eth2::types::BlockId;
use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};

use crate::archiver::Archiver;

mod archiver;

#[tokio::main]
async fn main() {
    let beacon_client = BeaconNodeHttpClient::new(
        SensitiveUrl::from_str("https://ethereum-beacon-api.publicnode.com").unwrap(),
        Timeouts::set_all(Duration::from_secs(30)),
    );
    let archiver = Archiver::new(beacon_client);

    let block_id = BlockId::Head;

    archiver
        .persist_blobs_for_block(block_id)
        .await
        .expect("TODO: panic message");
}
