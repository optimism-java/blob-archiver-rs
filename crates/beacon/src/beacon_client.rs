use crate::blob_test_helper::{
    new_blob_sidecars, FIVE, FOUR, ONE, ORIGIN_BLOCK, START_SLOT, THREE, TWO,
};
use async_trait::async_trait;
use eth2::types::{
    BeaconBlockHeader, BlobSidecarList, BlockHeaderAndSignature, BlockHeaderData, BlockId, EthSpec,
    ExecutionOptimisticFinalizedResponse, GenericResponse, Hash256, MainnetEthSpec, SignatureBytes,
    Slot,
};
use eth2::{BeaconNodeHttpClient, Error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub beacon_endpoint: String,
    pub beacon_client_timeout: Duration,
}

#[async_trait]
pub trait BeaconClient: Send + Sync {
    async fn get_beacon_headers_block_id(
        &self,
        block_id: BlockId,
    ) -> Result<Option<ExecutionOptimisticFinalizedResponse<BlockHeaderData>>, Error>;

    async fn get_blobs(
        &self,
        block_id: BlockId,
        indices: Option<&[u64]>,
    ) -> Result<Option<GenericResponse<BlobSidecarList<MainnetEthSpec>>>, Error>;
}

pub struct BeaconClientStub<E: EthSpec> {
    pub headers: HashMap<String, BlockHeaderData>,
    pub blobs: HashMap<String, BlobSidecarList<E>>,
}

#[async_trait]
impl BeaconClient for BeaconClientStub<MainnetEthSpec> {
    async fn get_beacon_headers_block_id(
        &self,
        block_id: BlockId,
    ) -> Result<Option<ExecutionOptimisticFinalizedResponse<BlockHeaderData>>, Error> {
        let header = self.headers.get(block_id.to_string().as_str());

        Ok(header.map(|h| ExecutionOptimisticFinalizedResponse {
            execution_optimistic: Some(true),
            finalized: Some(true),
            data: h.clone(),
        }))
    }

    async fn get_blobs(
        &self,
        block_id: BlockId,
        _indices: Option<&[u64]>,
    ) -> Result<Option<GenericResponse<BlobSidecarList<MainnetEthSpec>>>, Error> {
        let blobs = self.blobs.get(block_id.to_string().as_str());

        Ok(blobs.map(|b| GenericResponse { data: b.clone() }))
    }
}

impl Default for BeaconClientStub<MainnetEthSpec> {
    fn default() -> Self {
        let make_header = |slot: Slot, hash: Hash256, parent: Hash256| -> BlockHeaderData {
            BlockHeaderData {
                root: hash,
                canonical: false,
                header: BlockHeaderAndSignature {
                    message: BeaconBlockHeader {
                        slot,
                        proposer_index: 0,
                        parent_root: parent,
                        state_root: Hash256::default(),
                        body_root: Hash256::default(),
                    },
                    signature: SignatureBytes::empty(),
                },
            }
        };

        let start_slot = Slot::new(START_SLOT);
        let origin_blobs = new_blob_sidecars(1);
        let one_blobs = new_blob_sidecars(2);
        let two_blobs = new_blob_sidecars(0);
        let three_blobs = new_blob_sidecars(4);
        let four_blobs = new_blob_sidecars(5);
        let five_blobs = new_blob_sidecars(6);

        BeaconClientStub {
            headers: HashMap::from([
                (
                    format!("0x{}", hex::encode(ORIGIN_BLOCK.as_bytes())),
                    make_header(
                        start_slot,
                        *ORIGIN_BLOCK,
                        Hash256::from_str(
                            "0x0909090000000000000000000000000000000000000000000000000000000000",
                        )
                        .unwrap(),
                    ),
                ),
                (
                    format!("0x{}", hex::encode(ONE.as_bytes())),
                    make_header(start_slot + 1, *ONE, *ORIGIN_BLOCK),
                ),
                (
                    format!("0x{}", hex::encode(TWO.as_bytes())),
                    make_header(start_slot + 2, *TWO, *ONE),
                ),
                (
                    format!("0x{}", hex::encode(THREE.as_bytes())),
                    make_header(start_slot + 3, *THREE, *TWO),
                ),
                (
                    format!("0x{}", hex::encode(FOUR.as_bytes())),
                    make_header(start_slot + 4, *FOUR, *THREE),
                ),
                (
                    format!("0x{}", hex::encode(FIVE.as_bytes())),
                    make_header(start_slot + 5, *FIVE, *FOUR),
                ),
                (
                    "head".to_string(),
                    make_header(start_slot + 5, *FIVE, *FOUR),
                ),
                (
                    "finalized".to_string(),
                    make_header(start_slot + 3, *THREE, *TWO),
                ),
                (
                    start_slot.as_u64().to_string(),
                    make_header(
                        start_slot,
                        *ORIGIN_BLOCK,
                        Hash256::from_str(
                            "0x0909090000000000000000000000000000000000000000000000000000000000",
                        )
                        .unwrap(),
                    ),
                ),
                (
                    (start_slot + 1).as_u64().to_string(),
                    make_header(start_slot + 1, *ONE, *ORIGIN_BLOCK),
                ),
                (
                    (start_slot + 2).as_u64().to_string(),
                    make_header(start_slot + 2, *TWO, *ONE),
                ),
                (
                    (start_slot + 3).as_u64().to_string(),
                    make_header(start_slot + 3, *THREE, *TWO),
                ),
                (
                    (start_slot + 4).as_u64().to_string(),
                    make_header(start_slot + 4, *FOUR, *THREE),
                ),
                (
                    (start_slot + 5).as_u64().to_string(),
                    make_header(start_slot + 5, *FIVE, *FOUR),
                ),
            ]),

            blobs: HashMap::from([
                (
                    format!("0x{}", hex::encode(ORIGIN_BLOCK.as_bytes())),
                    origin_blobs.clone(),
                ),
                (
                    format!("0x{}", hex::encode(ONE.as_bytes())),
                    one_blobs.clone(),
                ),
                (
                    format!("0x{}", hex::encode(TWO.as_bytes())),
                    two_blobs.clone(),
                ),
                (
                    format!("0x{}", hex::encode(THREE.as_bytes())),
                    three_blobs.clone(),
                ),
                (
                    format!("0x{}", hex::encode(FOUR.as_bytes())),
                    four_blobs.clone(),
                ),
                (
                    format!("0x{}", hex::encode(FIVE.as_bytes())),
                    five_blobs.clone(),
                ),
                ("head".to_string(), five_blobs.clone()),
                ("finalized".to_string(), three_blobs.clone()),
                (start_slot.as_u64().to_string(), origin_blobs.clone()),
                ((start_slot + 1).as_u64().to_string(), one_blobs.clone()),
                ((start_slot + 2).as_u64().to_string(), two_blobs.clone()),
                ((start_slot + 3).as_u64().to_string(), three_blobs.clone()),
                ((start_slot + 4).as_u64().to_string(), four_blobs.clone()),
                ((start_slot + 5).as_u64().to_string(), five_blobs.clone()),
            ]),
        }
    }
}

impl BeaconClientStub<MainnetEthSpec> {
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
            blobs: HashMap::new(),
        }
    }
}

pub struct BeaconClientEth2 {
    pub beacon_client: BeaconNodeHttpClient,
}

#[async_trait]
impl BeaconClient for BeaconClientEth2 {
    async fn get_beacon_headers_block_id(
        &self,
        block_id: BlockId,
    ) -> Result<Option<ExecutionOptimisticFinalizedResponse<BlockHeaderData>>, Error> {
        return self
            .beacon_client
            .get_beacon_headers_block_id(block_id)
            .await;
    }

    async fn get_blobs(
        &self,
        block_id: BlockId,
        indices: Option<&[u64]>,
    ) -> Result<Option<GenericResponse<BlobSidecarList<MainnetEthSpec>>>, Error> {
        return self.beacon_client.get_blobs(block_id, indices).await;
    }
}
