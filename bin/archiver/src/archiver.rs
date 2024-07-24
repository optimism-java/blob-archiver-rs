use blob_archiver_storage::{BlobData, BlobSidecars, Header};
use eth2::types::{BlockId, MainnetEthSpec};
use eth2::{BeaconNodeHttpClient, Error};
use tracing::log::trace;

pub struct Archiver {
    pub beacon_client: BeaconNodeHttpClient,
}

impl Archiver {
    pub fn new(beacon_client: BeaconNodeHttpClient) -> Self {
        Self { beacon_client }
    }

    pub async fn persist_blobs_for_block(&self, block_id: BlockId) -> Result<(), Error> {
        let header_resp_opt = self
            .beacon_client
            .get_beacon_headers_block_id(block_id)
            .await?;
        if let Some(header) = header_resp_opt {
            let beacon_client = self.beacon_client.clone();
            let blobs_resp_opt = beacon_client
                .get_blobs::<MainnetEthSpec>(BlockId::Root(header.data.root), None)
                .await?;
            if let Some(blob_sidecars) = blobs_resp_opt {
                let blob_sidecar_list = blob_sidecars.data;
                let blob_data = BlobData::new(
                    Header {
                        beacon_block_hash: header.data.root,
                    },
                    BlobSidecars {
                        data: blob_sidecar_list,
                    },
                );
                trace!("Persisting blobs for block: {:?}", blob_data);
                return Ok(());
            }
            return Ok(());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use eth2::{SensitiveUrl, Timeouts};

    use super::*;

    #[tokio::test]
    async fn test_persist_blobs_for_block() {
        let beacon_client = BeaconNodeHttpClient::new(
            SensitiveUrl::from_str("https://ethereum-beacon-api.publicnode.com").unwrap(),
            Timeouts::set_all(Duration::from_secs(30)),
        );
        let archiver = Archiver::new(beacon_client);

        let block_id = BlockId::Head;
        archiver.persist_blobs_for_block(block_id).await.unwrap();
    }
}
