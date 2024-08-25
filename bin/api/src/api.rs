use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use eth2::types::{Accept, BlobIndicesQuery, BlobSidecarList, BlockId, GenericResponse};
use serde_json::json;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::{reply, Filter, Rejection, Reply};
use warp_utils::query::multi_key_query;

use blob_archiver_beacon::beacon_client::BeaconClient;
use blob_archiver_storage::storage::Storage;

pub struct Api {
    #[allow(dead_code)]
    pub beacon_client: Arc<Mutex<dyn BeaconClient>>,

    #[allow(dead_code)]
    pub storage: Arc<Mutex<dyn Storage>>,
}

impl Api {
    #[allow(dead_code)]
    pub fn new(
        beacon_client: Arc<Mutex<dyn BeaconClient>>,
        storage: Arc<Mutex<dyn Storage>>,
    ) -> Self {
        Self {
            beacon_client,
            storage,
        }
    }

    #[allow(dead_code)]
    pub fn routes(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        let beacon_client = self.beacon_client.clone();
        let storage = self.storage.clone();
        warp::path!("eth" / "v1" / "beacon" / "blob_sidecars" / String)
            .and(warp::get())
            .and(multi_key_query::<BlobIndicesQuery>())
            .and(warp::any().map(move || beacon_client.clone()))
            .and(warp::any().map(move || storage.clone()))
            .and(warp::header::optional::<Accept>("accept"))
            .and_then(Self::blob_sidecar_list_filtered)
            .or(warp::path!("eth" / "v1" / "node" / "version")
                .and(warp::get())
                .and_then(Self::version))
    }

    #[allow(dead_code)]
    async fn version() -> Result<impl Reply, Rejection> {
        Ok(reply::json(&json!({"version": "0.1.0"})))
    }

    #[allow(dead_code)]
    async fn blob_sidecar_list_filtered(
        id: String,
        indices_res: Result<BlobIndicesQuery, Rejection>,
        beacon_client: Arc<Mutex<dyn BeaconClient>>,
        storage: Arc<Mutex<dyn Storage>>,
        accept_header: Option<Accept>,
    ) -> Result<impl Reply, Rejection> {
        let root = match BlockId::from_str(id.as_str()) {
            Ok(block_id) => {
                let client = beacon_client.lock().await;
                match block_id {
                    BlockId::Root(root) => root,
                    _ => {
                        client
                            .get_beacon_headers_block_id(block_id)
                            .await
                            .map_err(|e| {
                                warp_utils::reject::custom_server_error(format!("{:?}", e))
                            })?
                            .ok_or_else(|| {
                                warp_utils::reject::custom_server_error(
                                    "Block not found".to_string(),
                                )
                            })?
                            .data
                            .root
                    }
                }
            }
            Err(e) => return Err(warp_utils::reject::custom_bad_request(format!("{:?}", e))),
        };

        let blob_sidecar_list = storage
            .lock()
            .await
            .read_blob_data(&root)
            .await
            .map_err(|e| {
                if let Some(io_error) = e.downcast_ref::<std::io::Error>() {
                    if io_error.kind() == std::io::ErrorKind::NotFound {
                        return warp::reject();
                    }
                }

                if let Some(io_error) = e.downcast_ref::<SdkError<GetObjectError>>() {
                    if matches!(io_error, SdkError::ServiceError(err) if err.err().is_no_such_key())
                    {
                        return warp::reject();
                    }
                }

                warp_utils::reject::custom_server_error(format!("{:?}", e))
            })?
            .blob_sidecars
            .data;

        let indices = indices_res?;
        let blob_sidecar_list_filtered = match indices.indices {
            Some(vec) => {
                if vec.iter().any(|i| (*i as usize) >= blob_sidecar_list.len()) {
                    return Err(warp_utils::reject::custom_bad_request(
                        "Index out of bounds".to_string(),
                    ));
                }
                let list = blob_sidecar_list
                    .into_iter()
                    .filter(|blob_sidecar| vec.contains(&(blob_sidecar.index)))
                    .collect();
                BlobSidecarList::new(list)
                    .map_err(|e| warp_utils::reject::custom_server_error(format!("{:?}", e)))?
            }
            None => blob_sidecar_list,
        };

        match accept_header {
            Some(Accept::Ssz) => Err(warp_utils::reject::custom_server_error(
                "SSZ not implemented".to_string(),
            )),
            _ => {
                Ok(reply::json(&GenericResponse::from(blob_sidecar_list_filtered)).into_response())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::from_utf8;

    use eth2::types::{
        BeaconBlockHeader, BlockHeaderAndSignature, BlockHeaderData, Hash256, MainnetEthSpec,
        SignatureBytes, Slot,
    };
    use tokio::sync::watch::Receiver;
    use tracing_subscriber::fmt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use blob_archiver_beacon::beacon_client::BeaconClientStub;
    use blob_archiver_beacon::blob_test_helper;
    use blob_archiver_storage::fs::{FSStorage, TestFSStorage};
    use blob_archiver_storage::storage::{BlobData, BlobSidecars, Header, StorageWriter};

    use crate::INIT;

    use super::*;

    fn setup_tracing() {
        INIT.call_once(|| {
            tracing_subscriber::registry().with(fmt::layer()).init();
        });
    }

    pub async fn create_test_archiver(
        storage: Arc<Mutex<dyn Storage>>,
        _shutdown_rx: Receiver<bool>,
    ) -> (
        Arc<Mutex<BeaconClientStub<MainnetEthSpec>>>,
        Arc<Mutex<dyn Storage>>,
    ) {
        setup_tracing();
        let beacon_client = Arc::new(Mutex::new(BeaconClientStub::default()));
        (beacon_client, storage)
    }

    #[tokio::test]
    async fn test_version() {
        let (_, rx) = tokio::sync::watch::channel(false);
        let dir = &PathBuf::from("test_version");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let test_storage = Arc::new(Mutex::new(TestFSStorage::new(storage).await.unwrap()));
        let (beacon_client, _) = create_test_archiver(test_storage.clone(), rx).await;
        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/node/version")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(from_utf8(res.body()).unwrap(), "{\"version\":\"0.1.0\"}");
        clean_dir(dir);
    }

    #[tokio::test]
    async fn test_blob_sidecar_list() {
        let (_, rx) = tokio::sync::watch::channel(false);
        let dir = &PathBuf::from("test_blob_sidecar_list");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let test_storage = Arc::new(Mutex::new(TestFSStorage::new(storage).await.unwrap()));
        let (beacon_client, _) = create_test_archiver(test_storage.clone(), rx).await;

        let root_one =
            Hash256::from_str("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
                .unwrap();
        let root_two =
            Hash256::from_str("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890222222")
                .unwrap();
        let block_one = BlobData {
            header: Header {
                beacon_block_hash: root_one,
            },
            blob_sidecars: BlobSidecars {
                data: blob_test_helper::new_blob_sidecars(2),
            },
        };

        let block_two = BlobData {
            header: Header {
                beacon_block_hash: root_two,
            },
            blob_sidecars: BlobSidecars {
                data: blob_test_helper::new_blob_sidecars(2),
            },
        };

        let mut res = test_storage.lock().await.write_blob_data(&block_one).await;
        assert!(res.is_ok());

        res = test_storage.lock().await.write_blob_data(&block_two).await;
        assert!(res.is_ok());

        beacon_client.lock().await.headers.insert(
            "finalized".to_string(),
            BlockHeaderData {
                root: root_one,
                canonical: false,
                header: BlockHeaderAndSignature {
                    message: BeaconBlockHeader {
                        slot: Slot::new(1),
                        proposer_index: 0,
                        parent_root: Hash256::default(),
                        state_root: Hash256::default(),
                        body_root: Hash256::default(),
                    },
                    signature: SignatureBytes::empty(),
                },
            },
        );

        beacon_client.lock().await.headers.insert(
            "head".to_string(),
            BlockHeaderData {
                root: root_two,
                canonical: false,
                header: BlockHeaderAndSignature {
                    message: BeaconBlockHeader {
                        slot: Slot::new(2),
                        proposer_index: 0,
                        parent_root: Hash256::default(),
                        state_root: Hash256::default(),
                        body_root: Hash256::default(),
                    },
                    signature: SignatureBytes::empty(),
                },
            },
        );

        beacon_client.lock().await.headers.insert(
            "1234".to_string(),
            BlockHeaderData {
                root: root_two,
                canonical: false,
                header: BlockHeaderAndSignature {
                    message: BeaconBlockHeader {
                        slot: Slot::new(2),
                        proposer_index: 0,
                        parent_root: Hash256::default(),
                        state_root: Hash256::default(),
                        body_root: Hash256::default(),
                    },
                    signature: SignatureBytes::empty(),
                },
            },
        );

        let mut res = warp::test::request()
            .method("GET")
            .path(format!("/eth/v1/beacon/blob_sidecars/{:#?}", root_one).as_str())
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_one.blob_sidecars).unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path(format!("/eth/v1/beacon/blob_sidecars/{:#?}", root_two).as_str())
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_two.blob_sidecars).unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abc111")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 404);

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/head")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_two.blob_sidecars).unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/finalized")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_one.blob_sidecars).unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_two.blob_sidecars).unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=1")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&BlobSidecars {
                data: BlobSidecarList::new(vec![block_two
                    .blob_sidecars
                    .data
                    .clone()
                    .to_vec()
                    .get(1)
                    .unwrap()
                    .clone()])
                .unwrap(),
            })
            .unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=1,1,1")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&BlobSidecars {
                data: BlobSidecarList::new(vec![block_two
                    .blob_sidecars
                    .data
                    .clone()
                    .to_vec()
                    .get(1)
                    .unwrap()
                    .clone()])
                .unwrap(),
            })
            .unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=0&indices=1")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_two.blob_sidecars).unwrap()
        );

        res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=0,1")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            from_utf8(res.body()).unwrap(),
            &serde_json::to_string(&block_two.blob_sidecars).unwrap()
        );

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=3")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=1,10")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=2")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234?indices=-2")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/0x1234567890abcdef123")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/foobar")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 500);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/beacon/blob_sidecars/")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 404);

        let res = warp::test::request()
            .method("GET")
            .path("/eth/v1/")
            .reply(&Api::new(beacon_client.clone(), test_storage.clone()).routes())
            .await;
        assert_eq!(res.status(), 404);
        clean_dir(dir);
    }

    fn clean_dir(dir: &PathBuf) {
        if dir.exists() {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
