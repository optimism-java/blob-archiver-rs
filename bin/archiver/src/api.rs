use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::archiver::Archiver;
use serde::Serialize;
use warp::reject::Reject;
use warp::{Filter, Rejection, Reply};

pub struct Api {
    archiver: Arc<Archiver>,
}

#[derive(Serialize)]
struct RearchiveResponse {
    success: bool,
    message: String,
    block_start: u64,
    block_end: u64,
}

impl Debug for RearchiveResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RearchiveResponse")
            .field("success", &self.success)
            .field("message", &self.message)
            .field("block_start", &self.block_start)
            .field("block_end", &self.block_end)
            .finish()
    }
}

impl Reject for RearchiveResponse {}

impl Api {
    pub fn new(archiver: Archiver) -> Self {
        Self {
            archiver: Arc::new(archiver),
        }
    }

    pub fn routes(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        let archiver = self.archiver.clone();
        warp::path!("rearchive")
            .and(warp::get())
            .and(warp::query::<RearchiveQuery>())
            .and(warp::any().map(move || archiver.clone()))
            .and_then(Self::rearchive_range)
            .or(warp::path!("healthz")
                .and(warp::get())
                .and_then(Self::healthz))
    }

    async fn healthz() -> Result<impl Reply, Rejection> {
        Ok(warp::reply::json(&serde_json::json!({
            "status": "ok"
        })))
    }

    async fn rearchive_range(
        query: RearchiveQuery,
        archiver: Arc<Archiver>,
    ) -> Result<impl Reply, Rejection> {
        if query.from.is_none() || query.to.is_none() {
            return Err(warp::reject::custom(RearchiveResponse {
                success: false,
                message: "Invalid query parameters".to_string(),
                block_start: 0,
                block_end: 0,
            }));
        }

        if query.from > query.to {
            return Err(warp::reject::custom(RearchiveResponse {
                success: false,
                message: "Invalid query parameters".to_string(),
                block_start: 0,
                block_end: 0,
            }));
        }

        let res = archiver
            .rearchive_range(query.from.unwrap(), query.to.unwrap())
            .await;
        if res.error.is_some() {
            return Err(warp::reject::custom(RearchiveResponse {
                success: false,
                message: res.error.unwrap(),
                block_start: res.from,
                block_end: res.to,
            }));
        }
        Ok(warp::reply::json(&res))
    }
}

#[derive(serde::Deserialize)]
struct RearchiveQuery {
    from: Option<u64>,
    to: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use eth2::types::MainnetEthSpec;
    use tokio::sync::watch::Receiver;
    use tokio::sync::Mutex;
    use tracing_subscriber::fmt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use blob_archiver_beacon::beacon_client::BeaconClientStub;
    use blob_archiver_beacon::blob_test_helper;
    use blob_archiver_storage::fs::{FSStorage, TestFSStorage};
    use blob_archiver_storage::Storage;

    use crate::api::Api;
    use crate::archiver::{Archiver, Config};
    use crate::INIT;

    fn setup_tracing() {
        INIT.call_once(|| {
            tracing_subscriber::registry().with(fmt::layer()).init();
        });
    }

    pub async fn create_test_archiver(
        storage: Arc<Mutex<dyn Storage>>,
        shutdown_rx: Receiver<bool>,
    ) -> (Archiver, Arc<Mutex<BeaconClientStub<MainnetEthSpec>>>) {
        setup_tracing();
        let beacon_client = Arc::new(Mutex::new(BeaconClientStub::default()));
        let config = Config {
            poll_interval: Duration::from_secs(5),
            listen_addr: "".to_string(),
            origin_block: *blob_test_helper::ORIGIN_BLOCK,
        };
        let archiver = Archiver::new(beacon_client.clone(), storage, config, shutdown_rx);
        (archiver, beacon_client)
    }

    #[tokio::test]
    async fn test_healthz() {
        let (_, rx) = tokio::sync::watch::channel(false);
        let dir = &PathBuf::from("test_healthz");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let test_storage = Arc::new(Mutex::new(TestFSStorage::new(storage).await.unwrap()));
        let (archiver, _) = create_test_archiver(test_storage.clone(), rx).await;
        let res = warp::test::request()
            .method("GET")
            .path("/healthz")
            .reply(&Api::new(archiver).routes())
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            std::str::from_utf8(res.body()).unwrap(),
            "{\"status\":\"ok\"}"
        );
        clean_dir(dir);
    }

    #[tokio::test]
    async fn test_rearchive_range() {
        let (_, rx) = tokio::sync::watch::channel(false);
        let dir = &PathBuf::from("test_rearchive_range");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let test_storage = Arc::new(Mutex::new(TestFSStorage::new(storage).await.unwrap()));
        let (archiver, _) = create_test_archiver(test_storage.clone(), rx).await;
        let res = warp::test::request()
            .method("GET")
            .path("/rearchive?from=2001&to=2000")
            .reply(&Api::new(archiver).routes())
            .await;
        assert_eq!(res.status(), 500);
        clean_dir(dir);
    }

    fn clean_dir(dir: &PathBuf) {
        if dir.exists() {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
