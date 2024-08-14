use std::sync::Arc;

use crate::archiver::Archiver;
use serde::Serialize;
use warp::http::header::CONTENT_TYPE;
use warp::http::{HeaderValue, StatusCode};
use warp::hyper::Body;
use warp::{Filter, Rejection, Reply};

#[allow(dead_code)]
pub struct Api {
    archiver: Arc<Archiver>,
}

#[allow(dead_code)]
#[derive(Serialize)]
struct RearchiveResponse {
    success: bool,
    message: String,
    block_start: u64,
    block_end: u64,
}

impl Reply for RearchiveResponse {
    fn into_response(self) -> warp::reply::Response {
        let body = serde_json::to_string(&self).unwrap();
        let mut res = warp::reply::Response::new(Body::from(body));
        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        res
    }
}

impl Api {
    #[allow(dead_code)]
    pub fn new(archiver: Archiver) -> Self {
        Self {
            archiver: Arc::new(archiver),
        }
    }

    #[allow(dead_code)]
    pub fn routes(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        let archiver = self.archiver.clone();
        warp::path!("rearchive")
            .and(warp::get())
            // todo: make validation error return json https://stackoverflow.com/questions/60554783/is-there-a-way-to-do-validation-as-part-of-a-filter-in-warp
            .and(warp::query::<RearchiveQuery>())
            .and(warp::any().map(move || archiver.clone()))
            .and_then(Self::rearchive_range)
            .or(warp::path!("healthz")
                .and(warp::get())
                .and_then(Self::healthz))
    }

    #[allow(dead_code)]
    async fn healthz() -> Result<impl Reply, Rejection> {
        Ok(warp::reply::json(&serde_json::json!({
            "status": "ok"
        })))
    }

    #[allow(dead_code)]
    async fn rearchive_range(
        query: RearchiveQuery,
        archiver: Arc<Archiver>,
    ) -> Result<impl Reply, Rejection> {
        if query.from.is_none() || query.to.is_none() {
            return Ok(warp::reply::with_status(
                RearchiveResponse {
                    success: false,
                    message: "Invalid query parameters".to_string(),
                    block_start: 0,
                    block_end: 0,
                },
                StatusCode::BAD_REQUEST,
            ));
        }

        if query.from > query.to {
            return Ok(warp::reply::with_status(
                RearchiveResponse {
                    success: false,
                    message: "Invalid query parameters".to_string(),
                    block_start: 0,
                    block_end: 0,
                },
                StatusCode::BAD_REQUEST,
            ));
        }

        let res = archiver
            .rearchive_range(query.from.unwrap(), query.to.unwrap())
            .await;
        if res.error.is_some() {
            return Ok(warp::reply::with_status(
                RearchiveResponse {
                    success: false,
                    message: res.error.unwrap(),
                    block_start: 0,
                    block_end: 0,
                },
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }

        Ok(warp::reply::with_status(
            RearchiveResponse {
                success: true,
                message: "Rearchive successful".to_string(),
                block_start: res.from,
                block_end: res.to,
            },
            StatusCode::OK,
        ))
    }
}

#[derive(serde::Deserialize)]
struct RearchiveQuery {
    #[allow(dead_code)]
    from: Option<u64>,

    #[allow(dead_code)]
    to: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::from_utf8;
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
        assert_eq!(from_utf8(res.body()).unwrap(), "{\"status\":\"ok\"}");
        clean_dir(dir);
    }

    #[tokio::test]
    async fn test_api_rearchive_range() {
        let (_, rx) = tokio::sync::watch::channel(false);
        let dir = &PathBuf::from("test_api_rearchive_range");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let test_storage = Arc::new(Mutex::new(TestFSStorage::new(storage).await.unwrap()));
        let (archiver, _) = create_test_archiver(test_storage.clone(), rx).await;
        let api = Api::new(archiver);
        let mut res = warp::test::request()
            .method("GET")
            .path("/rearchive?from=2001&to=2000")
            .reply(&api.routes())
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(from_utf8(res.body()).unwrap(), "{\"success\":false,\"message\":\"Invalid query parameters\",\"block_start\":0,\"block_end\":0}");

        res = warp::test::request()
            .method("GET")
            .path("/rearchive?to=2000")
            .reply(&api.routes())
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(from_utf8(res.body()).unwrap(), "{\"success\":false,\"message\":\"Invalid query parameters\",\"block_start\":0,\"block_end\":0}");

        res = warp::test::request()
            .method("GET")
            .path("/rearchive?from=2000")
            .reply(&api.routes())
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(from_utf8(res.body()).unwrap(), "{\"success\":false,\"message\":\"Invalid query parameters\",\"block_start\":0,\"block_end\":0}");

        res = warp::test::request()
            .method("GET")
            .path("/rearchive?from=bbb&to=2000")
            .reply(&api.routes())
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(from_utf8(res.body()).unwrap(), "Invalid query string");

        res = warp::test::request()
            .method("GET")
            .path("/rearchive?from=100&to=aaa")
            .reply(&api.routes())
            .await;
        assert_eq!(res.status(), 400);
        assert_eq!(from_utf8(res.body()).unwrap(), "Invalid query string");

        res = warp::test::request()
            .method("GET")
            .path("/rearchive?from=11&to=15")
            .reply(&api.routes())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(from_utf8(res.body()).unwrap(), "{\"success\":true,\"message\":\"Rearchive successful\",\"block_start\":11,\"block_end\":15}");
        clean_dir(dir);
    }

    fn clean_dir(dir: &PathBuf) {
        if dir.exists() {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
