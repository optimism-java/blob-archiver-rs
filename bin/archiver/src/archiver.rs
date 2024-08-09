use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use again::RetryPolicy;
use eth2::types::Slot;
use eth2::types::{BlockHeaderData, BlockId, Hash256};
use eth2::Error;
use eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch::Receiver;
use tokio::time::{interval, sleep};
use tracing::log::{debug, error, info, trace};

use blob_archiver_beacon::beacon_client::BeaconClient;
use blob_archiver_storage::{
    BackfillProcess, BackfillProcesses, BlobData, BlobSidecars, Header, LockFile, Storage,
};

#[allow(dead_code)]
const LIVE_FETCH_BLOB_MAXIMUM_RETRIES: usize = 10;
#[allow(dead_code)]
const STARTUP_FETCH_BLOB_MAXIMUM_RETRIES: usize = 3;
#[allow(dead_code)]
const REARCHIVE_MAXIMUM_RETRIES: usize = 3;
#[allow(dead_code)]
const BACKFILL_ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(5);
#[allow(dead_code)]
const LOCK_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
#[allow(dead_code)]
const LOCK_TIMEOUT: Duration = Duration::from_secs(20);
#[allow(dead_code)]
const OBTAIN_LOCK_RETRY_INTERVAL_SECS: u64 = 10;
#[allow(dead_code)]
static OBTAIN_LOCK_RETRY_INTERVAL: AtomicU64 = AtomicU64::new(OBTAIN_LOCK_RETRY_INTERVAL_SECS);

#[derive(Debug, Serialize, Deserialize)]
pub struct RearchiveResp {
    pub from: u64,
    pub to: u64,
    pub error: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    pub poll_interval: Duration,

    pub listen_addr: String,

    pub origin_block: Hash256,
}

pub struct Archiver {
    pub beacon_client: Arc<dyn BeaconClient>,

    storage: Arc<dyn Storage>,

    id: String,

    pub config: Config,

    shutdown_rx: Receiver<bool>,
}

impl Archiver {
    pub fn new(
        beacon_client: Arc<dyn BeaconClient>,
        storage: Arc<dyn Storage>,
        shutdown_rx: Receiver<bool>,
    ) -> Self {
        Self {
            beacon_client,
            storage,
            id: "".to_string(),
            config: Default::default(),
            shutdown_rx,
        }
    }

    pub async fn persist_blobs_for_block(
        &self,
        block_id: BlockId,
        overwrite: bool,
    ) -> Result<Option<(BlockHeaderData, bool)>> {
        let header_resp_opt = self
            .beacon_client
            .get_beacon_headers_block_id(block_id)
            .await
            .map_err(|e| eyre::eyre!(e))?;

        match header_resp_opt {
            None => Ok(None),
            Some(header) => {
                let exists = self.storage.exists(&header.data.root).await;

                if exists && !overwrite {
                    return Ok(Some((header.data, true)));
                }

                let blobs_resp_opt = self
                    .beacon_client
                    .get_blobs(BlockId::Root(header.data.root), None)
                    .await
                    .map_err(|e| eyre::eyre!(e))?;
                if let Some(blob_sidecars) = blobs_resp_opt {
                    let blob_sidecar_list = blob_sidecars.data;
                    let blob_data = &BlobData::new(
                        Header {
                            beacon_block_hash: header.data.root,
                        },
                        BlobSidecars {
                            data: blob_sidecar_list,
                        },
                    );
                    self.storage.write_blob_data(blob_data).await?;
                    trace!("Persisting blobs for block: {:?}", blob_data);
                    return Ok(Some((header.data, exists)));
                }
                Ok(Some((header.data, exists)))
            }
        }
    }

    #[allow(dead_code)]
    async fn wait_obtain_storage_lock(&self) {
        let mut lock_file_res = self.storage.read_lock_file().await;
        let mut now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut shutdown = self.shutdown_rx.clone();
        match lock_file_res {
            Ok(mut lock_file) => {
                trace!("Lock file: {:#?}", lock_file);
                if lock_file == LockFile::default() {
                    while lock_file.archiver_id != self.id
                        && lock_file.timestamp + LOCK_TIMEOUT.as_secs() > now
                    {
                        tokio::select! {
                            _ = shutdown.changed() => {
                                info!("Received shutdown signal, exiting wait_obtain_storage_lock");
                                return;
                            }
                            _ = sleep(Duration::from_secs(OBTAIN_LOCK_RETRY_INTERVAL.load(Ordering::Relaxed))) => {
                                lock_file_res = self.storage.read_lock_file().await;
                                match lock_file_res {
                                    Ok(new_lock_file) => {
                                        lock_file = new_lock_file;
                                        now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_secs();
                                    }
                                    Err(e) => {
                                        error!("Error reading lock file: {:#?}", e);
                                        panic!("Error reading lock file: {:#?}", e);
                                    }
                                }
                            }
                        }
                    }
                }

                let written_res = self
                    .storage
                    .write_lock_file(&LockFile {
                        archiver_id: lock_file.archiver_id.clone(),
                        timestamp: now,
                    })
                    .await;

                match written_res {
                    Ok(_) => {
                        info!("Obtained storage lock");
                    }
                    Err(e) => {
                        error!("Error writing lock file: {:#?}", e);
                        panic!("Error writing lock file: {:#?}", e);
                    }
                }

                let storage = self.storage.clone();
                let archiver_id = self.id.clone();
                let mut shutdown_clone = shutdown.clone();

                tokio::spawn(async move {
                    let mut ticket = interval(LOCK_UPDATE_INTERVAL);
                    loop {
                        tokio::select! {
                            _ = shutdown_clone.changed() => {
                                info!("Received shutdown signal, exiting lock update loop");
                                break;
                            }
                            _ = ticket.tick() => {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                let written_res = storage.write_lock_file(&LockFile {
                                    archiver_id: archiver_id.clone(),
                                    timestamp: now,
                                }).await;

                                if let Err(e) = written_res {
                                    error!("Error update lockfile timestamp: {:#?}", e);
                                }
                            }
                        }
                    }
                });
            }
            Err(e) => {
                error!("Error reading lock file: {:#?}", e);
                panic!("Error reading lock file: {:#?}", e);
            }
        }
    }

    #[allow(dead_code)]
    async fn backfill_blobs(&self, latest: &BlockHeaderData) {
        let backfill_processes_res = self.storage.read_backfill_processes().await;

        match backfill_processes_res {
            Ok(mut backfill_processes) => {
                let backfill_process = BackfillProcess {
                    start_block: latest.clone(),
                    current_block: latest.clone(),
                };
                backfill_processes.insert(latest.root, backfill_process);
                let _ = self
                    .storage
                    .write_backfill_processes(&backfill_processes)
                    .await;

                let mut processes = backfill_processes.clone();
                for (_, process) in backfill_processes.iter() {
                    self.backfill_loop(
                        &process.start_block,
                        &process.current_block,
                        &mut processes,
                    )
                    .await;
                }
            }
            Err(e) => {
                error!("Error reading backfill processes: {:#?}", e);
                panic!("Error reading backfill processes: {:#?}", e);
            }
        }
    }

    #[allow(dead_code)]
    async fn backfill_loop(
        &self,
        start: &BlockHeaderData,
        current: &BlockHeaderData,
        backfill_processes: &mut BackfillProcesses,
    ) {
        let mut curr = current.clone();
        let mut already_exists = false;
        let mut count = 0;
        let mut res: Result<Option<(BlockHeaderData, bool)>>;
        let shutdown_rx = self.shutdown_rx.clone();
        info!("backfill process initiated, curr_hash: {:#?}, curr_slot: {:#?}, start_hash: {:#?},start_slot: {:#?}", curr.root, curr.header.message.slot.clone(), start.root, start.header.message.slot.clone());

        while !already_exists {
            if *shutdown_rx.borrow() {
                info!("Shutdown signal received, breaking backfill loop");
                return;
            }

            if curr.root == self.config.origin_block {
                info!("reached origin block, hash: {:#?}", curr.root);
                self.defer_fn(start, &curr, backfill_processes).await;
                return;
            }

            res = self
                .persist_blobs_for_block(BlockId::Root(curr.header.message.parent_root), false)
                .await;
            if let Err(e) = res {
                error!(
                    "failed to persist blobs for block, will retry: {:#?}, hash: {:#?}",
                    e, curr.header.message.parent_root
                );
                sleep(BACKFILL_ERROR_RETRY_INTERVAL).await;
                continue;
            };

            let Some((parent, parent_exists)) = res.unwrap() else {
                error!(
                    "failed to persist blobs for block, will retry, hash: {:#?}",
                    curr.header.message.parent_root
                );
                sleep(BACKFILL_ERROR_RETRY_INTERVAL).await;
                continue;
            };
            curr = parent;
            already_exists = parent_exists;

            if !already_exists {
                // todo: metrics
            }

            count += 1;
            if count % 10 == 0 {
                let backfill_process = BackfillProcess {
                    start_block: start.to_owned(),
                    current_block: curr.clone(),
                };
                backfill_processes.insert(start.root, backfill_process);
                let _ = self
                    .storage
                    .write_backfill_processes(backfill_processes)
                    .await;
            }
        }
        self.defer_fn(start, &curr, backfill_processes).await;
    }

    #[allow(dead_code)]
    async fn defer_fn(
        &self,
        start: &BlockHeaderData,
        current: &BlockHeaderData,
        backfill_processes: &mut BackfillProcesses,
    ) {
        info!("backfill process complete, end_hash: {:#?}, end_slot: {:#?}, start_hash: {:#?},start_slot: {:#?}", current.root, current.header.message.slot.clone(), start.root, start.header.message.slot.clone());
        backfill_processes.remove(&start.root);
        let _ = self
            .storage
            .write_backfill_processes(backfill_processes)
            .await;
    }

    #[allow(dead_code)]
    async fn process_blocks_until_known_block(&self) {
        debug!("refreshing live data");
        let mut start: Option<BlockHeaderData> = None;
        let mut current_block_id = BlockId::Head;

        loop {
            let retry_policy = RetryPolicy::exponential(Duration::from_millis(250))
                .with_jitter(true)
                .with_max_delay(Duration::from_secs(10))
                .with_max_retries(LIVE_FETCH_BLOB_MAXIMUM_RETRIES);
            let res = retry_policy
                .retry(|| self.persist_blobs_for_block(current_block_id, false))
                .await;

            if let Err(e) = res {
                error!("Error fetching blobs for block: {:#?}", e);
                return;
            }

            let Some((curr, already_exists)) = res.unwrap() else {
                error!("Error fetching blobs for block");
                return;
            };

            if start.is_none() {
                start = Some(curr.clone());
            }

            if !already_exists {
                // todo: metrics
            } else {
                debug!("blob already exists, hash: {:#?}", curr.clone().root);
                break;
            }

            current_block_id = BlockId::Root(curr.clone().header.message.parent_root)
        }

        info!(
            "live data refreshed,startHash: {:#?},endHash: {:#?}",
            start.unwrap().root,
            current_block_id
        );
    }

    #[allow(dead_code)]
    async fn track_latest_block(&self) {
        let mut ticket = interval(self.config.poll_interval);
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            tokio::select! {
                    _ = ticket.tick() => {
                        self.process_blocks_until_known_block().await;
                    }

                    _ = shutdown_rx.changed() => {
                        return;
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn start(&self) {}

    #[allow(dead_code)]
    async fn rearchive_range(&self, from: u64, to: u64) -> RearchiveResp {
        for i in from..=to {
            info!("rearchiving block: {}", i);
            let retry_policy = RetryPolicy::exponential(Duration::from_millis(250))
                .with_jitter(true)
                .with_max_delay(Duration::from_secs(10))
                .with_max_retries(REARCHIVE_MAXIMUM_RETRIES);
            let r = retry_policy.retry(|| self.rearchive(i)).await;

            match r {
                Err(e) => {
                    error!("Error fetching blobs for block: {:#?}", e);
                    return RearchiveResp {
                        from,
                        to,
                        error: Some(e.downcast::<Error>().unwrap().to_string()),
                    };
                }
                Ok(false) => {
                    info!("block not found, skipping");
                }
                Ok(true) => {
                    info!("block rearchived successfully")
                }
            }
        }
        RearchiveResp {
            from,
            to,
            error: None,
        }
    }

    async fn rearchive(&self, i: u64) -> Result<bool> {
        let res = self
            .persist_blobs_for_block(BlockId::Slot(Slot::new(i)), true)
            .await;

        match res {
            Err(e) => Err(eyre!(e)),
            Ok(None) => Ok(false),
            Ok(Some(_)) => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;

    use eth2::{BeaconNodeHttpClient, SensitiveUrl, Timeouts};

    use blob_archiver_beacon::beacon_client::{BeaconClientEth2, BeaconClientStub};
    use blob_archiver_beacon::blob_test_helper;
    use blob_archiver_beacon::blob_test_helper::{new_blob_sidecars, START_SLOT};
    use blob_archiver_storage::fs::FSStorage;

    use super::*;

    #[tokio::test]
    async fn test_rearchive_range() {
        let dir = &PathBuf::from("test_rearchive_range");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let (_, rx) = tokio::sync::watch::channel(false);
        let beacon_client = Arc::new(BeaconClientStub::default());
        let archiver = Archiver::new(beacon_client.clone(), Arc::new(storage), rx);

        let blob_sidecars_data = new_blob_sidecars(6);
        let blob_data = BlobData {
            header: Header {
                beacon_block_hash: *blob_test_helper::THREE,
            },
            blob_sidecars: BlobSidecars {
                data: blob_sidecars_data,
            },
        };
        let res = archiver.storage.write_blob_data(&blob_data).await;
        assert!(res.is_ok());

        assert!(!archiver.storage.exists(&blob_test_helper::ONE).await);
        assert!(!archiver.storage.exists(&blob_test_helper::TWO).await);
        assert!(archiver.storage.exists(&blob_test_helper::THREE).await);
        assert!(!archiver.storage.exists(&blob_test_helper::FOUR).await);

        let from = START_SLOT + 1;
        let to = START_SLOT + 4;

        let result = archiver.rearchive_range(from, to).await;
        assert!(result.error.is_none());
        assert_eq!(from, result.from);
        assert_eq!(to, result.to);

        assert!(archiver.storage.exists(&blob_test_helper::ONE).await);
        assert!(archiver.storage.exists(&blob_test_helper::TWO).await);
        assert!(archiver.storage.exists(&blob_test_helper::THREE).await);
        assert!(archiver.storage.exists(&blob_test_helper::FOUR).await);

        assert_eq!(
            archiver
                .storage
                .read_blob_data(&blob_test_helper::THREE)
                .await
                .unwrap()
                .blob_sidecars
                .data
                .len(),
            4
        );
        clean_dir(dir);
    }

    #[tokio::test]
    async fn test_persist_blobs_for_block() {
        let beacon_client = BeaconNodeHttpClient::new(
            SensitiveUrl::from_str("https://ethereum-beacon-api.publicnode.com").unwrap(),
            Timeouts::set_all(Duration::from_secs(30)),
        );
        let dir = &PathBuf::from("test_persist_blobs_for_block");
        let storage = FSStorage::new(dir.clone()).await.unwrap();
        tokio::fs::create_dir_all(dir).await.unwrap();
        let (_, rx) = tokio::sync::watch::channel(false);
        let beacon_client_eth2 = BeaconClientEth2 { beacon_client };
        let archiver = Archiver::new(Arc::new(beacon_client_eth2), Arc::new(storage), rx);

        let block_id = BlockId::Head;
        archiver
            .persist_blobs_for_block(block_id, false)
            .await
            .unwrap();
        clean_dir(dir);
    }

    fn clean_dir(dir: &PathBuf) {
        if dir.exists() {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
