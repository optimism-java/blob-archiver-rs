use crate::s3::S3Config;
use async_trait::async_trait;
use eth2::types::{
    BeaconBlockHeader, BlobSidecarList, BlockHeaderAndSignature, BlockHeaderData, Hash256,
    MainnetEthSpec, SignatureBytes,
};
use eyre::Result;
use serde::{Deserialize, Serialize};
use spin::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;

pub type BackfillProcesses = HashMap<Hash256, BackfillProcess>;
pub static BACKFILL_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub enum StorageType {
    #[default]
    FS,
    S3,
}

impl std::str::FromStr for StorageType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fs" => Ok(StorageType::FS),
            "s3" => Ok(StorageType::S3),
            _ => Err(format!("Invalid storage type: {}", s)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    pub storage_type: StorageType,
    pub s3_config: Option<S3Config>,
    pub fs_dir: Option<PathBuf>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct BackfillProcess {
    pub start_block: BlockHeaderData,
    pub current_block: BlockHeaderData,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub struct Header {
    pub beacon_block_hash: Hash256,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct BlobSidecars {
    pub data: BlobSidecarList<MainnetEthSpec>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct BlobData {
    pub header: Header,
    pub blob_sidecars: BlobSidecars,
}

impl BlobData {
    pub fn new(header: Header, blob_sidecars: BlobSidecars) -> Self {
        Self {
            header,
            blob_sidecars,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct LockFile {
    pub archiver_id: String,
    pub timestamp: u64,
}

#[async_trait]
pub trait StorageReader {
    async fn read_blob_data(&self, hash: &Hash256) -> Result<BlobData>;

    async fn exists(&self, hash: &Hash256) -> bool;

    async fn read_lock_file(&self) -> Result<LockFile>;

    async fn read_backfill_processes(&self) -> Result<BackfillProcesses>;
}

#[async_trait]
pub trait StorageWriter {
    async fn write_blob_data(&mut self, blob_data: &BlobData) -> Result<()>;

    async fn write_lock_file(&self, lock_file: &LockFile) -> Result<()>;

    async fn write_backfill_processes(&self, backfill_process: &BackfillProcesses) -> Result<()>;
}

#[async_trait]
pub trait Storage: StorageReader + StorageWriter + Send + Sync {}

pub fn create_test_lock_file() -> LockFile {
    LockFile {
        archiver_id: "test_archiver".to_string(),
        timestamp: 0,
    }
}

pub fn create_test_test_backfill_processes() -> BackfillProcesses {
    let mut backfill_processes: BackfillProcesses = HashMap::new();
    let header_hash = Hash256::random();
    let backfill_process = BackfillProcess {
        start_block: create_test_block_header(),
        current_block: create_test_block_header(),
    };
    backfill_processes.insert(header_hash, backfill_process);
    backfill_processes
}

pub fn create_test_blob_data() -> BlobData {
    BlobData::new(create_test_header(), create_test_blob_sidecars())
}

fn create_test_header() -> Header {
    Header {
        beacon_block_hash: Hash256::random(),
    }
}

fn create_test_blob_sidecars() -> BlobSidecars {
    BlobSidecars {
        data: BlobSidecarList::default(),
    }
}

fn create_test_block_header() -> BlockHeaderData {
    BlockHeaderData {
        root: Default::default(),
        canonical: false,
        header: BlockHeaderAndSignature {
            message: BeaconBlockHeader::empty(),
            signature: SignatureBytes::empty(),
        },
    }
}
