use std::str::FromStr;
use std::time::Duration;
use eth2::types::Hash256;
use anyhow::Result;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BeaconConfig {
    pub beacon_url: String,
    pub beacon_client_timeout: Duration,
    pub enforce_json: bool,
}

/// Configuration for the archiver.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiverConfig {
    pub beacon: BeaconConfig,
    pub poll_interval: Duration,
    pub origin_block: Hash256,
}