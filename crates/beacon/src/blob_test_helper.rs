use std::sync::Arc;

use eth2::types::test_utils::TestRandom;
use eth2::types::{
    Blob, BlobSidecar, BlobSidecarList, FixedVector, Hash256, KzgCommitment, KzgProof,
    MainnetEthSpec, SignedBeaconBlockHeader,
};
use once_cell::sync::Lazy;

pub static ORIGIN_BLOCK: Lazy<Hash256> = Lazy::new(|| Hash256::from([0x09; 5] + [0x00; 27]));
pub static ONE: Lazy<Hash256> = Lazy::new(|| Hash256::from([0x01] + [0x00; 31]));
pub static TWO: Lazy<Hash256> = Lazy::new(|| Hash256::from([0x02] + [0x00; 31]));
pub static THREE: Lazy<Hash256> = Lazy::new(|| Hash256::from([0x03] + [0x00; 31]));
pub static FOUR: Lazy<Hash256> = Lazy::new(|| Hash256::from([0x04] + [0x00; 31]));
pub static FIVE: Lazy<Hash256> = Lazy::new(|| Hash256::from([0x05] + [0x00; 31]));

pub const START_SLOT: u64 = 10;
#[allow(dead_code)]
pub const END_SLOT: u64 = 15;

pub fn new_blob_sidecar(i: u64) -> BlobSidecar<MainnetEthSpec> {
    let mut rng = rand::thread_rng();
    BlobSidecar {
        index: i,
        blob: Blob::<MainnetEthSpec>::random_for_test(&mut rng),
        kzg_commitment: KzgCommitment::random_for_test(&mut rng),
        kzg_proof: KzgProof::random_for_test(&mut rng),
        signed_block_header: SignedBeaconBlockHeader::random_for_test(&mut rng),
        kzg_commitment_inclusion_proof: FixedVector::random_for_test(&mut rng),
    }
}

pub fn new_blob_sidecars(n: usize) -> BlobSidecarList<MainnetEthSpec> {
    let mut blob_sidecars = BlobSidecarList::default();
    for i in 0..n {
        blob_sidecars
            .push(Arc::from(new_blob_sidecar(i as u64)))
            .expect("TODO: panic message");
    }
    blob_sidecars
}