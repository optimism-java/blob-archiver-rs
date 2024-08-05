use std::sync::Arc;

use eth2::types::{Blob, BlobSidecar, BlobSidecarList, FixedVector, Hash256, KzgCommitment, KzgProof, MainnetEthSpec, SignedBeaconBlockHeader, Slot};
use eth2::types::test_utils::TestRandom;
use once_cell::sync::Lazy;

pub static ORIGIN_BLOCK: Lazy<Hash256> = Lazy::new(|| Hash256::from_slice(&[9, 9, 9, 9, 9]));
pub static ONE: Lazy<Hash256> = Lazy::new(|| Hash256::from_slice(&[1]));
pub static TWO: Lazy<Hash256> = Lazy::new(|| Hash256::from_slice(&[2]));
pub static THREE: Lazy<Hash256> = Lazy::new(|| Hash256::from_slice(&[3]));
pub static FOUR: Lazy<Hash256> = Lazy::new(|| Hash256::from_slice(&[4]));
pub static FIVE: Lazy<Hash256> = Lazy::new(|| Hash256::from_slice(&[5]));

pub const START_SLOT: Slot = Slot::new(10);
#[allow(dead_code)]
pub const END_SLOT: Slot = Slot::new(15);

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
        blob_sidecars.push(Arc::from(new_blob_sidecar(i as u64))).expect("TODO: panic message");
    }
    blob_sidecars
}