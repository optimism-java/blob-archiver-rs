extern crate core;

pub mod fs;
pub mod s3;
pub mod storage;

pub use storage::*;

#[cfg(test)]
mod tests {}
