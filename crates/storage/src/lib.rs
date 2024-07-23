extern crate core;

pub mod storage;
mod s3;

pub use storage::*;

#[cfg(test)]
mod tests {}
