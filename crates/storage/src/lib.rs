extern crate core;

mod fs;
mod s3;
pub mod storage;

pub use storage::*;

#[cfg(test)]
mod tests {}
