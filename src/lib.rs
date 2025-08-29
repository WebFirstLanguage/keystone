//! Keystone: A pure-Rust object storage system
//! 
//! This library provides a high-performance, ACID-compliant object storage system
//! built on top of redb. It follows a bucket/key/object hierarchy similar to S3.

pub mod storage;
pub mod data;

// Re-export main types that will be used by consumers
pub use storage::StorageError;
pub use data::{Object, ObjectMetadata};