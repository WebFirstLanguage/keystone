//! Keystone: A pure-Rust object storage system
//!
//! This library provides a high-performance, ACID-compliant object storage system
//! built on top of redb. It follows a bucket/key/object hierarchy similar to S3.

pub mod data;
pub mod storage;

// Re-export main types that will be used by consumers
pub use data::{Object, ObjectData, ObjectMetadata};
pub use storage::StorageError;
