//! Keystone: A pure-Rust object storage system
//!
//! This library provides a high-performance, ACID-compliant object storage system
//! built on top of redb. It follows a bucket/key/object hierarchy similar to S3.

pub mod bucket;
pub mod data;
pub mod datastore;
pub mod error;
pub mod storage;

// Re-export main types that will be used by consumers
pub use bucket::Bucket;
pub use data::{Object, ObjectData, ObjectMetadata};
pub use datastore::Datastore;
pub use error::{Error, Result};
pub use storage::StorageError;
