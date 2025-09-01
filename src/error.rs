use crate::storage::StorageError;
use thiserror::Error;

/// Error type for datastore and bucket operations
#[derive(Debug, Error)]
pub enum Error {
    /// Underlying storage layer error
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Serialization or deserialization error
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// Bucket already exists
    #[error("bucket already exists")]
    BucketExists,

    /// Requested bucket was not found
    #[error("bucket not found")]
    BucketNotFound,

    /// Requested object was not found
    #[error("object not found")]
    ObjectNotFound,

    /// Invalid bucket name (e.g., contains NUL)
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),

    /// Invalid object key or prefix (e.g., contains NUL)
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),
}

/// Result type for public API operations
pub type Result<T> = std::result::Result<T, Error>;
