//! Storage abstraction layer for keystone
//! 
//! This module provides a clean abstraction over different key-value storage backends.
//! The KeyValueStore trait isolates the core object storage logic from the specific
//! implementation details of the underlying database.

use thiserror::Error;

pub mod redb_adapter;

/// Errors that can occur during storage operations
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    DatabaseError(String),
    
    #[error("Transaction error: {0}")]
    TransactionError(String),
    
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Key not found")]
    KeyNotFound,
    
    #[error("Invalid key format")]
    InvalidKey,
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Iterator over key-value pairs returned by prefix scans
pub type PrefixIterator = Box<dyn Iterator<Item = StorageResult<(Vec<u8>, Vec<u8>)>>>;

/// Transaction handle for atomic operations
pub trait Transaction {
    /// Get a value by key within the transaction
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    
    /// Put a key-value pair within the transaction
    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()>;
    
    /// Delete a key within the transaction
    fn delete(&mut self, key: &[u8]) -> StorageResult<()>;
    
    /// Scan for keys with a given prefix within the transaction
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator>;
}

/// Core trait defining the interface for key-value storage backends
/// 
/// This trait provides the essential operations needed for the object storage system:
/// - Basic CRUD operations (get, put, delete)
/// - Prefix scanning for bucket listing operations
/// - Transaction support for atomic multi-operation sequences
pub trait KeyValueStore: Send + Sync {
    /// Transaction type for this storage backend
    type Transaction: Transaction;
    
    /// Open or create a database at the specified path
    fn open<P: AsRef<std::path::Path>>(path: P) -> StorageResult<Self>
    where
        Self: Sized;
    
    /// Get a value by key
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    
    /// Put a key-value pair
    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()>;
    
    /// Delete a key
    fn delete(&self, key: &[u8]) -> StorageResult<()>;
    
    /// Scan for keys with a given prefix, returning key-value pairs
    /// 
    /// This is essential for implementing bucket listing operations efficiently.
    /// The iterator should return keys in lexicographic order.
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator>;
    
    /// Begin a new transaction for atomic operations
    fn begin_transaction(&self) -> StorageResult<Self::Transaction>;
    
    /// Execute a closure within a transaction, automatically committing on success
    /// or rolling back on error
    fn transaction<T, F>(&self, f: F) -> StorageResult<T>
    where
        F: FnOnce(&mut Self::Transaction) -> StorageResult<T>;
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // These tests will validate that our trait design is sound
    // Specific implementation tests will be in the adapter modules
    
    #[test]
    fn storage_error_display() {
        let err = StorageError::KeyNotFound;
        assert_eq!(format!("{}", err), "Key not found");
        
        let err = StorageError::InvalidKey;
        assert_eq!(format!("{}", err), "Invalid key format");
    }
    
    #[test]
    fn storage_error_debug() {
        let err = StorageError::DatabaseError("test error".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("DatabaseError"));
        assert!(debug_str.contains("test error"));
    }
}