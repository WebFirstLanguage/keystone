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
pub type PrefixIterator<'a> = Box<dyn Iterator<Item = StorageResult<(Vec<u8>, Vec<u8>)>> + 'a>;

/// Transaction handle for atomic operations
/// 
/// Transactions provide ACID guarantees for multiple operations. Changes made within
/// a transaction are not visible to other operations until `commit()` is called.
/// If the transaction is dropped without calling `commit()`, all changes are rolled back.
pub trait Transaction {
    /// Get a value by key within the transaction
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    
    /// Put a key-value pair within the transaction
    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()>;
    
    /// Delete a key within the transaction
    fn delete(&mut self, key: &[u8]) -> StorageResult<()>;
    
    /// Scan for keys with a given prefix within the transaction
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>>;
    
    /// Commit the transaction, making all changes permanent and visible to other operations
    /// 
    /// This method consumes the transaction. After calling commit(), the transaction
    /// cannot be used for further operations. If commit() fails, all changes in the
    /// transaction are rolled back.
    fn commit(self) -> StorageResult<()>;
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
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>>;
    
    /// Begin a new transaction for atomic operations
    fn begin_transaction(&self) -> StorageResult<Self::Transaction>;
    
    /// Execute a closure within a transaction, automatically committing on success
    /// or rolling back on error
    /// 
    /// This method provides a default implementation that:
    /// 1. Begins a new transaction via `begin_transaction()`
    /// 2. Executes the provided closure with the transaction
    /// 3. Commits the transaction if the closure returns `Ok`
    /// 4. Automatically rolls back if the closure returns `Err` (transaction is dropped)
    /// 
    /// Implementations can override this if they need custom transaction behavior.
    fn transaction<T, F>(&self, f: F) -> StorageResult<T>
    where
        F: FnOnce(&mut Self::Transaction) -> StorageResult<T>,
    {
        let mut txn = self.begin_transaction()?;
        
        match f(&mut txn) {
            Ok(result) => {
                // Commit the transaction on success
                txn.commit()?;
                Ok(result)
            },
            Err(err) => {
                // Transaction will be automatically rolled back when dropped
                // This is the standard behavior for most transaction implementations
                // including redb, where uncommitted transactions are rolled back
                Err(err)
            }
        }
    }
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
    
    #[test]
    fn test_default_transaction_implementation() {
        use super::redb_adapter::RedbAdapter;
        use tempfile::tempdir;
        
        // Test that the default transaction implementation works with RedbAdapter
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let adapter = RedbAdapter::open(&db_path).unwrap();
        
        // Test successful transaction using default implementation
        let result = adapter.transaction(|txn| {
            txn.put(b"default_key1", b"default_value1")?;
            txn.put(b"default_key2", b"default_value2")?;
            Ok("success")
        });
        
        assert_eq!(result.unwrap(), "success");
        
        // Verify the changes were committed
        assert_eq!(adapter.get(b"default_key1").unwrap(), Some(b"default_value1".to_vec()));
        assert_eq!(adapter.get(b"default_key2").unwrap(), Some(b"default_value2".to_vec()));
        
        // Test failed transaction using default implementation  
        adapter.put(b"existing_key", b"existing_value").unwrap();
        
        let result: StorageResult<()> = adapter.transaction(|txn| {
            txn.put(b"temp_key", b"temp_value")?;
            txn.delete(b"existing_key")?;
            Err(StorageError::InvalidKey) // Force failure
        });
        
        assert!(result.is_err());
        
        // Verify the transaction was rolled back
        assert_eq!(adapter.get(b"temp_key").unwrap(), None); // Should not exist
        assert_eq!(adapter.get(b"existing_key").unwrap(), Some(b"existing_value".to_vec())); // Should be unchanged
    }
}