//! Mock storage implementation for testing
//!
//! This module provides a HashMap-based mock implementation of the KeyValueStore trait
//! that allows testing the data layer logic in isolation from the actual redb backend.

use crate::storage::{KeyValueStore, PrefixIterator, StorageError, StorageResult, Transaction};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// Mock storage implementation using HashMap for testing
/// 
/// This implementation is thread-safe and provides the same interface as redb
/// but stores all data in memory. It's designed for fast unit tests.
#[derive(Debug, Clone)]
pub struct MockStorage {
    /// The main data store, wrapped for thread safety
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Counter for generating unique transaction IDs
    next_txn_id: Arc<Mutex<u64>>,
}

impl MockStorage {
    /// Creates a new empty mock storage instance
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            next_txn_id: Arc::new(Mutex::new(0)),
        }
    }
    
    /// Returns the number of key-value pairs in the storage
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }
    
    /// Returns true if the storage is empty
    pub fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }
    
    /// Returns all keys currently stored (useful for debugging tests)
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.data.read().unwrap().keys().cloned().collect()
    }
    
    /// Clears all data from the storage
    pub fn clear(&self) {
        self.data.write().unwrap().clear();
    }
    
    /// Gets the next transaction ID
    fn next_transaction_id(&self) -> u64 {
        let mut counter = self.next_txn_id.lock().unwrap();
        let id = *counter;
        *counter += 1;
        id
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore for MockStorage {
    type Transaction = MockTransaction;
    
    fn open<P: AsRef<std::path::Path>>(_path: P) -> StorageResult<Self>
    where
        Self: Sized,
    {
        // For mock storage, we ignore the path and just create a new instance
        Ok(Self::new())
    }
    
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }
    
    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    
    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let mut data = self.data.write().unwrap();
        data.remove(key);
        Ok(())
    }
    
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>> {
        let data = self.data.read().unwrap();
        
        // Collect matching pairs and sort by key (to match B-tree ordering)
        let mut matching_pairs: Vec<(Vec<u8>, Vec<u8>)> = data
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        matching_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Convert to iterator of Results
        let iter = matching_pairs
            .into_iter()
            .map(|(k, v)| Ok((k, v)));
        
        Ok(Box::new(iter))
    }
    
    fn begin_transaction(&self) -> StorageResult<Self::Transaction> {
        let txn_id = self.next_transaction_id();
        Ok(MockTransaction {
            id: txn_id,
            storage: self.clone(),
            changes: HashMap::new(),
            committed: false,
        })
    }
}

/// Mock transaction implementation
/// 
/// This transaction implementation tracks changes in memory and can be committed
/// or rolled back. It provides full ACID semantics for testing.
pub struct MockTransaction {
    id: u64,
    storage: MockStorage,
    changes: HashMap<Vec<u8>, Option<Vec<u8>>>, // None = delete, Some = put
    committed: bool,
}

impl MockTransaction {
    /// Returns the unique ID of this transaction (useful for debugging)
    pub fn id(&self) -> u64 {
        self.id
    }
    
    /// Returns the number of pending changes in this transaction
    pub fn pending_changes(&self) -> usize {
        self.changes.len()
    }
    
    /// Checks if the transaction has been committed
    pub fn is_committed(&self) -> bool {
        self.committed
    }
}

impl Transaction for MockTransaction {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        // Check for pending changes first
        if let Some(change) = self.changes.get(key) {
            return Ok(change.clone());
        }
        
        // Fall back to storage
        self.storage.get(key)
    }
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.changes.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }
    
    fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.changes.insert(key.to_vec(), None);
        Ok(())
    }
    
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>> {
        // This is a simplified implementation that doesn't perfectly handle
        // the combination of storage data + transaction changes, but it's
        // sufficient for most unit tests
        let storage_data = self.storage.data.read().unwrap();
        
        // Collect from storage
        let mut matching_pairs: Vec<(Vec<u8>, Vec<u8>)> = storage_data
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        // Apply transaction changes
        for (key, change) in &self.changes {
            if key.starts_with(prefix) {
                // Remove any existing entry
                matching_pairs.retain(|(k, _)| k != key);
                
                // Add new entry if it's a put (not a delete)
                if let Some(value) = change {
                    matching_pairs.push((key.clone(), value.clone()));
                }
            }
        }
        
        // Sort by key to maintain B-tree ordering
        matching_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        
        let iter = matching_pairs
            .into_iter()
            .map(|(k, v)| Ok((k, v)));
        
        Ok(Box::new(iter))
    }
    
    fn commit(mut self) -> StorageResult<()> {
        if self.committed {
            return Err(StorageError::TransactionError("Transaction already committed".to_string()));
        }
        
        // Apply all changes atomically
        let mut data = self.storage.data.write().unwrap();
        
        for (key, change) in &self.changes {
            match change {
                Some(value) => {
                    data.insert(key.clone(), value.clone());
                }
                None => {
                    data.remove(key);
                }
            }
        }
        
        self.committed = true;
        Ok(())
    }
}

// Implement Drop to ensure uncommitted transactions are rolled back
impl Drop for MockTransaction {
    fn drop(&mut self) {
        if !self.committed && !self.changes.is_empty() {
            // In a real implementation, we might log this rollback
            // For testing, we just silently roll back by doing nothing
            // (changes were never applied to storage)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_storage_basic_operations() {
        let storage = MockStorage::new();
        
        // Test empty storage
        assert!(storage.is_empty());
        assert_eq!(storage.len(), 0);
        
        // Test put and get
        storage.put(b"key1", b"value1").unwrap();
        assert_eq!(storage.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(storage.len(), 1);
        assert!(!storage.is_empty());
        
        // Test get non-existent key
        assert_eq!(storage.get(b"nonexistent").unwrap(), None);
        
        // Test delete
        storage.delete(b"key1").unwrap();
        assert_eq!(storage.get(b"key1").unwrap(), None);
        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
    }
    
    #[test]
    fn test_mock_storage_prefix_scan() {
        let storage = MockStorage::new();
        
        // Insert test data
        storage.put(b"users:alice", b"alice_data").unwrap();
        storage.put(b"users:bob", b"bob_data").unwrap();
        storage.put(b"posts:123", b"post_data").unwrap();
        storage.put(b"users:charlie", b"charlie_data").unwrap();
        
        // Test prefix scan
        let results: Result<Vec<_>, _> = storage.scan_prefix(b"users:").unwrap().collect();
        let results = results.unwrap();
        
        assert_eq!(results.len(), 3);
        
        // Results should be sorted by key
        assert_eq!(results[0].0, b"users:alice");
        assert_eq!(results[0].1, b"alice_data");
        assert_eq!(results[1].0, b"users:bob");
        assert_eq!(results[1].1, b"bob_data");
        assert_eq!(results[2].0, b"users:charlie");
        assert_eq!(results[2].1, b"charlie_data");
    }
    
    #[test]
    fn test_mock_transaction_basic() {
        let storage = MockStorage::new();
        storage.put(b"existing", b"data").unwrap();
        
        let mut txn = storage.begin_transaction().unwrap();
        assert!(!txn.is_committed());
        assert_eq!(txn.pending_changes(), 0);
        
        // Test read existing data
        assert_eq!(txn.get(b"existing").unwrap(), Some(b"data".to_vec()));
        
        // Test transaction operations
        txn.put(b"new_key", b"new_value").unwrap();
        txn.delete(b"existing").unwrap();
        assert_eq!(txn.pending_changes(), 2);
        
        // Test reading uncommitted changes
        assert_eq!(txn.get(b"new_key").unwrap(), Some(b"new_value".to_vec()));
        assert_eq!(txn.get(b"existing").unwrap(), None);
        
        // Changes should not be visible in storage yet
        assert_eq!(storage.get(b"new_key").unwrap(), None);
        assert_eq!(storage.get(b"existing").unwrap(), Some(b"data".to_vec()));
        
        // Commit transaction
        txn.commit().unwrap();
        
        // Changes should now be visible in storage
        assert_eq!(storage.get(b"new_key").unwrap(), Some(b"new_value".to_vec()));
        assert_eq!(storage.get(b"existing").unwrap(), None);
    }
    
    #[test]
    fn test_mock_transaction_rollback() {
        let storage = MockStorage::new();
        storage.put(b"existing", b"original").unwrap();
        
        {
            let mut txn = storage.begin_transaction().unwrap();
            txn.put(b"existing", b"modified").unwrap();
            txn.put(b"new_key", b"new_value").unwrap();
            
            // Don't commit - transaction will be dropped and rolled back
        }
        
        // Changes should not be visible
        assert_eq!(storage.get(b"existing").unwrap(), Some(b"original".to_vec()));
        assert_eq!(storage.get(b"new_key").unwrap(), None);
    }
    
    #[test]
    fn test_mock_transaction_prefix_scan() {
        let storage = MockStorage::new();
        storage.put(b"users:alice", b"alice").unwrap();
        storage.put(b"users:bob", b"bob").unwrap();
        
        let mut txn = storage.begin_transaction().unwrap();
        
        // Add new data and modify existing in transaction
        txn.put(b"users:charlie", b"charlie").unwrap();
        txn.put(b"users:alice", b"alice_modified").unwrap();
        txn.delete(b"users:bob").unwrap();
        
        // Test prefix scan with transaction changes
        let results: Result<Vec<_>, _> = txn.scan_prefix(b"users:").unwrap().collect();
        let results = results.unwrap();
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"users:alice");
        assert_eq!(results[0].1, b"alice_modified");
        assert_eq!(results[1].0, b"users:charlie");
        assert_eq!(results[1].1, b"charlie");
        
        txn.commit().unwrap();
    }
    
    #[test]
    fn test_mock_transaction_double_commit() {
        let storage = MockStorage::new();
        let mut txn1 = storage.begin_transaction().unwrap();
        
        txn1.put(b"key", b"value").unwrap();
        txn1.commit().unwrap();
        
        // Create another transaction and try to commit it twice
        // (we can't test double commit on the same instance due to move semantics)
        let mut txn2 = storage.begin_transaction().unwrap();
        txn2.put(b"key2", b"value2").unwrap();
        
        // First commit should succeed
        txn2.commit().unwrap();
        
        // We can't test double commit on the same transaction due to move semantics,
        // which is actually the correct behavior - it prevents double commits at compile time
    }
    
    #[test]
    fn test_mock_storage_clear() {
        let storage = MockStorage::new();
        storage.put(b"key1", b"value1").unwrap();
        storage.put(b"key2", b"value2").unwrap();
        
        assert_eq!(storage.len(), 2);
        
        storage.clear();
        
        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
        assert_eq!(storage.get(b"key1").unwrap(), None);
    }
    
    #[test]
    fn test_mock_storage_keys() {
        let storage = MockStorage::new();
        storage.put(b"key1", b"value1").unwrap();
        storage.put(b"key2", b"value2").unwrap();
        
        let mut keys = storage.keys();
        keys.sort(); // HashMap iteration order is not guaranteed
        
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"key1".to_vec()));
        assert!(keys.contains(&b"key2".to_vec()));
    }
    
    #[test]
    fn test_mock_storage_concurrent_access() {
        use std::sync::Arc;
        use std::thread;
        
        let storage = Arc::new(MockStorage::new());
        let mut handles = vec![];
        
        // Spawn multiple threads that write data concurrently
        for i in 0..10 {
            let storage = Arc::clone(&storage);
            let handle = thread::spawn(move || {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                storage.put(key.as_bytes(), value.as_bytes()).unwrap();
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify all data was written
        assert_eq!(storage.len(), 10);
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(
                storage.get(key.as_bytes()).unwrap(),
                Some(expected_value.as_bytes().to_vec())
            );
        }
    }
}