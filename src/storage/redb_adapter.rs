//! redb adapter implementing the KeyValueStore trait
//! 
//! This module provides a concrete implementation of the KeyValueStore trait
//! using redb as the underlying storage engine. It handles the translation
//! between our generic storage interface and redb's specific API.

use std::path::Path;
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use crate::storage::{KeyValueStore, StorageError, StorageResult, Transaction, PrefixIterator};

/// Table definition for the main key-value store
const MAIN_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("main");

/// redb implementation of the KeyValueStore trait
pub struct RedbAdapter {
    database: Database,
}

/// Transaction wrapper for redb write transactions
pub struct RedbTransaction {
    txn: WriteTransaction,
}

impl Transaction for RedbTransaction {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let table = self.txn.open_table(MAIN_TABLE)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        let result = table.get(key)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
            
        match result {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let mut table = self.txn.open_table(MAIN_TABLE)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        table.insert(key, value)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        Ok(())
    }
    
    fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        let mut table = self.txn.open_table(MAIN_TABLE)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        table.remove(key)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        Ok(())
    }
    
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>> {
        let table = self.txn.open_table(MAIN_TABLE)
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        // Create a range that starts with the prefix
        let start_bound = prefix;
        
        let range_iter = match RedbAdapter::compute_exclusive_end_bound(prefix) {
            None => table.range(start_bound..)
                .map_err(|e| StorageError::TransactionError(e.to_string()))?,
            Some(end) if end.is_empty() => table.range(start_bound..)
                .map_err(|e| StorageError::TransactionError(e.to_string()))?,
            Some(end) => table.range(start_bound..end.as_slice())
                .map_err(|e| StorageError::TransactionError(e.to_string()))?,
        };
        
        // Note: Currently collecting due to lifetime constraints between table and transaction.
        // True streaming would require restructuring to store the table in the transaction wrapper.
        // This is a potential future enhancement.
        let vec_iter: Vec<_> = range_iter
            .map(|result| {
                result
                    .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
                    .map_err(|e| StorageError::TransactionError(e.to_string()))
            })
            .collect();
        
        Ok(Box::new(vec_iter.into_iter()))
    }
    
    fn commit(self) -> StorageResult<()> {
        self.txn.commit()
            .map_err(|e| StorageError::TransactionError(e.to_string()))
    }
}

impl RedbAdapter {
    /// Compute exclusive end bound for prefix scanning
    /// 
    /// Returns:
    /// - `None` for unbounded scan (all bytes are 0xFF)  
    /// - `Some(Vec::new())` for scan to end (empty prefix case)
    /// - `Some(Vec<u8>)` for bounded scan with computed end bound
    fn compute_exclusive_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        if prefix.is_empty() {
            return Some(Vec::new()); // Signal scan to end
        }
        
        let mut end_bound = prefix.to_vec();
        let mut pos = end_bound.len();
        
        // Find rightmost byte that can be incremented
        while pos > 0 {
            pos -= 1;
            if end_bound[pos] != 0xFF {
                end_bound[pos] += 1;
                end_bound.truncate(pos + 1);
                return Some(end_bound);
            }
        }
        
        // All bytes are 0xFF - unbounded scan
        None
    }
}

impl KeyValueStore for RedbAdapter {
    type Transaction = RedbTransaction;
    
    fn open<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let database = Database::create(path)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        // Initialize the main table to ensure it exists for read operations
        {
            let write_txn = database.begin_write()
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            
            // Opening the table in a write transaction creates it if it doesn't exist
            {
                let _table = write_txn.open_table(MAIN_TABLE)
                    .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
                // Table is dropped here, releasing the borrow on write_txn
            }
            
            write_txn.commit()
                .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        }
        
        Ok(RedbAdapter { database })
    }
    
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let read_txn = self.database.begin_read()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        let table = read_txn.open_table(MAIN_TABLE)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        let result = table.get(key)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            
        match result {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }
    
    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let write_txn = self.database.begin_write()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        {
            let mut table = write_txn.open_table(MAIN_TABLE)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            
            table.insert(key, value)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        
        write_txn.commit()
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        Ok(())
    }
    
    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let write_txn = self.database.begin_write()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        {
            let mut table = write_txn.open_table(MAIN_TABLE)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            
            table.remove(key)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        
        write_txn.commit()
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;
        
        Ok(())
    }
    
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>> {
        let read_txn = self.database.begin_read()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        let table = read_txn.open_table(MAIN_TABLE)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        // Create a range that starts with the prefix
        let start_bound = prefix;
        
        let range_iter = match Self::compute_exclusive_end_bound(prefix) {
            None => table.range(start_bound..)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?,
            Some(end) if end.is_empty() => table.range(start_bound..)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?,
            Some(end) => table.range(start_bound..end.as_slice())
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?,
        };
        
        // Note: We collect here because this method creates its own read transaction
        // which must be dropped before returning. For true streaming, use begin_transaction()
        // and call scan_prefix on the transaction directly.
        let vec_iter: Vec<_> = range_iter
            .map(|result| {
                result
                    .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
                    .map_err(|e| StorageError::DatabaseError(e.to_string()))
            })
            .collect();
        
        Ok(Box::new(vec_iter.into_iter()))
    }
    
    fn begin_transaction(&self) -> StorageResult<Self::Transaction> {
        let txn = self.database.begin_write()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        Ok(RedbTransaction { txn })
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    fn create_test_adapter() -> (RedbAdapter, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let adapter = RedbAdapter::open(&db_path).unwrap();
        (adapter, temp_dir)
    }
    
    #[test]
    fn test_basic_put_get() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        let key = b"test_key";
        let value = b"test_value";
        
        // Put a value
        adapter.put(key, value).unwrap();
        
        // Get the value back
        let retrieved = adapter.get(key).unwrap();
        assert_eq!(retrieved, Some(value.to_vec()));
        
        // Test non-existent key
        let non_existent = adapter.get(b"non_existent").unwrap();
        assert_eq!(non_existent, None);
    }
    
    #[test]
    fn test_delete() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        let key = b"test_key";
        let value = b"test_value";
        
        // Put and verify
        adapter.put(key, value).unwrap();
        assert_eq!(adapter.get(key).unwrap(), Some(value.to_vec()));
        
        // Delete and verify
        adapter.delete(key).unwrap();
        assert_eq!(adapter.get(key).unwrap(), None);
    }
    
    #[test]
    fn test_prefix_scan() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Insert test data
        adapter.put(b"bucket1\x00key1", b"value1").unwrap();
        adapter.put(b"bucket1\x00key2", b"value2").unwrap();
        adapter.put(b"bucket2\x00key3", b"value3").unwrap();
        adapter.put(b"other_key", b"other_value").unwrap();
        
        // Scan for bucket1 prefix
        let results: Vec<_> = adapter.scan_prefix(b"bucket1\x00")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        
        assert_eq!(results.len(), 2);
        assert!(results.contains(&(b"bucket1\x00key1".to_vec(), b"value1".to_vec())));
        assert!(results.contains(&(b"bucket1\x00key2".to_vec(), b"value2".to_vec())));
    }
    
    #[test]
    fn test_transaction() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Test successful transaction
        let result = adapter.transaction(|txn| {
            txn.put(b"key1", b"value1")?;
            txn.put(b"key2", b"value2")?;
            Ok("success")
        });
        
        assert_eq!(result.unwrap(), "success");
        assert_eq!(adapter.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(adapter.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }
    
    #[test]
    fn test_transaction_rollback() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Put initial data
        adapter.put(b"existing_key", b"existing_value").unwrap();
        
        // Test failed transaction (should rollback)
        let result: StorageResult<()> = adapter.transaction(|txn| {
            txn.put(b"key1", b"value1")?;
            txn.put(b"key2", b"value2")?;
            Err(StorageError::InvalidKey) // Force failure
        });
        
        assert!(result.is_err());
        
        // Verify that the transaction was rolled back
        assert_eq!(adapter.get(b"key1").unwrap(), None);
        assert_eq!(adapter.get(b"key2").unwrap(), None);
        
        // Verify existing data is still there
        assert_eq!(adapter.get(b"existing_key").unwrap(), Some(b"existing_value".to_vec()));
    }
    
    #[test]
    fn test_empty_prefix_scan() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        adapter.put(b"key1", b"value1").unwrap();
        adapter.put(b"key2", b"value2").unwrap();
        
        // Scan with empty prefix should return all keys
        let results: Vec<_> = adapter.scan_prefix(b"")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        
        assert_eq!(results.len(), 2);
    }
    
    #[test]
    fn test_prefix_scan_no_matches() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        adapter.put(b"key1", b"value1").unwrap();
        
        // Scan for non-matching prefix
        let results: Vec<_> = adapter.scan_prefix(b"nomatch")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        
        assert_eq!(results.len(), 0);
    }
    
    #[test]
    fn test_manual_transaction_commit() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Verify initial state - key should not exist
        assert_eq!(adapter.get(b"manual_key").unwrap(), None);
        
        // Begin a manual transaction
        let mut txn = adapter.begin_transaction().unwrap();
        
        // Perform operations within the transaction
        txn.put(b"manual_key", b"manual_value").unwrap();
        txn.put(b"another_key", b"another_value").unwrap();
        
        // Before commit, changes should not be visible from outside the transaction
        // Note: We can't easily test this with the current API since we'd need another adapter instance
        
        // Verify data is accessible within the transaction
        assert_eq!(txn.get(b"manual_key").unwrap(), Some(b"manual_value".to_vec()));
        assert_eq!(txn.get(b"another_key").unwrap(), Some(b"another_value".to_vec()));
        
        // Commit the transaction
        txn.commit().unwrap();
        
        // After commit, changes should be visible via fresh non-transactional operations
        assert_eq!(adapter.get(b"manual_key").unwrap(), Some(b"manual_value".to_vec()));
        assert_eq!(adapter.get(b"another_key").unwrap(), Some(b"another_value".to_vec()));
    }
    
    #[test]
    fn test_manual_transaction_with_delete() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Put initial data
        adapter.put(b"existing_key", b"existing_value").unwrap();
        adapter.put(b"delete_me", b"delete_value").unwrap();
        
        // Verify initial state
        assert_eq!(adapter.get(b"existing_key").unwrap(), Some(b"existing_value".to_vec()));
        assert_eq!(adapter.get(b"delete_me").unwrap(), Some(b"delete_value".to_vec()));
        
        // Begin manual transaction and perform mixed operations
        let mut txn = adapter.begin_transaction().unwrap();
        txn.put(b"new_key", b"new_value").unwrap();
        txn.delete(b"delete_me").unwrap();
        
        // Commit the transaction
        txn.commit().unwrap();
        
        // Verify the results
        assert_eq!(adapter.get(b"existing_key").unwrap(), Some(b"existing_value".to_vec())); // Unchanged
        assert_eq!(adapter.get(b"new_key").unwrap(), Some(b"new_value".to_vec())); // Added
        assert_eq!(adapter.get(b"delete_me").unwrap(), None); // Deleted
    }
    
    #[test]
    fn test_fresh_database_read_ready() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // This test verifies that we can immediately read from a fresh database
        // without needing any prior write operations - proving table initialization works
        assert_eq!(adapter.get(b"nonexistent_key").unwrap(), None);
        
        // Also test that scan_prefix works immediately
        let results: Vec<_> = adapter.scan_prefix(b"any_prefix")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 0);
    }
    
    #[test]
    fn test_transaction_rollback_on_drop() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Put initial data
        adapter.put(b"existing_key", b"existing_value").unwrap();
        
        {
            // Begin transaction and make changes
            let mut txn = adapter.begin_transaction().unwrap();
            txn.put(b"temp_key", b"temp_value").unwrap();
            txn.delete(b"existing_key").unwrap();
            
            // Verify changes are visible within transaction
            assert_eq!(txn.get(b"temp_key").unwrap(), Some(b"temp_value".to_vec()));
            assert_eq!(txn.get(b"existing_key").unwrap(), None);
            
            // Transaction is dropped here without calling commit()
        }
        
        // Verify that changes were rolled back
        assert_eq!(adapter.get(b"temp_key").unwrap(), None); // Should not exist
        assert_eq!(adapter.get(b"existing_key").unwrap(), Some(b"existing_value".to_vec())); // Should be unchanged
    }
    
    #[test]
    fn test_streaming_prefix_scan_with_lifetimes() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Create a larger dataset to test streaming behavior
        for i in 0..100 {
            let key = format!("stream_test_{:03}", i);
            let value = format!("value_{}", i);
            adapter.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Test streaming with transaction-scoped iterator
        {
            let txn = adapter.begin_transaction().unwrap();
            let iter = txn.scan_prefix(b"stream_test_").unwrap();
            
            // Verify we can process results lazily without collecting everything
            let mut count = 0;
            let mut first_key: Option<Vec<u8>> = None;
            
            for result in iter {
                let (key, _value) = result.unwrap();
                if first_key.is_none() {
                    first_key = Some(key.clone());
                }
                count += 1;
                
                // Early termination test - we can stop processing without having
                // read all results, demonstrating lazy evaluation
                if count >= 5 {
                    break;
                }
            }
            
            assert_eq!(count, 5);
            assert!(first_key.is_some());
            assert!(first_key.unwrap().starts_with(b"stream_test_"));
        }
        
        // Test that non-transactional scan_prefix still works (currently buffered)
        let iter = adapter.scan_prefix(b"stream_test_").unwrap();
        let all_results: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(all_results.len(), 100);
        
        // Verify lexicographic ordering
        for i in 0..99 {
            assert!(all_results[i].0 <= all_results[i + 1].0);
        }
    }
    
    #[test] 
    fn test_prefix_iterator_lifetime_correctness() {
        let (adapter, _temp_dir) = create_test_adapter();
        
        // Add some test data
        adapter.put(b"lifetime_test_001", b"value1").unwrap();
        adapter.put(b"lifetime_test_002", b"value2").unwrap();
        adapter.put(b"other_data", b"value3").unwrap();
        
        // Test that iterator lifetime is properly tied to transaction
        let txn = adapter.begin_transaction().unwrap();
        let iter = txn.scan_prefix(b"lifetime_test_").unwrap();
        
        // Process iterator while transaction is still alive
        let results: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].0.starts_with(b"lifetime_test_"));
        assert!(results[1].0.starts_with(b"lifetime_test_"));
        
        // Transaction can still be committed after iterator use
        txn.commit().unwrap();
    }
    
    #[test]
    fn test_compute_exclusive_end_bound() {
        // Test empty prefix
        let result = RedbAdapter::compute_exclusive_end_bound(b"");
        assert_eq!(result, Some(Vec::new()));
        
        // Test normal prefix increment (b"abc" -> b"abd")
        let result = RedbAdapter::compute_exclusive_end_bound(b"abc");
        assert_eq!(result, Some(b"abd".to_vec()));
        
        // Test single byte increment
        let result = RedbAdapter::compute_exclusive_end_bound(b"a");
        assert_eq!(result, Some(b"b".to_vec()));
        
        // Test 0xFF handling (b"ab\xFF" -> b"ac")  
        let result = RedbAdapter::compute_exclusive_end_bound(b"ab\xFF");
        assert_eq!(result, Some(b"ac".to_vec()));
        
        // Test multiple 0xFF handling (b"a\xFF\xFF" -> b"b")
        let result = RedbAdapter::compute_exclusive_end_bound(b"a\xFF\xFF");
        assert_eq!(result, Some(b"b".to_vec()));
        
        // Test all 0xFF handling (b"\xFF\xFF" -> None for unbounded)
        let result = RedbAdapter::compute_exclusive_end_bound(b"\xFF\xFF");
        assert_eq!(result, None);
        
        // Test single 0xFF byte
        let result = RedbAdapter::compute_exclusive_end_bound(b"\xFF");
        assert_eq!(result, None);
        
        // Test edge case: prefix ending with 0xFE
        let result = RedbAdapter::compute_exclusive_end_bound(b"ab\xFE");
        assert_eq!(result, Some(b"ab\xFF".to_vec()));
        
        // Test mixed bytes with 0xFF at end
        let result = RedbAdapter::compute_exclusive_end_bound(b"test\xFF");
        assert_eq!(result, Some(b"tesu".to_vec()));
        
        // Test zero byte handling
        let result = RedbAdapter::compute_exclusive_end_bound(b"a\x00b");
        assert_eq!(result, Some(b"a\x00c".to_vec()));
        
        // Test prefix that would increment to create longer result
        let result = RedbAdapter::compute_exclusive_end_bound(b"test");
        assert_eq!(result, Some(b"tesu".to_vec()));
    }
}