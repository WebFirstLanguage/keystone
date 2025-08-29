//! Large object chunking strategy for keystone object storage
//!
//! This module handles the chunking of large objects to avoid memory pressure
//! and enable streaming operations. It implements a manifest-based approach
//! where large objects are split into chunks and a manifest tracks the chunks.

use crate::data::{Object, ObjectMetadata};
use crate::data::keys::{generate_chunk_key, generate_chunk_prefix, parse_chunk_key};
use crate::storage::{KeyValueStore, StorageError, StorageResult, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};

/// Default chunk size: 8MB
/// This provides a good balance between memory usage and I/O efficiency
pub const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Minimum size for chunking - objects smaller than this are stored as single objects
/// This avoids the overhead of chunking for small objects
pub const MIN_CHUNK_THRESHOLD: usize = 1024 * 1024; // 1MB

/// Maximum number of chunks supported per object (prevents memory issues with manifests)
pub const MAX_CHUNKS: u64 = 10000;

/// Manifest data structure that tracks chunks of a large object
/// 
/// This is stored as the primary object data when an object is chunked.
/// The actual object data is stored separately in chunks.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ChunkManifest {
    /// Total number of chunks
    pub chunk_count: u64,
    /// Size of each chunk except possibly the last one
    pub chunk_size: usize,
    /// Total size of the original object
    pub total_size: u64,
    /// Checksum or hash of the original object (for integrity verification)
    /// Using a simple checksum for now - could be upgraded to SHA-256 later
    pub checksum: u64,
    /// The content type of the original object
    pub content_type: String,
}

impl ChunkManifest {
    /// Creates a new chunk manifest
    pub fn new(
        chunk_count: u64,
        chunk_size: usize,
        total_size: u64,
        checksum: u64,
        content_type: String,
    ) -> Self {
        Self {
            chunk_count,
            chunk_size,
            total_size,
            checksum,
            content_type,
        }
    }
    
    /// Calculates the size of a specific chunk
    /// 
    /// Returns 0 if the chunk index is out of range or if the calculated size
    /// would overflow usize on this platform (e.g., on 32-bit systems with very large objects).
    pub fn chunk_size_for_index(&self, chunk_index: u64) -> usize {
        if chunk_index >= self.chunk_count {
            return 0;
        }
        
        if chunk_index == self.chunk_count - 1 {
            // Last chunk might be smaller
            let full_chunks_size = (self.chunk_count - 1) * self.chunk_size as u64;
            let remainder = self.total_size.saturating_sub(full_chunks_size);
            
            // Use try_from to safely convert from u64 to usize, 
            // returning 0 as a safe fallback if the value is too large for this platform
            usize::try_from(remainder).unwrap_or(0)
        } else {
            self.chunk_size
        }
    }
    
    /// Validates that the manifest is consistent
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.chunk_count == 0 {
            return Err(StorageError::InvalidKey);
        }
        
        if self.chunk_count > MAX_CHUNKS {
            return Err(StorageError::InvalidKey);
        }
        
        if self.chunk_size == 0 {
            return Err(StorageError::InvalidKey);
        }
        
        // Verify total size is consistent with chunk count and size
        let expected_min_size = (self.chunk_count - 1) * self.chunk_size as u64;
        let expected_max_size = self.chunk_count * self.chunk_size as u64;
        
        if self.total_size < expected_min_size || self.total_size > expected_max_size {
            return Err(StorageError::InvalidKey);
        }
        
        Ok(())
    }
}

/// Strategy for handling object chunking operations
pub struct ChunkingStrategy {
    chunk_size: usize,
}

impl ChunkingStrategy {
    /// Creates a new chunking strategy with the default chunk size
    pub fn new() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
    
    /// Creates a new chunking strategy with a custom chunk size
    #[deprecated(note = "use `try_with_chunk_size` for fallible validation")]
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        if chunk_size == 0 {
            panic!("Chunk size cannot be zero");
        }
        Self { chunk_size }
    }

    /// Fallible constructor that returns an error instead of panicking.
    pub fn try_with_chunk_size(chunk_size: usize) -> StorageResult<Self> {
        if chunk_size == 0 {
            return Err(StorageError::DatabaseError("chunk_size must be > 0".to_string()));
        }
        Ok(Self { chunk_size })
    }
    
    /// Determines if an object should be chunked based on its size
    pub fn should_chunk(&self, data_size: usize) -> bool {
        data_size > MIN_CHUNK_THRESHOLD
    }
    
    /// Stores an object, automatically choosing between direct storage and chunking
    /// 
    /// Returns the metadata of the stored object
    pub fn store_object<T, S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
        object: &Object<T>,
    ) -> StorageResult<ObjectMetadata>
    where
        T: Serialize,
        S: KeyValueStore,
    {
        let serialized_data = bincode::serialize(&object)
            .map_err(|e| StorageError::DatabaseError(format!("Serialization error: {}", e)))?;
        
        if self.should_chunk(serialized_data.len()) {
            self.store_chunked_object(storage, bucket_name, object_key, &serialized_data, &object.metadata)
        } else {
            self.store_direct_object(storage, bucket_name, object_key, &serialized_data, &object.metadata.content_type)
        }
    }
    
    /// Stores an object using chunking strategy
    fn store_chunked_object<S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
        serialized_data: &[u8],
        original_metadata: &ObjectMetadata,
    ) -> StorageResult<ObjectMetadata>
    where
        S: KeyValueStore,
    {
        storage.transaction(|txn| {
            let chunk_count = (serialized_data.len() + self.chunk_size - 1) / self.chunk_size;
            let chunk_count = chunk_count as u64;
            
            if chunk_count > MAX_CHUNKS {
                return Err(StorageError::DatabaseError(
                    format!("Object too large: {} chunks exceeds maximum of {}", chunk_count, MAX_CHUNKS)
                ));
            }
            
            // Calculate checksum
            let checksum = self.calculate_checksum(serialized_data);
            
            // Create and store chunks
            for chunk_index in 0..chunk_count {
                let start = (chunk_index as usize) * self.chunk_size;
                let end = std::cmp::min(start + self.chunk_size, serialized_data.len());
                let chunk_data = &serialized_data[start..end];
                
                let chunk_key = generate_chunk_key(bucket_name, object_key, chunk_index)?;
                txn.put(&chunk_key, chunk_data)?;
            }
            
            // Create manifest
            let manifest = ChunkManifest::new(
                chunk_count,
                self.chunk_size,
                serialized_data.len() as u64,
                checksum,
                original_metadata.content_type.clone(),
            );
            
            // Store manifest as the main object
            let manifest_object = Object {
                metadata: ObjectMetadata::new(
                    serialized_data.len() as u64,
                    "application/x-keystone-chunked-manifest".to_string(),
                ),
                data: manifest,
            };
            
            let manifest_data = bincode::serialize(&manifest_object)
                .map_err(|e| StorageError::DatabaseError(format!("Manifest serialization error: {}", e)))?;
            
            let object_key_bytes = crate::data::keys::generate_object_key(bucket_name, object_key)?;
            txn.put(&object_key_bytes, &manifest_data)?;
            
            Ok(manifest_object.metadata)
        })
    }
    
    /// Stores an object directly without chunking
    fn store_direct_object<S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
        serialized_data: &[u8],
        content_type: &str,
    ) -> StorageResult<ObjectMetadata>
    where
        S: KeyValueStore,
    {
        let object_key_bytes = crate::data::keys::generate_object_key(bucket_name, object_key)?;
        
        storage.put(&object_key_bytes, serialized_data)?;
        
        Ok(ObjectMetadata::new(
            serialized_data.len() as u64,
            content_type.to_string(),
        ))
    }
    
    /// Retrieves an object, handling both chunked and direct storage
    pub fn retrieve_object<T, S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
    ) -> StorageResult<Object<T>>
    where
        T: for<'de> Deserialize<'de>,
        S: KeyValueStore,
    {
        let object_key_bytes = crate::data::keys::generate_object_key(bucket_name, object_key)?;
        
        let data = storage.get(&object_key_bytes)?
            .ok_or(StorageError::KeyNotFound)?;
        
        // Try to deserialize as a regular object first
        if let Ok(object) = bincode::deserialize::<Object<T>>(&data) {
            return Ok(object);
        }
        
        // Try to deserialize as a chunked manifest
        let manifest_object: Object<ChunkManifest> = bincode::deserialize(&data)
            .map_err(|_| StorageError::DatabaseError("Object data corrupted".to_string()))?;
        
        // If it's a chunked manifest, reassemble the object
        if manifest_object.metadata.content_type == "application/x-keystone-chunked-manifest" {
            self.retrieve_chunked_object(storage, bucket_name, object_key, &manifest_object.data)
        } else {
            Err(StorageError::DatabaseError("Unknown object format".to_string()))
        }
    }
    
    /// Retrieves and reassembles a chunked object
    fn retrieve_chunked_object<T, S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
        manifest: &ChunkManifest,
    ) -> StorageResult<Object<T>>
    where
        T: for<'de> Deserialize<'de>,
        S: KeyValueStore,
    {
        manifest.validate()?;
        
        // Reassemble the data from chunks
        if manifest.total_size > usize::MAX as u64 {
            return Err(StorageError::DatabaseError(
                "Object too large to allocate on this platform".to_string(),
            ));
        }
        let mut reassembled_data = Vec::with_capacity(manifest.total_size as usize);
        
        for chunk_index in 0..manifest.chunk_count {
            let chunk_key = generate_chunk_key(bucket_name, object_key, chunk_index)?;
            let chunk_data = storage.get(&chunk_key)?
                .ok_or_else(|| StorageError::DatabaseError(
                    format!("Missing chunk {} for object {}:{}", chunk_index, bucket_name, object_key)
                ))?;
            
            // Verify chunk size
            let expected_size = manifest.chunk_size_for_index(chunk_index);
            if chunk_data.len() != expected_size {
                return Err(StorageError::DatabaseError(
                    format!("Chunk {} has incorrect size: expected {}, got {}", 
                           chunk_index, expected_size, chunk_data.len())
                ));
            }
            
            reassembled_data.extend_from_slice(&chunk_data);
        }
        
        // Verify checksum
        let calculated_checksum = self.calculate_checksum(&reassembled_data);
        if calculated_checksum != manifest.checksum {
            return Err(StorageError::DatabaseError(
                "Object integrity check failed: checksum mismatch".to_string()
            ));
        }
        
        // Deserialize the reassembled data
        bincode::deserialize(&reassembled_data)
            .map_err(|e| StorageError::DatabaseError(format!("Deserialization error: {}", e)))
    }
    
    /// Deletes an object, handling both chunked and direct storage
    pub fn delete_object<S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
    ) -> StorageResult<()>
    where
        S: KeyValueStore,
    {
        let object_key_bytes = crate::data::keys::generate_object_key(bucket_name, object_key)?;
        
        storage.transaction(|txn| {
            // Check if object exists and if it's chunked
            let data = match txn.get(&object_key_bytes)? {
                Some(data) => data,
                None => return Ok(()), // Object doesn't exist, nothing to delete
            };
            
            // Try to parse as chunked manifest
            if let Ok(manifest_object) = bincode::deserialize::<Object<ChunkManifest>>(&data) {
                if manifest_object.metadata.content_type == "application/x-keystone-chunked-manifest" {
                    // Delete all chunks
                    for chunk_index in 0..manifest_object.data.chunk_count {
                        let chunk_key = generate_chunk_key(bucket_name, object_key, chunk_index)?;
                        txn.delete(&chunk_key)?;
                    }
                }
            }
            
            // Delete the main object (either direct object or manifest)
            txn.delete(&object_key_bytes)?;
            
            Ok(())
        })
    }
    
    /// Lists all chunks for a specific object (useful for debugging)
    pub fn list_object_chunks<S>(
        &self,
        storage: &S,
        bucket_name: &str,
        object_key: &str,
    ) -> StorageResult<Vec<(u64, usize)>>
    where
        S: KeyValueStore,
    {
        let chunk_prefix = generate_chunk_prefix(bucket_name, object_key)?;
        let mut chunks = Vec::new();
        
        for result in storage.scan_prefix(&chunk_prefix)? {
            let (key, value) = result?;
            let (_, _, chunk_number) = parse_chunk_key(&key)?;
            chunks.push((chunk_number, value.len()));
        }
        
        // Sort by chunk number
        chunks.sort_by_key(|&(chunk_num, _)| chunk_num);
        
        Ok(chunks)
    }
    
    /// Calculate SHA-256 checksum for data integrity verification
    /// 
    /// Uses cryptographic hash function for robust integrity checking
    /// and tamper detection. Returns first 8 bytes of SHA-256 hash as u64.
    fn calculate_checksum(&self, data: &[u8]) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash_result = hasher.finalize();
        
        // Convert first 8 bytes of SHA-256 to u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash_result[..8]);
        u64::from_be_bytes(bytes)
    }
}

impl Default for ChunkingStrategy {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::mock_storage::MockStorage;
    use std::collections::HashMap;

    #[test]
    fn test_chunk_manifest_creation() {
        let manifest = ChunkManifest::new(3, 1024, 2500, 12345, "text/plain".to_string());
        
        assert_eq!(manifest.chunk_count, 3);
        assert_eq!(manifest.chunk_size, 1024);
        assert_eq!(manifest.total_size, 2500);
        assert_eq!(manifest.checksum, 12345);
        
        manifest.validate().unwrap();
    }
    
    #[test]
    fn test_chunk_manifest_chunk_sizes() {
        let manifest = ChunkManifest::new(3, 1024, 2500, 0, "text/plain".to_string());
        
        // First two chunks should be full size
        assert_eq!(manifest.chunk_size_for_index(0), 1024);
        assert_eq!(manifest.chunk_size_for_index(1), 1024);
        
        // Last chunk should be smaller (2500 - 2*1024 = 452)
        assert_eq!(manifest.chunk_size_for_index(2), 452);
        
        // Out of range should return 0
        assert_eq!(manifest.chunk_size_for_index(3), 0);
    }
    
    #[test]
    fn test_chunk_manifest_validation() {
        // Valid manifest
        let manifest = ChunkManifest::new(2, 1000, 1500, 0, "text/plain".to_string());
        assert!(manifest.validate().is_ok());
        
        // Invalid: zero chunks
        let manifest = ChunkManifest::new(0, 1000, 1500, 0, "text/plain".to_string());
        assert!(manifest.validate().is_err());
        
        // Invalid: too many chunks
        let manifest = ChunkManifest::new(MAX_CHUNKS + 1, 1000, 1000000, 0, "text/plain".to_string());
        assert!(manifest.validate().is_err());
        
        // Invalid: inconsistent size
        let manifest = ChunkManifest::new(2, 1000, 3000, 0, "text/plain".to_string());
        assert!(manifest.validate().is_err());
    }
    
    #[test]
    fn test_chunking_strategy_should_chunk() {
        let strategy = ChunkingStrategy::new();
        
        // Small objects should not be chunked
        assert!(!strategy.should_chunk(1000));
        assert!(!strategy.should_chunk(MIN_CHUNK_THRESHOLD));
        
        // Large objects should be chunked
        assert!(strategy.should_chunk(MIN_CHUNK_THRESHOLD + 1));
        assert!(strategy.should_chunk(10 * 1024 * 1024));
    }
    
    #[test]
    fn test_store_and_retrieve_small_object() {
        let storage = MockStorage::new();
        let strategy = ChunkingStrategy::new();
        
        let data = "Small test object".to_string();
        let original_content_type = "text/plain".to_string();
        let object = Object::with_metadata(data.clone(), original_content_type.clone());
        
        // Store object
        let metadata = strategy.store_object(&storage, "test-bucket", "small-obj", &object).unwrap();
        assert!(metadata.size > 0);
        // Verify that the returned metadata preserves the original content type
        assert_eq!(metadata.content_type, original_content_type, 
                   "Content type should be preserved for small objects");
        
        // Retrieve object
        let retrieved: Object<String> = strategy.retrieve_object(&storage, "test-bucket", "small-obj").unwrap();
        assert_eq!(retrieved.data, data);
        assert_eq!(retrieved.metadata.content_type, original_content_type,
                   "Content type should be preserved during retrieval");
    }
    
    #[test]
    fn test_store_and_retrieve_chunked_object() {
        let storage = MockStorage::new();
        let strategy = ChunkingStrategy::try_with_chunk_size(8192).unwrap(); // 8KB chunks for testing
        
        // Create a large object that will be chunked - needs to be > MIN_CHUNK_THRESHOLD (1MB)
        let large_data = "A".repeat(MIN_CHUNK_THRESHOLD + 10000); // 1MB + 10KB
        let object = Object::with_metadata(large_data.clone(), "text/plain".to_string());
        
        // Verify chunking will happen
        let serialized_size = bincode::serialize(&object).unwrap().len();
        assert!(strategy.should_chunk(serialized_size), "Test object should be chunked. Size: {}, threshold: {}", serialized_size, MIN_CHUNK_THRESHOLD);
        
        // Store object
        let metadata = strategy.store_object(&storage, "test-bucket", "large-obj", &object).unwrap();
        assert!(metadata.size > 0);
        
        // Verify chunks were created
        let chunks = strategy.list_object_chunks(&storage, "test-bucket", "large-obj").unwrap();
        assert!(chunks.len() > 1, "Expected multiple chunks, got {}", chunks.len());
        
        // Retrieve object
        let retrieved: Object<String> = strategy.retrieve_object(&storage, "test-bucket", "large-obj").unwrap();
        assert_eq!(retrieved.data, large_data);
    }
    
    #[test]
    fn test_delete_chunked_object() {
        let storage = MockStorage::new();
        let strategy = ChunkingStrategy::try_with_chunk_size(8192).unwrap(); // 8KB chunks
        
        // Create a large object that will be chunked - needs to be > MIN_CHUNK_THRESHOLD (1MB)
        let large_data = "B".repeat(MIN_CHUNK_THRESHOLD + 20000); // 1MB + 20KB 
        let object = Object::with_metadata(large_data, "text/plain".to_string());
        
        // Verify this will be chunked
        let serialized_size = bincode::serialize(&object).unwrap().len();
        assert!(strategy.should_chunk(serialized_size), "Test object should be chunked. Size: {}, threshold: {}", serialized_size, MIN_CHUNK_THRESHOLD);
        
        // Store object
        strategy.store_object(&storage, "test-bucket", "delete-test", &object).unwrap();
        
        // Verify object and chunks exist
        let chunks_before = strategy.list_object_chunks(&storage, "test-bucket", "delete-test").unwrap();
        assert!(!chunks_before.is_empty(), "Expected chunks to exist before deletion");
        
        // Delete object
        strategy.delete_object(&storage, "test-bucket", "delete-test").unwrap();
        
        // Verify object and all chunks are gone
        let result: Result<Object<String>, _> = strategy.retrieve_object(&storage, "test-bucket", "delete-test");
        assert!(matches!(result, Err(StorageError::KeyNotFound)));
        
        let chunks_after = strategy.list_object_chunks(&storage, "test-bucket", "delete-test").unwrap();
        assert!(chunks_after.is_empty());
    }
    
    #[test]
    fn test_checksum_calculation() {
        let strategy = ChunkingStrategy::new();
        
        let data1 = b"test data";
        let data2 = b"test data";
        let data3 = b"different data";
        
        let checksum1 = strategy.calculate_checksum(data1);
        let checksum2 = strategy.calculate_checksum(data2);
        let checksum3 = strategy.calculate_checksum(data3);
        
        // Same data should produce same checksum
        assert_eq!(checksum1, checksum2);
        
        // Different data should produce different checksums (with high probability)
        assert_ne!(checksum1, checksum3);
    }
    
    #[test]
    fn test_sha256_checksum_properties() {
        let strategy = ChunkingStrategy::new();
        
        // Test that we're using SHA-256 by verifying properties
        let data = b"test data for SHA-256 verification";
        let checksum = strategy.calculate_checksum(data);
        
        // Verify the checksum is deterministic
        let checksum2 = strategy.calculate_checksum(data);
        assert_eq!(checksum, checksum2);
        
        // Verify small changes produce completely different checksums (avalanche effect)
        let similar_data = b"test data for SHA-256 verificatioN"; // Changed last 'n' to 'N'
        let similar_checksum = strategy.calculate_checksum(similar_data);
        assert_ne!(checksum, similar_checksum);
        
        // Verify empty data produces a consistent checksum
        let empty_checksum = strategy.calculate_checksum(b"");
        let empty_checksum2 = strategy.calculate_checksum(b"");
        assert_eq!(empty_checksum, empty_checksum2);
        
        // Verify the checksum is not zero (SHA-256 should never produce all zeros for any input)
        assert_ne!(checksum, 0);
        assert_ne!(empty_checksum, 0);
    }
    
    #[test]
    fn test_corrupted_chunk_detection() {
        let storage = MockStorage::new();
        let strategy = ChunkingStrategy::try_with_chunk_size(8192).unwrap(); // 8KB chunks
        
        // Create a large object that will be chunked - needs to be > MIN_CHUNK_THRESHOLD (1MB)
        let large_data = "C".repeat(MIN_CHUNK_THRESHOLD + 15000); // 1MB + 15KB
        let object = Object::with_metadata(large_data, "text/plain".to_string());
        
        // Verify this will be chunked
        let serialized_size = bincode::serialize(&object).unwrap().len();
        assert!(strategy.should_chunk(serialized_size), "Test object should be chunked. Size: {}, threshold: {}", serialized_size, MIN_CHUNK_THRESHOLD);
        
        // Store object
        strategy.store_object(&storage, "test-bucket", "corrupt-test", &object).unwrap();
        
        // Manually corrupt a chunk
        let chunk_key = generate_chunk_key("test-bucket", "corrupt-test", 1).unwrap();
        storage.put(&chunk_key, b"corrupted data").unwrap();
        
        // Attempt to retrieve should fail with integrity error
        let result: Result<Object<String>, _> = strategy.retrieve_object(&storage, "test-bucket", "corrupt-test");
        assert!(matches!(result, Err(StorageError::DatabaseError(_))), "Expected DatabaseError, got {:?}", result);
    }
    
    #[test]
    fn test_complex_data_types_chunking() {
        let storage = MockStorage::new();
        let strategy = ChunkingStrategy::try_with_chunk_size(100).unwrap();
        
        // Create complex data structure
        let mut data = HashMap::new();
        for i in 0..50 {
            data.insert(format!("key_{}", i), vec![i; 20]);
        }
        
        let object = Object::with_metadata(data.clone(), "application/json".to_string());
        
        // Store and retrieve
        strategy.store_object(&storage, "test-bucket", "complex-obj", &object).unwrap();
        let retrieved: Object<HashMap<String, Vec<usize>>> = 
            strategy.retrieve_object(&storage, "test-bucket", "complex-obj").unwrap();
        
        assert_eq!(retrieved.data, data);
    }
    
    #[test]
    fn test_content_type_preservation_small_objects() {
        let storage = MockStorage::new();
        let strategy = ChunkingStrategy::new();
        
        // Test various content types for small objects
        let test_cases = vec![
            ("application/json", "{}"),
            ("image/png", "PNG binary data"),
            ("text/html", "<html></html>"),
            ("application/pdf", "PDF binary data"),
            ("video/mp4", "MP4 binary data"),
        ];
        
        for (content_type, data) in test_cases {
            let object = Object::with_metadata(data.to_string(), content_type.to_string());
            
            // Store object
            let metadata = strategy.store_object(&storage, "test-bucket", &format!("obj-{}", content_type.replace('/', "-")), &object).unwrap();
            
            // Verify content type is preserved in returned metadata
            assert_eq!(metadata.content_type, content_type, 
                      "Content type {} should be preserved for small objects", content_type);
            
            // Retrieve and verify
            let retrieved: Object<String> = strategy.retrieve_object(&storage, "test-bucket", &format!("obj-{}", content_type.replace('/', "-"))).unwrap();
            assert_eq!(retrieved.metadata.content_type, content_type,
                      "Content type {} should be preserved during retrieval", content_type);
            assert_eq!(retrieved.data, data);
        }
    }
    
    #[test]
    fn test_chunk_size_overflow_protection() {
        // Test that chunk_size_for_index safely handles u64 to usize overflow
        // This is especially important on 32-bit platforms where usize::MAX is ~4GB
        
        // Test normal case first (should work on all platforms)
        let normal_manifest = ChunkManifest::new(3, 1024, 2500, 0, "text/plain".to_string());
        assert_eq!(normal_manifest.chunk_size_for_index(0), 1024);  // First chunk
        assert_eq!(normal_manifest.chunk_size_for_index(1), 1024);  // Middle chunk  
        assert_eq!(normal_manifest.chunk_size_for_index(2), 452);   // Last chunk (2500 - 2*1024 = 452)
        assert_eq!(normal_manifest.chunk_size_for_index(3), 0);     // Out of bounds
        
        // Test the overflow protection by creating a scenario that would definitely overflow
        // We'll simulate a 32-bit environment by using a smaller "artificial max" value
        const ARTIFICIAL_MAX_USIZE: u64 = 1024 * 1024; // 1MB as our "max usize" for testing
        
        // Test case where remainder would exceed our artificial limit
        let manifest_overflow = ChunkManifest::new(
            2,
            1024,
            1024 + ARTIFICIAL_MAX_USIZE + 1,  // remainder = ARTIFICIAL_MAX_USIZE + 1
            0,
            "text/plain".to_string()
        );
        assert_eq!(manifest_overflow.chunk_size_for_index(0), 1024);
        
        // The actual result depends on the platform:
        // - On 64-bit: usize is large enough, so should return the actual remainder
        // - On 32-bit: would return 0 due to overflow protection
        let remainder = 1024 + ARTIFICIAL_MAX_USIZE + 1 - 1024; // ARTIFICIAL_MAX_USIZE + 1
        let result = manifest_overflow.chunk_size_for_index(1);
        
        // The key thing is that it doesn't panic - either returns the value or 0
        assert!(result == 0 || result == remainder as usize, 
                "Expected either 0 (overflow protection) or {} (valid conversion), got {}", 
                remainder, result);
        
        // Test extremely large case that would definitely overflow on any platform  
        // where the remainder calculation itself would be huge
        let manifest_huge = ChunkManifest::new(
            2,
            1024,
            u64::MAX,  // Maximum possible total size
            0,
            "text/plain".to_string()
        );
        assert_eq!(manifest_huge.chunk_size_for_index(0), 1024);  // First chunk normal
        
        // The remainder would be u64::MAX - 1024, which on 64-bit platforms fits in usize
        // but on 32-bit would overflow. Our code should handle both cases gracefully.
        let huge_result = manifest_huge.chunk_size_for_index(1);
        let expected_remainder = u64::MAX - 1024;
        
        // Either it fits in usize and returns the value, or it overflows and returns 0
        assert!(huge_result == 0 || huge_result == expected_remainder as usize,
                "Expected either 0 (overflow protection) or {} (valid conversion), got {}", 
                expected_remainder, huge_result);
        
        // Test saturating_sub protection (total_size < full_chunks_size edge case)
        let manifest_underflow = ChunkManifest::new(
            3,
            1000,
            1500,  // 1500 < 3*1000, but saturating_sub prevents underflow
            0,
            "text/plain".to_string()
        );
        assert_eq!(manifest_underflow.chunk_size_for_index(0), 1000);  // First chunk
        assert_eq!(manifest_underflow.chunk_size_for_index(1), 1000);  // Second chunk
        assert_eq!(manifest_underflow.chunk_size_for_index(2), 0);     // Last chunk (saturating_sub returns 0)
    }
    
    #[test]
    fn test_chunk_size_edge_cases() {
        // Test various edge cases for chunk size calculation
        
        // Single chunk case
        let single_chunk = ChunkManifest::new(1, 2048, 1000, 0, "text/plain".to_string());
        assert_eq!(single_chunk.chunk_size_for_index(0), 1000);  // Last (only) chunk is smaller than chunk_size
        
        // Exact multiple case (no partial last chunk)
        let exact_multiple = ChunkManifest::new(3, 1000, 3000, 0, "text/plain".to_string());
        assert_eq!(exact_multiple.chunk_size_for_index(0), 1000);  // First chunk
        assert_eq!(exact_multiple.chunk_size_for_index(1), 1000);  // Second chunk
        assert_eq!(exact_multiple.chunk_size_for_index(2), 1000);  // Last chunk (exact fit)
        
        // Very small last chunk
        let tiny_last = ChunkManifest::new(3, 1000, 2001, 0, "text/plain".to_string());
        assert_eq!(tiny_last.chunk_size_for_index(0), 1000);  // First chunk
        assert_eq!(tiny_last.chunk_size_for_index(1), 1000);  // Second chunk
        assert_eq!(tiny_last.chunk_size_for_index(2), 1);     // Last chunk (1 byte)
        
        // Zero-sized object edge case
        let zero_sized = ChunkManifest::new(1, 1000, 0, 0, "text/plain".to_string());
        assert_eq!(zero_sized.chunk_size_for_index(0), 0);  // Even first chunk is 0 for zero-sized object
    }
}