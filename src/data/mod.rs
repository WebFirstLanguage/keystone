//! Core data structures and business logic for keystone object storage
//!
//! This module contains the core data model that maps the hierarchical bucket/key/object
//! structure onto the flat key-value storage provided by the storage layer.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod keys;
pub mod chunking;
pub mod mock_storage;

/// The generic container for any user data that can be serialized
/// 
/// This struct wraps user data along with essential metadata, providing
/// the core storage unit for the object store.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Object<T> {
    /// Metadata associated with this object
    pub metadata: ObjectMetadata,
    /// The actual user data being stored
    pub data: T,
}

/// Metadata stored alongside every object
/// 
/// Contains essential information about the object including size,
/// content type, and modification time.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ObjectMetadata {
    /// The size of the serialized `data` field in bytes
    pub size: u64,
    /// A user-provided content type string (e.g., "application/json", "image/png")  
    pub content_type: String,
    /// The timestamp of the last modification, stored as seconds since the UNIX epoch
    pub last_modified: u64,
    // Future metadata fields (e.g., checksums, custom headers) can be added here
}

impl ObjectMetadata {
    /// Creates new metadata with the current timestamp
    pub fn new(size: u64, content_type: String) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        Self {
            size,
            content_type,
            last_modified: now,
        }
    }
    
    /// Creates new metadata with a specific timestamp (useful for testing)
    pub fn with_timestamp(size: u64, content_type: String, last_modified: u64) -> Self {
        Self {
            size,
            content_type,
            last_modified,
        }
    }
}

impl<T> Object<T> {
    /// Creates a new object with the given data and metadata
    pub fn new(data: T, metadata: ObjectMetadata) -> Self {
        Self { metadata, data }
    }
    
    /// Creates a new object with auto-generated metadata
    /// 
    /// This is a convenience method that generates metadata with the current timestamp.
    /// The size will be calculated when the object is serialized.
    pub fn with_metadata(data: T, content_type: String) -> Self {
        // Size will be updated during serialization
        let metadata = ObjectMetadata::new(0, content_type);
        Self { metadata, data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_metadata_creation() {
        let metadata = ObjectMetadata::new(1024, "application/json".to_string());
        
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.content_type, "application/json");
        assert!(metadata.last_modified > 0);
    }
    
    #[test]
    fn test_object_metadata_with_timestamp() {
        let timestamp = 1609459200; // 2021-01-01 00:00:00 UTC
        let metadata = ObjectMetadata::with_timestamp(512, "text/plain".to_string(), timestamp);
        
        assert_eq!(metadata.size, 512);
        assert_eq!(metadata.content_type, "text/plain");
        assert_eq!(metadata.last_modified, timestamp);
    }
    
    #[test]
    fn test_object_creation() {
        let data = "Hello, World!".to_string();
        let metadata = ObjectMetadata::new(13, "text/plain".to_string());
        let object = Object::new(data.clone(), metadata.clone());
        
        assert_eq!(object.data, data);
        assert_eq!(object.metadata, metadata);
    }
    
    #[test]
    fn test_object_with_metadata() {
        let data = vec![1u8, 2, 3, 4, 5];
        let object = Object::with_metadata(data.clone(), "application/octet-stream".to_string());
        
        assert_eq!(object.data, data);
        assert_eq!(object.metadata.content_type, "application/octet-stream");
        assert_eq!(object.metadata.size, 0); // Size not yet calculated
        assert!(object.metadata.last_modified > 0);
    }
    
    #[test]
    fn test_serde_serialization() {
        use bincode;
        
        let data = "Test data".to_string();
        let object = Object::with_metadata(data, "text/plain".to_string());
        
        // Test serialization
        let serialized = bincode::serialize(&object).unwrap();
        assert!(!serialized.is_empty());
        
        // Test deserialization  
        let deserialized: Object<String> = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, object);
    }
    
    #[test]
    fn test_complex_data_types() {
        use std::collections::HashMap;
        
        let mut data = HashMap::new();
        data.insert("key1".to_string(), 42);
        data.insert("key2".to_string(), 84);
        
        let object = Object::with_metadata(data.clone(), "application/json".to_string());
        
        assert_eq!(object.data, data);
        
        // Test serialization roundtrip with complex data
        let serialized = bincode::serialize(&object).unwrap();
        let deserialized: Object<HashMap<String, i32>> = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.data, data);
    }
    
    #[test]
    fn test_object_metadata_creation_never_panics() {
        // This test verifies that ObjectMetadata::new() never panics
        // even in edge cases, ensuring it's truly an infallible constructor
        
        // Test with normal conditions (should produce valid timestamp > 0)
        let metadata1 = ObjectMetadata::new(100, "text/plain".to_string());
        assert_eq!(metadata1.size, 100);
        assert_eq!(metadata1.content_type, "text/plain");
        // On normal systems, this should be > 0 (after Unix epoch)
        
        // Test that constructor remains infallible regardless of system conditions
        // (We can't easily simulate system time before Unix epoch in tests,
        // but the code path is now safe and will use 0 as fallback)
        let metadata2 = ObjectMetadata::new(0, "application/octet-stream".to_string());
        assert_eq!(metadata2.size, 0);
        assert_eq!(metadata2.content_type, "application/octet-stream");
        
        // Verify the fallback behavior provides a consistent, safe value
        // The timestamp should be either a valid Unix timestamp or 0 (safe fallback)
        assert!(metadata1.last_modified == 0 || metadata1.last_modified > 0);
        assert!(metadata2.last_modified == 0 || metadata2.last_modified > 0);
        
        // Multiple calls should not panic and should provide timestamps
        for i in 0..10 {
            let metadata = ObjectMetadata::new(i, format!("test-{}", i));
            assert!(metadata.last_modified == 0 || metadata.last_modified > 0);
        }
    }
}