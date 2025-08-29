//! Key generation and parsing logic for keystone object storage
//!
//! This module handles the mapping between the hierarchical bucket/key/object model 
//! and the flat key space used by the underlying key-value store.

use crate::storage::StorageError;

/// The null byte separator used in composite keys
const SEPARATOR: u8 = 0;

/// Maximum length for bucket names (in bytes) - prevents memory exhaustion
pub const MAX_BUCKET_NAME_LENGTH: usize = 1024; // 1KB

/// Maximum length for object keys (in bytes) - prevents memory exhaustion  
pub const MAX_OBJECT_KEY_LENGTH: usize = 1024; // 1KB

/// Prefix for bucket metadata keys
const BUCKET_METADATA_PREFIX: &[u8] = b"__meta__";
const BUCKET_SECTION: &[u8] = b"bucket";

/// Prefix for large object chunk data keys  
const CHUNK_DATA_PREFIX: &[u8] = b"__data__";

/// Generates a composite key for storing an object
/// 
/// Format: `<bucket_name>\0<object_key>`
/// 
/// # Arguments
/// * `bucket_name` - The bucket containing the object
/// * `object_key` - The key identifying the object within the bucket
/// 
/// # Returns
/// A byte vector representing the composite key
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if bucket_name or object_key contain null bytes
pub fn generate_object_key(bucket_name: &str, object_key: &str) -> Result<Vec<u8>, StorageError> {
    validate_user_input(bucket_name, "bucket name")?;
    validate_user_input(object_key, "object key")?;
    
    let mut key = Vec::with_capacity(bucket_name.len() + 1 + object_key.len());
    key.extend_from_slice(bucket_name.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(object_key.as_bytes());
    
    Ok(key)
}

/// Generates a key for storing bucket metadata
/// 
/// Format: `__meta__\0bucket\0<bucket_name>`
/// 
/// # Arguments  
/// * `bucket_name` - The name of the bucket
/// 
/// # Returns
/// A byte vector representing the bucket metadata key
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if bucket_name contains null bytes
pub fn generate_bucket_metadata_key(bucket_name: &str) -> Result<Vec<u8>, StorageError> {
    validate_user_input(bucket_name, "bucket name")?;
    
    let mut key = Vec::with_capacity(
        BUCKET_METADATA_PREFIX.len() + 1 + BUCKET_SECTION.len() + 1 + bucket_name.len()
    );
    
    key.extend_from_slice(BUCKET_METADATA_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(BUCKET_SECTION);
    key.push(SEPARATOR);
    key.extend_from_slice(bucket_name.as_bytes());
    
    Ok(key)
}

/// Generates a key for storing a chunk of a large object
/// 
/// Format: `__data__\0<bucket_name>\0<object_key>\0<chunk_number>`
/// 
/// # Arguments
/// * `bucket_name` - The bucket containing the object
/// * `object_key` - The key identifying the object within the bucket  
/// * `chunk_number` - The sequential chunk number (0-based)
/// 
/// # Returns
/// A byte vector representing the chunk data key
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if bucket_name or object_key contain null bytes
pub fn generate_chunk_key(
    bucket_name: &str, 
    object_key: &str, 
    chunk_number: u64
) -> Result<Vec<u8>, StorageError> {
    validate_user_input(bucket_name, "bucket name")?;
    validate_user_input(object_key, "object key")?;
    
    let chunk_str = chunk_number.to_string();
    let mut key = Vec::with_capacity(
        CHUNK_DATA_PREFIX.len() + 1 + 
        bucket_name.len() + 1 + 
        object_key.len() + 1 + 
        chunk_str.len()
    );
    
    key.extend_from_slice(CHUNK_DATA_PREFIX);
    key.push(SEPARATOR);
    key.extend_from_slice(bucket_name.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(object_key.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(chunk_str.as_bytes());
    
    Ok(key)
}

/// Generates a prefix for scanning all objects in a bucket
/// 
/// Format: `<bucket_name>\0`
/// 
/// # Arguments
/// * `bucket_name` - The bucket to scan
/// 
/// # Returns
/// A byte vector representing the bucket prefix
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if bucket_name contains null bytes
pub fn generate_bucket_prefix(bucket_name: &str) -> Result<Vec<u8>, StorageError> {
    validate_user_input(bucket_name, "bucket name")?;
    
    let mut prefix = Vec::with_capacity(bucket_name.len() + 1);
    prefix.extend_from_slice(bucket_name.as_bytes());
    prefix.push(SEPARATOR);
    
    Ok(prefix)
}

/// Generates a prefix for scanning object keys within a bucket that start with a given prefix
/// 
/// Format: `<bucket_name>\0<key_prefix>`
/// 
/// # Arguments
/// * `bucket_name` - The bucket to scan
/// * `key_prefix` - The prefix to match object keys against
/// 
/// # Returns  
/// A byte vector representing the composite prefix
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if bucket_name or key_prefix contain null bytes
pub fn generate_object_prefix(bucket_name: &str, key_prefix: &str) -> Result<Vec<u8>, StorageError> {
    validate_user_input(bucket_name, "bucket name")?;
    validate_user_input(key_prefix, "key prefix")?;
    
    let mut prefix = Vec::with_capacity(bucket_name.len() + 1 + key_prefix.len());
    prefix.extend_from_slice(bucket_name.as_bytes());
    prefix.push(SEPARATOR);
    prefix.extend_from_slice(key_prefix.as_bytes());
    
    Ok(prefix)
}

/// Generates a prefix for scanning all chunks of a specific object
/// 
/// Format: `__data__\0<bucket_name>\0<object_key>\0`
/// 
/// # Arguments
/// * `bucket_name` - The bucket containing the object
/// * `object_key` - The key identifying the object
/// 
/// # Returns
/// A byte vector representing the chunk prefix
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if bucket_name or object_key contain null bytes
pub fn generate_chunk_prefix(bucket_name: &str, object_key: &str) -> Result<Vec<u8>, StorageError> {
    validate_user_input(bucket_name, "bucket name")?;
    validate_user_input(object_key, "object key")?;
    
    let mut prefix = Vec::with_capacity(
        CHUNK_DATA_PREFIX.len() + 1 + 
        bucket_name.len() + 1 + 
        object_key.len() + 1
    );
    
    prefix.extend_from_slice(CHUNK_DATA_PREFIX);
    prefix.push(SEPARATOR);
    prefix.extend_from_slice(bucket_name.as_bytes());
    prefix.push(SEPARATOR);
    prefix.extend_from_slice(object_key.as_bytes());
    prefix.push(SEPARATOR);
    
    Ok(prefix)
}

/// Parses an object key to extract the bucket name and object key
/// 
/// # Arguments
/// * `composite_key` - The composite key bytes to parse
/// 
/// # Returns
/// A tuple of (bucket_name, object_key) if parsing succeeds
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if the key format is invalid
pub fn parse_object_key(composite_key: &[u8]) -> Result<(String, String), StorageError> {
    if let Some(separator_pos) = composite_key.iter().position(|&b| b == SEPARATOR) {
        let bucket_bytes = &composite_key[..separator_pos];
        let object_bytes = &composite_key[separator_pos + 1..];
        
        let bucket_name = String::from_utf8(bucket_bytes.to_vec())
            .map_err(|_| StorageError::InvalidKey)?;
        let object_key = String::from_utf8(object_bytes.to_vec())
            .map_err(|_| StorageError::InvalidKey)?;
            
        Ok((bucket_name, object_key))
    } else {
        Err(StorageError::InvalidKey)
    }
}

/// Parses a bucket metadata key to extract the bucket name
/// 
/// # Arguments
/// * `metadata_key` - The metadata key bytes to parse
/// 
/// # Returns
/// The bucket name if parsing succeeds
/// 
/// # Errors
/// Returns `StorageError::InvalidKey` if the key format is invalid
pub fn parse_bucket_metadata_key(metadata_key: &[u8]) -> Result<String, StorageError> {
    let expected_prefix_len = BUCKET_METADATA_PREFIX.len() + 1 + BUCKET_SECTION.len() + 1;
    
    if metadata_key.len() <= expected_prefix_len {
        return Err(StorageError::InvalidKey);
    }
    
    // Verify the prefix matches
    if !metadata_key.starts_with(BUCKET_METADATA_PREFIX) {
        return Err(StorageError::InvalidKey);
    }
    
    let remaining = &metadata_key[BUCKET_METADATA_PREFIX.len()..];
    if remaining[0] != SEPARATOR {
        return Err(StorageError::InvalidKey);
    }
    
    let remaining = &remaining[1..];
    if !remaining.starts_with(BUCKET_SECTION) {
        return Err(StorageError::InvalidKey);
    }
    
    let remaining = &remaining[BUCKET_SECTION.len()..];
    if remaining[0] != SEPARATOR {
        return Err(StorageError::InvalidKey);
    }
    
    let bucket_bytes = &remaining[1..];
    String::from_utf8(bucket_bytes.to_vec())
        .map_err(|_| StorageError::InvalidKey)
}

/// Parses a chunk key to extract bucket name, object key, and chunk number
/// 
/// # Arguments
/// * `chunk_key` - The chunk key bytes to parse
/// 
/// # Returns
/// A tuple of (bucket_name, object_key, chunk_number) if parsing succeeds
/// 
/// # Errors  
/// Returns `StorageError::InvalidKey` if the key format is invalid
pub fn parse_chunk_key(chunk_key: &[u8]) -> Result<(String, String, u64), StorageError> {
    if !chunk_key.starts_with(CHUNK_DATA_PREFIX) {
        return Err(StorageError::InvalidKey);
    }
    
    let remaining = &chunk_key[CHUNK_DATA_PREFIX.len()..];
    if remaining.is_empty() || remaining[0] != SEPARATOR {
        return Err(StorageError::InvalidKey);
    }
    
    let remaining = &remaining[1..];
    let parts: Vec<&[u8]> = remaining.split(|&b| b == SEPARATOR).collect();
    
    if parts.len() != 3 {
        return Err(StorageError::InvalidKey);
    }
    
    let bucket_name = String::from_utf8(parts[0].to_vec())
        .map_err(|_| StorageError::InvalidKey)?;
    let object_key = String::from_utf8(parts[1].to_vec())
        .map_err(|_| StorageError::InvalidKey)?;
    let chunk_number_str = String::from_utf8(parts[2].to_vec())
        .map_err(|_| StorageError::InvalidKey)?;
    let chunk_number = chunk_number_str.parse::<u64>()
        .map_err(|_| StorageError::InvalidKey)?;
    
    Ok((bucket_name, object_key, chunk_number))
}

/// Validates user input for bucket names and object keys
/// 
/// Checks for null bytes (which would break key parsing) and enforces length limits
/// to prevent memory exhaustion attacks.
fn validate_user_input(input: &str, field_name: &str) -> Result<(), StorageError> {
    if input.contains('\0') {
        return Err(StorageError::InvalidKey);
    }
    
    let max_length = match field_name {
        "bucket name" => MAX_BUCKET_NAME_LENGTH,
        "object key" | "key prefix" => MAX_OBJECT_KEY_LENGTH,
        _ => MAX_OBJECT_KEY_LENGTH, // Default to object key limit
    };
    
    if input.len() > max_length {
        return Err(StorageError::InvalidKey);
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_object_key() {
        let key = generate_object_key("users", "user-123").unwrap();
        assert_eq!(key, b"users\0user-123");
    }
    
    #[test]
    fn test_generate_object_key_with_null_bytes() {
        let result = generate_object_key("users\0bad", "key");
        assert!(matches!(result, Err(StorageError::InvalidKey)));
        
        let result = generate_object_key("users", "key\0bad");
        assert!(matches!(result, Err(StorageError::InvalidKey)));
    }
    
    #[test]
    fn test_generate_bucket_metadata_key() {
        let key = generate_bucket_metadata_key("users").unwrap();
        assert_eq!(key, b"__meta__\0bucket\0users");
    }
    
    #[test]
    fn test_generate_chunk_key() {
        let key = generate_chunk_key("media", "video.mp4", 5).unwrap();
        assert_eq!(key, b"__data__\0media\0video.mp4\05");
    }
    
    #[test]
    fn test_generate_bucket_prefix() {
        let prefix = generate_bucket_prefix("users").unwrap();
        assert_eq!(prefix, b"users\0");
    }
    
    #[test]
    fn test_generate_object_prefix() {
        let prefix = generate_object_prefix("logs", "2024-01").unwrap();
        assert_eq!(prefix, b"logs\02024-01");
    }
    
    #[test]
    fn test_generate_chunk_prefix() {
        let prefix = generate_chunk_prefix("media", "video.mp4").unwrap();
        assert_eq!(prefix, b"__data__\0media\0video.mp4\0");
    }
    
    #[test]
    fn test_parse_object_key() {
        let composite_key = b"users\0user-123";
        let (bucket, key) = parse_object_key(composite_key).unwrap();
        assert_eq!(bucket, "users");
        assert_eq!(key, "user-123");
    }
    
    #[test]
    fn test_parse_object_key_invalid() {
        let composite_key = b"no-separator";
        let result = parse_object_key(composite_key);
        assert!(matches!(result, Err(StorageError::InvalidKey)));
    }
    
    #[test]
    fn test_parse_bucket_metadata_key() {
        let metadata_key = b"__meta__\0bucket\0users";
        let bucket = parse_bucket_metadata_key(metadata_key).unwrap();
        assert_eq!(bucket, "users");
    }
    
    #[test]
    fn test_parse_bucket_metadata_key_invalid() {
        let metadata_key = b"wrong-prefix\0bucket\0users";
        let result = parse_bucket_metadata_key(metadata_key);
        assert!(matches!(result, Err(StorageError::InvalidKey)));
    }
    
    #[test]
    fn test_parse_chunk_key() {
        let chunk_key = b"__data__\0media\0video.mp4\05";
        let (bucket, key, chunk_num) = parse_chunk_key(chunk_key).unwrap();
        assert_eq!(bucket, "media");
        assert_eq!(key, "video.mp4");
        assert_eq!(chunk_num, 5);
    }
    
    #[test]
    fn test_parse_chunk_key_invalid() {
        let chunk_key = b"__data__\0media\0video.mp4"; // Missing chunk number
        let result = parse_chunk_key(chunk_key);
        assert!(matches!(result, Err(StorageError::InvalidKey)));
    }
    
    #[test]
    fn test_key_generation_roundtrip() {
        // Test object key roundtrip
        let bucket = "test-bucket";
        let object = "test-object";
        let key = generate_object_key(bucket, object).unwrap();
        let (parsed_bucket, parsed_object) = parse_object_key(&key).unwrap();
        assert_eq!(parsed_bucket, bucket);
        assert_eq!(parsed_object, object);
        
        // Test bucket metadata key roundtrip
        let metadata_key = generate_bucket_metadata_key(bucket).unwrap();
        let parsed_bucket = parse_bucket_metadata_key(&metadata_key).unwrap();
        assert_eq!(parsed_bucket, bucket);
        
        // Test chunk key roundtrip
        let chunk_num = 42;
        let chunk_key = generate_chunk_key(bucket, object, chunk_num).unwrap();
        let (parsed_bucket, parsed_object, parsed_chunk) = parse_chunk_key(&chunk_key).unwrap();
        assert_eq!(parsed_bucket, bucket);
        assert_eq!(parsed_object, object);
        assert_eq!(parsed_chunk, chunk_num);
    }

    #[test]
    fn test_key_length_validation() {
        // Test bucket name length validation
        let max_bucket = "a".repeat(MAX_BUCKET_NAME_LENGTH);
        let too_long_bucket = "a".repeat(MAX_BUCKET_NAME_LENGTH + 1);
        
        // Valid bucket name should work
        assert!(generate_object_key(&max_bucket, "test").is_ok());
        
        // Too long bucket name should fail
        assert!(generate_object_key(&too_long_bucket, "test").is_err());
        
        // Test object key length validation  
        let max_object_key = "b".repeat(MAX_OBJECT_KEY_LENGTH);
        let too_long_object_key = "b".repeat(MAX_OBJECT_KEY_LENGTH + 1);
        
        // Valid object key should work
        assert!(generate_object_key("test", &max_object_key).is_ok());
        
        // Too long object key should fail
        assert!(generate_object_key("test", &too_long_object_key).is_err());
        
        // Test that both bucket and object key validation work together
        assert!(generate_object_key(&too_long_bucket, &too_long_object_key).is_err());
    }
    
    #[test] 
    fn test_key_length_validation_edge_cases() {
        // Test empty strings (should be valid)
        assert!(generate_object_key("", "").is_ok());
        
        // Test single character (should be valid)
        assert!(generate_object_key("a", "b").is_ok());
        
        // Test exactly at the limit
        let exactly_max_bucket = "x".repeat(MAX_BUCKET_NAME_LENGTH);
        let exactly_max_object = "y".repeat(MAX_OBJECT_KEY_LENGTH);
        assert!(generate_object_key(&exactly_max_bucket, &exactly_max_object).is_ok());
        
        // Test one over the limit  
        let one_over_bucket = "x".repeat(MAX_BUCKET_NAME_LENGTH + 1);
        let one_over_object = "y".repeat(MAX_OBJECT_KEY_LENGTH + 1);
        assert!(generate_object_key(&one_over_bucket, "test").is_err());
        assert!(generate_object_key("test", &one_over_object).is_err());
    }
    
    #[test]
    fn test_bucket_metadata_key_length_validation() {
        // Test bucket metadata key generation with length limits
        let max_bucket = "z".repeat(MAX_BUCKET_NAME_LENGTH);
        let too_long_bucket = "z".repeat(MAX_BUCKET_NAME_LENGTH + 1);
        
        // Valid bucket name should work
        assert!(generate_bucket_metadata_key(&max_bucket).is_ok());
        
        // Too long bucket name should fail
        assert!(generate_bucket_metadata_key(&too_long_bucket).is_err());
    }
    
    #[test]
    fn test_chunk_key_length_validation() {
        // Test chunk key generation with length limits
        let max_bucket = "a".repeat(MAX_BUCKET_NAME_LENGTH);
        let max_object = "b".repeat(MAX_OBJECT_KEY_LENGTH);
        let too_long_bucket = "a".repeat(MAX_BUCKET_NAME_LENGTH + 1);
        let too_long_object = "b".repeat(MAX_OBJECT_KEY_LENGTH + 1);
        
        // Valid keys should work
        assert!(generate_chunk_key(&max_bucket, &max_object, 0).is_ok());
        
        // Too long keys should fail
        assert!(generate_chunk_key(&too_long_bucket, "test", 0).is_err());
        assert!(generate_chunk_key("test", &too_long_object, 0).is_err());
    }
    
    #[test]
    fn test_empty_inputs() {
        let result = generate_object_key("", "key");
        assert!(result.is_ok()); // Empty bucket name is technically valid
        
        let result = generate_object_key("bucket", "");
        assert!(result.is_ok()); // Empty object key is technically valid
    }
    
    #[test]
    fn test_unicode_inputs() {
        let bucket = "测试桶";
        let object = "тест-файл.txt";
        let key = generate_object_key(bucket, object).unwrap();
        let (parsed_bucket, parsed_object) = parse_object_key(&key).unwrap();
        assert_eq!(parsed_bucket, bucket);
        assert_eq!(parsed_object, object);
    }
}