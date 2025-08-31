//! Utility functions for generating and parsing keys used in the object store

/// Generate the key for storing an object in a bucket using the format:
/// `<bucket_name>\0<object_key>`
pub fn object_key(bucket: &str, object: &str) -> Vec<u8> {
    let mut key = bucket.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(object.as_bytes());
    key
}

/// Parse an object key into its bucket and object components
pub fn parse_object_key(key: &[u8]) -> Option<(String, String)> {
    let parts: Vec<&[u8]> = key.split(|b| *b == 0).collect();
    if parts.len() != 2 {
        return None;
    }
    Some((
        String::from_utf8(parts[0].to_vec()).ok()?,
        String::from_utf8(parts[1].to_vec()).ok()?,
    ))
}

/// Generate the key for bucket metadata using the format:
/// `__meta__\0bucket\0<bucket_name>`
pub fn bucket_metadata_key(bucket: &str) -> Vec<u8> {
    let mut key = b"__meta__\0bucket\0".to_vec();
    key.extend_from_slice(bucket.as_bytes());
    key
}

/// Parse a bucket metadata key returning the bucket name
pub fn parse_bucket_metadata_key(key: &[u8]) -> Option<String> {
    let expected_prefix = b"__meta__\0bucket\0";
    if !key.starts_with(expected_prefix) {
        return None;
    }
    let name = &key[expected_prefix.len()..];
    String::from_utf8(name.to_vec()).ok()
}

/// Generate a key for a chunk of a large object using the format:
/// `__data__\0<bucket_name>\0<object_key>\0<chunk_part_number>`
pub fn chunk_key(bucket: &str, object: &str, part: u32) -> Vec<u8> {
    let mut key = b"__data__\0".to_vec();
    key.extend_from_slice(bucket.as_bytes());
    key.push(0);
    key.extend_from_slice(object.as_bytes());
    key.push(0);
    key.extend_from_slice(part.to_string().as_bytes());
    key
}

/// Parse a chunk key returning the bucket, object, and part number
pub fn parse_chunk_key(key: &[u8]) -> Option<(String, String, u32)> {
    let expected_prefix = b"__data__\0";
    if !key.starts_with(expected_prefix) {
        return None;
    }
    let rest = &key[expected_prefix.len()..];
    let parts: Vec<&[u8]> = rest.split(|b| *b == 0).collect();
    if parts.len() != 3 {
        return None;
    }
    let bucket = String::from_utf8(parts[0].to_vec()).ok()?;
    let object = String::from_utf8(parts[1].to_vec()).ok()?;
    let part = String::from_utf8(parts[2].to_vec()).ok()?.parse().ok()?;
    Some((bucket, object, part))
}
