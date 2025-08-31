//! Utility functions for generating and parsing keys used in the object store

/// Generate the key for storing an object in a bucket using the format:
/// `<bucket_name>\0<object_key>`
pub fn object_key(bucket: &str, object: &str) -> Vec<u8> {
    assert!(!bucket.as_bytes().contains(&0), "bucket contains NUL byte");
    assert!(!object.as_bytes().contains(&0), "object contains NUL byte");

    let mut key = Vec::with_capacity(bucket.len() + 1 + object.len());
    key.extend_from_slice(bucket.as_bytes());
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
    assert!(!bucket.contains('\0'), "bucket contains NUL byte");
    assert!(!object.contains('\0'), "object contains NUL byte");

    let prefix = b"__data__\0";
    let capacity = prefix.len() + bucket.len() + 1 + object.len() + 1 + 10;
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(prefix);
    key.extend_from_slice(bucket.as_bytes());
    key.push(0);
    key.extend_from_slice(object.as_bytes());
    key.push(0);
    let part_str = format!("{:010}", part);
    key.extend_from_slice(part_str.as_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_zero_pads_part() {
        let key = chunk_key("bucket", "obj", 1);
        let mut expected = b"__data__\0bucket\0obj\0".to_vec();
        expected.extend_from_slice(b"0000000001");
        assert_eq!(key, expected);
    }

    #[test]
    #[should_panic]
    fn object_key_rejects_nul() {
        let _ = object_key("buck\0et", "obj");
    }

    #[test]
    #[should_panic]
    fn chunk_key_rejects_nul() {
        let _ = chunk_key("bucket", "ob\0j", 0);
    }
}
