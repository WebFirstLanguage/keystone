use serde::{Deserialize, Serialize};

use crate::data::keys;
use crate::storage::{KeyValueStore, StorageResult};

/// Default chunk size for large object storage (1 MiB)
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Information about a single chunk of a large object
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ChunkInfo {
    pub part_number: u32,
    pub size: usize,
}

/// Manifest describing how a large object is split into chunks
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ObjectManifest {
    pub chunks: Vec<ChunkInfo>,
}

/// Split raw data into chunks and store them in the provided key-value store
/// returning a manifest describing the chunks.
pub fn store_chunks<K: KeyValueStore>(
    store: &K,
    bucket: &str,
    object: &str,
    data: &[u8],
    chunk_size: usize,
) -> StorageResult<ObjectManifest> {
    let mut chunks = Vec::new();
    for (i, chunk) in data.chunks(chunk_size).enumerate() {
        let part = i as u32;
        let key = keys::chunk_key(bucket, object, part);
        store.put(&key, chunk)?;
        chunks.push(ChunkInfo {
            part_number: part,
            size: chunk.len(),
        });
    }
    Ok(ObjectManifest { chunks })
}

/// Retrieve chunked data using the provided manifest and assemble it into a single buffer
pub fn retrieve_chunks<K: KeyValueStore>(
    store: &K,
    bucket: &str,
    object: &str,
    manifest: &ObjectManifest,
) -> StorageResult<Vec<u8>> {
    let mut data = Vec::new();
    for chunk in &manifest.chunks {
        let key = keys::chunk_key(bucket, object, chunk.part_number);
        if let Some(bytes) = store.get(&key)? {
            data.extend_from_slice(&bytes);
        }
    }
    Ok(data)
}

/// Convenience function that splits bytes without storing them, useful for tests
pub fn chunk_bytes(data: &[u8], chunk_size: usize) -> ObjectManifest {
    let chunks = data
        .chunks(chunk_size)
        .enumerate()
        .map(|(i, c)| ChunkInfo {
            part_number: i as u32,
            size: c.len(),
        })
        .collect();
    ObjectManifest { chunks }
}
