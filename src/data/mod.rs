use serde::{Deserialize, Serialize};

pub mod chunking;
pub mod keys;

use chunking::ObjectManifest;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ObjectMetadata {
    pub content_length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ObjectData {
    Inline(Vec<u8>),
    Chunked(ObjectManifest),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Object {
    pub metadata: ObjectMetadata,
    pub data: ObjectData,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::chunking::{retrieve_chunks, store_chunks, DEFAULT_CHUNK_SIZE};
    use crate::data::keys;
    use crate::storage::{KeyValueStore, PrefixIterator, StorageResult, Transaction};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MockStore {
        inner: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    struct MockTxn {
        inner: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl Transaction for MockTxn {
        fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let map = self.inner.lock().unwrap();
            Ok(map.get(key).cloned())
        }

        fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
            let mut map = self.inner.lock().unwrap();
            map.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
            let mut map = self.inner.lock().unwrap();
            map.remove(key);
            Ok(())
        }

        fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>> {
            let map = self.inner.lock().unwrap();
            let items: Vec<_> = map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect();
            Ok(Box::new(items.into_iter()))
        }

        fn commit(self) -> StorageResult<()> {
            Ok(())
        }
    }

    impl KeyValueStore for MockStore {
        type Transaction = MockTxn;

        fn open<P: AsRef<std::path::Path>>(_path: P) -> StorageResult<Self> {
            Ok(Self::default())
        }

        fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let map = self.inner.lock().unwrap();
            Ok(map.get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
            let mut map = self.inner.lock().unwrap();
            map.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &[u8]) -> StorageResult<()> {
            let mut map = self.inner.lock().unwrap();
            map.remove(key);
            Ok(())
        }

        fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<PrefixIterator<'_>> {
            let map = self.inner.lock().unwrap();
            let items: Vec<_> = map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect();
            Ok(Box::new(items.into_iter()))
        }

        fn begin_transaction(&self) -> StorageResult<Self::Transaction> {
            Ok(MockTxn {
                inner: self.inner.clone(),
            })
        }
    }

    #[test]
    fn object_serialization_roundtrip() {
        let metadata = ObjectMetadata {
            content_length: 5,
            content_type: Some("text/plain".into()),
        };
        let obj = Object {
            metadata: metadata.clone(),
            data: ObjectData::Inline(b"hello".to_vec()),
        };

        let encoded = bincode::serialize(&obj).unwrap();
        let decoded: Object = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }

    #[test]
    fn key_generation_and_parsing() {
        let obj_key = keys::object_key("bucket", "object");
        let (b, o) = keys::parse_object_key(&obj_key).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(o, "object");

        let meta_key = keys::bucket_metadata_key("bucket");
        let parsed_bucket = keys::parse_bucket_metadata_key(&meta_key).unwrap();
        assert_eq!(parsed_bucket, "bucket");

        let chunk_key = keys::chunk_key("bucket", "object", 2);
        let (cb, co, part) = keys::parse_chunk_key(&chunk_key).unwrap();
        assert_eq!(cb, "bucket");
        assert_eq!(co, "object");
        assert_eq!(part, 2);
    }

    #[test]
    fn chunking_roundtrip() {
        let store = MockStore::default();
        let data = vec![42u8; DEFAULT_CHUNK_SIZE * 2 + 10];
        let manifest = store_chunks(&store, "bucket", "obj", &data, DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(manifest.chunks.len(), 3);

        let obj = Object {
            metadata: ObjectMetadata {
                content_length: data.len() as u64,
                content_type: None,
            },
            data: ObjectData::Chunked(manifest.clone()),
        };
        let obj_key = keys::object_key("bucket", "obj");
        store
            .put(&obj_key, &bincode::serialize(&obj).unwrap())
            .unwrap();

        let retrieved = retrieve_chunks(&store, "bucket", "obj", &manifest).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn crud_operations() {
        let store = MockStore::default();
        let key = keys::object_key("bucket", "obj");

        // Create
        let original = Object {
            metadata: ObjectMetadata {
                content_length: 3,
                content_type: Some("text/plain".into()),
            },
            data: ObjectData::Inline(b"abc".to_vec()),
        };
        store
            .put(&key, &bincode::serialize(&original).unwrap())
            .unwrap();

        // Read
        let retrieved = store.get(&key).unwrap().unwrap();
        let decoded: Object = bincode::deserialize(&retrieved).unwrap();
        assert_eq!(decoded, original);

        // Update
        let updated = Object {
            metadata: ObjectMetadata {
                content_length: 6,
                content_type: Some("text/plain".into()),
            },
            data: ObjectData::Inline(b"abcdef".to_vec()),
        };
        store
            .put(&key, &bincode::serialize(&updated).unwrap())
            .unwrap();

        let retrieved = store.get(&key).unwrap().unwrap();
        let decoded: Object = bincode::deserialize(&retrieved).unwrap();
        assert_eq!(decoded, updated);

        // Delete
        store.delete(&key).unwrap();
        assert!(store.get(&key).unwrap().is_none());
    }
}
