use crate::data::keys;
use crate::error::{Error, Result};
use crate::storage::KeyValueStore;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

/// Handle to a specific bucket in the datastore
pub struct Bucket<S: KeyValueStore> {
    pub(crate) store: Arc<S>,
    pub(crate) name: String,
}

impl<S: KeyValueStore> Bucket<S> {
    /// Store a serializable object under the given key
    pub fn put<T: Serialize>(&self, key: &str, data: &T) -> Result<()> {
        let obj_key = keys::object_key(&self.name, key);
        let encoded = bincode::serialize(data)?;
        self.store.put(&obj_key, &encoded)?;
        Ok(())
    }

    /// Retrieve and deserialize an object by key
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T> {
        let obj_key = keys::object_key(&self.name, key);
        match self.store.get(&obj_key)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Err(Error::ObjectNotFound),
        }
    }

    /// Delete an object by key
    pub fn delete(&self, key: &str) -> Result<()> {
        let obj_key = keys::object_key(&self.name, key);
        self.store.delete(&obj_key)?;
        Ok(())
    }

    /// List object keys in the bucket that start with the given prefix
    pub fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut full_prefix = self.name.as_bytes().to_vec();
        full_prefix.push(0);
        full_prefix.extend_from_slice(prefix.as_bytes());
        let iter = self.store.scan_prefix(&full_prefix)?;
        let mut keys_vec = Vec::new();
        for item in iter {
            let (key_bytes, _value) = item?;
            if let Some((_bucket, object)) = keys::parse_object_key(&key_bytes) {
                keys_vec.push(object);
            }
        }
        Ok(keys_vec)
    }
}
