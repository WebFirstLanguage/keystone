use crate::data::keys;
use crate::error::{Error, Result};
use crate::storage::{KeyValueStore, Transaction};
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
        if key.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(key.to_string()));
        }
        let obj_key = keys::object_key(&self.name, key);
        let encoded = bincode::serialize(data)?;
        self.store.put(&obj_key, &encoded)?;
        Ok(())
    }

    /// Retrieve and deserialize an object by key
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T> {
        if key.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(key.to_string()));
        }
        let obj_key = keys::object_key(&self.name, key);
        match self.store.get(&obj_key)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Err(Error::ObjectNotFound),
        }
    }

    /// Delete an object by key
    pub fn delete(&self, key: &str) -> Result<()> {
        if key.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(key.to_string()));
        }
        let obj_key = keys::object_key(&self.name, key);
        self.store.delete(&obj_key)?;
        Ok(())
    }

    /// List object keys in the bucket that start with the given prefix
    pub fn list(&self, prefix: &str) -> Result<Vec<String>> {
        if prefix.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(prefix.to_string()));
        }
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

    /// Store a serializable object under the given key within a transaction
    pub fn put_tx<T: Serialize>(
        &self,
        txn: &mut dyn Transaction,
        key: &str,
        data: &T,
    ) -> Result<()> {
        if key.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(key.to_string()));
        }
        let obj_key = keys::object_key(&self.name, key);
        let encoded = bincode::serialize(data)?;
        txn.put(&obj_key, &encoded)?;
        Ok(())
    }

    /// Retrieve and deserialize an object by key within a transaction
    pub fn get_tx<T: DeserializeOwned>(&self, txn: &mut dyn Transaction, key: &str) -> Result<T> {
        if key.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(key.to_string()));
        }
        let obj_key = keys::object_key(&self.name, key);
        match txn.get(&obj_key)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Err(Error::ObjectNotFound),
        }
    }

    /// Delete an object by key within a transaction
    pub fn delete_tx(&self, txn: &mut dyn Transaction, key: &str) -> Result<()> {
        if key.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(key.to_string()));
        }
        let obj_key = keys::object_key(&self.name, key);
        txn.delete(&obj_key)?;
        Ok(())
    }

    /// List object keys in the bucket that start with the given prefix within a transaction
    pub fn list_tx(&self, txn: &mut dyn Transaction, prefix: &str) -> Result<Vec<String>> {
        if prefix.as_bytes().contains(&0) {
            return Err(Error::InvalidObjectKey(prefix.to_string()));
        }
        let mut full_prefix = self.name.as_bytes().to_vec();
        full_prefix.push(0);
        full_prefix.extend_from_slice(prefix.as_bytes());
        let iter = txn.scan_prefix(&full_prefix)?;
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
