use crate::bucket::Bucket;
use crate::data::keys;
use crate::error::{Error, Result};
use crate::storage::redb_adapter::RedbAdapter;
use crate::storage::{KeyValueStore, Transaction};
use std::path::Path;
use std::sync::Arc;

/// Primary interface to the object store
pub struct Datastore {
    store: Arc<RedbAdapter>,
}

impl Datastore {
    /// Open or create a datastore at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = RedbAdapter::open(path).map_err(Error::from)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Create a new bucket
    pub fn create_bucket(&self, name: &str) -> Result<()> {
        let meta_key = keys::bucket_metadata_key(name);
        if self.store.get(&meta_key)?.is_some() {
            return Err(Error::BucketExists);
        }
        self.store.put(&meta_key, &[])?;
        Ok(())
    }

    /// Delete a bucket and all of its contents
    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        let meta_key = keys::bucket_metadata_key(name);
        if self.store.get(&meta_key)?.is_none() {
            return Err(Error::BucketNotFound);
        }
        // Delete all objects in the bucket
        let mut prefix = name.as_bytes().to_vec();
        prefix.push(0);
        let iter = self.store.scan_prefix(&prefix)?;
        for item in iter {
            let (key, _value) = item?;
            self.store.delete(&key)?;
        }
        // Delete the bucket metadata
        self.store.delete(&meta_key)?;
        Ok(())
    }

    /// Get a handle to an existing bucket
    pub fn bucket(&self, name: &str) -> Result<Bucket<RedbAdapter>> {
        let meta_key = keys::bucket_metadata_key(name);
        if self.store.get(&meta_key)?.is_none() {
            return Err(Error::BucketNotFound);
        }
        Ok(Bucket {
            store: Arc::clone(&self.store),
            name: name.to_string(),
        })
    }

    /// Execute a closure within a transaction for atomic operations
    pub fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut dyn Transaction) -> Result<T>,
    {
        let mut txn = self.store.begin_transaction()?;
        match f(&mut txn as &mut dyn Transaction) {
            Ok(result) => {
                txn.commit()?;
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }
}
