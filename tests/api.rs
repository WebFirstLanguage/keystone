use keystone::{Datastore, Error, Result};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestData {
    value: String,
}

#[test]
fn basic_bucket_operations() -> Result<()> {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.redb");
    let ds = Datastore::new(&db_path)?;

    ds.create_bucket("photos")?;
    let bucket = ds.bucket("photos")?;

    let data = TestData {
        value: "hello".to_string(),
    };
    bucket.put("greeting", &data)?;
    let retrieved: TestData = bucket.get("greeting")?;
    assert_eq!(retrieved, data);

    let list = bucket.list("gre")?;
    assert_eq!(list, vec!["greeting".to_string()]);

    bucket.delete("greeting")?;
    assert!(matches!(
        bucket.get::<TestData>("greeting"),
        Err(Error::ObjectNotFound)
    ));

    ds.delete_bucket("photos")?;
    assert!(matches!(ds.bucket("photos"), Err(Error::BucketNotFound)));

    Ok(())
}

#[test]
fn error_cases_and_transactions() -> Result<()> {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test2.redb");
    let ds = Datastore::new(&db_path)?;

    ds.create_bucket("b1")?;
    assert!(matches!(ds.create_bucket("b1"), Err(Error::BucketExists)));

    // transaction commit
    ds.transaction(|txn| {
        txn.put(b"raw_key", b"raw_value")?;
        Ok(())
    })?;

    let value = ds.transaction(|txn| Ok(txn.get(b"raw_key")?))?;
    assert_eq!(value, Some(b"raw_value".to_vec()));

    // transaction rollback
    let res: Result<()> = ds.transaction(|txn| {
        txn.put(b"temp", b"val")?;
        Err(Error::ObjectNotFound)
    });
    assert!(res.is_err());
    let value = ds.transaction(|txn| Ok(txn.get(b"temp")?))?;
    assert!(value.is_none());

    Ok(())
}
