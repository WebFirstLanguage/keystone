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

    let bucket = ds.bucket("b1")?;

    // transaction commit using bucket methods
    let commit_data = TestData {
        value: "v1".to_string(),
    };
    ds.transaction(|txn| {
        bucket.put_tx(txn, "k1", &commit_data)?;
        Ok(())
    })?;

    let value: TestData = ds.transaction(|txn| bucket.get_tx(txn, "k1"))?;
    assert_eq!(value, commit_data);

    // list inside a transaction
    let list = ds.transaction(|txn| bucket.list_tx(txn, "k"))?;
    assert_eq!(list, vec!["k1".to_string()]);

    // transaction rollback
    let res: Result<()> = ds.transaction(|txn| {
        let temp_data = TestData {
            value: "temp".to_string(),
        };
        bucket.put_tx(txn, "temp", &temp_data)?;
        Err(Error::ObjectNotFound)
    });
    assert!(res.is_err());
    assert!(matches!(
        bucket.get::<TestData>("temp"),
        Err(Error::ObjectNotFound)
    ));

    Ok(())
}
