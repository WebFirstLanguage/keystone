use keystone::{Datastore, Error};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    value: i32,
}

#[test]
fn test_bucket_name_validation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_file = NamedTempFile::new()?;
    let datastore = Datastore::new(temp_file.path())?;

    // Test invalid bucket names with NUL bytes
    let invalid_names = [
        "bucket\0name",
        "bucket\0",
        "\0bucket",
        "bu\0cket",
    ];

    for invalid_name in &invalid_names {
        // create_bucket should return InvalidBucketName error
        match datastore.create_bucket(invalid_name) {
            Err(Error::InvalidBucketName(name)) => {
                assert_eq!(name, *invalid_name);
            }
            result => panic!("Expected InvalidBucketName error for '{}', got: {:?}", invalid_name, result),
        }

        // delete_bucket should return InvalidBucketName error
        match datastore.delete_bucket(invalid_name) {
            Err(Error::InvalidBucketName(name)) => {
                assert_eq!(name, *invalid_name);
            }
            result => panic!("Expected InvalidBucketName error for '{}', got: {:?}", invalid_name, result),
        }

        // bucket should return InvalidBucketName error
        match datastore.bucket(invalid_name) {
            Err(Error::InvalidBucketName(name)) => {
                assert_eq!(name, *invalid_name);
            }
            Ok(_) => panic!("Expected InvalidBucketName error for '{}', got Ok", invalid_name),
            Err(other) => panic!("Expected InvalidBucketName error for '{}', got: {:?}", invalid_name, other),
        }
    }

    Ok(())
}

#[test]
fn test_object_key_validation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_file = NamedTempFile::new()?;
    let datastore = Datastore::new(temp_file.path())?;

    // Create a valid bucket first
    datastore.create_bucket("test-bucket")?;
    let bucket = datastore.bucket("test-bucket")?;

    let test_data = TestData { value: 42 };

    // Test invalid object keys with NUL bytes
    let invalid_keys = [
        "key\0name",
        "key\0",
        "\0key",
        "ke\0y",
    ];

    for invalid_key in &invalid_keys {
        // put should return InvalidObjectKey error
        match bucket.put(invalid_key, &test_data) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, *invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }

        // get should return InvalidObjectKey error
        match bucket.get::<TestData>(invalid_key) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, *invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }

        // delete should return InvalidObjectKey error
        match bucket.delete(invalid_key) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, *invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }

        // list should return InvalidObjectKey error for invalid prefix
        match bucket.list(invalid_key) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, *invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }
    }

    Ok(())
}

#[test]
fn test_transactional_validation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_file = NamedTempFile::new()?;
    let datastore = Datastore::new(temp_file.path())?;

    // Create a valid bucket first
    datastore.create_bucket("test-bucket")?;
    let bucket = datastore.bucket("test-bucket")?;

    let test_data = TestData { value: 42 };
    let invalid_key = "key\0name";

    // Test transactional methods also validate inputs
    datastore.transaction(|txn| {
        match bucket.put_tx(txn, invalid_key, &test_data) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }
        Ok(())
    })?;

    datastore.transaction(|txn| {
        match bucket.get_tx::<TestData>(txn, invalid_key) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }
        Ok(())
    })?;

    datastore.transaction(|txn| {
        match bucket.delete_tx(txn, invalid_key) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }
        Ok(())
    })?;

    datastore.transaction(|txn| {
        match bucket.list_tx(txn, invalid_key) {
            Err(Error::InvalidObjectKey(key)) => {
                assert_eq!(key, invalid_key);
            }
            result => panic!("Expected InvalidObjectKey error for '{}', got: {:?}", invalid_key, result),
        }
        Ok(())
    })?;

    Ok(())
}