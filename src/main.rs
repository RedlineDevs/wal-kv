mod kv;
mod wal;

use std::fs;
use std::path::Path;

fn main() {
    let _ = fs::remove_file("db.log");

    let mut store = kv::KVStore::open(Path::new("db.log")).expect("Failed to open store");

    store
        .set(b"key1".to_vec(), b"value1".to_vec())
        .expect("Failed to set key1");
    store
        .set(b"key2".to_vec(), b"value2".to_vec())
        .expect("Failed to set key2");
    store
        .set(b"key1".to_vec(), b"value1_updated".to_vec())
        .expect("Failed to update key1");

    assert_eq!(store.len(), 2);
    assert_eq!(
        store.get(b"key1" as &[u8]),
        Some(&b"value1_updated".to_vec())
    );
    assert_eq!(store.get(b"key2" as &[u8]), Some(&b"value2".to_vec()));

    println!("Set {} entries", store.len());

    drop(store);

    let recovered_store = kv::KVStore::open(Path::new("db.log")).expect("Failed to reopen store");

    assert_eq!(recovered_store.len(), 2);
    assert_eq!(
        recovered_store.get(b"key1" as &[u8]),
        Some(&b"value1_updated".to_vec())
    );
    assert_eq!(
        recovered_store.get(b"key2" as &[u8]),
        Some(&b"value2".to_vec())
    );

    println!("Recovered {} entries after restart", recovered_store.len());

    // Test compaction
    let _ = fs::remove_file("db.log");

    let mut store = kv::KVStore::open(Path::new("db.log")).expect("Failed to open store");

    store
        .set(b"test_key".to_vec(), b"initial_value".to_vec())
        .expect("Failed to set initial value");

    for i in 0..100 {
        let value = format!("value_{}", i).into_bytes();
        store
            .set(b"test_key".to_vec(), value)
            .expect("Failed to update key");
    }

    let size_before = fs::metadata("db.log")
        .expect("Failed to get metadata")
        .len();
    println!("Log file size before compaction: {} bytes", size_before);

    store.compact().expect("Failed to compact");

    let size_after = fs::metadata("db.log")
        .expect("Failed to get metadata")
        .len();
    println!("Log file size after compaction: {} bytes", size_after);

    assert!(
        size_after < size_before,
        "Compaction should reduce file size"
    );
    assert_eq!(store.len(), 1);
    assert_eq!(store.get(b"test_key" as &[u8]), Some(&b"value_99".to_vec()));

    drop(store);

    let final_store =
        kv::KVStore::open(Path::new("db.log")).expect("Failed to reopen after compaction");
    assert_eq!(final_store.len(), 1);
    assert_eq!(
        final_store.get(b"test_key" as &[u8]),
        Some(&b"value_99".to_vec())
    );

    println!("Compaction test passed");
}
