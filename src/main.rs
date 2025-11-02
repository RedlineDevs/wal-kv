mod kv;
mod wal;

use std::fs;
use std::path::Path;

fn main() {
    let _ = fs::remove_file("db.log");
    
    let mut store = kv::KVStore::open(Path::new("db.log")).expect("Failed to open store");
    
    store.set(b"key1".to_vec(), b"value1".to_vec()).expect("Failed to set key1");
    store.set(b"key2".to_vec(), b"value2".to_vec()).expect("Failed to set key2");
    store.set(b"key1".to_vec(), b"value1_updated".to_vec()).expect("Failed to update key1");
    
    assert_eq!(store.len(), 2);
    assert_eq!(store.get(b"key1" as &[u8]), Some(&b"value1_updated".to_vec()));
    assert_eq!(store.get(b"key2" as &[u8]), Some(&b"value2".to_vec()));
    
    println!("Set {} entries", store.len());
    
    drop(store);
    
    let recovered_store = kv::KVStore::open(Path::new("db.log")).expect("Failed to reopen store");
    
    assert_eq!(recovered_store.len(), 2);
    assert_eq!(recovered_store.get(b"key1" as &[u8]), Some(&b"value1_updated".to_vec()));
    assert_eq!(recovered_store.get(b"key2" as &[u8]), Some(&b"value2".to_vec()));
    
    println!("Recovered {} entries after restart", recovered_store.len());
}

