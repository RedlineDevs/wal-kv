mod wal;

use std::fs;
use std::path::Path;
use wal::{LogRecord, WriteAheadLog, serialize, deserialize};

fn main() {
    let _ = fs::remove_file("db.log");
    
    let mut wal = WriteAheadLog::new(Path::new("db.log")).expect("Failed to create WAL");
    
    let record1 = LogRecord {
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
    };
    wal.append(&record1).expect("Failed to append record1");
    
    let record2 = LogRecord {
        key: b"key2".to_vec(),
        value: b"value2".to_vec(),
    };
    wal.append(&record2).expect("Failed to append record2");
    
    drop(wal);
    
    let raw_bytes = fs::read("db.log").expect("Failed to read db.log");
    println!("File size: {} bytes", raw_bytes.len());
    
    let record1_serialized = serialize(&record1);
    let first_record_len = record1_serialized.len();
    
    let deserialized1 = deserialize(&raw_bytes[..first_record_len]).expect("Failed to deserialize record1");
    println!(
        "Record 1: key={:?}, value={:?}",
        String::from_utf8_lossy(&deserialized1.key),
        String::from_utf8_lossy(&deserialized1.value)
    );
    
    let deserialized2 = deserialize(&raw_bytes[first_record_len..]).expect("Failed to deserialize record2");
    println!(
        "Record 2: key={:?}, value={:?}",
        String::from_utf8_lossy(&deserialized2.key),
        String::from_utf8_lossy(&deserialized2.value)
    );
}

