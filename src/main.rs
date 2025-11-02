mod wal;

use std::fs;
use std::path::Path;
use wal::{LogRecord, WriteAheadLog, LogIterator};

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
    
    let record3 = LogRecord {
        key: b"key3".to_vec(),
        value: b"value3".to_vec(),
    };
    wal.append(&record3).expect("Failed to append record3");
    
    drop(wal);
    
    let iterator = LogIterator::new(Path::new("db.log")).expect("Failed to create iterator");
    let mut count = 0;
    
    for record in iterator {
        count += 1;
        println!(
            "Record {}: key={:?}, value={:?}",
            count,
            String::from_utf8_lossy(&record.key),
            String::from_utf8_lossy(&record.value)
        );
    }
    
    assert_eq!(count, 3);
    println!("Successfully read {} records", count);
}

