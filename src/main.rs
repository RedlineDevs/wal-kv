mod wal;

use std::fs;
use std::io::Write;
use wal::{LogRecord, serialize, deserialize};

fn main() {
    // Demonstrate serialization and deserialization
    let record = LogRecord {
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    };
    let serialized = serialize(&record);
    println!("Serialized {} bytes", serialized.len());
    
    let deserialized = deserialize(&serialized).expect("Failed to deserialize");
    println!(
        "Deserialized: key={:?}, value={:?}",
        String::from_utf8_lossy(&deserialized.key),
        String::from_utf8_lossy(&deserialized.value)
    );
    
    // Basic file I/O
    let mut file = fs::File::create("db.log").expect("Failed to create file");
    file.write_all(b"hello world").expect("Failed to write to file");
    drop(file);
    
    let contents = fs::read_to_string("db.log").expect("Failed to read file");
    println!("{}", contents);
}

