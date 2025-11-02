use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::io::BufReader;
use std::path::Path;
use crate::wal::{LogIterator, WriteAheadLog};

pub struct KVStore {
    map: HashMap<Vec<u8>, Vec<u8>>,
    wal: WriteAheadLog,
}

impl KVStore {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        
        let file_for_wal = file.try_clone()?;
        let reader = BufReader::new(file);
        
        let iterator = LogIterator::new(reader);
        let mut map = HashMap::new();
        for record in iterator {
            map.insert(record.key, record.value);
        }
        
        let wal = WriteAheadLog::new(file_for_wal);
        
        Ok(KVStore { map, wal })
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.map.get(key)
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        use crate::wal::LogRecord;
        let record = LogRecord {
            key: key.clone(),
            value: value.clone(),
        };
        self.wal.append(&record)?;
        self.map.insert(key, value);
        Ok(())
    }
}

