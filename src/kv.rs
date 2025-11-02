use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use crate::wal::{LogIterator, WriteAheadLog};

pub struct KVStore {
    map: HashMap<Vec<u8>, Vec<u8>>,
    wal: WriteAheadLog,
    path: PathBuf,
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
        
        Ok(KVStore { 
            map, 
            wal, 
            path: path.to_path_buf(),
        })
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

    pub fn compact(&mut self) -> io::Result<()> {
        use crate::wal::LogRecord;
        
        let compact_path = {
            let mut compact_path = self.path.clone();
            if let Some(file_name) = compact_path.file_name().and_then(|n| n.to_str()) {
                let compact_name = format!("{}.compact", file_name);
                compact_path.set_file_name(&compact_name);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"));
            }
            compact_path
        };
        
        let compact_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&compact_path)?;
        
        let mut compact_wal = WriteAheadLog::new(compact_file);
        
        for (key, value) in &self.map {
            let record = LogRecord {
                key: key.clone(),
                value: value.clone(),
            };
            compact_wal.append(&record)?;
        }
        
        drop(compact_wal);
        
        std::fs::rename(&compact_path, &self.path)?;
        
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        
        self.wal = WriteAheadLog::new(file);
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_set_and_get() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut store = KVStore::open(path).unwrap();
        
        store.set(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        
        assert_eq!(store.get(b"key1"), Some(&b"value1".to_vec()));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_recovery() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        {
            let mut store = KVStore::open(path).unwrap();
            store.set(b"key1".to_vec(), b"value1".to_vec()).unwrap();
            store.set(b"key2".to_vec(), b"value2".to_vec()).unwrap();
            store.set(b"key1".to_vec(), b"value1_updated".to_vec()).unwrap();
        }
        
        let recovered_store = KVStore::open(path).unwrap();
        
        assert_eq!(recovered_store.len(), 2);
        assert_eq!(recovered_store.get(b"key1"), Some(&b"value1_updated".to_vec()));
        assert_eq!(recovered_store.get(b"key2"), Some(&b"value2".to_vec()));
    }

    #[test]
    fn test_compaction() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut store = KVStore::open(path).unwrap();
        
        store.set(b"test_key".to_vec(), b"initial_value".to_vec()).unwrap();
        
        for i in 0..100 {
            let value = format!("value_{}", i).into_bytes();
            store.set(b"test_key".to_vec(), value).unwrap();
        }
        
        let size_before = fs::metadata(path).unwrap().len();
        
        store.compact().unwrap();
        
        let size_after = fs::metadata(path).unwrap().len();
        
        assert!(size_after < size_before, "Compaction should reduce file size");
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(b"test_key"), Some(&b"value_99".to_vec()));
        
        drop(store);
        
        let final_store = KVStore::open(path).unwrap();
        assert_eq!(final_store.len(), 1);
        assert_eq!(final_store.get(b"test_key"), Some(&b"value_99".to_vec()));
    }
}

