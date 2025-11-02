use crc32fast::Hasher;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};

#[derive(Debug)]
pub struct LogRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub fn serialize(record: &LogRecord) -> Vec<u8> {
    let key_len = record.key.len() as u64;
    let value_len = record.value.len() as u64;

    // Calculate CRC32 over the data: key_len + value_len + key + value
    let mut hasher = Hasher::new();
    hasher.update(&key_len.to_le_bytes());
    hasher.update(&value_len.to_le_bytes());
    hasher.update(&record.key);
    hasher.update(&record.value);
    let crc = hasher.finalize();

    // Build the serialized format: [CRC32 (4)][Key Length (8)][Value Length (8)][Key][Value]
    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&key_len.to_le_bytes());
    result.extend_from_slice(&value_len.to_le_bytes());
    result.extend_from_slice(&record.key);
    result.extend_from_slice(&record.value);

    result
}

pub struct WriteAheadLog {
    file: File,
}

impl WriteAheadLog {
    pub fn new(file: File) -> Self {
        WriteAheadLog { file }
    }

    pub fn append(&mut self, record: &LogRecord) -> io::Result<()> {
        let serialized = serialize(record);
        self.file.write_all(&serialized)?;
        self.file.sync_data()?; // Ensure durability
        Ok(())
    }
}

pub struct LogIterator {
    reader: BufReader<File>,
}

impl LogIterator {
    pub fn new(reader: BufReader<File>) -> Self {
        LogIterator { reader }
    }
}

impl Iterator for LogIterator {
    type Item = LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header = [0u8; 20];

        if self.reader.read_exact(&mut header).is_err() {
            return None;
        }

        let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let key_len_bytes = [
            header[4], header[5], header[6], header[7], header[8], header[9], header[10],
            header[11],
        ];
        let value_len_bytes = [
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ];

        let key_len = u64::from_le_bytes(key_len_bytes) as usize;
        let value_len = u64::from_le_bytes(value_len_bytes) as usize;
        let data_len = key_len + value_len;

        let mut data = vec![0u8; data_len];

        if self.reader.read_exact(&mut data).is_err() {
            return None;
        }

        let mut hasher = Hasher::new();
        hasher.update(&key_len_bytes);
        hasher.update(&value_len_bytes);
        hasher.update(&data);
        let calculated_crc = hasher.finalize();

        if stored_crc != calculated_crc {
            return None;
        }

        let key = data[..key_len].to_vec();
        let value = data[key_len..].to_vec();

        Some(LogRecord { key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crc32fast::Hasher;

    fn deserialize(bytes: &[u8]) -> Result<LogRecord, String> {
        if bytes.len() < 20 {
            return Err("Not enough bytes for header".to_string());
        }

        let stored_crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        let key_len = u64::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
        ]);

        let value_len = u64::from_le_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ]);

        let data_start = 20;
        let required_len = data_start + (key_len as usize) + (value_len as usize);
        if bytes.len() < required_len {
            return Err(format!(
                "Not enough bytes: need {}, have {}",
                required_len,
                bytes.len()
            ));
        }

        let key_start = data_start;
        let key_end = key_start + (key_len as usize);
        let value_start = key_end;
        let value_end = value_start + (value_len as usize);

        let key = bytes[key_start..key_end].to_vec();
        let value = bytes[value_start..value_end].to_vec();

        let mut hasher = Hasher::new();
        hasher.update(&key_len.to_le_bytes());
        hasher.update(&value_len.to_le_bytes());
        hasher.update(&key);
        hasher.update(&value);
        let calculated_crc = hasher.finalize();

        if stored_crc != calculated_crc {
            return Err(format!(
                "CRC mismatch: stored {}, calculated {}",
                stored_crc, calculated_crc
            ));
        }

        Ok(LogRecord { key, value })
    }

    #[test]
    fn test_serialize() {
        let record = LogRecord {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let serialized = serialize(&record);

        // Verify structure
        assert_eq!(serialized.len(), 4 + 8 + 8 + 8 + 10); // CRC + key_len + value_len + key + value

        // Verify CRC32 is at the beginning (4 bytes)
        let stored_crc =
            u32::from_le_bytes([serialized[0], serialized[1], serialized[2], serialized[3]]);

        // Verify key length (8 bytes, starting at index 4)
        let stored_key_len = u64::from_le_bytes([
            serialized[4],
            serialized[5],
            serialized[6],
            serialized[7],
            serialized[8],
            serialized[9],
            serialized[10],
            serialized[11],
        ]);
        assert_eq!(stored_key_len, 8);

        // Verify value length (8 bytes, starting at index 12)
        let stored_value_len = u64::from_le_bytes([
            serialized[12],
            serialized[13],
            serialized[14],
            serialized[15],
            serialized[16],
            serialized[17],
            serialized[18],
            serialized[19],
        ]);
        assert_eq!(stored_value_len, 10);

        // Verify key content (starting at index 20)
        assert_eq!(&serialized[20..28], b"test_key");

        // Verify value content (starting at index 28)
        assert_eq!(&serialized[28..38], b"test_value");

        // Verify CRC matches what we calculate manually
        let mut hasher = Hasher::new();
        hasher.update(&(8u64).to_le_bytes());
        hasher.update(&(10u64).to_le_bytes());
        hasher.update(b"test_key");
        hasher.update(b"test_value");
        let expected_crc = hasher.finalize();
        assert_eq!(stored_crc, expected_crc);
    }

    #[test]
    fn test_deserialize() {
        let record = LogRecord {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let serialized = serialize(&record);
        let deserialized = deserialize(&serialized).unwrap();

        assert_eq!(deserialized.key, record.key);
        assert_eq!(deserialized.value, record.value);
    }

    #[test]
    fn test_deserialize_truncated() {
        let record = LogRecord {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let mut serialized = serialize(&record);
        serialized.truncate(15); // Too short for header

        let result = deserialize(&serialized);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Not enough bytes"));
    }

    #[test]
    fn test_deserialize_bad_crc() {
        let record = LogRecord {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let mut serialized = serialize(&record);
        // Corrupt the CRC
        serialized[0] = serialized[0].wrapping_add(1);

        let result = deserialize(&serialized);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CRC mismatch"));
    }
}
