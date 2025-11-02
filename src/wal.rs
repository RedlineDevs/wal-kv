use crc32fast::Hasher;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crc32fast::Hasher;

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
        let stored_crc = u32::from_le_bytes([serialized[0], serialized[1], serialized[2], serialized[3]]);
        
        // Verify key length (8 bytes, starting at index 4)
        let stored_key_len = u64::from_le_bytes([
            serialized[4], serialized[5], serialized[6], serialized[7],
            serialized[8], serialized[9], serialized[10], serialized[11],
        ]);
        assert_eq!(stored_key_len, 8);
        
        // Verify value length (8 bytes, starting at index 12)
        let stored_value_len = u64::from_le_bytes([
            serialized[12], serialized[13], serialized[14], serialized[15],
            serialized[16], serialized[17], serialized[18], serialized[19],
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
}

