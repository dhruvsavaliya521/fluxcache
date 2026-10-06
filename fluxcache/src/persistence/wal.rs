// FluxCache - Write-Ahead Log (WAL)
//
// Append-only log for recording cache mutations to provide crash recovery.
//
// WAL Record Format:
//   MAGIC (4 bytes): 0x464C5558 ("FLUX")
//   VERSION (1 byte): 0x01
//   SEQUENCE (8 bytes, big-endian u64)
//   TYPE (1 byte): SET=1, DELETE=2, EXPIRE=3
//   KEY_LENGTH (4 bytes, big-endian u32)
//   VALUE_LENGTH (4 bytes, big-endian u32) — 0 for DELETE/EXPIRE
//   TTL_SECS (8 bytes, big-endian u64) — 0 if no TTL (for SET/EXPIRE)
//   PAYLOAD: key bytes + value bytes
//   CHECKSUM (4 bytes, CRC32 of everything before checksum)

use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;
use std::io::{self, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// WAL magic bytes: "FLUX"
const WAL_MAGIC: [u8; 4] = [0x46, 0x4C, 0x55, 0x58];
/// WAL format version
const WAL_VERSION: u8 = 0x01;

/// WAL record header size (magic + version + seq + type + key_len + val_len + ttl)
const HEADER_SIZE: usize = 4 + 1 + 8 + 1 + 4 + 4 + 8; // = 30 bytes
/// Checksum size
const CHECKSUM_SIZE: usize = 4;

/// WAL record type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    Set = 1,
    Delete = 2,
    Expire = 3,
}

impl TryFrom<u8> for WalRecordType {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(WalRecordType::Set),
            2 => Ok(WalRecordType::Delete),
            3 => Ok(WalRecordType::Expire),
            _ => Err(WalError::InvalidRecordType(value)),
        }
    }
}

/// A WAL record representing a single mutation.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub sequence: u64,
    pub record_type: WalRecordType,
    pub key: String,
    pub value: Option<Bytes>,
    pub ttl_secs: u64,
}

/// Errors from WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid magic bytes")]
    InvalidMagic,

    #[error("Unsupported WAL version: {0}")]
    UnsupportedVersion(u8),

    #[error("Invalid record type: {0}")]
    InvalidRecordType(u8),

    #[error("Checksum mismatch: expected {expected:#x}, got {actual:#x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Corrupt record at sequence {0}")]
    CorruptRecord(u64),

    #[error("Truncated record")]
    TruncatedRecord,

    #[error("Invalid UTF-8 in key")]
    InvalidKey,
}

/// Write-ahead log writer.
pub struct WalWriter {
    writer: BufWriter<std::fs::File>,
    sequence: u64,
    path: PathBuf,
    bytes_written: u64,
    broadcast_tx: Option<tokio::sync::broadcast::Sender<Bytes>>,
}

impl WalWriter {
    /// Open or create a WAL file for appending.
    pub fn open(
        path: &Path,
        broadcast_tx: Option<tokio::sync::broadcast::Sender<Bytes>>,
    ) -> Result<Self, WalError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        // Determine current sequence from existing records
        let sequence = if path.exists() && std::fs::metadata(path)?.len() > 0 {
            let records = WalReader::read_all(path)?;
            records.last().map_or(0, |r| r.sequence)
        } else {
            0
        };

        let bytes_written = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

        Ok(WalWriter {
            writer: BufWriter::new(file),
            sequence,
            path: path.to_path_buf(),
            bytes_written,
            broadcast_tx,
        })
    }

    /// Append a SET record to the WAL.
    pub fn append_set(&mut self, key: &str, value: &[u8], ttl_secs: u64) -> Result<u64, WalError> {
        self.sequence += 1;
        let record = WalRecord {
            sequence: self.sequence,
            record_type: WalRecordType::Set,
            key: key.to_string(),
            value: Some(Bytes::copy_from_slice(value)),
            ttl_secs,
        };
        self.write_record(&record)?;
        Ok(self.sequence)
    }

    /// Append a DELETE record to the WAL.
    pub fn append_delete(&mut self, key: &str) -> Result<u64, WalError> {
        self.sequence += 1;
        let record = WalRecord {
            sequence: self.sequence,
            record_type: WalRecordType::Delete,
            key: key.to_string(),
            value: None,
            ttl_secs: 0,
        };
        self.write_record(&record)?;
        Ok(self.sequence)
    }

    /// Append an EXPIRE record to the WAL.
    pub fn append_expire(&mut self, key: &str, ttl_secs: u64) -> Result<u64, WalError> {
        self.sequence += 1;
        let record = WalRecord {
            sequence: self.sequence,
            record_type: WalRecordType::Expire,
            key: key.to_string(),
            value: None,
            ttl_secs,
        };
        self.write_record(&record)?;
        Ok(self.sequence)
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Get total bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Get the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Write a single record to the WAL.
    fn write_record(&mut self, record: &WalRecord) -> Result<(), WalError> {
        let key_bytes = record.key.as_bytes();
        let value_bytes = record.value.as_deref().unwrap_or(&[]);

        let mut buf =
            Vec::with_capacity(HEADER_SIZE + key_bytes.len() + value_bytes.len() + CHECKSUM_SIZE);

        // Magic
        buf.extend_from_slice(&WAL_MAGIC);
        // Version
        buf.push(WAL_VERSION);
        // Sequence
        buf.extend_from_slice(&record.sequence.to_be_bytes());
        // Type
        buf.push(record.record_type as u8);
        // Key length
        buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
        // Value length
        buf.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
        // TTL
        buf.extend_from_slice(&record.ttl_secs.to_be_bytes());
        // Key
        buf.extend_from_slice(key_bytes);
        // Value
        buf.extend_from_slice(value_bytes);

        // Checksum (CRC32 of everything before checksum)
        let mut hasher = Crc32Hasher::new();
        hasher.update(&buf);
        let checksum = hasher.finalize();
        buf.extend_from_slice(&checksum.to_be_bytes());

        self.writer.write_all(&buf)?;
        self.bytes_written += buf.len() as u64;

        if let Some(ref tx) = self.broadcast_tx {
            let _ = tx.send(Bytes::copy_from_slice(&buf));
        }

        Ok(())
    }
}

/// WAL reader for recovery.
pub struct WalReader;

impl WalReader {
    /// Read all valid records from a WAL file.
    /// Handles corrupted tail gracefully (stops at first corrupt record).
    pub fn read_all(path: &Path) -> Result<Vec<WalRecord>, WalError> {
        let mut file = std::fs::File::open(path)?;
        let file_len = file.metadata()?.len() as usize;
        let mut records = Vec::new();
        let mut pos = 0;

        while pos < file_len {
            match Self::read_record(&mut file, pos) {
                Ok(record) => {
                    let key_len = record.key.len();
                    let val_len = record.value.as_ref().map_or(0, |v| v.len());
                    pos += HEADER_SIZE + key_len + val_len + CHECKSUM_SIZE;
                    records.push(record);
                }
                Err(WalError::TruncatedRecord) => {
                    // Corrupted tail — stop here (crash recovery)
                    tracing::warn!(
                        "WAL truncated at position {pos}, recovering {} records",
                        records.len()
                    );
                    break;
                }
                Err(WalError::ChecksumMismatch { .. }) => {
                    tracing::warn!(
                        "WAL checksum mismatch at position {pos}, stopping recovery at {} records",
                        records.len()
                    );
                    break;
                }
                Err(WalError::InvalidMagic) => {
                    tracing::warn!(
                        "WAL invalid magic at position {pos}, stopping recovery at {} records",
                        records.len()
                    );
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Read a single record from the file at the given position.
    fn read_record(file: &mut std::fs::File, pos: usize) -> Result<WalRecord, WalError> {
        file.seek(io::SeekFrom::Start(pos as u64))?;

        // Read header
        let mut header = [0u8; HEADER_SIZE];
        match file.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalError::TruncatedRecord);
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        // Validate magic
        if header[0..4] != WAL_MAGIC {
            return Err(WalError::InvalidMagic);
        }

        // Validate version
        let version = header[4];
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }

        // Parse header fields
        let sequence = u64::from_be_bytes(header[5..13].try_into().unwrap());
        let record_type = WalRecordType::try_from(header[13])?;
        let key_len = u32::from_be_bytes(header[14..18].try_into().unwrap()) as usize;
        let val_len = u32::from_be_bytes(header[18..22].try_into().unwrap()) as usize;
        let ttl_secs = u64::from_be_bytes(header[22..30].try_into().unwrap());

        // Read payload
        let payload_len = key_len + val_len;
        let mut payload = vec![0u8; payload_len];
        match file.read_exact(&mut payload) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalError::TruncatedRecord);
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        // Read checksum
        let mut checksum_bytes = [0u8; 4];
        match file.read_exact(&mut checksum_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalError::TruncatedRecord);
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        let stored_checksum = u32::from_be_bytes(checksum_bytes);

        // Verify checksum
        let mut hasher = Crc32Hasher::new();
        hasher.update(&header);
        hasher.update(&payload);
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum {
            return Err(WalError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        // Parse key and value
        let key =
            String::from_utf8(payload[..key_len].to_vec()).map_err(|_| WalError::InvalidKey)?;

        let value = if val_len > 0 {
            Some(Bytes::copy_from_slice(&payload[key_len..]))
        } else {
            None
        };

        Ok(WalRecord {
            sequence,
            record_type,
            key,
            value,
            ttl_secs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        // Write records
        {
            let mut writer = WalWriter::open(&wal_path, None).unwrap();
            writer.append_set("key1", b"value1", 0).unwrap();
            writer.append_set("key2", b"value2", 60).unwrap();
            writer.append_delete("key1").unwrap();
            writer.append_expire("key2", 120).unwrap();
            writer.sync().unwrap();
        }

        // Read records
        let records = WalReader::read_all(&wal_path).unwrap();
        assert_eq!(records.len(), 4);

        assert_eq!(records[0].sequence, 1);
        assert_eq!(records[0].record_type, WalRecordType::Set);
        assert_eq!(records[0].key, "key1");
        assert_eq!(records[0].value.as_deref(), Some(b"value1".as_slice()));

        assert_eq!(records[1].sequence, 2);
        assert_eq!(records[1].key, "key2");
        assert_eq!(records[1].ttl_secs, 60);

        assert_eq!(records[2].sequence, 3);
        assert_eq!(records[2].record_type, WalRecordType::Delete);

        assert_eq!(records[3].sequence, 4);
        assert_eq!(records[3].record_type, WalRecordType::Expire);
        assert_eq!(records[3].ttl_secs, 120);
    }

    #[test]
    fn test_wal_empty_file() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("empty.wal");
        std::fs::write(&wal_path, b"").unwrap();

        let records = WalReader::read_all(&wal_path).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_wal_corrupted_tail() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("corrupt.wal");

        // Write a valid record
        {
            let mut writer = WalWriter::open(&wal_path, None).unwrap();
            writer.append_set("key1", b"value1", 0).unwrap();
            writer.sync().unwrap();
        }

        // Append garbage to simulate crash
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            f.write_all(b"GARBAGE_DATA").unwrap();
        }

        // Should recover the valid record and ignore the corruption
        let records = WalReader::read_all(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "key1");
    }

    #[test]
    fn test_wal_checksum_corruption() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("badcrc.wal");

        // Write a valid record
        {
            let mut writer = WalWriter::open(&wal_path, None).unwrap();
            writer.append_set("key1", b"value1", 0).unwrap();
            writer.sync().unwrap();
        }

        // Corrupt a byte in the middle of the file
        {
            let mut data = std::fs::read(&wal_path).unwrap();
            if data.len() > 20 {
                data[20] ^= 0xFF; // Flip bits
            }
            std::fs::write(&wal_path, data).unwrap();
        }

        // Should detect the corruption
        let records = WalReader::read_all(&wal_path).unwrap();
        assert!(records.is_empty()); // First (only) record is corrupt
    }

    #[test]
    fn test_wal_append_resume() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("resume.wal");

        // First session
        {
            let mut writer = WalWriter::open(&wal_path, None).unwrap();
            writer.append_set("key1", b"value1", 0).unwrap();
            writer.sync().unwrap();
        }

        // Second session — should continue from last sequence
        {
            let mut writer = WalWriter::open(&wal_path, None).unwrap();
            assert_eq!(writer.sequence(), 1);
            writer.append_set("key2", b"value2", 0).unwrap();
            writer.sync().unwrap();
        }

        let records = WalReader::read_all(&wal_path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence, 1);
        assert_eq!(records[1].sequence, 2);
    }

    #[test]
    fn test_wal_large_value() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("large.wal");

        let large_value = vec![0xAB; 100_000];

        {
            let mut writer = WalWriter::open(&wal_path, None).unwrap();
            writer.append_set("bigkey", &large_value, 0).unwrap();
            writer.sync().unwrap();
        }

        let records = WalReader::read_all(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref().unwrap().len(), 100_000);
    }
}
