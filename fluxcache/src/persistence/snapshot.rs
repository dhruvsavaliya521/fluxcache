// FluxCache - Snapshot
//
// Creates consistent point-in-time snapshots of cache state for persistence.
// Snapshots are used alongside the WAL for crash recovery:
//   snapshot + WAL records after snapshot = full state
//
// Snapshot Format:
//   MAGIC (4 bytes): "FLXS"
//   VERSION (1 byte): 0x01
//   ENTRY_COUNT (8 bytes, big-endian u64)
//   WAL_SEQUENCE (8 bytes, big-endian u64): WAL sequence at snapshot time
//   ENTRIES:
//     KEY_LEN (4 bytes)
//     VALUE_LEN (4 bytes)
//     TTL_REMAINING_SECS (8 bytes, 0 = no TTL)
//     KEY (key_len bytes)
//     VALUE (value_len bytes)
//   CHECKSUM (4 bytes, CRC32 of everything before)

use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;
use std::io::{self, Write};
use std::path::Path;
use thiserror::Error;

/// Snapshot magic bytes: "FLXS"
const SNAP_MAGIC: [u8; 4] = [0x46, 0x4C, 0x58, 0x53];
const SNAP_VERSION: u8 = 0x01;

/// Errors from snapshot operations.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid snapshot magic")]
    InvalidMagic,

    #[error("Unsupported snapshot version: {0}")]
    UnsupportedVersion(u8),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Corrupted snapshot data")]
    Corrupt,

    #[error("Invalid UTF-8 in key")]
    InvalidKey,
}

/// A single entry in a snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: Bytes,
    pub ttl_remaining_secs: u64,
}

/// Write a snapshot to a generic writer.
pub fn write_snapshot_to_writer<W: Write>(
    writer: &mut W,
    entries: &[(String, Bytes, Option<u64>)],
    wal_sequence: u64,
) -> Result<(), SnapshotError> {
    let mut hasher = Crc32Hasher::new();

    // Header
    let mut header = Vec::with_capacity(21);
    header.extend_from_slice(&SNAP_MAGIC);
    header.push(SNAP_VERSION);
    header.extend_from_slice(&(entries.len() as u64).to_be_bytes());
    header.extend_from_slice(&wal_sequence.to_be_bytes());

    writer.write_all(&header)?;
    hasher.update(&header);

    // Entries
    for (key, value, ttl) in entries {
        let key_bytes = key.as_bytes();
        let ttl_secs = ttl.unwrap_or(0);

        let mut entry_buf = Vec::with_capacity(16 + key_bytes.len() + value.len());
        entry_buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
        entry_buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        entry_buf.extend_from_slice(&ttl_secs.to_be_bytes());
        entry_buf.extend_from_slice(key_bytes);
        entry_buf.extend_from_slice(value);

        writer.write_all(&entry_buf)?;
        hasher.update(&entry_buf);
    }

    // Checksum
    let checksum = hasher.finalize();
    writer.write_all(&checksum.to_be_bytes())?;

    Ok(())
}

/// Write a snapshot to disk.
pub fn write_snapshot(
    path: &Path,
    entries: &[(String, Bytes, Option<u64>)],
    wal_sequence: u64,
) -> Result<(), SnapshotError> {
    let tmp_path = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp_path)?;

    write_snapshot_to_writer(&mut file, entries, wal_sequence)?;

    file.sync_all()?;

    // Atomic rename
    std::fs::rename(&tmp_path, path)?;

    Ok(())
}

/// Read a snapshot from disk.
pub fn read_snapshot(path: &Path) -> Result<(Vec<SnapshotEntry>, u64), SnapshotError> {
    let data = std::fs::read(path)?;

    if data.len() < 21 + 4 {
        // Header (21) + checksum (4)
        return Err(SnapshotError::Corrupt);
    }

    // Verify checksum
    let payload = &data[..data.len() - 4];
    let stored_checksum = u32::from_be_bytes(data[data.len() - 4..].try_into().unwrap());
    let mut hasher = Crc32Hasher::new();
    hasher.update(payload);
    let computed_checksum = hasher.finalize();

    if stored_checksum != computed_checksum {
        return Err(SnapshotError::ChecksumMismatch);
    }

    // Parse header
    if data[0..4] != SNAP_MAGIC {
        return Err(SnapshotError::InvalidMagic);
    }
    if data[4] != SNAP_VERSION {
        return Err(SnapshotError::UnsupportedVersion(data[4]));
    }

    let entry_count = u64::from_be_bytes(data[5..13].try_into().unwrap()) as usize;
    let wal_sequence = u64::from_be_bytes(data[13..21].try_into().unwrap());

    // Parse entries
    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = 21;

    for _ in 0..entry_count {
        if pos + 16 > payload.len() {
            return Err(SnapshotError::Corrupt);
        }

        let key_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        let val_len = u32::from_be_bytes(payload[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let ttl_secs = u64::from_be_bytes(payload[pos + 8..pos + 16].try_into().unwrap());
        pos += 16;

        if pos + key_len + val_len > payload.len() {
            return Err(SnapshotError::Corrupt);
        }

        let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
            .map_err(|_| SnapshotError::InvalidKey)?;
        pos += key_len;

        let value = Bytes::copy_from_slice(&payload[pos..pos + val_len]);
        pos += val_len;

        entries.push(SnapshotEntry {
            key,
            value,
            ttl_remaining_secs: ttl_secs,
        });
    }

    Ok((entries, wal_sequence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_write_read() {
        let tmp = TempDir::new().unwrap();
        let snap_path = tmp.path().join("test.snap");

        let entries = vec![
            ("key1".to_string(), Bytes::from("value1"), None),
            ("key2".to_string(), Bytes::from("value2"), Some(60)),
            ("key3".to_string(), Bytes::from("value3"), Some(120)),
        ];

        write_snapshot(&snap_path, &entries, 42).unwrap();

        let (loaded, wal_seq) = read_snapshot(&snap_path).unwrap();
        assert_eq!(wal_seq, 42);
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].key, "key1");
        assert_eq!(loaded[0].value, Bytes::from("value1"));
        assert_eq!(loaded[0].ttl_remaining_secs, 0);
        assert_eq!(loaded[1].key, "key2");
        assert_eq!(loaded[1].ttl_remaining_secs, 60);
    }

    #[test]
    fn test_snapshot_empty() {
        let tmp = TempDir::new().unwrap();
        let snap_path = tmp.path().join("empty.snap");

        write_snapshot(&snap_path, &[], 0).unwrap();
        let (loaded, seq) = read_snapshot(&snap_path).unwrap();
        assert!(loaded.is_empty());
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_snapshot_corruption() {
        let tmp = TempDir::new().unwrap();
        let snap_path = tmp.path().join("corrupt.snap");

        let entries = vec![("key".to_string(), Bytes::from("value"), None)];
        write_snapshot(&snap_path, &entries, 1).unwrap();

        // Corrupt data
        let mut data = std::fs::read(&snap_path).unwrap();
        data[15] ^= 0xFF;
        std::fs::write(&snap_path, data).unwrap();

        let result = read_snapshot(&snap_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_atomic_write() {
        let tmp = TempDir::new().unwrap();
        let snap_path = tmp.path().join("atomic.snap");

        // Write should not leave a .tmp file
        let entries = vec![("key".to_string(), Bytes::from("value"), None)];
        write_snapshot(&snap_path, &entries, 1).unwrap();

        assert!(snap_path.exists());
        assert!(!snap_path.with_extension("tmp").exists());
    }
}
