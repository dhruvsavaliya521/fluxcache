use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid protocol format")]
    InvalidFormat,
}

/// Commands sent by the replica to the primary
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplCommand {
    /// Request synchronization starting from a specific WAL sequence
    Psync { seq: u64 },
}

impl ReplCommand {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32);
        match self {
            ReplCommand::Psync { seq } => {
                buf.put_slice(b"PSYNC ");
                buf.put_slice(seq.to_string().as_bytes());
                buf.put_slice(b"\n");
            }
        }
        buf.freeze()
    }
}

/// Responses sent by the primary to the replica
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplResponse {
    /// Full resynchronization required. The payload is the entire snapshot file.
    FullResync { data: Bytes },
    /// Can continue from the requested sequence. WAL records will follow.
    Continue,
}
