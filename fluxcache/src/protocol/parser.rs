// FluxCache - Protocol Parser
//
// Parses incoming TCP frames into Command structures.
//
// Wire format (length-prefixed framing):
//
//   [4 bytes: frame length (big-endian u32)][frame payload]
//
// Frame payload is a text-based command:
//   PING\r\n
//   GET <key>\r\n
//   SET <key> <length>\r\n<value bytes>  (optionally: EX <seconds>)
//   DEL <key>\r\n
//   EXISTS <key>\r\n
//   EXPIRE <key> <seconds>\r\n
//   TTL <key>\r\n
//   STATS\r\n
//
// For simplicity, we also support a line-based mode (newline-delimited)
// for interactive/telnet use.

use crate::protocol::command::Command;
use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;

/// Maximum frame size: 1 MB
const MAX_FRAME_SIZE: u32 = 1_048_576;

/// Errors during protocol parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Incomplete frame")]
    Incomplete,

    #[error("Frame too large: {0} bytes (max: {MAX_FRAME_SIZE})")]
    FrameTooLarge(u32),

    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    #[error("Missing argument: {0}")]
    MissingArgument(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Try to extract a complete frame from the buffer.
/// Returns the frame payload if complete, or None if more data is needed.
///
/// Supports two modes:
/// 1. Length-prefixed: [4-byte big-endian length][payload]
/// 2. Line-based: payload terminated by \r\n or \n
pub fn extract_frame(buf: &mut BytesMut) -> Result<Option<BytesMut>, ParseError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // Check if first byte looks like a length prefix (non-printable / high byte)
    // or a text command. We use a heuristic: if the first 4 bytes form a
    // reasonable length (< MAX_FRAME_SIZE), treat as length-prefixed.
    // Otherwise, treat as line-based.

    if buf.len() >= 4 {
        let len_bytes = [buf[0], buf[1], buf[2], buf[3]];
        let frame_len = u32::from_be_bytes(len_bytes);

        // Check if this looks like a binary length prefix
        // (the first byte would be non-ASCII for large frames,
        // or we check if it's a reasonable size and the payload
        // after would be valid)
        if buf[0] == 0 || (buf[0] < 0x20 && buf[0] != b'\r' && buf[0] != b'\n') {
            // Binary framing mode
            if frame_len > MAX_FRAME_SIZE {
                return Err(ParseError::FrameTooLarge(frame_len));
            }

            let total_len = 4 + frame_len as usize;
            if buf.len() < total_len {
                return Ok(None); // Need more data
            }

            buf.advance(4);
            let frame = buf.split_to(frame_len as usize);
            return Ok(Some(frame));
        }
    }

    // Line-based mode: look for \r\n or \n
    // But we need to handle SET specially since it has inline binary data
    if let Some(pos) = find_line_end(buf) {
        let line = &buf[..pos];
        let line_str = String::from_utf8_lossy(line);
        let upper = line_str.trim().to_uppercase();

        // Check if this is a SET command that needs to read value data
        if upper.starts_with("SET ") {
            let parts: Vec<&str> = line_str.trim().splitn(4, ' ').collect();
            if parts.len() >= 3 {
                if let Ok(value_len) = parts[2].parse::<usize>() {
                    // Need: line + \r\n + value_len bytes
                    let line_end = if pos + 2 <= buf.len() && buf[pos] == b'\r' {
                        pos + 2
                    } else {
                        pos + 1
                    };
                    let total_needed = line_end + value_len;
                    if buf.len() < total_needed {
                        return Ok(None); // Need more data for the value
                    }
                    let frame = buf.split_to(total_needed);
                    // Remove trailing \r\n from line portion
                    return Ok(Some(frame));
                }
            }
        }

        // Regular line-based command
        let end = if pos + 2 <= buf.len() && buf[pos] == b'\r' && buf[pos + 1] == b'\n' {
            pos + 2
        } else {
            pos + 1
        };

        let frame = buf.split_to(end);
        Ok(Some(frame))
    } else if buf.len() > MAX_FRAME_SIZE as usize {
        Err(ParseError::FrameTooLarge(buf.len() as u32))
    } else {
        Ok(None) // Need more data
    }
}

/// Find the position of line ending (\r\n or \n) in the buffer.
fn find_line_end(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len() {
        if buf[i] == b'\n' {
            return Some(if i > 0 && buf[i - 1] == b'\r' {
                i - 1
            } else {
                i
            });
        }
    }
    None
}

/// Parse a frame payload into a Command.
pub fn parse_command(frame: &[u8]) -> Result<Command, ParseError> {
    // Convert to string, handling the SET value case specially
    let frame_str = String::from_utf8_lossy(frame);
    let trimmed = frame_str.trim();

    if trimmed.is_empty() {
        return Err(ParseError::InvalidCommand("empty command".to_string()));
    }

    // Split on first whitespace to get the command verb
    let (verb, rest) = match trimmed.split_once(char::is_whitespace) {
        Some((v, r)) => (v.to_uppercase(), r.trim()),
        None => (trimmed.to_uppercase(), ""),
    };

    match verb.as_str() {
        "PING" => Ok(Command::Ping),

        "GET" => {
            if rest.is_empty() {
                return Err(ParseError::MissingArgument("key".to_string()));
            }
            Ok(Command::Get {
                key: rest.to_string(),
            })
        }

        "SET" => parse_set_command(rest, frame),

        "DEL" => {
            if rest.is_empty() {
                return Err(ParseError::MissingArgument("key".to_string()));
            }
            Ok(Command::Del {
                key: rest.to_string(),
            })
        }

        "EXISTS" => {
            if rest.is_empty() {
                return Err(ParseError::MissingArgument("key".to_string()));
            }
            Ok(Command::Exists {
                key: rest.to_string(),
            })
        }

        "EXPIRE" => {
            let parts: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
            if parts.len() < 2 {
                return Err(ParseError::MissingArgument("key and seconds".to_string()));
            }
            let seconds = parts[1].trim().parse::<u64>().map_err(|_| {
                ParseError::InvalidArgument(format!("invalid seconds: {}", parts[1]))
            })?;
            Ok(Command::Expire {
                key: parts[0].to_string(),
                seconds,
            })
        }

        "TTL" => {
            if rest.is_empty() {
                return Err(ParseError::MissingArgument("key".to_string()));
            }
            Ok(Command::Ttl {
                key: rest.to_string(),
            })
        }

        "STATS" => Ok(Command::Stats),

        _ => Err(ParseError::InvalidCommand(format!(
            "unknown command: {verb}"
        ))),
    }
}

/// Parse a SET command with its arguments.
/// Supports two formats:
///   SET key value [EX seconds]         -- inline value (no spaces in value)
///   SET key <length>\r\n<value bytes>  -- length-prefixed value
fn parse_set_command(rest: &str, raw_frame: &[u8]) -> Result<Command, ParseError> {
    let parts: Vec<&str> = rest.splitn(4, char::is_whitespace).collect();
    if parts.is_empty() {
        return Err(ParseError::MissingArgument("key".to_string()));
    }
    if parts.len() < 2 {
        return Err(ParseError::MissingArgument("value".to_string()));
    }

    let key = parts[0].to_string();

    // Check if parts[1] is a length (numeric) -> binary value follows
    if let Ok(value_len) = parts[1].parse::<usize>() {
        // Find where the line ends and value begins
        let frame_str = String::from_utf8_lossy(raw_frame);
        if let Some(nl_pos) = frame_str.find('\n') {
            let value_start = nl_pos + 1;
            if raw_frame.len() >= value_start + value_len {
                let value =
                    Bytes::copy_from_slice(&raw_frame[value_start..value_start + value_len]);

                // Check for EX argument after the value length on the command line
                let ttl_secs = if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("EX") {
                    Some(parts[3].parse::<u64>().map_err(|_| {
                        ParseError::InvalidArgument(format!("invalid TTL: {}", parts[3]))
                    })?)
                } else {
                    None
                };

                return Ok(Command::Set {
                    key,
                    value,
                    ttl_secs,
                });
            }
        }
    }

    // Inline value mode: SET key value [EX seconds]
    let value = Bytes::from(parts[1].to_string());
    let ttl_secs = if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("EX") {
        Some(
            parts[3]
                .trim()
                .parse::<u64>()
                .map_err(|_| ParseError::InvalidArgument(format!("invalid TTL: {}", parts[3])))?,
        )
    } else {
        None
    };

    Ok(Command::Set {
        key,
        value,
        ttl_secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let cmd = parse_command(b"PING\r\n").unwrap();
        assert_eq!(cmd, Command::Ping);
    }

    #[test]
    fn test_parse_ping_lowercase() {
        let cmd = parse_command(b"ping\r\n").unwrap();
        assert_eq!(cmd, Command::Ping);
    }

    #[test]
    fn test_parse_get() {
        let cmd = parse_command(b"GET mykey\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Get {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_set_inline() {
        let cmd = parse_command(b"SET mykey myvalue\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: "mykey".to_string(),
                value: Bytes::from("myvalue"),
                ttl_secs: None,
            }
        );
    }

    #[test]
    fn test_parse_set_with_ttl() {
        let cmd = parse_command(b"SET mykey myvalue EX 60\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: "mykey".to_string(),
                value: Bytes::from("myvalue"),
                ttl_secs: Some(60),
            }
        );
    }

    #[test]
    fn test_parse_del() {
        let cmd = parse_command(b"DEL mykey\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Del {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_exists() {
        let cmd = parse_command(b"EXISTS mykey\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Exists {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_expire() {
        let cmd = parse_command(b"EXPIRE mykey 300\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Expire {
                key: "mykey".to_string(),
                seconds: 300,
            }
        );
    }

    #[test]
    fn test_parse_ttl() {
        let cmd = parse_command(b"TTL mykey\r\n").unwrap();
        assert_eq!(
            cmd,
            Command::Ttl {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_stats() {
        let cmd = parse_command(b"STATS\r\n").unwrap();
        assert_eq!(cmd, Command::Stats);
    }

    #[test]
    fn test_parse_unknown_command() {
        let result = parse_command(b"UNKNOWN\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty() {
        let result = parse_command(b"\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_get_missing_key() {
        let result = parse_command(b"GET\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_line_frame() {
        let mut buf = BytesMut::from("PING\r\n");
        let frame = extract_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&frame[..], b"PING\r\n");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_extract_incomplete_frame() {
        let mut buf = BytesMut::from("PIN");
        let result = extract_frame(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_multiple_frames() {
        let mut buf = BytesMut::from("PING\r\nGET key\r\n");
        let frame1 = extract_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&frame1[..], b"PING\r\n");
        let frame2 = extract_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&frame2[..], b"GET key\r\n");
    }
}
