// FluxCache Client - Protocol Encoding/Decoding

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Incomplete response")]
    Incomplete,
    #[error("Invalid response format")]
    InvalidFormat,
    #[error("Server returned an error: {0}")]
    ServerError(String),
}

/// A response from the FluxCache server
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok(String),
    Data(Bytes),
    Integer(i64),
    Null,
}

/// Format a command for sending to the server
pub fn format_command(cmd: &str, args: &[&[u8]]) -> Bytes {
    let mut buf = BytesMut::with_capacity(128);

    // Most commands are simple text: COMMAND ARG1 ARG2\r\n
    // However, if we're setting binary data, we use length prefixed format:
    // SET KEY LENGTH\r\nDATA

    if cmd.eq_ignore_ascii_case("SET") && args.len() >= 2 {
        let key = args[0];
        let value = args[1];

        buf.put_slice(b"SET ");
        buf.put_slice(key);
        buf.put_u8(b' ');
        buf.put_slice(value.len().to_string().as_bytes());

        // Handle optional TTL (EX seconds)
        if args.len() >= 4 && args[2].eq_ignore_ascii_case(b"EX") {
            buf.put_slice(b" EX ");
            buf.put_slice(args[3]);
        }

        buf.put_slice(b"\r\n");
        buf.put_slice(value);
        buf.put_slice(b"\r\n"); // End of frame

        return buf.freeze();
    }

    // Standard space-separated command
    buf.put_slice(cmd.as_bytes());
    for arg in args {
        buf.put_u8(b' ');
        buf.put_slice(arg);
    }
    buf.put_slice(b"\r\n");

    buf.freeze()
}

/// Parse a response from the server wire format
pub fn parse_response(buf: &mut BytesMut) -> Result<Option<Response>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let prefix = buf[0];

    match prefix {
        b'+' => {
            // Simple string: +OK\r\n
            if let Some(pos) = find_crlf(buf) {
                let line = buf.split_to(pos + 2);
                let msg = String::from_utf8_lossy(&line[1..line.len() - 2]).to_string();
                Ok(Some(Response::Ok(msg)))
            } else {
                Ok(None)
            }
        }
        b'$' => {
            // Bulk string: $<length>\r\n<data>\r\n
            if let Some(pos) = find_crlf(buf) {
                let len_str = String::from_utf8_lossy(&buf[1..pos]);
                let len: usize = len_str.parse().map_err(|_| ProtocolError::InvalidFormat)?;

                let total_needed = pos + 2 + len + 2; // length line + \r\n + data + \r\n
                if buf.len() >= total_needed {
                    let _length_line = buf.split_to(pos + 2);
                    let data = buf.split_to(len);
                    buf.advance(2); // Skip trailing \r\n
                    Ok(Some(Response::Data(data.freeze())))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
        b':' => {
            // Integer: :42\r\n
            if let Some(pos) = find_crlf(buf) {
                let line = buf.split_to(pos + 2);
                let num_str = String::from_utf8_lossy(&line[1..line.len() - 2]);
                let num: i64 = num_str.parse().map_err(|_| ProtocolError::InvalidFormat)?;
                Ok(Some(Response::Integer(num)))
            } else {
                Ok(None)
            }
        }
        b'_' => {
            // Null: _\r\n
            if buf.len() >= 3 && buf[1] == b'\r' && buf[2] == b'\n' {
                buf.advance(3);
                Ok(Some(Response::Null))
            } else if buf.len() < 3 {
                Ok(None)
            } else {
                Err(ProtocolError::InvalidFormat)
            }
        }
        b'-' => {
            // Error: -ERR message\r\n
            if let Some(pos) = find_crlf(buf) {
                let line = buf.split_to(pos + 2);
                let msg = String::from_utf8_lossy(&line[1..line.len() - 2]).to_string();
                Err(ProtocolError::ServerError(msg))
            } else {
                Ok(None)
            }
        }
        _ => Err(ProtocolError::InvalidFormat),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_ping() {
        let cmd = format_command("PING", &[]);
        assert_eq!(cmd, Bytes::from("PING\r\n"));
    }

    #[test]
    fn test_format_get() {
        let cmd = format_command("GET", &[b"mykey"]);
        assert_eq!(cmd, Bytes::from("GET mykey\r\n"));
    }

    #[test]
    fn test_format_set_binary() {
        let cmd = format_command("SET", &[b"mykey", b"hello world"]);
        assert_eq!(cmd, Bytes::from("SET mykey 11\r\nhello world\r\n"));
    }

    #[test]
    fn test_parse_ok() {
        let mut buf = BytesMut::from("+OK\r\n");
        let resp = parse_response(&mut buf).unwrap().unwrap();
        assert_eq!(resp, Response::Ok("OK".to_string()));
    }

    #[test]
    fn test_parse_data() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let resp = parse_response(&mut buf).unwrap().unwrap();
        assert_eq!(resp, Response::Data(Bytes::from("hello")));
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":42\r\n");
        let resp = parse_response(&mut buf).unwrap().unwrap();
        assert_eq!(resp, Response::Integer(42));
    }

    #[test]
    fn test_parse_null() {
        let mut buf = BytesMut::from("_\r\n");
        let resp = parse_response(&mut buf).unwrap().unwrap();
        assert_eq!(resp, Response::Null);
    }
}
