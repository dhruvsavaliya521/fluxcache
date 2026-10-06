// FluxCache - Protocol Response
//
// Defines response types and their wire encoding.

use bytes::{BufMut, Bytes, BytesMut};

/// A response from the FluxCache server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// Simple string response (e.g., "OK", "PONG")
    Ok(String),

    /// Bulk data response
    Data(Bytes),

    /// Integer response
    Integer(i64),

    /// Null / not found
    Null,

    /// Error response
    Error(String),
}

impl Response {
    /// Encode the response to the FluxCache wire format.
    ///
    /// Wire format:
    /// - '+' prefix for simple strings: +OK\r\n
    /// - '$' prefix for bulk data: $<len>\r\n<data>\r\n
    /// - ':' prefix for integers: :42\r\n
    /// - '_' prefix for null: _\r\n
    /// - '-' prefix for errors: -ERR message\r\n
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(128);

        match self {
            Response::Ok(msg) => {
                buf.put_u8(b'+');
                buf.put_slice(msg.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Response::Data(data) => {
                buf.put_u8(b'$');
                buf.put_slice(data.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }
            Response::Integer(n) => {
                buf.put_u8(b':');
                buf.put_slice(n.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }
            Response::Null => {
                buf.put_slice(b"_\r\n");
            }
            Response::Error(msg) => {
                buf.put_u8(b'-');
                buf.put_slice(msg.as_bytes());
                buf.put_slice(b"\r\n");
            }
        }

        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_ok() {
        let resp = Response::Ok("OK".to_string());
        assert_eq!(resp.encode(), Bytes::from("+OK\r\n"));
    }

    #[test]
    fn test_encode_data() {
        let resp = Response::Data(Bytes::from("hello"));
        assert_eq!(resp.encode(), Bytes::from("$5\r\nhello\r\n"));
    }

    #[test]
    fn test_encode_integer() {
        let resp = Response::Integer(42);
        assert_eq!(resp.encode(), Bytes::from(":42\r\n"));
    }

    #[test]
    fn test_encode_null() {
        let resp = Response::Null;
        assert_eq!(resp.encode(), Bytes::from("_\r\n"));
    }

    #[test]
    fn test_encode_error() {
        let resp = Response::Error("ERR unknown command".to_string());
        assert_eq!(resp.encode(), Bytes::from("-ERR unknown command\r\n"));
    }

    #[test]
    fn test_encode_negative_integer() {
        let resp = Response::Integer(-1);
        assert_eq!(resp.encode(), Bytes::from(":-1\r\n"));
    }

    #[test]
    fn test_encode_empty_data() {
        let resp = Response::Data(Bytes::new());
        assert_eq!(resp.encode(), Bytes::from("$0\r\n\r\n"));
    }
}
