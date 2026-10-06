// FluxCache Client - Connection handling

use crate::protocol::{format_command, parse_response, ProtocolError, Response};
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("Connection timed out")]
    Timeout,
    #[error("Server error: {0}")]
    Server(String),
}

/// A single TCP connection to a FluxCache server
pub struct Connection {
    stream: TcpStream,
    buf: BytesMut,
}

impl Connection {
    /// Connect to a FluxCache server
    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Connection {
            stream,
            buf: BytesMut::with_capacity(4096),
        })
    }

    /// Send a raw command and wait for the response
    pub async fn execute_raw(&mut self, cmd: &str, args: &[&[u8]]) -> Result<Response, Error> {
        let payload = format_command(cmd, args);
        self.stream.write_all(&payload).await?;

        loop {
            // Try to parse from existing buffer
            match parse_response(&mut self.buf) {
                Ok(Some(response)) => return Ok(response),
                Ok(None) => {
                    // Need more data
                    if self.buf.capacity() == self.buf.len() {
                        self.buf.reserve(4096);
                    }
                    let n = self.stream.read_buf(&mut self.buf).await?;
                    if n == 0 {
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "Connection closed by server",
                        )));
                    }
                }
                Err(ProtocolError::ServerError(e)) => return Err(Error::Server(e)),
                Err(e) => return Err(Error::Protocol(e)),
            }
        }
    }

    /// PING the server
    pub async fn ping(&mut self) -> Result<bool, Error> {
        match self.execute_raw("PING", &[]).await? {
            Response::Ok(msg) if msg == "PONG" => Ok(true),
            _ => Ok(false),
        }
    }

    /// GET a value
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Error> {
        match self.execute_raw("GET", &[key.as_bytes()]).await? {
            Response::Data(data) => Ok(Some(data)),
            Response::Null => Ok(None),
            resp => Err(Error::Protocol(ProtocolError::ServerError(format!(
                "Unexpected response: {:?}",
                resp
            )))),
        }
    }

    /// SET a value
    pub async fn set(
        &mut self,
        key: &str,
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Result<(), Error> {
        let mut args: Vec<&[u8]> = vec![key.as_bytes(), value];
        let ttl_str;
        if let Some(ttl) = ttl_secs {
            args.push(b"EX");
            ttl_str = ttl.to_string();
            args.push(ttl_str.as_bytes());
        }

        match self.execute_raw("SET", &args).await? {
            Response::Ok(_) => Ok(()),
            resp => Err(Error::Protocol(ProtocolError::ServerError(format!(
                "Unexpected response: {:?}",
                resp
            )))),
        }
    }

    /// DEL a value
    pub async fn del(&mut self, key: &str) -> Result<bool, Error> {
        match self.execute_raw("DEL", &[key.as_bytes()]).await? {
            Response::Integer(1) => Ok(true),
            Response::Integer(0) => Ok(false),
            resp => Err(Error::Protocol(ProtocolError::ServerError(format!(
                "Unexpected response: {:?}",
                resp
            )))),
        }
    }

    /// EXISTS check
    pub async fn exists(&mut self, key: &str) -> Result<bool, Error> {
        match self.execute_raw("EXISTS", &[key.as_bytes()]).await? {
            Response::Integer(1) => Ok(true),
            Response::Integer(0) => Ok(false),
            resp => Err(Error::Protocol(ProtocolError::ServerError(format!(
                "Unexpected response: {:?}",
                resp
            )))),
        }
    }

    /// TTL check
    pub async fn ttl(&mut self, key: &str) -> Result<Option<Option<u64>>, Error> {
        match self.execute_raw("TTL", &[key.as_bytes()]).await? {
            Response::Integer(-2) => Ok(None),       // Not found
            Response::Integer(-1) => Ok(Some(None)), // Found but no TTL
            Response::Integer(secs) if secs >= 0 => Ok(Some(Some(secs as u64))),
            resp => Err(Error::Protocol(ProtocolError::ServerError(format!(
                "Unexpected response: {:?}",
                resp
            )))),
        }
    }
}
