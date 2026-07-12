//! RESP (REdis Serialization Protocol) wire codec.
//!
//! This module is the heart of the project. RESP frames travel over a *streaming* TCP
//! connection, so a single `read()` may return a partial frame, exactly one frame, or
//! several frames concatenated. The parser must therefore be a **buffering, resumable
//! decoder**: it is fed raw byte slices as they arrive and returns either a complete
//! decoded value or "need more bytes", never panicking on a fragment boundary.
//!
//! RESP2 value types (leading byte):
//!   `+` simple string   `-` error   `:` integer
//!   `$` bulk string     `*` array
//! Every element is terminated by CRLF (`\r\n`).
//!
//! Clients send commands as an array of bulk strings, e.g. `SET foo bar` on the wire is:
//!   `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

/// A decoded RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    /// Null bulk string (`$-1\r\n`) — the RESP way to say "no value".
    Null,
    Array(Vec<Value>),
}

/// Errors the decoder can surface. `Incomplete` is *not* fatal — it means "call me again
/// once more bytes have been appended to the buffer".
#[derive(Debug)]
pub enum ParseError {
    /// The buffer does not yet contain a full frame. Caller should read more from the socket.
    Incomplete,
    /// The bytes present violate RESP framing (bad length prefix, unknown type byte, ...).
    Protocol(String),
}

/// A resumable RESP decoder that owns an internal read buffer.
///
/// Usage contract (implemented in Этап 1):
///   1. `feed(&bytes_from_socket)` appends whatever `read()` returned.
///   2. `try_parse()` returns `Ok(value)` for each complete frame, or `Err(Incomplete)`
///      when more bytes are required. Consumed bytes are drained from the buffer so the
///      next call resumes cleanly after a fragment boundary.
#[derive(Default)]
pub struct Parser {
    // TODO(Этап 1): internal Vec<u8> accumulation buffer + cursor bookkeeping.
    _buf: Vec<u8>,
}

impl Parser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly-read bytes to the internal buffer.
    pub fn feed(&mut self, _bytes: &[u8]) {
        // TODO(Этап 1): self._buf.extend_from_slice(_bytes);
        unimplemented!("TODO(Этап 1): implement RESP buffered feed")
    }

    /// Attempt to decode one complete frame from the buffered bytes.
    pub fn try_parse(&mut self) -> Result<Value, ParseError> {
        // TODO(Этап 1): decode a single frame; return Err(ParseError::Incomplete) on a
        //               fragment boundary without consuming partial bytes.
        Err(ParseError::Incomplete)
    }
}

/// Serialize a [`Value`] into RESP wire bytes for the reply path.
pub fn encode(_value: &Value) -> Vec<u8> {
    // TODO(Этап 1): implement the encoder (mirror of the decoder).
    unimplemented!("TODO(Этап 1): implement RESP encode")
}
