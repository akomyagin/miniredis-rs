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

/// Matches the default `proto-max-bulk-len` of real Redis; rejects absurd length prefixes
/// before any allocation is attempted.
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// Guards against OOM from a hostile `*<huge>\r\n` prefix.
const MAX_ARRAY_LEN: i64 = 1_048_576;

/// A decoded RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    /// Null bulk string (`$-1\r\n`) — the RESP way to say "no value". A null array
    /// (`*-1\r\n`) on input decodes to this same variant; the RESP2 distinction between
    /// null bulk and null array is not preserved at the `Value` level.
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
/// Usage contract:
///   1. `feed(&bytes_from_socket)` appends whatever `read()` returned.
///   2. `try_parse()` returns `Ok(value)` for each complete frame, or `Err(Incomplete)`
///      when more bytes are required. Consumed bytes are drained from the buffer so the
///      next call resumes cleanly after a fragment boundary.
#[derive(Default)]
pub struct Parser {
    // No separate consumption cursor: try_parse parses from the start of `buf` and drains
    // consumed bytes on success, so `buf` always begins at the next unparsed frame boundary.
    buf: Vec<u8>,
}

impl Parser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly-read bytes to the internal buffer.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Attempt to decode one complete frame from the buffered bytes.
    ///
    /// On `Incomplete` or `Protocol` the buffer is left untouched: a protocol error is a
    /// signal for the caller to close the connection rather than resynchronize on a
    /// corrupted stream.
    pub fn try_parse(&mut self) -> Result<Value, ParseError> {
        let (value, consumed) = parse_value(&self.buf)?;
        self.buf.drain(..consumed);
        Ok(value)
    }
}

/// First occurrence of `\r\n` at or after `from`, as an index into `buf`.
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf.get(from..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|i| from + i)
}

/// Parse the line `buf[1..crlf]` as an i64 (length prefixes and `:` integers).
fn parse_i64(bytes: &[u8], what: &str) -> Result<i64, ParseError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| ParseError::Protocol(format!("invalid {what}")))
}

/// Decode one value from the start of `buf`. Returns the value and the number of bytes
/// consumed. Never mutates anything; the caller drains on success.
fn parse_value(buf: &[u8]) -> Result<(Value, usize), ParseError> {
    let Some(&type_byte) = buf.first() else {
        return Err(ParseError::Incomplete);
    };
    match type_byte {
        b'+' | b'-' => {
            let crlf = find_crlf(buf, 1).ok_or(ParseError::Incomplete)?;
            let s = std::str::from_utf8(&buf[1..crlf])
                .map_err(|_| ParseError::Protocol("simple string is not valid UTF-8".into()))?
                .to_owned();
            let value = if type_byte == b'+' {
                Value::Simple(s)
            } else {
                Value::Error(s)
            };
            Ok((value, crlf + 2))
        }
        b':' => {
            let crlf = find_crlf(buf, 1).ok_or(ParseError::Incomplete)?;
            let n = parse_i64(&buf[1..crlf], "integer")?;
            Ok((Value::Integer(n), crlf + 2))
        }
        b'$' => {
            let crlf = find_crlf(buf, 1).ok_or(ParseError::Incomplete)?;
            let len = parse_i64(&buf[1..crlf], "bulk length")?;
            if len == -1 {
                return Ok((Value::Null, crlf + 2));
            }
            if len < -1 {
                return Err(ParseError::Protocol("negative bulk length".into()));
            }
            if len > MAX_BULK_LEN {
                return Err(ParseError::Protocol("bulk length exceeds limit".into()));
            }
            let n = len as usize;
            let body_start = crlf + 2;
            let total = body_start + n + 2;
            if buf.len() < total {
                return Err(ParseError::Incomplete);
            }
            if &buf[body_start + n..total] != b"\r\n" {
                return Err(ParseError::Protocol(
                    "bulk string missing trailing CRLF".into(),
                ));
            }
            Ok((Value::Bulk(buf[body_start..body_start + n].to_vec()), total))
        }
        b'*' => {
            let crlf = find_crlf(buf, 1).ok_or(ParseError::Incomplete)?;
            let count = parse_i64(&buf[1..crlf], "array length")?;
            if count == -1 {
                return Ok((Value::Null, crlf + 2));
            }
            if count < -1 {
                return Err(ParseError::Protocol("negative array length".into()));
            }
            if count > MAX_ARRAY_LEN {
                return Err(ParseError::Protocol("array length exceeds limit".into()));
            }
            // TODO(POST_MVP): no recursion-depth limit — a pathological stream of deeply
            // nested arrays (each frame a few bytes, well under MAX_ARRAY_LEN) can exhaust
            // the stack. Accepted for the pet-project threat model; see TECHNICAL_PLAN.md
            // Этап 1, "Открытые решения".
            let mut items = Vec::with_capacity(count as usize);
            let mut pos = crlf + 2;
            for _ in 0..count {
                let (item, consumed) = parse_value(&buf[pos..])?;
                items.push(item);
                pos += consumed;
            }
            Ok((Value::Array(items), pos))
        }
        _ => Err(ParseError::Protocol("invalid type byte".into())),
    }
}

/// Serialize a [`Value`] into RESP wire bytes for the reply path.
///
/// `Value::Null` always encodes as `$-1\r\n` (null bulk), never `*-1\r\n` — a deliberate
/// encoder simplification.
pub fn encode(value: &Value) -> Vec<u8> {
    match value {
        Value::Simple(s) => format!("+{s}\r\n").into_bytes(),
        Value::Error(s) => format!("-{s}\r\n").into_bytes(),
        Value::Integer(n) => format!(":{n}\r\n").into_bytes(),
        Value::Bulk(b) => {
            let mut out = format!("${}\r\n", b.len()).into_bytes();
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
            out
        }
        Value::Null => b"$-1\r\n".to_vec(),
        Value::Array(items) => {
            let mut out = format!("*{}\r\n", items.len()).into_bytes();
            for item in items {
                out.extend_from_slice(&encode(item));
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode exactly one frame from a complete byte slice.
    fn decode(bytes: &[u8]) -> Result<Value, ParseError> {
        let mut parser = Parser::new();
        parser.feed(bytes);
        parser.try_parse()
    }

    fn assert_protocol(result: Result<Value, ParseError>) {
        assert!(
            matches!(result, Err(ParseError::Protocol(_))),
            "expected Protocol error, got {result:?}"
        );
    }

    fn assert_incomplete(result: Result<Value, ParseError>) {
        assert!(
            matches!(result, Err(ParseError::Incomplete)),
            "expected Incomplete, got {result:?}"
        );
    }

    fn nested_array_sample() -> (Value, Vec<u8>) {
        let value = Value::Array(vec![
            Value::Bulk(b"SET".to_vec()),
            Value::Bulk(b"key".to_vec()),
            Value::Array(vec![Value::Integer(-42), Value::Null]),
            Value::Simple("OK".into()),
        ]);
        let bytes = encode(&value);
        (value, bytes)
    }

    // --- Encoding ---

    #[test]
    fn encodes_simple_string() {
        assert_eq!(encode(&Value::Simple("OK".into())), b"+OK\r\n");
    }

    #[test]
    fn encodes_error() {
        assert_eq!(
            encode(&Value::Error("ERR unknown command".into())),
            b"-ERR unknown command\r\n"
        );
    }

    #[test]
    fn encodes_integer() {
        assert_eq!(encode(&Value::Integer(-42)), b":-42\r\n");
    }

    #[test]
    fn encodes_bulk_string() {
        assert_eq!(encode(&Value::Bulk(b"hello".to_vec())), b"$5\r\nhello\r\n");
    }

    #[test]
    fn encodes_empty_bulk_distinct_from_null() {
        assert_eq!(encode(&Value::Bulk(vec![])), b"$0\r\n\r\n");
        assert_eq!(encode(&Value::Null), b"$-1\r\n");
    }

    #[test]
    fn encodes_array() {
        let v = Value::Array(vec![
            Value::Bulk(b"GET".to_vec()),
            Value::Bulk(b"foo".to_vec()),
        ]);
        assert_eq!(encode(&v), b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    }

    // --- Round-trip ---

    #[test]
    fn roundtrips_every_variant() {
        let values = [
            Value::Simple("PONG".into()),
            Value::Error("ERR oops".into()),
            Value::Integer(i64::MIN),
            Value::Bulk(b"binary\x00\xff\xfe not utf8".to_vec()),
            Value::Bulk(vec![]),
            Value::Null,
            Value::Array(vec![]),
        ];
        for v in values {
            assert_eq!(
                decode(&encode(&v)).unwrap(),
                v,
                "round-trip failed for {v:?}"
            );
        }
    }

    #[test]
    fn roundtrips_nested_array() {
        let (value, bytes) = nested_array_sample();
        assert_eq!(decode(&bytes).unwrap(), value);
    }

    // --- Fragmentation (mandatory set, see SKILL.md) ---

    #[test]
    fn parses_byte_by_byte() {
        let (expected, bytes) = nested_array_sample();
        let mut parser = Parser::new();
        for &b in &bytes[..bytes.len() - 1] {
            parser.feed(&[b]);
            assert_incomplete(parser.try_parse());
        }
        parser.feed(&bytes[bytes.len() - 1..]);
        assert_eq!(parser.try_parse().unwrap(), expected);
        assert_incomplete(parser.try_parse());
    }

    #[test]
    fn parses_random_chunks() {
        let (expected, bytes) = nested_array_sample();
        // Deterministic LCG instead of the `rand` crate: dev-dependencies stay empty.
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        for round in 0..100 {
            let mut parser = Parser::new();
            let mut pos = 0;
            let mut result = None;
            while pos < bytes.len() {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let max = bytes.len() - pos;
                let chunk = 1 + (seed >> 33) as usize % max;
                parser.feed(&bytes[pos..pos + chunk]);
                pos += chunk;
                match parser.try_parse() {
                    Ok(v) => result = Some(v),
                    Err(ParseError::Incomplete) => {}
                    Err(e) => panic!("round {round}: unexpected error {e:?}"),
                }
            }
            assert_eq!(result, Some(expected.clone()), "round {round} diverged");
        }
    }

    #[test]
    fn parses_multiple_frames_in_one_chunk() {
        let mut parser = Parser::new();
        parser.feed(b"+OK\r\n:7\r\n");
        assert_eq!(parser.try_parse().unwrap(), Value::Simple("OK".into()));
        assert_eq!(parser.try_parse().unwrap(), Value::Integer(7));
        assert_incomplete(parser.try_parse());
    }

    #[test]
    fn feed_across_frame_boundary_mixed() {
        // Chunk 1 = whole first frame + start of second; chunk 2 = rest of second.
        let mut parser = Parser::new();
        parser.feed(b"$3\r\nfoo\r\n*2\r\n:1\r");
        assert_eq!(parser.try_parse().unwrap(), Value::Bulk(b"foo".to_vec()));
        assert_incomplete(parser.try_parse());
        parser.feed(b"\n:2\r\n");
        assert_eq!(
            parser.try_parse().unwrap(),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)])
        );
        assert_incomplete(parser.try_parse());
    }

    #[test]
    fn property_split_invariant() {
        fn feed_fragmented(chunks: &[&[u8]]) -> Value {
            let mut parser = Parser::new();
            let mut result = None;
            for chunk in chunks {
                parser.feed(chunk);
                match parser.try_parse() {
                    Ok(v) => result = Some(v),
                    Err(ParseError::Incomplete) => {}
                    Err(e) => panic!("unexpected error {e:?}"),
                }
            }
            result.expect("frame never completed")
        }

        let (expected, bytes) = nested_array_sample();
        // Exhaustive sweep of every 2-way split point of the frame.
        for i in 1..bytes.len() {
            let got = feed_fragmented(&[&bytes[..i], &bytes[i..]]);
            assert_eq!(got, expected, "split at byte {i} diverged");
        }
    }

    // --- Incomplete input ---

    #[test]
    fn empty_buffer_is_incomplete() {
        assert_incomplete(Parser::new().try_parse());
    }

    #[test]
    fn line_without_crlf_is_incomplete() {
        assert_incomplete(decode(b"+OK"));
        assert_incomplete(decode(b"+OK\r"));
        assert_incomplete(decode(b":123"));
    }

    #[test]
    fn bulk_with_partial_body_is_incomplete() {
        assert_incomplete(decode(b"$5\r\nhel"));
        assert_incomplete(decode(b"$5\r\nhello"));
        assert_incomplete(decode(b"$5\r\nhello\r"));
    }

    #[test]
    fn array_with_missing_elements_is_incomplete() {
        assert_incomplete(decode(b"*2\r\n$3\r\nfoo\r\n"));
    }

    #[test]
    fn incomplete_does_not_consume_partial_bytes() {
        let mut parser = Parser::new();
        parser.feed(b"$5\r\nhel");
        assert_incomplete(parser.try_parse());
        // If partial bytes had been consumed, completing the frame would fail.
        parser.feed(b"lo\r\n");
        assert_eq!(parser.try_parse().unwrap(), Value::Bulk(b"hello".to_vec()));
    }

    // --- Protocol errors (must never panic) ---

    #[test]
    fn rejects_unknown_type_byte() {
        assert_protocol(decode(b"@oops\r\n"));
    }

    #[test]
    fn rejects_non_numeric_integer() {
        assert_protocol(decode(b":abc\r\n"));
    }

    #[test]
    fn rejects_invalid_utf8_simple_string() {
        assert_protocol(decode(b"+\xff\xfe\r\n"));
    }

    #[test]
    fn rejects_non_numeric_bulk_length() {
        assert_protocol(decode(b"$abc\r\n"));
    }

    #[test]
    fn rejects_non_numeric_array_length() {
        assert_protocol(decode(b"*abc\r\n"));
    }

    #[test]
    fn rejects_bulk_length_below_minus_one() {
        assert_protocol(decode(b"$-2\r\n"));
    }

    #[test]
    fn rejects_array_length_below_minus_one() {
        assert_protocol(decode(b"*-2\r\n"));
    }

    #[test]
    fn rejects_bulk_length_over_limit_before_allocation() {
        assert_protocol(decode(b"$536870913\r\n"));
    }

    #[test]
    fn rejects_array_length_over_limit() {
        assert_protocol(decode(b"*1048577\r\n"));
    }

    #[test]
    fn rejects_bulk_with_bad_trailing_crlf() {
        // Incomplete while the tail bytes are missing, Protocol once present but wrong.
        assert_incomplete(decode(b"$3\r\nfoo"));
        assert_protocol(decode(b"$3\r\nfooXY"));
    }

    #[test]
    fn nested_protocol_error_propagates() {
        assert_protocol(decode(b"*2\r\n:1\r\n$-5\r\n"));
    }

    // --- Nulls are values, not errors ---

    #[test]
    fn decodes_null_bulk() {
        assert_eq!(decode(b"$-1\r\n").unwrap(), Value::Null);
    }

    #[test]
    fn decodes_null_array_as_null() {
        assert_eq!(decode(b"*-1\r\n").unwrap(), Value::Null);
    }

    #[test]
    fn decodes_null_inside_array() {
        assert_eq!(
            decode(b"*1\r\n$-1\r\n").unwrap(),
            Value::Array(vec![Value::Null])
        );
    }
}
