//! Command layer: dispatches decoded RESP frames onto the shared store.
//!
//! Sits between the protocol codec (`resp`, knows nothing about commands) and the data
//! layer (`store`, knows nothing about RESP/networking). `main.rs` owns the network loop
//! and delegates every complete client frame here.

use crate::resp::Value;
use crate::store::Store;
use std::sync::{Arc, Mutex};

/// Dispatch one decoded client frame (must be a `Value::Array` of `Value::Bulk`) against
/// the shared store, returning the RESP reply to write back.
pub fn dispatch(store: &Arc<Mutex<Store>>, frame: Value) -> Value {
    let args = match extract_args(frame) {
        Ok(args) => args,
        Err(e) => return Value::Error(e),
    };
    if args.is_empty() {
        return Value::Error("ERR empty command".into());
    }
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match name.as_str() {
        "PING" => cmd_ping(&args),
        "SET" => cmd_set(store, &args),
        "GET" => cmd_get(store, &args),
        "DEL" => cmd_del(store, &args),
        _ => Value::Error(format!("ERR unknown command '{name}'")),
    }
}

/// A client frame must be an Array of Bulk strings (that is how redis-cli and real
/// clients send commands). Anything else is a protocol usage error.
fn extract_args(frame: Value) -> Result<Vec<Vec<u8>>, String> {
    match frame {
        Value::Array(items) => items
            .into_iter()
            .map(|v| match v {
                Value::Bulk(b) => Ok(b),
                _ => Err("ERR protocol error: expected bulk string array element".to_string()),
            })
            .collect(),
        _ => Err("ERR protocol error: expected array of bulk strings".to_string()),
    }
}

fn cmd_ping(args: &[Vec<u8>]) -> Value {
    match args.len() {
        1 => Value::Simple("PONG".into()),
        2 => Value::Bulk(args[1].clone()),
        _ => Value::Error("ERR wrong number of arguments for 'ping' command".into()),
    }
}

fn cmd_set(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() != 3 {
        return Value::Error("ERR wrong number of arguments for 'set' command".into());
    }
    let key = args[1].clone();
    let value = args[2].clone();
    // TODO(Этап 3): parse EX/PX options into an expires_at instant.
    store.lock().unwrap().set(key, value, None);
    Value::Simple("OK".into())
}

fn cmd_get(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() != 2 {
        return Value::Error("ERR wrong number of arguments for 'get' command".into());
    }
    match store.lock().unwrap().get(&args[1]) {
        Some(v) => Value::Bulk(v),
        None => Value::Null,
    }
}

fn cmd_del(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() < 2 {
        return Value::Error("ERR wrong number of arguments for 'del' command".into());
    }
    let mut store = store.lock().unwrap();
    let count = args[1..].iter().filter(|k| store.del(k)).count();
    Value::Integer(count as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp;
    use crate::resp::{ParseError, Parser};

    fn new_store() -> Arc<Mutex<Store>> {
        Arc::new(Mutex::new(Store::new()))
    }

    /// Build the `Value::Array` of `Value::Bulk` frame a real client would send.
    fn frame(parts: &[&[u8]]) -> Value {
        Value::Array(parts.iter().map(|p| Value::Bulk(p.to_vec())).collect())
    }

    #[test]
    fn ping_without_argument_returns_pong() {
        let store = new_store();
        assert_eq!(
            dispatch(&store, frame(&[b"PING"])),
            Value::Simple("PONG".into())
        );
    }

    #[test]
    fn ping_with_argument_echoes_it() {
        let store = new_store();
        assert_eq!(
            dispatch(&store, frame(&[b"PING", b"hello"])),
            Value::Bulk(b"hello".to_vec())
        );
    }

    #[test]
    fn ping_with_too_many_arguments_is_an_error() {
        let store = new_store();
        assert!(matches!(
            dispatch(&store, frame(&[b"PING", b"a", b"b"])),
            Value::Error(_)
        ));
    }

    #[test]
    fn set_then_get_round_trip() {
        let store = new_store();
        assert_eq!(
            dispatch(&store, frame(&[b"SET", b"foo", b"bar"])),
            Value::Simple("OK".into())
        );
        assert_eq!(
            dispatch(&store, frame(&[b"GET", b"foo"])),
            Value::Bulk(b"bar".to_vec())
        );
    }

    #[test]
    fn get_missing_key_returns_null() {
        let store = new_store();
        assert_eq!(dispatch(&store, frame(&[b"GET", b"absent"])), Value::Null);
    }

    #[test]
    fn del_existing_and_missing_key() {
        let store = new_store();
        dispatch(&store, frame(&[b"SET", b"foo", b"bar"]));
        assert_eq!(
            dispatch(&store, frame(&[b"DEL", b"foo"])),
            Value::Integer(1)
        );
        assert_eq!(
            dispatch(&store, frame(&[b"DEL", b"foo"])),
            Value::Integer(0)
        );
    }

    #[test]
    fn del_multiple_keys_counts_only_existing() {
        let store = new_store();
        dispatch(&store, frame(&[b"SET", b"a", b"1"]));
        dispatch(&store, frame(&[b"SET", b"b", b"2"]));
        assert_eq!(
            dispatch(&store, frame(&[b"DEL", b"a", b"missing", b"b"])),
            Value::Integer(2)
        );
    }

    #[test]
    fn set_wrong_arity_is_an_error() {
        let store = new_store();
        assert!(matches!(
            dispatch(&store, frame(&[b"SET", b"foo"])),
            Value::Error(_)
        ));
        assert!(matches!(
            dispatch(&store, frame(&[b"SET", b"foo", b"bar", b"extra"])),
            Value::Error(_)
        ));
    }

    #[test]
    fn get_wrong_arity_is_an_error() {
        let store = new_store();
        assert!(matches!(
            dispatch(&store, frame(&[b"GET"])),
            Value::Error(_)
        ));
        assert!(matches!(
            dispatch(&store, frame(&[b"GET", b"a", b"b"])),
            Value::Error(_)
        ));
    }

    #[test]
    fn del_without_keys_is_an_error() {
        let store = new_store();
        assert!(matches!(
            dispatch(&store, frame(&[b"DEL"])),
            Value::Error(_)
        ));
    }

    #[test]
    fn unknown_command_is_an_error() {
        let store = new_store();
        assert_eq!(
            dispatch(&store, frame(&[b"FLUSHALL"])),
            Value::Error("ERR unknown command 'FLUSHALL'".into())
        );
    }

    #[test]
    fn command_name_is_case_insensitive() {
        let store = new_store();
        assert_eq!(
            dispatch(&store, frame(&[b"sEt", b"foo", b"bar"])),
            Value::Simple("OK".into())
        );
        assert_eq!(
            dispatch(&store, frame(&[b"get", b"foo"])),
            Value::Bulk(b"bar".to_vec())
        );
    }

    #[test]
    fn empty_command_array_is_an_error() {
        let store = new_store();
        assert_eq!(
            dispatch(&store, frame(&[])),
            Value::Error("ERR empty command".into())
        );
    }

    #[test]
    fn non_array_frame_is_an_error_without_panic() {
        let store = new_store();
        assert!(matches!(
            dispatch(&store, Value::Simple("PING".into())),
            Value::Error(_)
        ));
        assert!(matches!(
            dispatch(&store, Value::Integer(42)),
            Value::Error(_)
        ));
    }

    #[test]
    fn array_with_non_bulk_element_is_an_error() {
        let store = new_store();
        let bad = Value::Array(vec![Value::Bulk(b"GET".to_vec()), Value::Integer(1)]);
        assert!(matches!(dispatch(&store, bad), Value::Error(_)));
    }

    #[test]
    fn overwriting_existing_key_returns_new_value() {
        let store = new_store();
        dispatch(&store, frame(&[b"SET", b"foo", b"old"]));
        dispatch(&store, frame(&[b"SET", b"foo", b"new"]));
        assert_eq!(
            dispatch(&store, frame(&[b"GET", b"foo"])),
            Value::Bulk(b"new".to_vec())
        );
    }

    #[test]
    fn binary_values_pass_through_unchanged() {
        let store = new_store();
        let key: &[u8] = &[0x00, 0xff, 0x80];
        let value: &[u8] = &[0xde, 0xad, 0x00, 0xbe, 0xef];
        assert_eq!(
            dispatch(&store, frame(&[b"SET", key, value])),
            Value::Simple("OK".into())
        );
        assert_eq!(
            dispatch(&store, frame(&[b"GET", key])),
            Value::Bulk(value.to_vec())
        );
    }

    // --- Deterministic integration fake: real Parser + dispatch + encode, no sockets. ---
    // Simulates the byte stream a client would send, exactly as main::handle_connection
    // would see it, and checks the encoded reply bytes.

    /// Encode a client command as the RESP Array-of-Bulk bytes a real client sends.
    fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
        resp::encode(&frame(parts))
    }

    /// Run raw request bytes through Parser + dispatch, returning the concatenated
    /// encoded replies — the fake counterpart of the socket loop in main.rs.
    fn run_session(store: &Arc<Mutex<Store>>, input: &[u8]) -> Vec<u8> {
        let mut parser = Parser::new();
        parser.feed(input);
        let mut out = Vec::new();
        loop {
            match parser.try_parse() {
                Ok(frame) => out.extend_from_slice(&resp::encode(&dispatch(store, frame))),
                Err(ParseError::Incomplete) => return out,
                Err(ParseError::Protocol(msg)) => panic!("unexpected protocol error: {msg}"),
            }
        }
    }

    #[test]
    fn integration_fake_set_get_del_over_wire_bytes() {
        let store = new_store();
        assert_eq!(
            run_session(&store, &encode_command(&[b"SET", b"foo", b"bar"])),
            b"+OK\r\n"
        );
        assert_eq!(
            run_session(&store, &encode_command(&[b"GET", b"foo"])),
            b"$3\r\nbar\r\n"
        );
        assert_eq!(
            run_session(&store, &encode_command(&[b"DEL", b"foo"])),
            b":1\r\n"
        );
        assert_eq!(
            run_session(&store, &encode_command(&[b"GET", b"foo"])),
            b"$-1\r\n"
        );
    }

    #[test]
    fn integration_fake_pipelined_commands_in_one_chunk() {
        let store = new_store();
        let mut input = encode_command(&[b"SET", b"foo", b"bar"]);
        input.extend_from_slice(&encode_command(&[b"GET", b"foo"]));
        input.extend_from_slice(&encode_command(&[b"PING"]));
        assert_eq!(
            run_session(&store, &input),
            b"+OK\r\n$3\r\nbar\r\n+PONG\r\n"
        );
    }

    #[test]
    fn integration_fake_survives_fragmented_feeding() {
        // The same SET/GET/DEL/GET scenario, but the request bytes arrive split at every
        // possible boundary into two chunks — codec + dispatch must be split-invariant.
        let mut input = encode_command(&[b"SET", b"foo", b"bar"]);
        input.extend_from_slice(&encode_command(&[b"GET", b"foo"]));
        input.extend_from_slice(&encode_command(&[b"DEL", b"foo"]));
        input.extend_from_slice(&encode_command(&[b"GET", b"foo"]));
        let expected: &[u8] = b"+OK\r\n$3\r\nbar\r\n:1\r\n$-1\r\n";

        for split in 1..input.len() {
            let store = new_store();
            let mut parser = Parser::new();
            let mut out = Vec::new();
            for chunk in [&input[..split], &input[split..]] {
                parser.feed(chunk);
                loop {
                    match parser.try_parse() {
                        Ok(frame) => {
                            out.extend_from_slice(&resp::encode(&dispatch(&store, frame)));
                        }
                        Err(ParseError::Incomplete) => break,
                        Err(ParseError::Protocol(msg)) => {
                            panic!("split {split}: unexpected protocol error: {msg}")
                        }
                    }
                }
            }
            assert_eq!(out, expected, "split at byte {split}");
        }
    }
}
