//! miniredis — a from-scratch in-memory KV server speaking the Redis RESP wire protocol.
//!
//! Concurrency model: thread-per-connection over `std::net` (no async runtime in v1).
//! See docs/TECHNICAL_PLAN.md for the staged build plan.
//!
//! The library root exists so that integration tests (notably `tests/network_e2e.rs`,
//! the only test tier that opens real TCP sockets) can drive `handle_connection` /
//! `spawn_sweeper` directly; `src/main.rs` is a thin binary wrapper around them.

pub mod command;
pub mod resp;
pub mod store;

use resp::{ParseError, Parser};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use store::Store;

/// How often the background sweeper reaps expired keys (fixed by TECHNICAL_PLAN.md, Этап 3).
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Background sweeper: periodically reaps expired keys so they disappear even if never
/// accessed again (lazy expiration alone would leak them). The thread is detached and
/// never joined — the process exits via Ctrl-C/signal like every other thread (no
/// graceful shutdown in v1; see TECHNICAL_PLAN.md, Этап 5).
pub fn spawn_sweeper(store: Arc<Mutex<Store>>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(SWEEP_INTERVAL);
        store.lock().unwrap().sweep_expired();
    });
}

/// Per-connection loop: read raw bytes, feed the resumable RESP parser, dispatch every
/// complete frame, and write the encoded reply back.
pub fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
    let mut parser = Parser::new();
    let mut read_buf = [0u8; 4096];

    loop {
        // Drain every already-buffered complete frame before blocking on the next read().
        // This is what makes pipelining work: several frames may arrive in one TCP segment.
        loop {
            match parser.try_parse() {
                Ok(frame) => {
                    let reply = command::dispatch(&store, frame);
                    if stream.write_all(&resp::encode(&reply)).is_err() {
                        return; // client went away
                    }
                }
                Err(ParseError::Incomplete) => break,
                Err(ParseError::Protocol(msg)) => {
                    let _ =
                        stream.write_all(&resp::encode(&resp::Value::Error(format!("ERR {msg}"))));
                    return; // protocol desync — unsafe to keep parsing this stream
                }
            }
        }

        match stream.read(&mut read_buf) {
            Ok(0) => return, // client closed the connection
            Ok(n) => parser.feed(&read_buf[..n]),
            Err(_) => return, // read error — drop the connection
        }
    }
}
