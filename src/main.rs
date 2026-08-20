//! miniredis — a from-scratch in-memory KV server speaking the Redis RESP wire protocol.
//!
//! Concurrency model: thread-per-connection over `std::net` (no async runtime in v1).
//! See docs/TECHNICAL_PLAN.md for the staged build plan.

mod command;
mod resp;
mod store;

use resp::{ParseError, Parser};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use store::Store;

/// Default listen address. Redis' canonical port is 6379; we bind the same so a plain
/// `redis-cli` (which defaults to 6379) connects with no extra flags.
const DEFAULT_ADDR: &str = "127.0.0.1:6379";

/// How often the background sweeper reaps expired keys (fixed by TECHNICAL_PLAN.md, Этап 3).
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_string());

    // Optional LRU capacity via the MINIREDIS_CAPACITY env var (simplest knob, no CLI
    // flag parser — see docs/TECHNICAL_PLAN.md, Этап 4). Unset or unparsable => unlimited.
    let capacity: Option<usize> = std::env::var("MINIREDIS_CAPACITY")
        .ok()
        .and_then(|s| s.parse().ok());

    let listener = TcpListener::bind(&addr)?;
    match capacity {
        Some(cap) => println!("miniredis listening on {addr} (LRU capacity: {cap})"),
        None => println!("miniredis listening on {addr} (unlimited capacity)"),
    }

    let store = Arc::new(Mutex::new(match capacity {
        Some(cap) => Store::with_capacity(cap),
        None => Store::new(),
    }));
    spawn_sweeper(Arc::clone(&store));

    for stream in listener.incoming() {
        let stream = stream?;
        let store = Arc::clone(&store);
        std::thread::spawn(move || handle_connection(stream, store));
    }

    Ok(())
}

/// Background sweeper: periodically reaps expired keys so they disappear even if never
/// accessed again (lazy expiration alone would leak them). The thread is detached and
/// never joined — the process exits via Ctrl-C/signal like every other thread (no
/// graceful shutdown in v1; see TECHNICAL_PLAN.md, Этап 5).
fn spawn_sweeper(store: Arc<Mutex<Store>>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(SWEEP_INTERVAL);
        store.lock().unwrap().sweep_expired();
    });
}

/// Per-connection loop: read raw bytes, feed the resumable RESP parser, dispatch every
/// complete frame, and write the encoded reply back.
fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
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
