//! Thin binary wrapper around the `miniredis` library: parses the listen address and
//! LRU capacity, binds the TCP listener, and spawns one handler thread per connection.
//! All actual logic (connection loop, command dispatch, store) lives in the library so
//! that integration tests can exercise it directly (see src/lib.rs).

use miniredis::{handle_connection, spawn_sweeper, store::Store};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

/// Default listen address. Redis' canonical port is 6379; we bind the same so a plain
/// `redis-cli` (which defaults to 6379) connects with no extra flags.
const DEFAULT_ADDR: &str = "127.0.0.1:6379";

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
