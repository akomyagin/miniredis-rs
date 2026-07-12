//! miniredis — a from-scratch in-memory KV server speaking the Redis RESP wire protocol.
//!
//! Concurrency model: thread-per-connection over `std::net` (no async runtime in v1).
//! See docs/TECHNICAL_PLAN.md for the staged build plan.

// Этап 0 skeleton: the resp/store APIs are defined but not yet wired up, so their items
// are legitimately unused until the stages that implement them land. Remove once used.
#![allow(dead_code)]

mod resp;
mod store;

use std::net::TcpListener;

/// Default listen address. Redis' canonical port is 6379; we bind the same so a plain
/// `redis-cli` (which defaults to 6379) connects with no extra flags.
const DEFAULT_ADDR: &str = "127.0.0.1:6379";

fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_string());

    let listener = TcpListener::bind(&addr)?;
    println!("miniredis listening on {addr}");

    // TODO(Этап 2): accept connections and spawn a handler thread per connection.
    // TODO(Этап 2): each handler drives a resp::Parser over the socket's byte stream,
    //               dispatches commands against a shared store::Store, and writes RESP replies.
    // For now, accept-and-drop so the binary is runnable end-to-end from Этап 0.
    for stream in listener.incoming() {
        let _stream = stream?;
        // TODO(Этап 2): std::thread::spawn(move || handle_connection(_stream, store.clone()));
    }

    Ok(())
}
