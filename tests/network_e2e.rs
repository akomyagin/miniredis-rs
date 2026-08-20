//! End-to-end tests over real TCP sockets — the only test tier in the whole suite that
//! binds an actual listener (TECHNICAL_PLAN.md, Этап 5). Everything else exercises the
//! codec/store/dispatch layers in-process.
//!
//! The server side is the real production loop: `miniredis::handle_connection` spawned
//! thread-per-connection off a `TcpListener` bound to port 0 (the OS picks a free port).

use miniredis::resp::{encode, ParseError, Parser, Value};
use miniredis::store::Store;
use miniredis::{handle_connection, spawn_sweeper};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Generous socket timeout: a hung server fails the test with an error instead of
/// blocking `cargo test` forever.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Start a real miniredis server on an OS-assigned free port; returns its address.
/// The accept-loop thread is detached and dies with the test process.
fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    let store = Arc::new(Mutex::new(Store::new()));
    spawn_sweeper(Arc::clone(&store));
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let store = Arc::clone(&store);
            std::thread::spawn(move || handle_connection(stream, store));
        }
    });
    addr
}

fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect to test server");
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .expect("set_read_timeout");
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .expect("set_write_timeout");
    stream
}

/// Encode a client command as the RESP Array-of-Bulk bytes a real client sends.
fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    encode(&Value::Array(
        parts.iter().map(|p| Value::Bulk(p.to_vec())).collect(),
    ))
}

/// Read from the socket until the client-side parser yields one complete reply.
fn read_reply(stream: &mut TcpStream, parser: &mut Parser) -> Value {
    let mut buf = [0u8; 4096];
    loop {
        match parser.try_parse() {
            Ok(value) => return value,
            Err(ParseError::Incomplete) => {}
            Err(ParseError::Protocol(msg)) => panic!("protocol error in server reply: {msg}"),
        }
        let n = stream.read(&mut buf).expect("read server reply");
        assert!(n > 0, "server closed the connection mid-reply");
        parser.feed(&buf[..n]);
    }
}

#[test]
fn ping_pong_over_real_socket() {
    let addr = start_server();
    let mut stream = connect(addr);
    let mut parser = Parser::new();

    stream
        .write_all(&encode_command(&[b"PING"]))
        .expect("write PING");
    assert_eq!(
        read_reply(&mut stream, &mut parser),
        Value::Simple("PONG".into())
    );
}

#[test]
fn pipelined_frames_in_one_write_over_real_socket() {
    let addr = start_server();
    let mut stream = connect(addr);
    let mut parser = Parser::new();

    // Three glued frames in a single write_all — one TCP push, three replies.
    let mut batch = encode_command(&[b"SET", b"pipe", b"lined"]);
    batch.extend_from_slice(&encode_command(&[b"GET", b"pipe"]));
    batch.extend_from_slice(&encode_command(&[b"PING"]));
    stream.write_all(&batch).expect("write pipelined batch");

    // Parse the reply stream back frame by frame and check each one individually.
    assert_eq!(
        read_reply(&mut stream, &mut parser),
        Value::Simple("OK".into())
    );
    assert_eq!(
        read_reply(&mut stream, &mut parser),
        Value::Bulk(b"lined".to_vec())
    );
    assert_eq!(
        read_reply(&mut stream, &mut parser),
        Value::Simple("PONG".into())
    );
}

#[test]
fn half_frame_then_disconnect_does_not_kill_the_server() {
    let addr = start_server();

    // Send only the first half of a valid SET frame, then drop the connection.
    {
        let mut stream = connect(addr);
        let full = encode_command(&[b"SET", b"half", b"frame"]);
        stream
            .write_all(&full[..full.len() / 2])
            .expect("write half frame");
        // `stream` dropped here => TCP connection torn down mid-frame.
    }

    // A fresh connection must still get served: the server neither crashed nor hung.
    let mut stream = connect(addr);
    let mut parser = Parser::new();
    stream
        .write_all(&encode_command(&[b"PING"]))
        .expect("write PING on fresh connection");
    assert_eq!(
        read_reply(&mut stream, &mut parser),
        Value::Simple("PONG".into())
    );
    // And the aborted half-SET must not have stored anything.
    stream
        .write_all(&encode_command(&[b"GET", b"half"]))
        .expect("write GET");
    assert_eq!(read_reply(&mut stream, &mut parser), Value::Null);
}

#[test]
fn twenty_parallel_clients_each_set_get_their_own_key() {
    let addr = start_server();

    let handles: Vec<_> = (0..20)
        .map(|c| {
            std::thread::spawn(move || {
                let mut stream = connect(addr);
                let mut parser = Parser::new();
                let key = format!("client-{c}-key").into_bytes();
                let value = format!("client-{c}-value").into_bytes();

                stream
                    .write_all(&encode_command(&[b"SET", &key, &value]))
                    .expect("write SET");
                assert_eq!(
                    read_reply(&mut stream, &mut parser),
                    Value::Simple("OK".into()),
                    "client {c}: SET reply"
                );

                stream
                    .write_all(&encode_command(&[b"GET", &key]))
                    .expect("write GET");
                assert_eq!(
                    read_reply(&mut stream, &mut parser),
                    Value::Bulk(value),
                    "client {c}: GET reply"
                );
            })
        })
        .collect();

    for h in handles {
        h.join().expect("client thread panicked");
    }
}
