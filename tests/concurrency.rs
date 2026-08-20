//! Concurrency tests without any networking: many threads hammer the shared
//! `Arc<Mutex<Store>>` through the real command dispatch layer (TECHNICAL_PLAN.md,
//! Этап 5). The Mutex must serialize access — no panics, no torn values.

use miniredis::command::{dispatch, lock_store};
use miniredis::resp::Value;
use miniredis::store::Store;
use std::sync::{Arc, Mutex};

const THREADS: usize = 8;
const CYCLES: usize = 1000;

fn new_store() -> Arc<Mutex<Store>> {
    Arc::new(Mutex::new(Store::new()))
}

/// Build the `Value::Array` of `Value::Bulk` frame a real client would send.
fn frame(parts: &[Vec<u8>]) -> Value {
    Value::Array(parts.iter().map(|p| Value::Bulk(p.clone())).collect())
}

#[test]
fn eight_threads_set_get_on_distinct_keys() {
    let store = new_store();

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..CYCLES {
                    let key = format!("t{t}-key{i}").into_bytes();
                    let value = format!("t{t}-value{i}").into_bytes();
                    assert_eq!(
                        dispatch(
                            &store,
                            frame(&[b"SET".to_vec(), key.clone(), value.clone()])
                        ),
                        Value::Simple("OK".into())
                    );
                    // Immediate read-back: nobody else writes this thread's keys.
                    assert_eq!(
                        dispatch(&store, frame(&[b"GET".to_vec(), key])),
                        Value::Bulk(value)
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // Final state check: every key written by every thread holds its exact value.
    for t in 0..THREADS {
        for i in 0..CYCLES {
            let key = format!("t{t}-key{i}").into_bytes();
            let expected = format!("t{t}-value{i}").into_bytes();
            assert_eq!(
                dispatch(&store, frame(&[b"GET".to_vec(), key])),
                Value::Bulk(expected),
                "final value for t{t}-key{i} is wrong"
            );
        }
    }
}

#[test]
fn eight_threads_writing_one_key_leave_one_of_the_written_values() {
    let store = new_store();
    let key = b"contested".to_vec();

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = Arc::clone(&store);
            let key = key.clone();
            std::thread::spawn(move || {
                for i in 0..CYCLES {
                    let value = format!("t{t}-v{i}").into_bytes();
                    assert_eq!(
                        dispatch(&store, frame(&[b"SET".to_vec(), key.clone(), value])),
                        Value::Simple("OK".into())
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("writer thread panicked");
    }

    // The Mutex serializes writes, so the final value must be exactly one of the
    // 8 * 1000 values that some thread wrote — never a torn/interleaved byte mess.
    let final_value = match dispatch(&store, frame(&[b"GET".to_vec(), key])) {
        Value::Bulk(v) => v,
        other => panic!("expected bulk reply, got {other:?}"),
    };
    let is_written_value =
        (0..THREADS).any(|t| (0..CYCLES).any(|i| final_value == format!("t{t}-v{i}").into_bytes()));
    assert!(
        is_written_value,
        "final value {:?} is not one of the written values",
        String::from_utf8_lossy(&final_value)
    );
}

#[test]
fn lock_store_recovers_from_poisoned_mutex() {
    let store = new_store();
    lock_store(&store).set(b"k".to_vec(), b"before-poison".to_vec(), None);

    // Deliberately poison the mutex: panic while holding the guard.
    let poisoner = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            let _guard = store.lock().unwrap();
            panic!("intentional panic while holding the store lock");
        })
    };
    assert!(poisoner.join().is_err(), "poisoner thread must panic");
    assert!(
        store.is_poisoned(),
        "mutex must be poisoned after the panic"
    );

    // lock_store must hand back a usable guard instead of panicking again.
    {
        let mut guard = lock_store(&store);
        assert_eq!(guard.get(b"k"), Some(b"before-poison".to_vec()));
        guard.set(b"k".to_vec(), b"after-poison".to_vec(), None);
    }

    // The whole command layer keeps working on the poisoned mutex too.
    assert_eq!(
        dispatch(&store, frame(&[b"GET".to_vec(), b"k".to_vec()])),
        Value::Bulk(b"after-poison".to_vec())
    );
}
