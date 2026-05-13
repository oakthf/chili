//! Sprint 16 — `EngineState::flush_handle` tests.
//!
//! Verifies the tplog fsync hook backing for mdata's PRD §5.1 part-2
//! durability requirement (`engine.flush_tplog()` Python API).
//!
//! Invariants:
//!   1. After writes via `state.sync(h, msg)` on a file:// handle, the
//!      `bytes_since_flush` counter reflects payload bytes written.
//!   2. `flush_handle(h)` returns the pre-flush byte count and resets
//!      the counter to 0.
//!   3. `flush_handle` errors for non-file:// handles (TCP / Outgoing /
//!      Subscribing / Publishing).
//!   4. Subsequent writes after flush advance the counter from 0.

use std::sync::Arc;

use chili_core::{EngineState, SpicyObj};

/// Open a temp file:// handle on the given engine and return its handle id.
fn open_tplog(engine: &Arc<EngineState>, path: &str) -> i64 {
    let uri = format!("file://{path}");
    match engine
        .fn_call(".handle.open", &[&SpicyObj::String(uri)])
        .unwrap()
    {
        SpicyObj::I64(h) => h,
        other => panic!(".handle.open returned non-i64: {other:?}"),
    }
}

#[test]
fn flush_handle_returns_bytes_then_resets_counter() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tplog.bin");
    let path_str = path.to_str().unwrap();

    let mut state = EngineState::initialize();
    state.enable_pepper();
    let engine = Arc::new(state);

    let h = open_tplog(&engine, path_str);

    // Write a single Symbol via sync(); for a fresh file:// handle that
    // routes through the New branch and writes `s + '\n'` (= 6 bytes).
    let bytes_written = engine.sync(&h, &SpicyObj::Symbol("hello".into())).unwrap();
    assert_eq!(
        bytes_written,
        SpicyObj::I64(5),
        "sync() returns s.len() (not bytes-on-disk)"
    );

    // First flush — counter should hold 6 (5 chars + '\n').
    let flushed = engine.flush_handle(&h).unwrap();
    assert_eq!(flushed, 6, "first flush returns bytes written so far");

    // Second flush with no writes between — counter is 0.
    let flushed_again = engine.flush_handle(&h).unwrap();
    assert_eq!(
        flushed_again, 0,
        "flush after-flush-with-no-writes returns 0"
    );

    // Write again — counter advances from 0.
    engine.sync(&h, &SpicyObj::Symbol("world!".into())).unwrap();
    let flushed_third = engine.flush_handle(&h).unwrap();
    assert_eq!(
        flushed_third, 7,
        "second flush returns only the bytes since last flush"
    );
}

#[test]
fn flush_handle_errors_on_unknown_handle() {
    // flush_handle on a handle id that doesn't exist returns Err with
    // InvalidHandleErr semantics. Caller (chili-py wrapper) translates
    // that into a Python RuntimeError.
    let mut state = EngineState::initialize();
    state.enable_pepper();
    let engine = Arc::new(state);

    let bogus_h: i64 = 99_999;
    let result = engine.flush_handle(&bogus_h);
    assert!(
        result.is_err(),
        "flush_handle on unknown handle id should error, got {result:?}"
    );
}
