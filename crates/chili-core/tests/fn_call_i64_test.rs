//! Sprint 19 (ADD-1) — regression guard for the upstream `606d1cc`
//! `fn_call` I64 dispatch arm (`feat: add open_handle and sync for IPC
//! remote queries`).
//!
//! `606d1cc` added `SpicyObj::I64(_) => eval_call(...)` to
//! `EngineState::fn_call`'s `match func`. Before that arm, calling a
//! var that resolves to a handle id via `fn_call` fell through to
//! `_ => Err("Not able to call 'Int'")`. Neither upstream nor claude-2
//! had a test naming this arm, so a future merge that drops it would
//! pass the gate silently (Sprint-19 audit ADD-1). This test fails if
//! the arm is removed.

use std::sync::Arc;

use chili_core::{EngineState, SpicyObj};

fn engine() -> Arc<EngineState> {
    let mut s = EngineState::initialize();
    s.enable_pepper();
    Arc::new(s)
}

/// A var resolving to an i64 handle, `fn_call`'d with a message, must
/// dispatch through the new I64 arm (→ `eval_call` → `sync`) and write
/// to the handle — NOT return the pre-`606d1cc`
/// `Err("Not able to call 'Int'")`.
#[test]
fn fn_call_on_i64_handle_var_dispatches_via_eval_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg");
    let uri = format!("file://{}", path.to_str().unwrap());

    let e = engine();
    let h = match e.open_handle(&uri, 0).unwrap() {
        SpicyObj::I64(h) => h,
        other => panic!("open_handle returned non-i64: {other:?}"),
    };
    // Model `engine.sync()`'s `set pyHandle <h>` step: a var that
    // resolves to the handle id.
    e.set_var(".testh", SpicyObj::I64(h)).unwrap();

    let msg = SpicyObj::MixedList(vec![
        SpicyObj::Symbol("upd".into()),
        SpicyObj::Symbol("trade".into()),
        SpicyObj::I64(7),
    ]);
    let res = e.fn_call(".testh", &[&msg]);

    assert!(
        res.is_ok(),
        "fn_call on an i64-handle var must route through the 606d1cc \
         I64 arm (eval_call→sync), got {res:?} — if this is \
         Err(\"Not able to call\"), the upstream fn_call I64 arm was \
         dropped by a merge"
    );
    // The frame reached the tplog file (sync wrote it).
    let bytes = std::fs::read(&path).unwrap();
    assert!(
        !bytes.is_empty(),
        "the dispatched call must have written the message to the handle"
    );
    assert_eq!(
        &bytes[0..4],
        &[255, 0, 0, 0],
        "wrote a valid sequence-file frame via the I64 dispatch path"
    );
}
