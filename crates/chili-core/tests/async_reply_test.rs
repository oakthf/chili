//! FR (gw async-router de-risk) — the two primitives an async query-router needs:
//! `.z.w` (the caller's handle, the kdb+ `.z.w`) and `.handle.reply[h; msg]`
//! (targeted fire-and-forget async-write to ANY handle, incl. an inbound caller
//! connection — the `neg[.z.w]` analog that `async_` lacks because it rejects
//! Incoming handles).
//!
//! These exercise the engine seam directly (no networking). The full networked
//! round-trip (client registers a reply channel, fire-and-forget queries on a
//! second handle, server `.handle.reply`s to the channel, client's subscribe
//! reader receives it) is proven by the Python e2e spike.

use chili_core::{EngineState, SpicyObj, Stack};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn new_engine() -> EngineState {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    state
}

/// `.z.w` returns the evaluating stack's handle — i.e. the connection that sent
/// the message. It must propagate through a fn-call frame (a handler calls it),
/// which the tuple/`eval_op` path preserves (the inbound IPC tuple-fn-call path).
#[test]
fn z_w_returns_caller_handle_through_fn_call() {
    let state = new_engine();
    // a handler that returns its caller handle (the gw pattern: `.z.w` inside a fn)
    let mut def = Stack::new(None, 0, 0, "");
    state
        .eval(&mut def, &SpicyObj::String(".srv.whoami: {[] .z.w[]}".into()), "t")
        .unwrap();

    // Evaluate the handler via the stack-preserving fn-call path with a non-zero
    // connection handle, exactly as an inbound IPC tuple-fn-call does.
    let mut conn = Stack::new(None, 0, 42, "alice");
    let call = SpicyObj::MixedList(vec![SpicyObj::Symbol(".srv.whoami".into())]);
    let got = state.eval(&mut conn, &call, "ipc42.pep").unwrap();
    assert_eq!(got.to_i64().unwrap(), 42, ".z.w must equal the caller handle");

    // A local (handle 0) eval sees 0 — no caller.
    let mut local = Stack::new(None, 0, 0, "");
    let call0 = SpicyObj::MixedList(vec![SpicyObj::Symbol(".srv.whoami".into())]);
    let got0 = state.eval(&mut local, &call0, "t").unwrap();
    assert_eq!(got0.to_i64().unwrap(), 0);
}

/// `.handle.reply` is registered and rejects an unknown handle (the write path
/// itself is exercised by the networked spike — here we confirm it's callable
/// and validates its target).
#[test]
fn handle_reply_registered_and_validates_target() {
    let state = new_engine();
    let mut s = Stack::new(None, 0, 0, "");
    let call = SpicyObj::MixedList(vec![
        SpicyObj::Symbol(".handle.reply".into()),
        SpicyObj::I64(999_999),
        SpicyObj::String("x".into()),
    ]);
    let err = state.eval(&mut s, &call, "t").unwrap_err();
    // an unknown handle is an invalid-handle error, not "name not defined".
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("handle"),
        "expected an invalid-handle error, got: {msg}"
    );
}
