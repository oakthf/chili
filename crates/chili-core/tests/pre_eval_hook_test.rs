//! FR-2 — pre-eval request hook (`eval_with_pre_hook`, TorQ `.z.pg`/`.z.ps`).
//!
//! The hook interposes on inbound IPC requests: `hook(user; handle; query)`
//! runs first, and its return value REPLACES the query that gets evaluated
//! (Allow = return the query, Rewrite = return a different/redirected request,
//! Deny = `raise`, whose error propagates back to the caller). With no hook
//! registered, `eval_with_pre_hook` is identical to `eval`.
//!
//! These exercise the engine seam directly (no networking); the network
//! conn handlers call the same method.

use chili_core::{EngineState, SpicyObj, Stack};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn new_engine() -> EngineState {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    // Register the operator/built-in fns (`+`, `~`, ...) exactly as the
    // Python `ChiliEngine(pepper=True)` constructor does.
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    state
}

/// Defines server-side vars + a 3-arg ACL hook and registers it. The hook
/// denies `secretvar`, redirects `redirect` -> `redirecttarget`, and allows
/// the rest. (Call-counting / arithmetic is covered by the Python e2e probe;
/// this harness exercises the interposition + allow/deny/rewrite branches.)
fn install_acl_hook(state: &EngineState) {
    let mut s = Stack::new(None, 0, 0, "");
    for src in [
        "allowedvar: 7;",
        "secretvar: 99;",
        "redirecttarget: 55;",
        ".acl.hook: {[u; h; q]
            $[ q ~ `secretvar;
               raise \"permission denied\";
               $[ q ~ `redirect; `redirecttarget; q ] ] };",
    ] {
        state
            .eval(&mut s, &SpicyObj::String(src.to_string()), "acl.pep")
            .unwrap_or_else(|e| panic!("setup eval failed for {src:?}: {e}"));
    }
    state.set_pre_eval_hook(Some(".acl.hook".to_string()));
}

#[test]
fn no_hook_is_identical_to_eval() {
    let state = new_engine();
    let mut s = Stack::new(None, 0, 0, "");
    state
        .eval(&mut s, &SpicyObj::String("x: 11;".to_string()), "t.pep")
        .unwrap();
    assert!(state.get_pre_eval_hook().is_none());
    // A var-lookup request resolves the same with or without the hook path.
    let q = SpicyObj::Symbol("x".into());
    let got = state.eval_with_pre_hook(&mut s, &q, "t.pep").unwrap();
    assert_eq!(got.to_i64().unwrap(), 11);
}

#[test]
fn hook_allows_passes_query_through() {
    let state = new_engine();
    install_acl_hook(&state);
    let mut s = Stack::new(None, 0, 1, "alice");
    let q = SpicyObj::Symbol("allowedvar".into());
    let got = state.eval_with_pre_hook(&mut s, &q, "ipc.pep").unwrap();
    assert_eq!(got.to_i64().unwrap(), 7, "allow should resolve the var");
}

#[test]
fn hook_rewrites_redirects_to_another_request() {
    let state = new_engine();
    install_acl_hook(&state);
    let mut s = Stack::new(None, 0, 1, "alice");
    let q = SpicyObj::Symbol("redirect".into());
    let got = state.eval_with_pre_hook(&mut s, &q, "ipc.pep").unwrap();
    assert_eq!(
        got.to_i64().unwrap(),
        55,
        "rewrite should redirect to redirecttarget"
    );
}

#[test]
fn hook_deny_raises_and_propagates() {
    let state = new_engine();
    install_acl_hook(&state);
    let mut s = Stack::new(None, 0, 1, "alice");
    let q = SpicyObj::Symbol("secretvar".into());
    let res = state.eval_with_pre_hook(&mut s, &q, "ipc.pep");
    let err = res.expect_err("deny must raise");
    assert!(
        err.to_string().contains("permission denied"),
        "deny error should carry the hook message, got: {err}"
    );
    // The denied query must NOT have been evaluated (no secret value leaks).
}

#[test]
fn clear_hook_restores_unfiltered_eval() {
    let state = new_engine();
    install_acl_hook(&state);
    state.set_pre_eval_hook(None);
    assert!(state.get_pre_eval_hook().is_none());
    let mut s = Stack::new(None, 0, 1, "alice");
    // Without the hook, even the previously-denied var resolves.
    let q = SpicyObj::Symbol("secretvar".into());
    let got = state.eval_with_pre_hook(&mut s, &q, "ipc.pep").unwrap();
    assert_eq!(got.to_i64().unwrap(), 99);
}

#[test]
fn missing_hook_fn_is_a_clear_error() {
    let state = new_engine();
    state.set_pre_eval_hook(Some(".nope.missing".to_string()));
    let mut s = Stack::new(None, 0, 1, "alice");
    let q = SpicyObj::Symbol("anything".into());
    let err = state
        .eval_with_pre_hook(&mut s, &q, "ipc.pep")
        .expect_err("a registered-but-undefined hook must error, not silently pass");
    assert!(err.to_string().contains("not defined"));
}
