//! Engine-wide conn open/close hook tests.

use chili_core::{EngineState, SpicyObj, Stack};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn new_engine() -> EngineState {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    state
}

fn install_lifecycle_hooks(state: &EngineState) {
    let mut s = Stack::new(None, 0, 0, "");
    state
        .eval(
            &mut s,
            &SpicyObj::String(
                ".conn.open.hook: {[u; h]
                    `open_user: u;
                    `open_handle: h; };
                 .conn.close.hook: {[u; h]
                    `close_user: u;
                    `close_handle: h; };"
                    .to_string(),
            ),
            "hooks.pep",
        )
        .unwrap();
    state.set_on_conn_open_hook(Some(".conn.open.hook".to_string()));
    state.set_on_conn_close_hook(Some(".conn.close.hook".to_string()));
}

#[test]
fn no_hooks_are_no_ops() {
    let state = new_engine();
    state.fire_on_conn_open_hook("alice", 7);
    state.fire_on_conn_close_hook("alice", 7);
    assert!(state.get_on_conn_open_hook().is_none());
    assert!(state.get_on_conn_close_hook().is_none());
}

#[test]
fn open_hook_records_user_and_handle() {
    let state = new_engine();
    install_lifecycle_hooks(&state);
    state.fire_on_conn_open_hook("alice", 42);
    assert_eq!(
        state.get_var("open_user").unwrap().str().unwrap(),
        "alice"
    );
    assert_eq!(state.get_var("open_handle").unwrap().to_i64().unwrap(), 42);
}

#[test]
fn close_hook_records_user_and_handle() {
    let state = new_engine();
    install_lifecycle_hooks(&state);
    state.fire_on_conn_close_hook("bob", 99);
    assert_eq!(state.get_var("close_user").unwrap().str().unwrap(), "bob");
    assert_eq!(state.get_var("close_handle").unwrap().to_i64().unwrap(), 99);
}

#[test]
fn hook_error_is_swallowed() {
    let state = new_engine();
    let mut s = Stack::new(None, 0, 0, "");
    state
        .eval(
            &mut s,
            &SpicyObj::String(".bad.open: {[u; h] raise \"boom\" };".to_string()),
            "hooks.pep",
        )
        .unwrap();
    state.set_on_conn_open_hook(Some(".bad.open".to_string()));
    state.fire_on_conn_open_hook("alice", 1);
}

#[test]
fn clear_hooks_stop_firing() {
    let state = new_engine();
    install_lifecycle_hooks(&state);
    state.set_on_conn_open_hook(None);
    state.set_on_conn_close_hook(None);
    state.fire_on_conn_open_hook("alice", 1);
    state.fire_on_conn_close_hook("alice", 1);
    assert!(state.get_var("open_user").is_err());
    assert!(state.get_var("close_user").is_err());
}
