//! Regression: eval timeout must shut down the TCP connection so the client's
//! next sync on the same handle fails fast instead of blocking forever.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chili_core::{EngineState, SpicyObj};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn new_engine() -> Arc<EngineState> {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    Arc::new(state)
}

fn start_server(timeout_ms: i64) -> (Arc<EngineState>, u16) {
    let engine = new_engine();
    engine.set_arc_self(Arc::clone(&engine)).unwrap();
    engine.set_eval_timeout_ms(timeout_ms);
    let listener = EngineState::bind_tcp_listener(0, false).expect("bind");
    let port = listener.local_addr().unwrap().port();
    engine
        .install_tcp_listener(listener, vec![])
        .expect("install listener");
    (engine, port)
}

#[test]
fn eval_timeout_shuts_connection_for_subsequent_sync() {
    let (_server, port) = start_server(50);
    let client = new_engine();
    let h = client
        .open_handle(&format!("chili://127.0.0.1:{port}"), 0)
        .expect("connect")
        .to_i64()
        .unwrap();

    let slow = SpicyObj::String(".os.sleep 500;".to_string());
    let started = Instant::now();
    let err = client.sync(&h, &slow).unwrap_err();
    assert!(
        err.to_string().contains("timed out"),
        "expected timeout error, got: {err}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "timeout should return quickly, took {:?}",
        started.elapsed()
    );

    let fast = SpicyObj::String("42;".to_string());
    let started = Instant::now();
    let err = client.sync(&h, &fast).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "second sync must not hang; took {:?}, err: {err}",
        started.elapsed()
    );
}
