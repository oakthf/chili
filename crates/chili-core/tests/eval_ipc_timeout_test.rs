use std::sync::Arc;
use std::time::{Duration, Instant};

use chili_core::{EngineState, IpcEvalResult, SpicyObj, eval_ipc_with_timeout};
use chili_op::{BUILT_IN_FN, LOG_FN};

fn new_engine() -> Arc<EngineState> {
    let mut state = EngineState::initialize();
    state.enable_pepper();
    state.register_fn(&LOG_FN);
    state.register_fn(&BUILT_IN_FN);
    Arc::new(state)
}

#[test]
fn disabled_timeout_evaluates_normally() {
    let state = new_engine();
    state.set_eval_timeout_ms(0);
    let q = SpicyObj::String("1+1;".to_string());
    match eval_ipc_with_timeout(&state, "u", 1, &q, "t.pep") {
        IpcEvalResult::Finished(Ok(v)) => assert_eq!(v.to_i64().unwrap(), 2),
        other => panic!("unexpected result: {:?}", other),
    }
}

#[test]
fn slow_eval_times_out() {
    let state = new_engine();
    state.set_eval_timeout_ms(50);
    let q = SpicyObj::String(".os.sleep 500;".to_string());
    let start = Instant::now();
    match eval_ipc_with_timeout(&state, "u", 1, &q, "t.pep") {
        IpcEvalResult::TimedOut => {}
        other => panic!("expected timeout, got {:?}", other),
    }
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "timeout should return quickly, took {:?}",
        start.elapsed()
    );
}

#[test]
fn fast_eval_finishes_within_timeout() {
    let state = new_engine();
    state.set_eval_timeout_ms(5_000);
    let q = SpicyObj::String("42;".to_string());
    match eval_ipc_with_timeout(&state, "u", 1, &q, "t.pep") {
        IpcEvalResult::Finished(Ok(v)) => assert_eq!(v.to_i64().unwrap(), 42),
        other => panic!("unexpected result: {:?}", other),
    }
}
