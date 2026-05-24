//! Sprint 23 W3 — `ExternalFnDispatcher` chili-core guard tests.
//!
//! ADR-0007: pepper dispatch routes through an installed
//! `ExternalFnDispatcher` when the target `Func` has `external_name =
//! Some(_)`. These tests verify the dispatch path + the lock-discipline
//! contract (no deadlock under concurrent register/dispatch).
//!
//! End-to-end Python-callable integration is in `crates/chili-py/tests/
//! test_register_fn.py` (the MC-4-style closure gate). chili-core tests
//! use a Rust-side stub dispatcher to keep the test independent of pyo3.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use chili_core::{EngineState, ExternalFnDispatcher, Func, SpicyError, SpicyObj, SpicyResult};
use parking_lot::Mutex;

/// Add-two stub: returns `args[0] + args[1]` as I64. Bumps a counter on
/// each call so concurrent tests can assert all invocations succeeded.
struct AddTwoStub {
    calls: AtomicUsize,
}

impl AddTwoStub {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl ExternalFnDispatcher for AddTwoStub {
    fn dispatch(&self, _name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let a = match args[0] {
            SpicyObj::I64(v) => *v,
            _ => return Err(SpicyError::EvalErr("arg 0 must be I64".to_owned())),
        };
        let b = match args[1] {
            SpicyObj::I64(v) => *v,
            _ => return Err(SpicyError::EvalErr("arg 1 must be I64".to_owned())),
        };
        Ok(SpicyObj::I64(a + b))
    }
}

/// Multi-name dispatcher: dispatches different names to different
/// closures. Used by tests that need >1 registered fn.
struct MultiStub {
    table: Mutex<std::collections::HashMap<String, i64>>,
}

impl MultiStub {
    fn new() -> Self {
        Self {
            table: Mutex::new(std::collections::HashMap::new()),
        }
    }
    fn set(&self, name: &str, val: i64) {
        self.table.lock().insert(name.to_owned(), val);
    }
}

impl ExternalFnDispatcher for MultiStub {
    fn dispatch(&self, name: &str, _args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
        match self.table.lock().get(name) {
            Some(v) => Ok(SpicyObj::I64(*v)),
            None => Err(SpicyError::EvalErr(format!("no stub for '{}'", name))),
        }
    }
}

fn install_external(state: &EngineState, name: &str, arity: usize) {
    state
        .set_var(name, SpicyObj::Fn(Func::new_external(name, arity)))
        .expect("set_var");
}

/// Test 1 — happy path. Register stub + Func placeholder; call via
/// `fn_call`; dispatcher returns the sum.
#[test]
fn dispatcher_invoked_via_pepper_eval() {
    let state = EngineState::initialize();
    let stub = Arc::new(AddTwoStub::new());
    state.set_external_dispatcher(Arc::clone(&stub) as Arc<dyn ExternalFnDispatcher>);
    install_external(&state, ".add_two", 2);

    let result = state
        .fn_call(".add_two", &[&SpicyObj::I64(3), &SpicyObj::I64(4)])
        .expect("fn_call");

    assert_eq!(result, SpicyObj::I64(7));
    assert_eq!(stub.calls.load(Ordering::SeqCst), 1);
}

/// Test 2 — Func with `external_name` set but no dispatcher installed
/// → clean EvalErr (not panic, not a wrong type).
#[test]
fn error_without_dispatcher() {
    let state = EngineState::initialize();
    install_external(&state, ".orphan", 2);

    let err = state
        .fn_call(".orphan", &[&SpicyObj::I64(1), &SpicyObj::I64(2)])
        .expect_err("should error without dispatcher");

    let msg = format!("{:?}", err);
    assert!(
        msg.contains("external fn '.orphan' registered but no dispatcher installed"),
        "wrong error: {}",
        msg
    );
}

/// Test 3 — arity mismatch → partial-application projection (matches
/// existing user-fn behavior at eval_fn_call:79-81).
#[test]
fn arity_mismatch_returns_projection() {
    let state = EngineState::initialize();
    let stub = Arc::new(AddTwoStub::new());
    state.set_external_dispatcher(Arc::clone(&stub) as Arc<dyn ExternalFnDispatcher>);
    install_external(&state, ".add_two", 2);

    // Call with 1 arg of a 2-arg fn → projection (Fn back, partial-applied).
    let result = state
        .fn_call(".add_two", &[&SpicyObj::I64(10)])
        .expect("fn_call (projection)");

    match result {
        SpicyObj::Fn(f) => {
            assert_eq!(f.external_name.as_deref(), Some(".add_two"));
            assert!(f.part_args.is_some(), "projected Func must carry part_args");
            // Dispatcher must NOT have been called for the projection.
            assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
        }
        other => panic!("expected partial-applied Fn, got {:?}", other),
    }
}

/// Test 4 — installing a new dispatcher replaces the previous one (the
/// pepper dispatch sees the latest installation).
#[test]
fn dispatcher_can_be_replaced() {
    let state = EngineState::initialize();
    let stub_a = Arc::new(AddTwoStub::new());
    state.set_external_dispatcher(Arc::clone(&stub_a) as Arc<dyn ExternalFnDispatcher>);
    install_external(&state, ".op", 2);

    let r1 = state
        .fn_call(".op", &[&SpicyObj::I64(1), &SpicyObj::I64(2)])
        .expect("fn_call A");
    assert_eq!(r1, SpicyObj::I64(3));
    assert_eq!(stub_a.calls.load(Ordering::SeqCst), 1);

    // Replace dispatcher. The same Func placeholder still routes — to B now.
    let stub_b = Arc::new(MultiStub::new());
    stub_b.set(".op", 999);
    state.set_external_dispatcher(Arc::clone(&stub_b) as Arc<dyn ExternalFnDispatcher>);

    let r2 = state
        .fn_call(".op", &[&SpicyObj::I64(1), &SpicyObj::I64(2)])
        .expect("fn_call B");
    assert_eq!(r2, SpicyObj::I64(999));
    // A's counter unchanged after replacement.
    assert_eq!(stub_a.calls.load(Ordering::SeqCst), 1);

    state.clear_external_dispatcher();
    let r3 = state.fn_call(".op", &[&SpicyObj::I64(1), &SpicyObj::I64(2)]);
    assert!(r3.is_err(), "after clear, dispatch must error");
}

/// Test 5 — concurrent dispatch + concurrent register/unregister (no
/// deadlock between the `vars` lock used by `set_var(placeholder)` and
/// the `external_dispatcher` lock used by the dispatch path).
///
/// Per audit MC-3: this is the structural guard for the new lock
/// introduced by Sprint 23 (`external_dispatcher` slot on EngineState).
/// The chili-py side adds another lock (`callables: RwLock<HashMap>`)
/// that has its own concurrent test in `test_register_fn.py`.
#[test]
fn concurrent_dispatch_no_deadlock() {
    const DISPATCH_THREADS: usize = 4;
    const DISPATCH_ITERS: usize = 1_000;
    const REGISTER_ITERS: usize = 100;
    const TIMEOUT: Duration = Duration::from_secs(15);

    let state = Arc::new(EngineState::initialize());
    let stub = Arc::new(AddTwoStub::new());
    state.set_external_dispatcher(Arc::clone(&stub) as Arc<dyn ExternalFnDispatcher>);
    install_external(&state, ".cc", 2);

    let start = Instant::now();
    let mut handles = Vec::new();

    // N dispatch threads.
    for _ in 0..DISPATCH_THREADS {
        let state = Arc::clone(&state);
        handles.push(thread::spawn(move || {
            for i in 0..DISPATCH_ITERS {
                let r = state
                    .fn_call(".cc", &[&SpicyObj::I64(i as i64), &SpicyObj::I64(1)])
                    .expect("concurrent fn_call");
                match r {
                    SpicyObj::I64(v) => assert_eq!(v, (i as i64) + 1),
                    other => panic!("expected I64, got {:?}", other),
                }
            }
        }));
    }

    // 1 register/unregister thread interleaved with dispatch.
    {
        let state = Arc::clone(&state);
        handles.push(thread::spawn(move || {
            for i in 0..REGISTER_ITERS {
                let name = format!(".temp_{}", i);
                state
                    .set_var(&name, SpicyObj::Fn(Func::new_external(&name, 1)))
                    .expect("register");
                let _ = state.del_var(&name).expect("unregister");
            }
        }));
    }

    for (idx, h) in handles.into_iter().enumerate() {
        // Each thread should complete well within the timeout. If we
        // hang here that's a deadlock and the test fails by exceeding
        // the test framework's default timeout (~60s).
        h.join()
            .unwrap_or_else(|_| panic!("thread {} panicked", idx));
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < TIMEOUT,
        "concurrent run took {:?}, likely deadlock",
        elapsed
    );
    assert_eq!(
        stub.calls.load(Ordering::SeqCst),
        DISPATCH_THREADS * DISPATCH_ITERS,
        "dispatcher should have been invoked exactly {} times",
        DISPATCH_THREADS * DISPATCH_ITERS
    );
}
