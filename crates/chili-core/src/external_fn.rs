//! ADR-0007 — W3 Python-callable bridge.
//!
//! `ExternalFnDispatcher` is the trait an external embedder (chili-py)
//! implements so chili-core can dispatch external (Python) callables by
//! name. The dispatcher is installed on `EngineState` via
//! `set_external_dispatcher`; pepper dispatch invokes it from
//! `eval_fn_call` when the target `Func` has `external_name = Some(_)`.
//!
//! ## Lock discipline (load-bearing)
//!
//! Implementations MUST NOT hold any chili-core lock for the duration of
//! the call — invocation runs lock-free per the existing `FuncType` /
//! `SideEffectFuncType` contract. chili-core's eval path
//! (`eval.rs:eval_fn_call`) clones the `Arc<dyn ExternalFnDispatcher>` out
//! from under a brief read lock and invokes outside the lock; the same
//! discipline applies to any locks the implementation holds internally
//! (e.g., chili-py's `PyExternalDispatcher::callables` RwLock — clone the
//! callable out under a brief read lock, then invoke).
//!
//! ## Re-entrancy
//!
//! A registered Python callable may freely call back into the engine
//! (`engine.fn_call`, `engine.set_var`, `engine.get_var`, `engine.eval`)
//! while running. This works because (a) chili-core eval holds zero
//! `vars` locks across function-dispatch boundaries (`grep -rn
//! "self.vars.\(read\|write\)" crates/chili-core/src/eval.rs
//! crates/chili-op/src/` returns empty), and (b) chili-py releases the GIL
//! around each chili-core call via `py.detach`, so the inner re-entry
//! takes locks fresh against no outer-held lock. Concurrent
//! `register`/`dispatch` is exercised by the `concurrent_dispatch` test
//! (see `tests/external_fn_test.rs`).

use crate::SpicyObj;
use crate::errors::SpicyResult;

pub trait ExternalFnDispatcher: Send + Sync {
    /// Dispatch a registered external function by name.
    ///
    /// `name` is the value stored in `Func::external_name`. `args` is the
    /// flattened argument list (including any partial-application args
    /// already substituted by `eval_fn_call`).
    fn dispatch(&self, name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj>;
}
