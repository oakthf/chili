# ADR-0007 — W3 Python-callable bridge via `ExternalFnDispatcher`

**Date:** 2026-05-24 (Sprint 23).
**Status:** **SUPERSEDED 2026-05-25 (Sprint 24 main-port).** mdata's Revision A explicitly withdrew the W3 ask ("we were wrong to request this") — under the user-of-chili reframe, the 3 control verbs (`.mdata.eod.fire`, `.mdata.wdb.finalize`, `.mdata.hdb.reload`) become pepper functions + polling via `engine.get_var()`, not Python callbacks dispatched over chili-IPC. The chili author also declined upstreaming W3 (per his §3.9 inline-comment + the "standalone-first" model in `docs/sync/upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md` §9). 0 mdata adopters; 0 upstream interest. Deleted in commit `da6b1a4` (Sprint 24 merge); 0.9.0 wheel ships without W3. ADR preserved as historical record. **Original status: Accepted pending impl validation, shipped Sprint 23 commit `ae5668b`+`3dc282c` (0.8.9 wheel, 5 Rust unit + 8 chili-py pytest); 0.8.9 wheel orphaned (mdata never adopted).**
**Cutover:** None for existing surface — purely additive (`engine.register_fn` / `engine.unregister_fn` on the Python side; `ExternalFnDispatcher` trait + `EngineState::external_dispatcher` slot + `Func::external_name` field in chili-core). No on-disk format change. No wire-format change for the call-form path. Existing user-defined / built-in / side-effect fn dispatch unchanged.
**Supersedes:** None.
**Related:** `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` §W3 (turn-9 revision); `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md` §W3 deferral gate (now cleared in-session 2026-05-24); ADR-0001 (pub/sub canonical — orthogonal); ADR-0006 (push-model FFI — structurally adjacent surface, similar lock-discipline reasoning); `docs/sim/sprint_23_dispatch_brief_2026-05-24.md` §Why-now §2 (the two hazard measurements that ground this ADR's "Re-entrancy is safe" + "GIL overhead is bounded" claims).

---

## Context

mdata operates 3 control verbs (`.mdata.eod.fire[date]`, `.mdata.wdb.finalize[date]`, `.mdata.hdb.reload[]`) that need to trigger Python-side daemon bookkeeping (drain a Polars buffer, finalize an idb partition, reload a partition cache) — not pure pepper. Today these dispatch via an attach-socket text protocol → Python handler. mdata's v1-36 migration retires the attach socket; W3 is the chili-side surface that lets the control verbs become pepper-invokable while keeping their Python bookkeeping context.

Two design hazards were measured against current code before this ADR was written:

1. **Re-entrancy / deadlock** — `grep -rn "self\.vars\.\(read\|write\)" crates/chili-core/src/eval.rs crates/chili-op/src/` returns ZERO hits. Function dispatch (`eval_fn_call:41-47`) invokes `f` / `f_with_side_effect` outside any held `vars` lock. A Python callback at the dispatch point inherits the same lock-free contract as builtins today. Re-entry via `engine.fn_call` / `engine.set_var` takes locks fresh; no nesting. **Not a deadlock hazard under the design below** (Decision §3).

2. **GIL acquire/release overhead** — measured against `get_var` (151ns) / `set_var` (152ns) over 100k iterations, post-warmup; each is one `py.detach + chili op + with_gil` cycle. A W3 callback dispatch adds ~2× this = **~300ns per callback round-trip** + Python interpreter time for the callback body. For mdata's 3 daily-cadence control verbs: negligible. (The 89ns figure that appears in some draft sources is `has_var`, which does NOT use `py.detach` — that number is the GIL-held-only floor, not the `py.detach` cost.)

---

## Decision

### 1. Func extension — `external_name: Option<String>`

`crates/chili-core/src/func.rs`:

```rust
pub struct Func {
    ... existing fields ...
    /// W3 — external-fn name dispatched via EngineState::external_dispatcher
    /// when set. Mutually exclusive with f / f_with_side_effect / user-defined nodes.
    pub external_name: Option<String>,
}

impl Func {
    pub fn new_external(name: &str, arity: usize) -> Self { ... }
}
```

- `Option<String>` preserves `Clone + PartialEq + Debug` derives.
- `fn_body = name` for an external Func (used for the wire-serialization fallback at §6).
- `Func::project` (partial application) preserves `external_name` via `..self.clone()` — partial-applied external Funcs retain their dispatcher binding (verified at `func.rs:147`).

### 2. ExternalFnDispatcher trait + EngineState slot

`crates/chili-core/src/external_fn.rs` (new):

```rust
pub trait ExternalFnDispatcher: Send + Sync {
    fn dispatch(&self, name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj>;
}
```

`crates/chili-core/src/engine_state.rs`:

```rust
external_dispatcher: RwLock<Option<Arc<dyn ExternalFnDispatcher>>>,

pub fn set_external_dispatcher(&self, d: Arc<dyn ExternalFnDispatcher>) { ... }
pub fn clear_external_dispatcher(&self) { ... }
```

Field is **private**; access only via the two setters. `EngineState::initialize()` (the sole literal-construction site, `engine_state.rs:201-221`) is updated with `external_dispatcher: RwLock::new(None)`.

### 3. Dispatch path — `eval_fn_call` new branch

`crates/chili-core/src/eval.rs` (after the `is_built_in_fn` branch, before the user-defined fallback):

```rust
} else if let Some(name) = &func.external_name {
    let dispatcher = {
        let guard = state.external_dispatcher.read();
        guard.as_ref().map(Arc::clone)
    };
    let dispatcher = dispatcher.ok_or_else(|| SpicyError::EvalErr(format!(
        "external fn '{}' registered but no dispatcher installed", name
    )))?;
    dispatcher.dispatch(name, &all_args)
}
```

Two lock discipline contracts establish that this is safe:

- **chili-core `vars` lock-free contract:** `eval.rs` + `chili-op/src/*.rs` hold zero `self.vars.read()/write()` calls across function-dispatch boundaries (grep proof above). The W3 branch inherits this — the dispatcher's `dispatch()` runs with no held `vars` lock.
- **`external_dispatcher` lock-free contract:** the dispatch path takes a brief read lock to clone the `Arc<dyn ExternalFnDispatcher>` out, then drops the lock before invoking. This is the same pattern as `EngineState::fn_call:1942-1953` (SpicyObj-clone-out-of-lock) and `engine_state.rs:1134-1136` (Arc-clone-out-of-lock for `arc_self`).

Re-entry from the callback (Python invokes `engine.fn_call` / `engine.set_var` / `engine.get_var`) takes chili locks fresh; no nesting against any outer-held lock.

### 4. chili-py — PyExternalDispatcher + register_fn API

`crates/chili-py/src/external_dispatcher.rs` (new):

```rust
pub struct PyExternalDispatcher {
    callables: RwLock<HashMap<String, Py<PyAny>>>,
}

impl ExternalFnDispatcher for PyExternalDispatcher {
    fn dispatch(&self, name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
        let callable = Python::with_gil(|py| {
            self.callables.read().get(name).map(|c| c.clone_ref(py))
        });
        let callable = callable.ok_or_else(|| SpicyError::EvalErr(format!(
            "no Python callable registered for external fn '{}'", name
        )))?;
        Python::with_gil(|py| invoke_python(py, &callable, name, args))
    }
}
```

`crates/chili-py/src/lib.rs`:

```rust
#[pyo3(signature = (name, callable, arity))]
fn register_fn(&self, name: &str, callable: Bound<'_, PyAny>, arity: usize) -> PyResult<()>;
fn unregister_fn(&self, py: Python<'_>, name: &str) -> PyResult<bool>;
```

**`callables` RwLock discipline (added per Sprint 23 audit MC-3):** the `callables: RwLock<HashMap<String, Py<PyAny>>>` is a NEW lock not covered by the chili-core grep proof. Write-lock is acquired ONLY in `register` / `unregister`, NEVER held while calling Python. The `dispatch()` path clones `Py<PyAny>` out under a brief read-lock then invokes outside the lock. The committed `concurrent_dispatch_no_deadlock` test exercises concurrent `register` + `dispatch` (interleaved threads) to assert the new lock doesn't deadlock against the dispatch path.

### 5. Exception semantics — stringified

Python exceptions raised inside the callback are caught, formatted with traceback, and wrapped as `SpicyError::EvalErr("external fn '<name>' raised: <ExcType>: <msg>\n  at <file>:<line>\n  ...")`. The caller sees a `ChiliError` on the chili side with the Python traceback embedded. **PyErr type is NOT preserved** — alternative below.

### 6. Wire serialization (`serde9.rs:941-950`)

External Funcs serialize only `fn_body` over the wire (existing serializer is unchanged). Implication: if a remote client retrieves an external Func by variable LOOKUP (`sync(h, ".mdata.eod.fire")` — str-form, not tuple-form), the deserialized Func on the remote side has no dispatcher, no `external_name`, and is not callable. Invocation of the deserialized form would error.

**Contract:** clients invoking external Funcs MUST use call-form sync (`sync(h, (".mdata.eod.fire", *args))`), not variable-lookup sync. The Func is resolved + invoked on the server side; only the result travels over the wire. An inline comment at `serde9.rs:941` records this.

### 7. set_var shadowing semantics

`register_fn(name, callable, arity)` stores the placeholder via `state.set_var(name, SpicyObj::Fn(placeholder))`. A user calling `engine.set_var(name, 42)` AFTER `register_fn(name, ...)` SILENTLY overwrites the Func placeholder (var is now an Int; subsequent pepper calls to `name` will error since it's no longer a Fn). The internal callable in the dispatcher is left in place until `unregister_fn` is called.

**This shadowing is intentional** for mdata's use case (they WANT to be able to define + redefine control verbs by name), but it implies a discipline: don't `set_var(name, ...)` with a name you've registered. The `register_fn` docstring documents this.

### 8. unregister_fn warn-on-inconsistency (per MC-13)

`unregister_fn` removes the callable from the dispatcher AND best-effort drops the Func placeholder via `del_var`. If the placeholder was already cleared (user shadowed via `set_var` or explicit `del_var`), `unregister_fn` emits `warnings.warn(...)` so the inconsistency surfaces in logs — NOT silently logged-and-ignored. The unregister itself still succeeds.

### 9. parking_lot::RwLock non-reentrancy + setter restriction

`parking_lot::RwLock` (used for `external_dispatcher` and `callables`) is NOT reentrant. `set_external_dispatcher` is therefore **NOT exposed to Python** in Sprint 23 — mdata doesn't need it, and exposing it would risk a Python callback calling `engine.set_external_dispatcher(...)` from within a dispatch (deadlock against the outer read-lock). Future re-evaluation if a use case appears.

---

## Consequences

### Golden rules preserved

- **GR5 (GIL released around `Engine::eval`):** callback invocation is OUTSIDE `Engine::eval`'s `py.detach`. The GIL is re-acquired ONLY for the callback duration (~300ns per round-trip), then released. The concurrent-throughput bench (~85k chili evals/sec from Sprint 7) is unaffected for non-W3 users (`external_dispatcher = None` → W3 branch never reached). For W3 users, callback dispatch is opt-in per-fn.
- **GR4 (Int64-quantized storage):** no on-disk format change.
- **GR6 (parse-cache hit ~385ns):** external fns are registered post-parse; cache is unaffected.

### Re-entrancy is safe

Verified by:
- chili-core code-trace (grep proof above): no `vars` lock held across function-dispatch.
- New `callables` lock-discipline contract (§4): write-lock never held while calling Python.
- Committed `concurrent_dispatch_no_deadlock` test (4 dispatch threads × 1000 iters + 1 register/unregister thread × 100 iters interleaved): asserts no hang, all returns correct, no `callables` deadlock.

### Sprint 23 test count delta

- Rust: +5 (`crates/chili-core/tests/external_fn_test.rs`)
- chili-py pytest: +6 (`crates/chili-py/tests/test_register_fn.py`)
- Expected final: Rust 210 → 215; pytest 100 → 106.

### What this ADR does NOT cover

- Async callbacks (`async def`). Sync only. Future ADR if mdata signals need.
- Multi-language callbacks (R / Julia / JS). The trait is generic enough that future dispatchers can install side-by-side, but no impl this sprint.
- PyErr type preservation. See alternatives §3.
- Implicit arity via `inspect.signature`. See alternatives §2.
- `set_external_dispatcher` exposed to Python. Deferred per §9.

---

## Alternatives considered

### 1. `set_var` extended to accept callables

Rejected. Overloads value-vs-fn semantics (`set_var(name, 42)` and `set_var(name, lambda: 42)` would have entirely different runtime behavior). The discoverability is also worse — `register_fn` is greppable, named, and explicitly about pepper-invocable functions.

### 2. Implicit arity via `inspect.signature(callable).parameters`

Rejected. Magical for normal Python fns and lambdas; doesn't work cleanly for: C extensions, `functools.partial`, callables with `*args`/`**kwargs`, callables with annotation-only optional args. Explicit `arity=N` is one extra parameter; the cleaner contract is worth it. Mismatched arity at call-site projects (matches existing pepper partial-application behavior) — same error story as user-defined pepper functions.

### 3. PyErr-preserving exceptions

Rejected. Would require a new `SpicyError` variant (`SpicyError::PyExc(opaque PyErr handle)`) plus threading PyErr through chili-core (which currently has zero pyo3 dependency — golden rule 5's structural guarantee). The added complexity is significant; the value is marginal: mdata's 3 control verbs use standard exception shapes (`ValueError`, `RuntimeError`) and the stringified traceback already conveys enough for caller-side `try/except` on substring matching. Future re-evaluation if a use case appears.

---

## Open questions (none blocking)

1. **mdata adoption order.** mdata may want to migrate one verb at a time (`.mdata.eod.fire` first) rather than all three. The W3 surface supports this — register and unregister are per-name. mdata-side scope.
2. **Cross-engine callable migration.** If mdata spawns engines via `multiprocessing.spawn` (per `check_fork` guidance), each child engine starts fresh — callables must be re-registered. Documented in the `register_fn` docstring.

---

## Cross-references

- Sprint 23 dispatch brief + audit appendix: `docs/sim/sprint_23_dispatch_brief_2026-05-24.md` (operative impl spec).
- Hazard-#1 grep methodology: brief §Why-now §2.1.
- Hazard-#2 microbench: brief §Why-now §2.2; throwaway `/tmp/bench_gil_overhead.py`.
- mdata wishlist (turn-9 revision): `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` §W3.
- Notification to mdata (pre-impl gate): `.cross_comms/outbox/<key>.json` (`design_question` topic).
- Lessons referenced: L5 (GIL release contract — preserved); L20 (cross-read normative lines at phase boundaries — this ADR has been cross-read against the brief, MC-1 through MC-13, and the round-2 fold-gap items before publication).
