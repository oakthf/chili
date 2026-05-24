# Sprint 23 dispatch brief — mdata W3 Python-callable bridge (P1)

**Kickoff:** 2026-05-24 — gate cleared by mdata's W3 unblock signal (in-session verbal during planning conversation 2026-05-24; v1-36 migration awaits chili W3 — see audit MC-2 for record-keeping action).
**Owner:** coordinator-solo
**Type:** implementation + ADR
**Predicted pp:** 13–17 (mid 15) — revised from 12–17 (mid 14) per audit MC-10 (docs overhead correction)
**Plan reference:** `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` §W3
**ADR references:** ADR-0007 (new — drafted FIRST per audit MC-1, before any impl)

---

## Sprint objective

Enable mdata daemons to register Python callables as pepper-invokable functions, so chili-IPC `sync(h, (".mdata.eod.fire", date))` dispatches into a Python handler with its full bookkeeping context. Closes the third (and final) gap from mdata's 2026-05-23 wishlist; unblocks mdata's v1-36 attach-socket retirement.

**Binary success criterion:** end-to-end pytest passes — Python callable registered via `engine.register_fn(name, callable, arity=N)`, invoked via `client.sync(h, (name, *args))` over chili:// TCP, return value flows back to caller; raised Python exceptions propagate as `ChiliError` with the Python traceback embedded; the existing 210 Rust + 100 chili-py pytest gate stays green with zero regression; **golden rule 5 (GIL released around `Engine::eval`) preserved** — verified by the existing concurrent-throughput bench remaining within ±2% of baseline.

---

## Why now

- **mdata gate flipped.** mdata's v1-36 migration is now blocked on W3 — poll-on-variable workaround (functional but ~100-200ms latency per control verb) is no longer "acceptable workaround" but "deferred until chili ships W3." mdata has bandwidth on other work in the interim.
- **Two pre-sprint hazards measured + closed** (this brief's design is grounded, not speculative):
  1. **Re-entrancy / deadlock** — `eval.rs` and `chili-op/src/*` contain ZERO `self.vars.read()/write()` calls. Function dispatch (`eval_fn_call:41-47`) invokes `f` / `f_with_side_effect` outside any held vars lock. A Python callback at the dispatch point inherits the same lock-free contract as builtins today. Re-entry via `engine.fn_call` / `engine.set_var` takes locks fresh; no nesting. **Not a deadlock hazard under the proposed design.**
  2. **GIL acquire/release overhead** — measured against `get_var/set_var` (each one full `py.detach + chili op + with_gil` cycle): 151ns / 152ns per call (100k-iter loop, post-warmup). A W3 callback dispatch adds ~2× this = **~300ns per callback round-trip + Python interpreter time for the callback body**. For mdata's 3 daily-cadence control verbs: negligible. *(Methodology correction per audit MC-4: the original draft cited `has_var`=89ns as the `py.detach` cost — that's wrong; `has_var` doesn't use `py.detach` and runs GIL-held. The real `py.detach`-inclusive number is 151ns. Conclusion holds; citation now correct.)*
- **0.8.8 just shipped, gate green** (210 Rust + 100 chili-py pytest, 0 failed). Clean baseline.
- **No competing scope.** Sprint 22 closed all of mdata's 2026-05-23 wishlist except W3; no other open items in `docs/sync/decisions-needed.md` or `docs/sync/ideas.md` require immediate action.

---

## Scope — Part A: chili-core (Func extension + dispatcher trait + eval_fn_call branch)

### A.1 Surface additions

**`Func` field** (`crates/chili-core/src/func.rs`):

```rust
pub struct Func {
    ...existing fields...
    /// W3 (Sprint 23) — external-fn name dispatched via
    /// `EngineState::external_dispatcher` when set. Mutually exclusive
    /// with `f` / `f_with_side_effect` / user-defined `nodes`.
    pub external_name: Option<String>,
}

impl Func {
    pub fn new_external(name: &str, arity: usize) -> Self {
        Self {
            fn_body: name.to_owned(),
            pos: SourcePos::new(0, 0),
            arg_num: arity,
            missing_index: (0..arity).collect(),
            params: (0..arity).map(|i| format!("arg{}", i)).collect(),
            nodes: Box::new(vec![]),
            part_args: None,
            f: None,
            f_with_side_effect: None,
            is_built_in_fn: false,
            is_raw: false,
            lang: Language::Pepper,
            external_name: Some(name.to_owned()),
        }
    }
}
```

**`ExternalFnDispatcher` trait** (new `crates/chili-core/src/external_fn.rs`):

```rust
pub trait ExternalFnDispatcher: Send + Sync {
    /// Dispatch a registered external (Python) function by name.
    /// Implementations MUST NOT hold any chili-core lock for the duration of
    /// the call — invocation runs lock-free per the FuncType contract.
    fn dispatch(&self, name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj>;
}
```

**`EngineState` slot** (`crates/chili-core/src/engine_state.rs`):

```rust
pub struct EngineState {
    ...existing fields...
    external_dispatcher: RwLock<Option<Arc<dyn ExternalFnDispatcher>>>,
}

impl EngineState {
    pub fn set_external_dispatcher(&self, d: Arc<dyn ExternalFnDispatcher>) {
        *self.external_dispatcher.write() = Some(d);
    }

    pub fn clear_external_dispatcher(&self) {
        *self.external_dispatcher.write() = None;
    }
}
```

**`eval_fn_call` new branch** (`crates/chili-core/src/eval.rs:19-82`):

```rust
if func.is_side_effect() {
    func.f_with_side_effect.as_ref().unwrap()(state, stack, &all_args)
} else if func.is_built_in_fn() {
    let f = func.f.as_ref().ok_or_else(...)?;
    f(&all_args)
} else if let Some(name) = &func.external_name {
    // W3 — lock-free dispatch via the engine's external dispatcher.
    // Mirror the read-clone-release pattern in `fn_call:1942-53`: clone the
    // Arc out from under a brief read lock, then invoke outside the lock.
    let dispatcher = {
        let guard = state.external_dispatcher.read();
        guard.as_ref().map(Arc::clone)
    };
    let dispatcher = dispatcher.ok_or_else(|| SpicyError::EvalErr(format!(
        "external fn '{}' registered but no dispatcher installed (chili-py engine init?)",
        name
    )))?;
    dispatcher.dispatch(name, &all_args)
} else {
    // existing user-defined function body path...
}
```

### A.2 Implementation hints

- **`SpicyObj::Fn(Func)` is `Clone + PartialEq + Debug`.** The new `external_name: Option<String>` field stays `Clone + PartialEq + Debug` (String is). No derive break.
- **Clone-out-of-lock pattern** is the existing convention. Two patterns to mirror: `EngineState::fn_call:1942-1953` (SpicyObj-clone-out-of-lock — `get_var` clones then releases, then `eval_fn_call` runs outside) and `engine_state.rs:1134-1136` (Arc-clone-out-of-lock — the `arc_self` slot pattern). Don't invent a new pattern.
- **Lock-free invocation contract** is the same one builtins already follow. The new comment in `eval_fn_call`'s W3 branch should call this out explicitly + reference the `ExternalFnDispatcher::dispatch` doc.
- **No on-disk format change** (golden rule 4 safe). `external_name` is per-instance; it doesn't serialize. (See MC-5 in appendix for wire-format note.)
- **No parse-cache impact** (golden rule 6 safe). External fns are registered post-parse; cache is unaffected.
- **DO NOT reuse `EngineState::register_fn` (`engine_state.rs:270`) for W3** (per MC-6). That fn registers static fn-maps (`LazyLock<HashMap<String, Func>>`) used by `LOG_FN` / `BUILT_IN_FN` at engine init. W3 registration goes via `set_var(name, SpicyObj::Fn(placeholder))` (per B.1). The existing `EngineState::register_fn` and the new `PyEngineState::register_fn` are intentionally orthogonal — same name, different namespace, different semantics.
- **Update `EngineState::initialize()` (`engine_state.rs:201-221`)** (per MC-7) to include `external_dispatcher: RwLock::new(None)`. Verified-grep: `initialize()` is the sole literal-construction site; `Default::default()` (line 165) and `EngineState::new()` both delegate to it; benches at `parse_cache.rs:18` and `common/mod.rs:238` also delegate. No other construction sites need updating.
- **`Func::project` preserves `external_name`** via `..self.clone()` at `func.rs:147` — already correct; partial-applied external Funcs retain their dispatcher binding. Mentioned here so a future maintainer doesn't second-guess.

### A.3 Storage / schema

None.

### A.4 Tests

- `crates/chili-core/tests/external_fn_test.rs` (new) — 5 Rust unit tests using a stub `ExternalFnDispatcher` impl:
  1. `dispatcher_invoked_via_pepper_eval` — register stub, eval `(".stub_fn", 1, 2)` → stub returns the sum
  2. `error_without_dispatcher` — Func with `external_name` set but `set_external_dispatcher` never called → clean EvalErr
  3. `arity_mismatch_returns_projection` — call with fewer args → partial-application path (matches existing user-fn behavior)
  4. `dispatcher_can_be_replaced` — install A, then install B; pepper sees B
  5. `concurrent_dispatch_no_deadlock` — 4 dispatch threads (1000× each) **+ 1 thread that interleaves `register("temp_<i>", stub, 1)` / `unregister("temp_<i>")` 100× concurrently** (per MC-3); assert no hang + all dispatch returns correct + no `callables`-RwLock deadlock against the read-clone-out-of-lock path

---

## Scope — Part B: chili-py (PyExternalDispatcher + register_fn + GIL semantics)

### B.1 Surface additions

**`PyExternalDispatcher` struct** (`crates/chili-py/src/external_dispatcher.rs`, new):

```rust
pub struct PyExternalDispatcher {
    callables: RwLock<HashMap<String, Py<PyAny>>>,
}

impl PyExternalDispatcher {
    pub fn new() -> Self {
        Self { callables: RwLock::new(HashMap::new()) }
    }

    pub fn register(&self, name: &str, callable: Py<PyAny>) {
        self.callables.write().insert(name.to_owned(), callable);
    }

    pub fn unregister(&self, name: &str) -> bool {
        self.callables.write().remove(name).is_some()
    }
}

impl ExternalFnDispatcher for PyExternalDispatcher {
    fn dispatch(&self, name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
        // Clone the Py<PyAny> out from under the read lock (with GIL held
        // for the clone_ref), then invoke outside the lock.
        let callable: Option<Py<PyAny>> = Python::with_gil(|py| {
            let guard = self.callables.read();
            guard.get(name).map(|c| c.clone_ref(py))
        });
        let callable = callable.ok_or_else(|| SpicyError::EvalErr(format!(
            "no Python callable registered for external fn '{}'", name
        )))?;
        Python::with_gil(|py| invoke_python(py, &callable, name, args))
    }
}

fn invoke_python(
    py: Python<'_>,
    callable: &Py<PyAny>,
    name: &str,
    args: &[&SpicyObj],
) -> SpicyResult<SpicyObj> {
    // 1. Convert chili args → Python objects.
    let py_args: Vec<Py<PyAny>> = args
        .iter()
        .map(|a| spicy_to_py(py, (*a).clone()).map_err(|e| SpicyError::EvalErr(format!(
            "external fn '{}' arg conversion failed: {}", name, e
        ))))
        .collect::<Result<Vec<_>, _>>()?;
    let py_tuple = PyTuple::new(py, py_args.iter())
        .map_err(|e| SpicyError::EvalErr(format!("PyTuple::new: {}", e)))?;

    // 2. Invoke. Python exceptions become stringified ChiliError with
    //    traceback embedded (per ratified design — explicit-arity option).
    let result = callable.bind(py).call1(py_tuple).map_err(|e| {
        let msg = format_pyerr_with_traceback(py, &e, name);
        SpicyError::EvalErr(msg)
    })?;

    // 3. Convert Python return → SpicyObj.
    spicy_from_py_bound(&result).map_err(|e| SpicyError::EvalErr(format!(
        "external fn '{}' return conversion failed: {}", name, e
    )))
}

/// Format a PyErr as a chili-side error message:
///   "external fn '<name>' raised: <ExcType>: <msg>\n  at <file>:<line>\n  ..."
fn format_pyerr_with_traceback(py: Python<'_>, e: &PyErr, name: &str) -> String {
    /* read e.traceback, format frames; fall back to plain repr on error */
}
```

**`PyEngineState::register_fn` method** (`crates/chili-py/src/lib.rs` add to `#[pymethods] impl PyEngineState`):

```rust
/// Register a Python callable as a pepper-invokable function.
///
/// The function becomes callable from pepper source via tuple-dispatch:
///   sync(h, (name, *args))                          # over IPC
///   engine.fn_call(name, args)                      # local
///   (in pepper source) name[arg1; arg2; ...]
///
/// Arguments:
///   name:     pepper function name (any valid identifier; mdata convention
///             uses dotted names like ".mdata.eod.fire").
///   callable: Python callable. Args + return are bridged via the existing
///             chili type system (primitives, dates, lists, dicts, DataFrames).
///   arity:    number of arguments the callable expects. Mismatch at
///             call-site projects (matches existing pepper partial-application
///             behavior).
///
/// Concurrency: callbacks dispatch on the chili-IPC thread that received the
/// call. The dispatcher acquires the GIL for the call duration; the callable
/// is responsible for any internal thread-safety. Re-entry into engine
/// methods (engine.fn_call / engine.set_var / engine.get_var) from within
/// the callback is safe — see ADR-0007.
///
/// Exceptions raised by the Python callable propagate as ChiliError on the
/// caller side with the Python traceback embedded.
#[pyo3(signature = (name, callable, arity))]
fn register_fn(
    &self,
    name: &str,
    callable: Bound<'_, PyAny>,
    arity: usize,
) -> PyResult<()> {
    self.check_fork()?;
    if !callable.is_callable() {
        return Err(PyTypeError::new_err(format!(
            "register_fn: 'callable' must be callable, got {}",
            callable.get_type().name()?
        )));
    }
    self.ensure_external_dispatcher();   // lazy-init on first register_fn
    self.external_dispatcher.register(name, callable.unbind());
    // Register the Func placeholder in chili-core vars so pepper dispatch
    // finds it by name lookup.
    let placeholder = Func::new_external(name, arity);
    map_spicy_error(self.inner.set_var(name, SpicyObj::Fn(placeholder)))?;
    Ok(())
}

/// Unregister a previously registered Python callable.
/// Returns True if a callable was removed, False if not registered.
///
/// If the Func placeholder in `vars` was already cleared by the user
/// (e.g., via engine.del_var(name) or engine.set_var(name, ...) shadowing),
/// emits `warnings.warn(...)` so the inconsistency surfaces in mdata's logs
/// (per MC-13). The unregister itself still succeeds — the callable IS
/// removed from the dispatcher.
fn unregister_fn(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
    self.check_fork()?;
    let removed = self.external_dispatcher.unregister(name);
    if removed {
        // Per MC-13: emit a UserWarning on del_var failure, not silent log.
        if let Err(_e) = self.inner.del_var(name) {
            let warnings = py.import("warnings")?;
            warnings.call_method1(
                "warn",
                (format!(
                    "unregister_fn: external Func placeholder '{}' was already cleared from vars; \
                     callable removed from dispatcher but no placeholder Func was present.",
                    name
                ),),
            )?;
        }
    }
    Ok(removed)
}
```

The `external_dispatcher` slot on `PyEngineState`: an `Arc<PyExternalDispatcher>` stored on the struct, lazy-init'd on first `register_fn`, installed on `EngineState` via `set_external_dispatcher`.

### B.2 Implementation hints

- **Mirror existing GIL-release pattern.** `chili-py` uses `py.detach(move || self.inner.X(...))` to release GIL around chili-core ops; this is exactly what `engine.fn_call` from within a callback will do too — the Python callback's call to `engine.fn_call` releases the GIL, lets chili dispatch on a freshly-locked-and-released cycle, reacquires GIL on return.
- **Re-entry-into-engine within callback works because:**
  - Outer pepper enters via `py.detach` → GIL released
  - Pepper dispatches W3 → dispatcher's `with_gil` → GIL re-acquired → Python callback runs
  - Callback calls `engine.fn_call` → pymethod entrypoint already inside GIL → method's internal `py.detach` releases → chili dispatch (no parent lock held; clean) → reacquires GIL on return
  - Callback returns → dispatcher's `with_gil` block exits → GIL released → outer pepper resumes
- **`spicy_to_py` / `spicy_from_py_bound`** already handle the full type matrix. Reuse; do not duplicate.
- **Lazy-init the dispatcher** so engines that never call `register_fn` pay zero cost (matches the existing pattern for `start_job_scheduler` etc.).
- **Don't install `external_dispatcher` until first registration** — preserves GR5 bench by leaving the dispatcher slot `None` and the eval_fn_call W3 branch never reached for non-W3 users.

### B.3 Tests

- `crates/chili-py/tests/test_register_fn.py` (new) — 6 chili-py pytest:
  1. `test_register_and_invoke_local` — register `add_two(a, b) → a+b`, call via `engine.fn_call(".add_two", [1, 2])` → 3
  2. `test_register_and_invoke_remote` — same, but via `client.sync(h, (".add_two", 1, 2))` over chili:// TCP (MC-mirror of Sprint 22's E2E closure-gate test pattern)
  3. `test_callback_reentry` — the mdata-shape: callback that does `engine.set_var("ack", v)` + `engine.fn_call(".other_fn", args)`; assert no deadlock, ack visible after return
  4. `test_python_exception_propagates` — callback raises `ValueError("bad date")`; `client.sync(h, ...)` raises `ChiliError` containing both `ValueError` and the traceback line
  5. `test_arity_mismatch` — register arity=2, call with 1 arg → projection (returns a partial-applied Func, matching pepper user-fn behavior)
  6. `test_unregister` — register, invoke OK, `engine.unregister_fn(name)` returns True, subsequent invoke raises clean error

### B.4 Bench gate (golden rule 5)

- Run the existing concurrent-throughput bench BEFORE the sprint to lock in the baseline number; run again AFTER landing W3 changes. Append to `docs/bench/post_pivot_baseline_2026-05-07.md`. Assert within ±2% of baseline. If outside ±2%, halt-and-escalate (criterion #1).

---

## Scope — Part C: ADR-0007 + docs

### C.1 ADR-0007 — W3 Python-callable bridging via ExternalFnDispatcher

Path: `docs/decisions/0007-w3-python-callable-bridge.md`.

Sections:
1. **Status:** Accepted (this sprint)
2. **Context:** mdata wishlist 2026-05-23 W3; v1-36 migration unblock
3. **Decision:**
   - Add `external_name: Option<String>` to `Func`
   - Add `ExternalFnDispatcher` trait + `EngineState::external_dispatcher` slot
   - Add `chili-py::PyExternalDispatcher` impl + `engine.register_fn(name, callable, arity)` API
   - Exceptions are stringified into `ChiliError` (with traceback); not type-preserved
   - Arity is explicit at registration
4. **Consequences:**
   - **GR5 preserved.** Callback invocation is OUTSIDE `Engine::eval`'s `py.detach`; GIL is re-acquired only for the callback duration. Measured cost ~200ns per dispatch.
   - **Re-entrancy is safe.** Verified by code-trace: `eval.rs` + `chili-op/src/*` hold zero `vars.read()/write()` across function-dispatch boundaries (`grep` returns empty). Callbacks inherit the same lock-free contract as builtins. Concurrent dispatch test (5-thread × 1000-iter) added as the structural invariant guard.
   - **No on-disk format / parse-cache impact.** GR4 + GR6 untouched.
   - **No async support.** Callbacks are sync. (Mirrors Sprint 22's W3 design-discussion of poll-on-variable's adequacy for control-plane verbs.)
5. **Alternatives considered:**
   - `set_var` extended to accept callables — rejected: overloads value-vs-fn semantics
   - PyErr-preserving exceptions — rejected: requires new SpicyError variant + threading PyErr through chili-core
   - Implicit arity via `inspect.signature` — rejected: doesn't work for C extensions / partial / variadic; explicit is cleaner
6. **Cross-references:** `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` §W3; lesson 5 (GIL release contract); the two hazard measurements documented in this brief.

### C.2 docstring updates

- `engine.register_fn` — full docstring per B.1 above (already drafted)
- `engine.unregister_fn` — short docstring
- `engine.fn_call` — add a "Note: also dispatches Python callables registered via register_fn" line
- `engine.sync` — already-shipped 0.8.8 docstring; no change

### C.3 README + CHANGELOG

- `crates/chili-py/README.md` — add a "Python-callable bridge" section with mdata-style example
- `CHANGELOG.md` — 0.8.9 entry summarizing W3

---

## Out of scope (defer)

- **Async callbacks (`async def`).** Per mdata's stated use case (3 control verbs, sync handlers); awaitable callbacks would need a runtime story in chili-core that doesn't exist. If mdata signals need: future ADR.
- **Multi-language callbacks.** Just Python for now. The `ExternalFnDispatcher` trait is generic enough that a future R / Julia / JS dispatcher can install side-by-side, but no need to scope it.
- **Removal of poll-on-variable workaround in mdata.** mdata-side scope, not chili.
- **PyErr type preservation.** Decided against per ratified design — stringified.
- **Implicit arity via `inspect.signature`.** Decided against per ratified design — explicit.
- ~~Removing the W3 placeholder Func from `vars` on `unregister_fn` failure paths — logged-and-ignored~~ **Superseded by MC-13: `unregister_fn` emits `warnings.warn(...)` on `del_var` failure so mdata's logs surface the inconsistency.** Strikethrough preserved here for audit trail.

---

## Deliverables

| # | Artifact | Type | Order |
|---|---|---|---|
| 0 | `docs/decisions/0007-w3-python-callable-bridge.md` (ADR — drafted FIRST per MC-1) | new | pre-impl gate (**must precede 0c**) |
| 0b | `docs/bench/post_pivot_baseline_2026-05-07.md` — **pre-sprint** concurrent-throughput bench number captured (MC-11) | edit | pre-impl gate (parallel-safe with 0/0c/0d) |
| 0c | `.cross_comms/outbox/<key>.json` — `design_question` to mdata with API contract + ADR-0007 link (MC-2) | new | pre-impl gate (**requires 0 done first**) |
| 0d | `docs/sync/decisions-needed.md` — record in-session gate-clear signal (MC-2) | edit | pre-impl gate (parallel-safe) |
| 1 | `crates/chili-core/src/func.rs` (Func + Func::new_external) | edit | Part A |
| 2 | `crates/chili-core/src/external_fn.rs` (trait) | new | Part A |
| 3 | `crates/chili-core/src/engine_state.rs` (slot + setter/clearer, `initialize()` updated per MC-7) | edit | Part A |
| 4 | `crates/chili-core/src/lib.rs` (re-export `ExternalFnDispatcher`) | edit | Part A |
| 5 | `crates/chili-core/src/eval.rs` (eval_fn_call W3 branch) | edit | Part A |
| 6 | `crates/chili-core/src/serde9.rs` (inline comment per MC-5 — external Func wire behavior) | edit | Part A |
| 7 | `crates/chili-core/tests/external_fn_test.rs` (5 unit, concurrent test extended per MC-3) | new | Part A |
| 8 | `crates/chili-py/src/external_dispatcher.rs` (impl + invoke_python + format_pyerr) | new | Part B |
| 9 | `crates/chili-py/src/lib.rs` (PyEngineState slot + register_fn + unregister_fn-with-warn per MC-13) | edit | Part B |
| 10 | `crates/chili-py/tests/test_register_fn.py` (6 pytest, unregister-warn check per MC-13) | new | Part B |
| 11 | `crates/chili-py/README.md` (callable-bridge section) | edit | Part C |
| 12 | `CHANGELOG.md` (0.8.9 entry) | edit | Part C |
| 13 | Workspace `Cargo.toml` + chili-py `Cargo.toml` + `pyproject.toml` (0.8.8 → 0.8.9; LESSON 14: bump BOTH or wheel is mis-labelled) | edit | wrap |
| 14 | `dist/chili_sauce-0.8.9-cp310-abi3-macosx_11_0_arm64.whl` | new | wrap |
| 15 | `docs/sync/mdata_chili_2026-05-XX_0.8.9_delivery.md` (date filled at wrap) | new | wrap |
| 16 | `docs/bench/post_pivot_baseline_2026-05-07.md` — **SECOND edit** (post-impl): append the post-W3 concurrent-throughput number alongside the #0b pre-sprint capture; assert ±2% delta. Do NOT overwrite #0b's row. | edit | wrap |
| 17 | `docs/sim/sprint_23_retro.md` | new | post-sprint |
| 18 | `docs/sim/cadence_metrics.md` (row 23) | edit | post-sprint |
| 19 | `docs/history/sprints/sprint_23_dispatch_brief_2026-05-24.md` | move | post-ratification |

---

## Lead allocation

**Coordinator-solo.** No subagent fanout. W3 is structurally tight (3 surface files + 1 ADR + tests + delivery), and the hazards are already measured so there's no fresh investigation block that would benefit from parallelism.

Subagent dispatches that ARE planned:
- **Pre-impl 3-agent audit** (this brief, per `~/.claude/rules/self-audit-on-plans.md`): Explore + code-reviewer + planner in parallel. Material corrections fold into audit appendix.
- **Post-impl code-reviewer** (Sprint 22 lesson 7): single code-reviewer pass on staged changes before commit.

No worktree (single-branch sprint).

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~7pp in), post a short status:

- Is the chili-core W3 branch landed + tests green?
- Does the chili-py callback re-entry pytest (`test_callback_reentry`) pass without deadlock?
- Did the concurrent-throughput bench stay within ±2% of baseline?
- ETA to wrap.

Halt-and-escalate criteria:

1. **GR5 regression** — concurrent-throughput bench falls outside ±2% of #0b pre-sprint baseline.
2. **Deadlock surfaces in re-entry tests** — hazard #1 measurement was wrong; needs ADR revision before continuing.
3. **mdata signals (via `.cross_comms/inbox/`) a different call shape** after the pre-impl API-contract notification (deliverable #0c) — pause and reconcile before further impl. (Per MC-12: this criterion is testable now that there's a notification path.)
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 7pp.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- Python-bindings wrap: `cd crates/chili-py && uv run maturin develop && uv run pytest`. Expected: 100 → ~106 pytest (W3 adds 6).
- Bench delta documented in `docs/bench/post_pivot_baseline_2026-05-07.md` (concurrent throughput within ±2%).
- Test-count delta documented (Rust 210 → ~215, +5; pytest 100 → ~106, +6).
- ADR-0007 **validated by impl** (pre-drafted at gate #0; revise + re-commit if impl revealed contract revisions, then re-publish to mdata; otherwise note "no revisions needed" in retro).
- 0.8.9 wheel cut + sha256 recorded + delivery doc written.
- Author retro at `docs/sim/sprint_23_retro.md`.
- Append row to `docs/sim/cadence_metrics.md`.
- Move this brief to `docs/history/sprints/`.
- HALT until user ratifies.

---

## Pp accounting reference

Closest historical comparable sprints from `docs/sim/cadence_metrics.md`:

- **Sprint 21 (push-model D-1/D-2/D-3)** — predicted 14–22 (mid 18, post-audit), actual ~16–18. Comparable because: (a) similar surface span — chili-core internals + chili-py FFI + new ADR + tests + wheel cut + mdata delivery; (b) involves cross-thread/lock reasoning + GR5 preservation + bench gate.
- **Sprint 22 (mdata wishlist W1+W2)** — predicted 7–13 (mid 10), actual ~9–11. Comparable because: (a) wishlist response, same surface shape (chili-core + chili-py + tests + ADR-aware + wheel + delivery); (b) similar audit + post-impl-reviewer ceremony.

Sprint 23 expected at the **mid band** of the predicted (mid 15, ~13–17 per MC-10 revision), capped above by:
- ADR-0007 drafting (1-2pp; new ADR with hazards section)
- The mdata-shape `test_callback_reentry` is the most-novel test; expect 1 iteration to settle exception-message wording
- Bench-gate verification adds ~1pp if any tuning needed
- Documentation surface is larger than Sprint 22 (full new API + ADR + README + CHANGELOG)

If audit appendix surfaces material corrections, expect predicted to revise to upper band (~15-17).

---

## Cross-references

- Wishlist: `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` §W3
- 0.8.8 delivery doc (W3 re-evaluation gate language): `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md`
- Sprint 22 retro (W3 deferral rationale): `docs/sim/sprint_22_retro.md`
- Hazard-#1 measurement methodology: this brief, "Why now" §2.1 (grep-based lock-discipline confirmation)
- Hazard-#2 measurement methodology: this brief, "Why now" §2.2 (`/tmp/bench_gil_overhead.py`, 100k-iter has_var loop)
- Related ADRs: ADR-0006 (push-model FFI; structurally adjacent surface); ADR-0007 (this sprint — W3 Python-callable bridge; pre-drafted at gate #0)
- Cross-project: mdata v1-36 migration plan (mdata-side; this sprint is the unblocker)
- Lessons referenced: L5 (GIL release contract), L7 (post-impl code-reviewer), L14 (pyproject.toml + Cargo.toml dual-bump), L19 (auto-merge cascades — unlikely this sprint but on watch), L20 (cross-read normative lines at phase boundaries — applies to ADR-0007 ratification step)

---

## Appendix — Independent audit (2026-05-24)

Three-agent parallel audit per `~/.claude/rules/self-audit-on-plans.md` (Explore + code-reviewer + planner). Findings tagged by source agent (E / R / P) + severity. **13 material corrections (MCs) below MUST be folded into impl before any code change.** Sprint sizing revised in MC-10.

### Material corrections (must be addressed before impl)

**MC-1 (🟡 P1) — ADR-0007 drafted FIRST, not at wrap.**
Original brief lists ADR-0007 as deliverable #10 alongside impl. Planner flagged: locking the contract (explicit-arity, stringified exceptions, no-async) BEFORE impl avoids ratifying a fait accompli if impl reveals the chosen contract causes mdata pain. **Action:** Promote ADR-0007 to deliverable #0; draft + commit before Part A starts; mark "Accepted pending impl validation"; the post-impl code-reviewer pass confirms no revisions needed.

**MC-2 (🟡 P2, R4) — Pre-impl mdata notification.**
Original brief has no step to surface the W3 API contract (explicit-arity, stringified exceptions, no async) to mdata before impl. The "gate cleared by mdata's W3 unblock signal" assertion is in-session verbal, not in `.cross_comms/inbox/` or `docs/sync/decisions-needed.md`. Sprint 22 saw a CRITICAL turn-9 wishlist drift caught only at post-impl review. **Action:** As part of ADR-0007 (per MC-1), publish a `design_question` event to mdata's outbox with the API contract + audit appendix link; await one-day objection window or explicit ack before Part A. Also record the in-session gate-clear signal in this brief's "Why now" §1 (added 2026-05-24 by user during planning conversation; reference this session in `docs/sync/decisions-needed.md`).

**MC-3 (🟡 P4) — Hazard-#1 coverage gap: new `PyExternalDispatcher.callables` RwLock not covered by the grep proof.**
The hazard-#1 proof in "Why now" §2.1 greps `crates/chili-core/src/eval.rs` + `crates/chili-op/src/*.rs` for `self.vars.read()/write()`. That covers the EXISTING dispatch path. The NEW `callables: RwLock<HashMap<String, Py<PyAny>>>` on `PyExternalDispatcher` is a fresh lock introduced by this sprint and is NOT covered by the existing proof. **Action:** Add to ADR-0007's "Lock discipline" section: `callables` write-lock is NEVER held while calling Python; the `dispatch()` path clones `Py<PyAny>` out under a brief read-lock then invokes outside the lock (matches the brief's B.1 pseudocode). The `concurrent_dispatch_no_deadlock` test (A.4 #5) MUST also exercise concurrent `register` + `dispatch` (separate threads) to assert the new lock doesn't deadlock against the dispatch path.

**MC-4 (🟡 R2) — Hazard-#2 measurement methodology correction.**
Original brief cites "89ns / one full `py.detach + chili op + with_gil` cycle" from `has_var`. **`has_var` at `lib.rs:444-446` does NOT use `py.detach`** — it calls `self.inner.has_var(id)` directly while holding the GIL. The 89ns is a GIL-held-only baseline (RwLock::read + HashMap::contains_key). The actual `py.detach`-inclusive cost is shown by `get_var` (151ns) / `set_var` (152ns). **Conclusion holds** (callback overhead ~200ns), but the citation must be corrected. **Action:** Re-state hazard-#2 numbers in ADR-0007 + brief "Why now" §2.2: "Per-callback `py.detach + with_gil` round-trip is ~150ns (measured via `get_var/set_var`); add 1× `with_gil` for the Python callback dispatch + 1× `py.detach` if the callback re-enters chili → ~300ns total + Python interpreter time for the callback body. The 89ns `has_var` number is the GIL-held-only floor, NOT the `py.detach` cost." This is a documentation-correctness MC, not a design correction.

**MC-5 (🟡 R3) — External `Func` serializes silently over IPC; document non-callable behavior on remote side.**
`serde9.rs:941-950` serializes `SpicyObj::Fn(f)` by writing `f.fn_body` as a string. An external Func has `fn_body = name` (e.g., `".mdata.eod.fire"`). If a client does `sync(h, ".mdata.eod.fire")` (variable LOOKUP, str-form), the Fn is serialized → deserialized on remote as `Func::new_raw_fn(name, lang)` — no dispatcher, no `external_name`, not callable. Invocation of the deserialized Func would hit the user-defined path and error. The call-form (`sync(h, (".mdata.eod.fire", args))`) is fine — the Func resolves locally on the server side; only the result travels over the wire. **Action:** Document in ADR-0007 + add an inline comment to `serde9.rs:941`: "External Funcs (`external_name.is_some()`) serialize as `fn_body` only; deserialized form is non-callable on the remote side. Clients invoking external Funcs MUST use call-form sync, not variable-lookup sync."

**MC-6 (🟡 R1) — `EngineState::register_fn` name collision warning.**
`engine_state.rs:270` already defines `pub fn register_fn(&self, map: &LazyLock<HashMap<String, Func>>)` — used at lib.rs:362-363 for LOG_FN / BUILT_IN_FN static registration. The brief's `PyEngineState::register_fn` is on the Python side (different namespace), so no Rust collision. **BUT** the impl coder must NOT "discover" the existing `EngineState::register_fn` and try to reuse it for W3 (different signature, wrong semantics). **Action:** Add explicit note to A.1 implementation hints: "DO NOT reuse `EngineState::register_fn` for W3. That fn registers static fn-maps (`LazyLock<HashMap<String, Func>>`). W3 registration goes via `set_var(name, SpicyObj::Fn(placeholder))` (per B.1 sketch). The existing `register_fn` and the new `PyEngineState::register_fn` are intentionally orthogonal."

**MC-7 (🟡 R5) — `EngineState::initialize()` impl-hint addition.**
`engine_state.rs:165-167` defines `Default::default()` → `initialize()`; `initialize()` at lines 201-221 constructs `EngineState` with literal field values. Adding `external_dispatcher: RwLock<Option<Arc<dyn ExternalFnDispatcher>>>` requires updating `initialize()` with `external_dispatcher: RwLock::new(None)`. **Action:** Add to A.2 impl hints: "Update `EngineState::initialize()` (`engine_state.rs:201-221`) to include `external_dispatcher: RwLock::new(None)`. No other literal EngineState construction sites found via grep — `initialize()` is the sole entry."

**MC-8 (🟡 E1) — `set_var` shadowing of registered external Func is intentional.**
The brief proposes `register_fn` stores the placeholder via `state.set_var(name, SpicyObj::Fn(placeholder))`. A user calling `engine.register_fn(".mdata.eod.fire", callable, 1)` overwrites any existing var named `.mdata.eod.fire`. **This is the intended behavior** for mdata's use case (they WANT to define the fn). But it also means: a subsequent `engine.set_var(".mdata.eod.fire", 42)` SILENTLY breaks the registered callback (var is now Int, dispatcher receives no calls for that name). **Action:** Document in ADR-0007 + `register_fn` docstring: "Calling `set_var(name, ...)` AFTER `register_fn(name, ...)` overwrites the Func placeholder; subsequent pepper calls to `name` will follow normal var dispatch (will likely error since name is no longer a Fn). To re-register, call `register_fn` again. The internal callable in the dispatcher is left in place until `unregister_fn` is called."

**MC-9 (🟡 E2) — parking_lot::RwLock NOT reentrant; `set_external_dispatcher` restriction.**
parking_lot::RwLock (used for `external_dispatcher` and `callables`) is NOT reentrant. If a Python callback calls `engine.set_external_dispatcher(...)` (we don't expose this from Python, but in principle a future API could), it would deadlock against the read-lock held by the outer dispatch path. **Action:** ADR-0007 must document: "`set_external_dispatcher` MUST NOT be called from within a registered callback. Use only during engine init or from outside callbacks." Don't expose `set_external_dispatcher` to Python in Sprint 23 (mdata doesn't need it; future re-evaluation if needed).

### Sprint sizing revision

**MC-10 (🟡 P3) — Sprint sizing underestimates docs overhead. Revise to 13–17pp (mid 15).**
Brief originally allocates 1-2pp for ADR-0007 + 1-2pp for README/CHANGELOG/delivery doc. Historical data across Sprints 16-21 shows ADR + delivery doc consistently runs 2-3pp per sprint. New ADR with hazards section + alternatives + register_fn docstring + README callable-bridge section + CHANGELOG + delivery doc adds up to ~3-4pp. Sprint 22 actual was upper-mid (~9-11pp on predicted 7-13); Sprint 23 has more docs surface. **Action:** Update top-of-brief Predicted pp from "12–17 (mid 14)" to "**13–17 (mid 15)**". Update "pp accounting reference" section accordingly. The floor of 13pp requires docs to stay minimal AND all tests pass first-try.

### Deliverables / wrap refinements

**MC-11 (🟢 P7) — Add deliverable #0 "pre-sprint bench baseline captured."**
The wrap ceremony asserts the bench stays within ±2% of baseline (`docs/bench/post_pivot_baseline_2026-05-07.md`). Sprint 20 surfaced a false-green verifier bug; an explicit before-and-after pattern is safer than narrative. **Action:** Insert deliverable #0 at the top of the table: "0 | `docs/bench/post_pivot_baseline_2026-05-07.md` — pre-sprint concurrent-throughput bench number captured | edit". Move existing #1-19 to #1-20 (renumber). Wrap checklist gets an explicit "pre-sprint baseline number locked" line.

**MC-12 (🟢 P6) — Halt criterion #3 tie to MC-2 (pre-impl mdata notification).**
Halt criterion #3 ("mdata signals a different call shape") is vacuous unless mdata has a notification channel. MC-2 provides that channel. **Action:** Re-word criterion #3 to: "mdata signals (via `.cross_comms/inbox/`) a different call shape after the pre-impl API-contract notification (MC-2). Halt and reconcile before further impl." This makes the criterion testable.

**MC-13 (🟢 P5) — `unregister_fn` should warn on `del_var` failure, not silently log.**
Original brief documents `del_var` failure path as "logged-and-ignored." If a dangling Func placeholder is left behind, subsequent pepper calls hit the "no dispatcher installed" error path (user-hostile). **Action:** In B.1 `unregister_fn`, change "best-effort: also drop the Func placeholder. Ignore error if the var was already cleared" to "best-effort: also drop the Func placeholder. Emit a `warnings.warn(...)` if `del_var` fails so mdata's logs surface the inconsistency." Add to test_unregister: assert no warning on the happy path; assert warning emitted if user pre-deletes the var.

### Other clean-up surfaced by audit (no impl action required; for the impl coder's awareness)

- **`Func::project` preserves `external_name`** via `..self.clone()` at `func.rs:147` — already correct, just call this out in A.2 so future maintainers don't second-guess.
- **`Arc-clone-out-of-lock pattern` citation correction.** Original brief says "mirrors `fn_call:1942-1953`." That's the SpicyObj-clone-out-of-lock pattern (correct conceptually). The actual Arc-clone-out-of-lock pattern is `engine_state.rs:1134-1136` (arc_self slot). Update A.2 to cite both.
- **`test_arity_mismatch` verification step.** Before writing the assertion, confirm `SpicyObj::Fn` round-trips through `spicy_to_py`/`spicy_from_py_bound`. If `Fn` is not in the Python type bridge, the assertion needs to handle whatever spicy_to_py emits (likely a string repr or error).

### Verified-correct (re-checked, no MC needed)

- `Func` derives `Clone + PartialEq + Debug` at `func.rs:16` — `Option<String>` field addition stays compatible.
- `SpicyObj::Fn(Func)` variant at `obj.rs:50`.
- All `SpicyObj::Fn` dispatch sites go through `eval_fn_call` (verified via grep): `eval.rs:583` (eval_call), `engine_state.rs:1946` (fn_call), `side_effect_fn.rs:389/446` (each/over/map). **A single W3 branch in `eval_fn_call` suffices** — no other dispatch branches needed.
- pyo3 0.27 has `Bound<PyAny>::is_callable()` (ships pyo3 0.21+).
- No Wasm/no_std build targets; parking_lot is std-only.
- `Func` is implicitly Send + Sync (all fields are; `Option<String>` preserves this).
- Pre-commit gate (`cargo test --workspace --exclude chili-py`) matches dev_setup.md.
- Lesson 14 dual-bump (Cargo.toml + pyproject.toml) correctly called out in deliverables #13.

### Net audit outcome

**Zero CRITICAL findings; 13 MATERIAL+MINOR corrections folded above.** Brief is design-solid pending the MCs. After ratification, impl proceeds with the MCs as authoritative addenda — original brief sections remain unchanged for audit trail; the audit appendix is the operative impl spec where it differs.
