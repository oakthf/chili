# Sprint 17 dispatch brief — mdata wishlist v1 P1 bundle (eod dispatch + publish_via_handle)

**Kickoff:** TBD — pending user ratification of brief + audit appendix
**Owner:** coordinator-solo (debugger subagent for Part A.2 if hypothesis-search exceeds 2pp)
**Type:** implementation — one debug-then-fix item + one thin-wrapper additive surface
**Predicted pp:** 10–18 (pre-audit estimate; full ranges in §Pp accounting)
**Plan reference:** `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md` (P1 §82-135 + §137-200) + `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md` (Q3 + Q4 lock-in)
**ADR references:** none expected (additive surface + bug fix; no architectural decisions)

---

## Sprint objective

Close mdata's two P1 wishlist items:

- **Part A — P1 subscriber-side `eod` dispatch.** Make pepper-level `eod` function fire on subscriber engine when publisher's `(`eod; date)` tuple arrives. mdata's failing acceptance test: `tests/rdb/test_rdb_subscriber.py::test_subscriber_eod_shim_triggered_by_publisher_eod` (xfail strict=True).
- **Part B — P1 `publish_via_handle(h, table, df)`.** Thin one-shot publish primitive (per Q3 lock-in Option B). mdata writes their `RemoteTpClient` connection-manager class on top, ~50–80 LOC mdata-internal.

Binary success criterion:

1. mdata's `test_subscriber_eod_shim_triggered_by_publisher_eod` ported into chili-py pytest **passes strict** (not xfail).
2. A chili-py pytest publisher → subscriber round-trip using `engine.publish_via_handle` lands the `(`upd; table; df)` tuple on the subscriber's table sink.
3. 0.8.5 wheel built; sha256 + delivery doc match the Sprint 16 / 0.8.4 handoff shape.

---

## Why now

- **Sprint 16's lock-in resolved both P1 ambiguities.** Q3 reversed to Option B (chili owns marshalling, mdata owns TCP client) — saves ~10pp vs the original wishlist. Q4 provided the full failing test source + boot order. Both items are now scope-locked.
- **Both are mdata production blockers.** mdata's PRD §3.4 (subscriber EOD dispatch) is currently worked around with a Python-side timer reading `tp.config.eod_time` — works but has clock-skew risk across processes. publish_via_handle unlocks mdata's RemoteTpClient for cross-process publish.
- **Sprint 16 already shipped P0+P2+P3.** Sprint 17 closes the wishlist completely on the chili side. mdata flips all 4 wishlist-tied xfails to strict-pass on 0.8.5 receipt.
- **Risk profile is low for Part B, medium-but-bounded for Part A.** Part B is a wrapper over an already-tested marshalling path (`sync()` in `Outgoing` branch). Part A is a debug-then-fix where the failure mode is NOT yet localized — the current-code reading suggests `eval_op` SHOULD dispatch correctly (line 439-453 already resolves Symbol → Fn → eval_call); the real bug is somewhere else (likely subscriber-thread namespace visibility or stack scope). The dispatch brief commits to **observe-first-then-fix**, not to a specific fix site.

---

## Pre-kickoff gates (must verify before any Part work begins)

- **K1 — `/tmp/polars-py-1.39.3` present.** `ls /tmp/polars-py-1.39.3/crates/polars-core/Cargo.toml` exists (the workspace polars fork path). If gutted: re-clone per `vendor/polars-core/README.md` before kickoff — it's the largest hidden surprise we've hit twice now.
- **K2 — rustc ≥ 1.95.** `rustc --version` ≥ 1.95.0 (main's `sysinfo 0.39` dep). If lower: `rustup update stable`.
- **K3 — chili-py wheel build path clean.** From `crates/chili-py/`: `uv sync` + `uv run maturin develop` succeeds against current HEAD. (Sprint 16 ratified at 172 Rust + 83 pytest; Sprint 17 should start from that baseline.)

K1 + K2 are blockers. K3 is verification only.

---

## Scope — Part A: P1 subscriber `eod` dispatch

### A.1 Hypothesis space (audit Part A is to narrow this BEFORE coding)

Current code reading suggests the dispatch chain SHOULD work:

| Step | File:line | Behavior |
|---|---|---|
| Publisher sends | `crates/chili-core/src/broker.rs:125-128` (`eod` fn) | Calls `state.signal_eod(message)` where `message = (`eod; date)` MixedList |
| signal_eod broadcasts | `crates/chili-core/src/engine_state.rs:1230-1249` | Iterates Publishing handles, calls `sync(h, args)` on each |
| sync marshals over IPC | `engine_state.rs:971` Outgoing branch | `serde9::serialize(msg, !is_local)` + `write_chili_ipc_msg` over chili IPC |
| Subscriber receives | `crates/chili-core/src/utils.rs:307-374` (`handle_chili_conn`) | Reads frame; `stack.clear_vars()`; calls `state.eval(stack, &any, src_path)` |
| eval routes MixedList | `engine_state.rs:1501-1525` (`eval`) | For MixedList → `eval_op(self, stack, &[args])` (line 1519) |
| eval_op dispatches Symbol-headed list | `crates/chili-core/src/eval.rs:419-460` | `list[0]` is Symbol("eod") → `state.get_var("eod")` → if `SpicyObj::Fn(f)` → `eval_call(state, stack, &f, args, ..., "")` (line 453) |

**This SHOULD invoke `eod[date]`.** But mdata's test consistently fails (`.sub.eod.fired` never written). So one of the following is wrong:

- **H1 — `eod` global var visibility.** `engine.eval("eod: {[msg] ...}")` from Python's main thread defines `eod` somewhere that `state.get_var("eod")` from the subscriber thread can't see. The Stack for the subscriber thread is `Stack::new(None, 0, handle, user)` at utils.rs:315 — fresh stack, no parent. `state.get_var` reads from `EngineState::vars` (a shared `RwLock<HashMap>`), so this SHOULD work. But `engine.eval` might define `eod` in a different namespace (e.g., a per-stack scope).
- **H2 — Function-body assignment scope.** `eod: {[msg] .sub.eod.fired: msg}` — the `.sub.eod.fired: msg` assignment from inside a function body called on the subscriber thread might land in the function's local stack instead of the global vars dict. Then `engine.get_var(".sub.eod.fired")` from main-thread Python doesn't see it.
- **H3 — Stack-handle wiring.** The subscriber thread's stack has `handle = handle` (the subscriber's TCP handle id). Some eval paths might use the stack's handle context to namespace var assignments. Less likely but possible.
- **H4 — Message shape mismatch.** signal_eod sends `MixedList[Symbol("eod"), Date(date)]` but the subscriber receives `MixedList[String("eod"), Date(date)]` or `[Symbol(".broker.eod"), ...]` due to a serialization shape divergence. `eval_op` at line 441 handles both Symbol and String (`SpicyObj::Symbol(s) | SpicyObj::String(s)`), but maybe `serde9::serialize` widens the type. Verify via a logger at utils.rs:349.

**Most likely:** H2 (function-body assignment scope). pepper assignment semantics inside function bodies aren't load-bearing for chili's other tests because Python rarely defines a function whose only purpose is to mutate global state from a callback. mdata's eod shim is the first.

**Sprint 17 commits to instrument-then-localize**, not to a specific fix site. The exact fix depends on which H wins.

### A.2 Port mdata's failing test (Step 1, mandatory before any fix)

From `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md` §Q4 (lines 120-213), copy the test into chili-py pytest:

**Path:** `crates/chili-py/tests/test_subscriber_eod_dispatch.py`

```python
# Acceptance test for mdata wishlist P1 — subscriber-side eod dispatch.
# Source: ~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md §Q4.
# Strict-pass on Sprint 17 wrap unlocks mdata's tests/rdb/test_rdb_subscriber.py::
# test_subscriber_eod_shim_triggered_by_publisher_eod xfail flip.

import time
import chili

def test_subscriber_eod_shim_triggered_by_publisher_eod(tmp_path):
    # Both engines in-process; publisher binds, subscriber connects via TCP loopback.
    # Use small port range; rely on chili's port-allocation if available, else loopback.
    pub = chili.ChiliEngine(pepper=True)
    sub = chili.ChiliEngine(pepper=True)

    # Publisher: bind a port; configure as tp; init tick log.
    pub.eval(".tick.cfg.port: 0; .tick.cfg.logFile: \"" + str(tmp_path / "tplog") + "\"")
    pub.eval(".tick.init[]")
    # ...exact boot reproduces mdata's tp boot per chili_wishlist_2026-05-13_mdata_reply.md
    # lines 135-180. Cite exact line refs in the port commit.

    # Subscriber: define eod shim BEFORE subscribing.
    sub.eval(".sub.eod.fired: ::")
    sub.eval("eod: {[msg] .sub.eod.fired: msg}")
    sub.subscribe(host="127.0.0.1", port=pub_port, topic="trades")

    # Publisher fires .tick.eod[date]
    pub.eval(".tick.eod[2026.05.13]")

    # Within 100ms, the subscriber's eod shim should have written .sub.eod.fired.
    for _ in range(20):  # 20 * 50ms = 1s ceiling
        try:
            msg = sub.get_var(".sub.eod.fired")
        except NameError:
            time.sleep(0.05)
            continue
        if msg == chili.Null:  # initial value, not yet written
            time.sleep(0.05)
            continue
        break
    else:
        # Loop exited without seeing a fire.
        raise AssertionError(
            "subscriber eod shim never fired — .sub.eod.fired still unset "
            "after publisher .tick.eod[date]"
        )

    # Final assertion: eod[date] message was received + applied.
    assert msg is not None
    assert msg != chili.Null
```

The test SHOULD fail on current HEAD (xfail equivalent). **Confirm it fails BEFORE drafting the fix.** The failure mode (which of H1-H4 wins) is the input to A.3.

### A.3 Localize the bug

Add three logger / println instrumentation points:

1. **Subscriber `handle_chili_conn`** at `utils.rs:349` (`debug!("eval chili IPC message: {:?}", any);`) — already exists. Crank verbosity to verify the actual SpicyObj shape arriving. Confirms / rejects H4.
2. **eval_op Symbol-resolution** at `eval.rs:441-450` — log whether `state.get_var(s)` succeeds and what type it returns. Confirms / rejects H1.
3. **Inside the `eod` fn body during execution** — add an outer `.sub.eod.entered: 1` assignment to the test's eod shim: `eod: {[msg] .sub.eod.entered: 1; .sub.eod.fired: msg}`. If `entered` is observable from Python but `fired` is not, that's H2 (different namespace for inside-fn assignment).

**Halt-and-escalate:** if A.3 burns > 4pp without localizing, halt → user escalation. Don't ship a speculative fix.

### A.4 Fix — depends on which H wins

| H | Likely fix site | Rough pp |
|---|---|---|
| H1 | `engine_state.rs::eval` line 1519 — pass a global-scope marker on subscribe-thread eval, OR fix `state.get_var` to chain through global namespace consistently | 2–4 |
| H2 | `eval.rs::eval_call` — global-assignment semantics for top-level `:` inside function body called from a thread; OR change pepper assignment semantics for namespaced (dot-containing) idents to always be global | 3–6 |
| H3 | `utils.rs:315` Stack construction — pass parent stack or use a global-scope sentinel | 1–3 |
| H4 | `engine_state.rs::sync` Outgoing branch / `serde9::serialize` — ensure Symbol round-trips as Symbol | 1–2 |

We DON'T pre-commit to a fix shape. The audit will sharpen this.

### A.5 Tests

- **1 acceptance test** (A.2 above) — `test_subscriber_eod_shim_triggered_by_publisher_eod`. Strict-pass.
- **1–2 regression tests** depending on which H wins. E.g., if H2: test that `eod: {[m] .global.var: m}` from a subscriber thread updates `state.vars` not the function's local stack.
- **No test count delta for the bug-fix itself** — the acceptance test IS the load-bearing artifact.

### A.6 Coordination with mdata

After A.5 passes locally: ping mdata to confirm the exact pepper construct in their wishlist `_EOD_SHIM_DEFINE` matches what we exercise. (We may need to handle slightly different shim shapes — e.g., if their shim uses a closure-captured outer variable vs a global assignment, the fix surface differs.)

---

## Scope — Part B: P1 `publish_via_handle(h, table, df)`

### B.1 Surface additions

**chili-core:**

```rust
// crates/chili-core/src/engine_state.rs

/// Publish a DataFrame to a remote tp via an open chili-IPC handle.
/// Thin wrapper over sync(h, (`upd; table; df)) — matches the in-process
/// `engine.eval(\".tick.upd[\\\"table\\\"; df]\")` semantics but skips
/// the eval round-trip on the publisher side.
///
/// Per Sprint 16 mdata reply Q3 lock-in (Option B): chili owns the
/// marshalling; mdata owns the connection-manager class on top.
///
/// Errors:
///   - InvalidHandleErr if h has no connection.
///   - Err if handle is not Outgoing (not a remote tp client connection).
///   - Err if df is not a DataFrame.
pub fn publish_via_handle(&self, h: &i64, table: &str, df: &SpicyObj) -> SpicyResult<()> {
    // Validate df is a DataFrame (not Symbol, not Series, not Dict).
    match df {
        SpicyObj::DataFrame(_) => {}
        other => return Err(SpicyError::Err(format!(
            "publish_via_handle: df must be DataFrame, got {}",
            other.get_type_name()
        ))),
    }
    // Validate conn is Outgoing — required for cross-process publish.
    {
        let handles = self.handle.read();
        let handle = handles.get(h).ok_or(SpicyError::InvalidHandleErr(*h))?;
        if handle.conn_type != ConnType::Outgoing {
            return Err(SpicyError::Err(format!(
                "publish_via_handle: handle {h} is not Outgoing (got {:?})",
                handle.conn_type
            )));
        }
    }
    let msg = SpicyObj::MixedList(vec![
        SpicyObj::Symbol("upd".into()),
        SpicyObj::Symbol(table.into()),
        df.clone(),
    ]);
    // sync() does the marshalling + write; ignore the response (publish is fire-and-forget).
    self.sync(h, &msg)?;
    Ok(())
}
```

**chili-py PyO3 binding** (`crates/chili-py/src/lib.rs`):

```rust
#[pyo3(signature = (h, table, df))]
pub fn publish_via_handle(&self, py: Python<'_>, h: i64, table: String, df: PyObject) -> PyResult<()> {
    // Convert PyObject (likely a pl.DataFrame) to SpicyObj::DataFrame via existing conversion path.
    let df_spicy = py_to_spicy(py, df)?;  // existing helper
    // GIL released around the IPC send (matches Sprint 14 P3.2b convention).
    py.detach(|| {
        self.engine
            .publish_via_handle(&h, &table, &df_spicy)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    })
}
```

**Python wrapper** (`crates/chili-py/chili/engine.py`):

```python
def publish_via_handle(self, h: int, table: str, df: pl.DataFrame) -> None:
    """Publish a DataFrame over an open chili-IPC handle.

    `h` must come from `engine.open_handle("chili://host:port")` and be
    Outgoing. This is a thin one-shot publish primitive — callers build
    their own connection-manager (open + cache handle + close) on top.

    See: ~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md §Q3
    for the rationale (chili owns marshalling, caller owns TCP client).
    """
    self.engine.publish_via_handle(h, table, df)
```

### B.2 Implementation hints

- **Validation order matters.** Validate `df` type FIRST (cheap) THEN look up handle (acquires read lock). Failing on a bad df shouldn't take the handle lock.
- **`Outgoing` is the right conn_type to gate on.** chili IPC client connections are `Outgoing`; `Publishing` is for subscriber broadcast; `Incoming` is server-side accept. Verify by reading `crates/chili-core/src/utils.rs::handle_open` (the path that opens a `chili://` URL).
- **Don't lock the handle map across the sync() call.** The handle map read lock is dropped at the end of the validation block scope. `sync()` re-acquires its own write lock internally — this is intentional to avoid lock-upgrade deadlocks.
- **`df.clone()` is cheap.** `SpicyObj::DataFrame` wraps a polars `LazyFrame` / `DataFrame` which is internally `Arc`-counted. The clone is a refcount bump.
- **No GIL release in `EngineState::publish_via_handle` itself.** The GIL release happens in the PyO3 binding (py.detach), matching Sprint 14 P3.2b convention. The Rust core is GIL-agnostic.

### B.3 Tests

- **1 Rust integration test** in `crates/chili-core/tests/publish_via_handle_test.rs`:
  - Open a file:// shadow handle (captures bytes written without needing TCP).
  - **Issue:** publish_via_handle gates on `Outgoing` conn_type; file:// handles have `New` / `File` / `Sequence` conn_type. Either (a) test with a fake `Outgoing` handle pointing at a `Cursor<Vec<u8>>` ReadWrite, or (b) loosen the gate to allow file:// in test-only via cfg flag.
  - **Recommended approach:** mock-handle-via-fixture (existing pattern). If no fixture exists, write a 30-LOC test helper that constructs a Handle with `conn_type: Outgoing` and `rw: Some(Box::new(Cursor::new(Vec::new())))`.
- **2 chili-py pytest** in `crates/chili-py/tests/test_publish_via_handle.py`:
  - Round-trip via loopback: publisher engine + subscriber engine in-process; subscriber binds, publisher opens `chili://127.0.0.1:port`, calls `publish_via_handle(h, "trades", df)`. Subscriber verifies `engine.get_var("trades")` has the rows.
  - Error path: `publish_via_handle(h=99999, "trades", df)` raises `RuntimeError` with InvalidHandleErr semantics.
- **No regression test for the `Outgoing` gate** — covered by the round-trip test (file:// handle would fail with the conn_type check).

### B.4 Acceptance for mdata

mdata's RemoteTpClient (~50–80 LOC mdata-internal) will be implemented after 0.8.5 lands. mdata's wishlist names no concrete acceptance test for publish_via_handle since the API is chili-side-only; mdata writes their own acceptance test on top.

We commit to **chili-side pytest coverage** (B.3 above) as the load-bearing artifact. mdata flips their own RemoteTpClient tests separately.

---

## Out of scope (deferred to Sprint 18+ or rejected)

| Item | Reason |
|---|---|
| RemoteTpClient class on chili side | Q3 lock-in: mdata builds this on their side. ~50-80 LOC mdata-internal Python; not a chili surface. |
| Subscriber `eod` argument expansion (multi-arg, e.g., `eod[date; market]`) | Out of scope — mdata's wishlist names a 1-arg shape. The fix should preserve general N-arg dispatch since `eval_op` already handles `args = &list[1..]`. |
| Connection pooling / reconnect on publish_via_handle | mdata owns this on their side. chili publish is fire-and-forget against a single open handle. |
| GIL release on subscriber-side `handle_chili_conn` eval | Already released — the subscriber thread is not a Python thread, doesn't hold GIL. Confirmed by Sprint 14 P3.2b's surrounding analysis. |

---

## Deliverables table

| Item | Path | Test count delta |
|---|---|---|
| Part A — eod dispatch fix | TBD (depends on H1-H4 outcome) | +1 chili-py pytest (acceptance) + 0-2 regression |
| Part B — `publish_via_handle` | `engine_state.rs` + `chili-py/src/lib.rs` + `chili-py/chili/engine.py` | +1 Rust integration + +2 chili-py pytest |
| 0.8.5 wheel | `dist/chili_sauce-0.8.5-cp310-abi3-macosx_11_0_arm64.whl` | 0 |
| Delivery doc | `docs/sync/mdata_chili_2026-05-XX_0.8.5_delivery.md` | 0 |
| Retro + cadence row | `docs/sim/sprint_17_retro.md` + `cadence_metrics.md` | 0 |

**Predicted test count delta:** +1 Rust (`publish_via_handle_test.rs`) and +3-5 chili-py pytest (1 eod acceptance + 0-2 eod regression + 2 publish_via_handle).

---

## Lead allocation

Coordinator-solo is the default. **debugger subagent** invocation is reserved for Part A.3 if hypothesis-search (after instrumentation) takes > 2pp without localizing — the debugger has the right shape for this (Read + Grep + Bash + Edit + Write).

**No worktree fanout.** Parts A + B touch different files (`eval.rs` / `engine_state.rs::eval` vs `engine_state.rs::publish_via_handle`) but are small enough that serial coordinator-solo work is cheaper than worktree setup.

---

## Mid-checkpoint halt-and-escalate triggers

Per `.claude/rules/sprint-cadence.md` mid-sprint contract:

1. **Part A.3 burns > 4pp without localizing the bug** → halt; user escalation; consider deferring Part A to Sprint 18 with a focused investigation-only dispatch brief.
2. **Part B Rust integration test infrastructure (mock handle) takes > 1pp** → halt; consider deferring the Rust integration test to "follow-up sprint" with only chili-py pytest as load-bearing coverage. Still ship Part B.
3. **Part A fix candidate breaks any of the 172 existing Rust tests OR 83 chili-py pytest** → halt; ratify alternative fix shape with user before proceeding.
4. **Pp burn at Part A end > 10pp** → halt; defer Part B to Sprint 18 to avoid run-overs.

---

## Wrap ceremony

1. `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py` — green.
2. From `crates/chili-py/`: `uv run maturin develop && uv run pytest` — green.
3. From `crates/chili-py/`: `uv run maturin build --release` — produce 0.8.5 wheel.
4. Capture sha256 of the wheel.
5. Update CLAUDE.md project state: 0.8.4 → 0.8.5 wheel; Sprint 17 ratified; test count deltas.
6. Write retro at `docs/sim/sprint_17_retro.md`.
7. Append cadence_metrics row.
8. Write delivery doc at `docs/sync/mdata_chili_2026-05-XX_0.8.5_delivery.md`.
9. Move dispatch brief to `docs/history/sprints/sprint_17_dispatch_brief_2026-05-13.md`.
10. **dispatch code-reviewer subagent** on the wrap commit per Sprint 3+ lesson 7.
11. **Sprint 17 is sprint #17 since the last every-5-sprint sweep was Sprint 16.** Next sweep is Sprint 21. No housekeeping triggered.

---

## Pp accounting (pre-audit)

| Sub-priority | Predicted pp band | Rationale |
|---|---|---|
| K1 + K2 + K3 pre-kickoff gates | 0.2 | /tmp polars + rustc + maturin develop. |
| Part A.2 port mdata's acceptance test | 1–2 | Mostly translation; chili-py engine boot semantics may diverge from mdata's by some amount. |
| Part A.3 instrument + localize bug | 2–4 | Three logger instrumentation points + observe failure mode + identify which H wins. |
| Part A.4 implement fix | 1–6 | Range reflects H1 (smallest) vs H2 (largest). |
| Part A.5 tests + regression coverage | 1–2 | Acceptance test is A.2; regression tests are H-specific. |
| Part B.1 + B.2 + B.3 publish_via_handle | 3–5 | Thin wrapper + PyO3 + 3 tests. The Rust integration test has the longest tail (mock handle setup). |
| 0.8.5 wheel cut + delivery doc | 0.5–1 | Same shape as 0.8.4. |
| Retro + cadence + history move | 1–2 | Standard wrap. |
| **Total** | **10–22** | Pre-audit. Audit-tightened range will be in the audit appendix. |

Cross-reference: Sprint 16 actual was 14pp on a 10.7–18.2 audited band. Sprint 17's profile (one bug-fix + one wrapper) suggests **midpoint = ~14pp** with the H2 vs H1 outcome dominating the variance.

---

## References

- mdata wishlist source: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md` §P1 (lines 82-200)
- mdata reply (Q3 + Q4 lock-in): `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`
- Sprint 16 dispatch brief (sets the pattern): `docs/history/sprints/sprint_16_dispatch_brief_2026-05-13.md`
- Sprint 16 retro (lessons 1 + 2 apply to Sprint 17): `docs/sim/sprint_16_retro.md`
- Code surfaces under investigation:
  - `crates/chili-core/src/eval.rs:419-460` (`eval_op`)
  - `crates/chili-core/src/engine_state.rs:1230-1249` (`signal_eod`)
  - `crates/chili-core/src/engine_state.rs:1501-1525` (`eval`)
  - `crates/chili-core/src/utils.rs:307-374` (`handle_chili_conn`)
  - `crates/chili-core/src/broker.rs:125-128` (`eod` pepper builtin)
  - `crates/chili-core/src/engine_state.rs:971` (`sync` Outgoing branch — Part B's marshalling path)

---

## Appendix — Independent audit (2026-05-14)

Three parallel review agents dispatched per `~/.claude/rules/self-audit-on-plans.md`: Explore
(codebase-scan), code-reviewer (adversarial), planner (sequencing). Findings folded below;
the original §A.1-§A.6 + §B.1-§B.4 draft above is preserved as audit trail. **Implementers
read this appendix first; it supersedes the original wherever they conflict.**

### Material corrections

#### C1 — H2 hypothesis is definitively wrong; re-rank H1, H4, H5 (timing) as candidates

**Audit (planner #1 + verify-against-code).** `eval.rs:125` shows pepper assignment
semantics route dot-namespaced identifiers (`.sub.eod.fired`) to `state.set_var` regardless
of fn-body context: the `!id.starts_with(".")` guard explicitly bypasses the local stack
for any `.foo.bar` identifier. So H2 ("function-body assignment scope") cannot explain the
failure: any `.sub.eod.fired: msg` inside `eod`'s body lands globally via `state.set_var`
regardless of which thread invoked the function.

**Consequence.** The original A.1 hypothesis ranking (H2 "most likely") is inverted. The
A.3 instrumentation point #3 (`.sub.eod.entered: 1` shim test for H2) is uninformative —
both `entered` and `fired` are dot-namespaced; if either reaches `state.set_var`, both
would. **Replace instrumentation #3 with: `eprintln!` directly inside `state.set_var`
showing var name + thread id.** This confirms whether the subscriber thread reaches the
write path at all.

**Re-ranked hypothesis order for A.3:**
- **H1 (eod var visibility) — most likely.** `engine.eval("eod: {[msg] ...}")` from Python
  main thread defines `eod` in `state.vars` (global). subscriber thread's `eval_op` looks up
  `state.get_var("eod")`. If `eval` defines local symbols differently (e.g., via a
  per-stack scope, or via `parse_global_assignment` vs `parse_local_assignment`), the
  subscriber thread won't see it.
- **H4 (message shape mismatch) — second most likely.** Verify via instrumentation point
  #1 (logger at utils.rs:349). serde9 may round-trip Symbol as String or wrap in an extra
  list layer.
- **H5 (NEW — timing race).** subscriber thread writes via `state.set_var` (acquires
  `vars` RwLock write); Python main-thread `engine.get_var(".sub.eod.fired")` reads before
  the write completes. The mdata test's 20×50ms polling ceiling should be enough, but
  verify by extending the polling loop to 100×50ms (5s) and observing whether the var
  eventually appears.
- **H2 (REMOVED).** Code-verified impossible.
- **H3 (stack-handle wiring) — unlikely.** `eval_fn_call` (eval.rs:49-78) creates a
  new_stack inheriting the outer stack's `h`, but `state.set_var` doesn't consult
  stack.h, so this can't gate the assignment.

#### C2 — §A.2 test code does NOT compile / run as drafted (4 separate bugs)

**Audit (code-reviewer CRITICAL-1 + CRITICAL-2 + MAJOR-2 + planner #4).** The pseudo-test
in §A.2 has four blocker bugs against the real chili-py API:

| # | Bug | Fix |
|---|---|---|
| 1 | `chili.Null` not exported (`chili/__init__.py` exposes only `ChiliEngine` + `ChiliError`). `SpicyObj::Null` round-trips to Python `None` per `lib.rs:186`. | Replace `chili.Null` with `None`; replace `msg == chili.Null` with `msg is None`. |
| 2 | `sub.subscribe(host="...", port=..., topic="...")` — signature wrong. Actual: `subscribe(tick_socket: str, topics: Optional[list[str]] = None)` at `engine.py:540`. | Use `sub.subscribe(f"chili://127.0.0.1:{pub_port}", ["trades"])`. |
| 3 | `except NameError` catches Python built-in, not `chili.engine_state.NameError` (a `create_exception!`-derived class, `lib.rs:59`). Latent test bug. | Use `if not sub.has_var(".sub.eod.fired"): continue` — `has_var` exists at `engine.py:98` and is the API. |
| 4 | `pub_port` referenced but never assigned (no port-readback after `.tick.cfg.port: 0`). | After `.tick.init[]`, read `pub.eval(".tick.cfg.port")` to capture the OS-assigned port. |

**Sprint 17 implementer:** the §A.2 test code in the original draft is a sketch; rewrite
using these four fixes when porting. The test must run before A.3 instrumentation begins
(it must observably fail on current HEAD; if it passes, the bug has already been fixed
elsewhere — surface as a finding).

#### C3 — Part B PyO3 helper is `spicy_from_py_bound`, not `py_to_spicy`

**Audit (Explore MAJOR-1, verified at `lib.rs:98`).** No `py_to_spicy` exists. The
existing PyO3 conversion path is `spicy_from_py_bound(any: &Bound<'_, PyAny>) -> PyResult<SpicyObj>`.

**B.1 PyO3 binding corrected:**

```rust
#[pyo3(signature = (h, table, df))]
pub fn publish_via_handle(&self, py: Python<'_>, h: i64, table: String, df: Bound<'_, PyAny>) -> PyResult<()> {
    let df_spicy = spicy_from_py_bound(&df)?;
    py.detach(|| {
        self.inner
            .publish_via_handle(&h, &table, &df_spicy)
            .map_err(map_spicy_error)  // or whatever the existing error mapper is named
    })
}
```

Reference patterns to match: `fn_call` at `lib.rs:520-529`, the eval body around `lib.rs:412-414`.

#### C4 — `sync()` is BLOCKING send+receive, not fire-and-forget — Part B brief mischaracterizes it

**Audit (code-reviewer MAJOR-1, verified at `engine_state.rs:1008-1025`).** For
`ConnType::Outgoing + IpcType::Chili`, `sync()` writes the message, then **immediately
reads a 16-byte response header + response body** (`rw.read_exact` at lines 1015-1019).
It blocks until the remote tp answers. Holds the handle map's **write lock** for the entire
duration.

**Implications for Part B:**

1. Brief's §B.1 comment "publish is fire-and-forget" is **wrong**. Correct to: "sync()
   is a blocking send+receive; the response value is discarded but the round-trip completes
   before publish_via_handle returns."
2. Brief's §B.2 "No GIL release in `EngineState::publish_via_handle` itself" remains
   correct — GIL release happens in the PyO3 binding via `py.detach`. But add: the handle
   map write-lock is held across the network read; if the remote tp is slow or unreachable,
   the lock is held indefinitely. **This is a known correctness concern inherited from
   `sync()`. It pre-dates Sprint 17 and is not Sprint 17's responsibility to fix.** Note
   in §B.2 for future-sprint reference.
3. The mdata-side RemoteTpClient must implement client-side timeout / cancellation if
   they need it; chili's `publish_via_handle` is "as blocking as `sync()` is."

#### C5 — Part B Rust integration test infrastructure is more constrained than the brief implies

**Audit (Explore MINOR-1 + planner #8).** The brief's §B.3 proposes a
`Cursor<Vec<u8>>`-backed mock handle. Three blockers:

1. `Cursor<Vec<u8>>` is `Read + Write` but **not `Send + Sync`** in the way the trait
   needs; `trait ReadWrite: Read + Write + Send + Sync` (engine_state.rs:71). Need
   `Arc<Mutex<Cursor<Vec<u8>>>>` newtype with explicit `impl ReadWrite`.
2. `Handle` struct construction visibility — verify it isn't `#[non_exhaustive]` AND that
   the test can set `conn_type: ConnType::Outgoing` directly. If not, fall back to
   loopback-TCP.
3. The cfg-test gate-relaxation path (allow `File` conn_type in tests only) touches golden
   rule logic and is rejected.

**Recommended:** **defer the Rust integration test to "follow-up sprint" or omit it
entirely** if §B.3 chili-py pytest provides round-trip coverage via loopback. The Python
test exercises the SAME marshalling path through PyO3 with strictly more realism (actual
TCP, actual IPC). Per Sprint 16 lesson 1, test-count delta should be honest about what's
load-bearing.

**Action:** revise §B.3 to 2 chili-py pytest (round-trip + error path); drop the Rust
integration test from Sprint 17 deliverables. Test count delta becomes +0 Rust,
+3-5 chili-py pytest.

### Additional opportunities surfaced

#### O1 — Multi-message subscriber test for `clear_vars` invariant

**Audit (planner #5).** `handle_chili_conn` calls `stack.clear_vars()` at the top of each
loop iteration (utils.rs:350). This is correct (each message gets a fresh stack), but no
test exercises a multi-message subscriber sequence end-to-end. Sprint 17 A.5 should add a
regression test: publisher fires `upd[trades, df]` THEN `eod[date]`; subscriber observes
both side effects. **Marginal cost:** +0.5pp. **Folded into A.5.**

#### O2 — ADR 0001 cross-reference

**Audit (planner #6).** ADR 0001 canonicalizes the pub/sub design. If Part A's fix
changes `handle_chili_conn` eval semantics OR `Stack::new` at utils.rs:315, that's
ADR-0001-adjacent. **Folded into A.4 acceptance gate:** if the chosen fix touches either
of those two surfaces, add a one-line comment to ADR 0001 documenting the change.
Don't write a new ADR.

#### O3 — Fire-and-forget `sync` variant (DEFERRED)

**Audit (Explore OPPORTUNITY).** For high-throughput publish paths, skipping the
response-read loop in `sync()` would cut latency. **Not Sprint 17 scope.** Capture as
`[architecture]` entry in `docs/sync/ideas.md` after Sprint 17 wraps.

### Cross-cutting gates (added to original §Mid-checkpoint)

#### G1 — No code edits to eval.rs / engine_state.rs / utils.rs until A.3 instrumentation produces observable output

**Audit (planner #7).** Brief's halt criterion #1 ("Part A.3 > 4pp without localizing") is
correct but undersells the risk: the original A.1 ranked H2 most likely, which the audit
disproved. If the implementer commits a fix on intuition before instrument logs print, the
fix will be wrong. **Hard gate:** no `Edit` calls to any of the three files above until the
A.3 logger prints have produced observable output and identified which H (H1, H4, or H5)
wins.

#### G2 — If A.4 touches `sync()` serialization path, dispatch code-reviewer before merge

**Audit (planner #10).** Part B's load-bearing marshalling path is `sync()` at
engine_state.rs:971. If Part A's fix happens to be H4 (serialization shape) AND the fix
edits `serde9` or the Outgoing branch, dispatching `code-reviewer` before merge prevents
silent Part B regression.

### Revised sizing

| Sub-priority | Pre-audit | **Post-audit** | Rationale |
|---|---|---|---|
| K1+K2+K3 | 0.2 | 0.2 | Unchanged. |
| A.2 port mdata test | 1–2 | **1.5–2.5** | +0.5pp for the four C2 fixes when porting. |
| A.3 instrument + localize | 2–4 | **2–4** | Unchanged (instrumentation #3 swap doesn't change cost). |
| A.4 implement fix | 1–6 | **2–8** | H4 (serde9) requires reading/editing serialization — wider tail. H2 removed (was midpoint); ranges shift up. |
| A.5 tests + regression | 1–2 | **1.5–2.5** | +0.5pp for O1 multi-message regression test. |
| B.1 + B.2 chili-core + PyO3 | 3–5 | **2.5–4** | −0.5pp by dropping Rust integration test (C5). |
| B.3 chili-py pytest only | (inside B.1) | (inside B.1) | Round-trip + error path; 2 tests. |
| 0.8.5 wheel + delivery doc | 0.5–1 | 0.5–1 | Unchanged. |
| Retro + cadence + history move | 1–2 | 1–2 | Unchanged. |
| **Total** | **10–22** | **11.4–24.7** | Midpoint ~18pp post-audit (vs ~16pp pre-audit). |

**Sprint 17 audited band: 11–25 pp, midpoint ~18pp.** Upper edge widened from 22→25 to
reflect H4 serialization-path risk; lower edge tightened by Rust-integration-test removal.

### Revised sequencing

Per planner #recommendation:

1. **Pre-kickoff gates K1+K2+K3.**
2. **Part B first** (3–5pp): Python wrapper + 2 chili-py pytest. Ships independently. No
   blockers on Part A. Lands the publish_via_handle surface for mdata to start writing
   their RemoteTpClient against.
3. **Part A.2 port mdata test** (1.5–2.5pp) — confirm it fails on current HEAD (with the
   four C2 fixes applied).
4. **Part A.3 instrument** (2–4pp) — three logger points (revised per C1); identify which
   H wins.
5. **Part A.4 fix** (2–8pp) — H-specific; gates G1 + G2 apply.
6. **Part A.5 tests + O1 regression** (1.5–2.5pp).
7. **Wheel cut + delivery + retro + history move** (2.5–4pp).

**Why Part B first:** mdata starts writing RemoteTpClient against publish_via_handle as
soon as the API is committed; this happens BEFORE Sprint 17 wraps. If Part A.3 stalls,
Part B is still shippable as a 0.8.5 wheel.

### Other small fixes

- **N1.** §Wrap step 9 history-move filename — change `_2026-05-13.md` to `_2026-05-14.md`
  (matches the actual brief filename).
- **N2.** §Wrap step 11 housekeeping wording — Sprint 16 housekeeping was executed
  (verified at commit `ed53c1f`). Sprint 17 is "+1 past Sprint 16 sweep; next sweep at
  Sprint 21." The brief's conclusion ("no housekeeping triggered") is correct; just the
  count phrasing was confusing.
- **N3.** §A.1 table row "eval_op dispatches Symbol-headed list" — remove the parenthetical
  about `src_path ""`. It's the caller_src_path argument used only for error-message
  formatting (eval.rs:453); not load-bearing for dispatch.
- **N4.** §Out-of-scope — keep the row "Subscriber `eod` argument expansion" but note
  that the fix MUST preserve N-arg dispatch since `eval_op` slices `args = &list[1..]`
  (eval.rs:451) for any arity.

### Audit verdict

Brief is structurally sound — Part A's "instrument-then-fix" framing is correct, Part B's
thin-wrapper scope is correct, halt criteria are well-formed. **Material corrections C1
through C5 + revised sequencing are folded as binding overrides.** Implementer reads this
appendix as the canonical scope; original draft above is audit trail.

Post-audit recommendation: **proceed to kickoff** after user ratification. Sprint 17 is
sized at 11–25pp, midpoint ~18pp, with Part B as the cheap-ship lead and Part A as the
investigation-driven follow-on.
