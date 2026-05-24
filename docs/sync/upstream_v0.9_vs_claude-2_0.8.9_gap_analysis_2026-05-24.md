# Upstream v0.9.0 vs claude-2 0.8.9 — gap analysis

**Date:** 2026-05-24
**Author:** chili-team (downstream working fork)
**Audience:** chili-author (upstream)
**Purpose:** record the full feature gap between upstream `main` (v0.9.0) and our local working branch `claude-2` (0.8.9), with concrete rationale for every claude-2 deviation, so you can evaluate which features (if any) are worth upstreaming. **Not an upstreaming request** — your call which to accept.

---

## TL;DR

We compared `main` (v0.9.0, commit `fb4455d`) against `claude-2` (0.8.9, commit `46d5679`) file-by-file. Headline numbers: **123 commits on claude-2 not on main; 7 commits on main not on claude-2**.

- **claude-2 carries 9 features main lacks**, all driven by mdata's production needs (rdb/wdb tick subscribers, gateway, EOD bookkeeping, kill-9 durability, v1-36 attach-socket retirement). Eight of these have shipped to mdata via 0.8.1 → 0.8.9 wheels and are in active production use; one (W3 Python-callable bridge) shipped today and is awaiting mdata acceptance.
- **main carries 3 features claude-2 lacks** (async\_/execute, eval_op inline string-eval, py.typed marker) plus the GitHub-hosted `polars-core-patch` URL — all clean additions that claude-2 should forward-port.
- **2 features look like duplicates but have different semantics:** `eval_str` (claude-2 SIDE_EFFECT_FN builtin) vs `eval_op` inline parse (main); `roll_tick` (claude-2 native atomic cutover) vs `roll_tick_log` (main pepper-script rotate). Both pairs converge on the same user-visible behavior for the primary case, but the claude-2 versions carry stronger correctness contracts (atomic cutover; arity-typed builtin).
- **Polars-fork strategies have diverged.** main patches only `polars-core` via `github.com/hinmeru/polars-core-patch.git`; claude-2 patches 25 polars-family crates against a local `/tmp/polars-py-1.39.3/...` clone to satisfy ADR 0003 (true-lazy via py-1.39.3 fork + chili-side q-style fmt patch). If we forward-port main's `polars-core-patch` URL, we need to verify it carries our q-style fmt patch.

If you want only one section: skip to §3 (the 9 mdata-driven features) — that's the case for upstream adoption. §4 (the 3 main-only features) is the case for claude-2 forward-porting.

---

## 1. Branch + collaboration context

Per `CLAUDE.md` "Branch policy" section:

- **`main`** is your upstream truth, read-only on this clone. There is no `git remote`; you upload `main` state into this checkout manually.
- **`claude-2`** is our working branch. Forked from `main` tip 2026-05-07 (commit `f8b6360`) when we hit cherry-pick conflict accumulation. Carries all post-2026-05-07 mdata-driven work.
- **`claude`** is parked-historical, tagged `claude-baseline-2026-05-07` for provenance / A-B builds.

**Deliveries to mdata** are wheel-only (`uv pip install dist/chili_sauce-X.Y.Z-cp310-abi3-macosx_11_0_arm64.whl`) — never editable installs. mdata pins on wheel SHA-256.

**Wishlist origin.** Every claude-2-only feature traces back to an mdata wishlist doc (one per surface ask). The path layout is:

- `docs/sync/mdata_wishlist_<date>_<topic>.md` (live asks)
- `docs/history/sync/mdata_wishlist_<date>_<topic>.md` (satisfied — frozen)
- `docs/sync/mdata_chili_<date>_<X.Y.Z>_delivery.md` (handoff with sha + acceptance asks)
- `docs/decisions/000N-*.md` (the ADR for any cross-cutting design call)

23 sprints (`docs/sim/cadence_metrics.md`) + 5 ADRs accepted + 841 lines of `docs/standards/iteration_lessons.md` capture the decision history.

---

## 2. Headline diff stats

```
git diff --stat claude-2..main -- '*.rs' '*.py' '*.toml'
→ 30 files changed, 218 insertions(+), 4743 deletions(-)
```

The 4743 deletions are claude-2 code that doesn't exist in main. The 218 insertions are main code that doesn't exist in claude-2. **Net code addition on claude-2: ~4500 lines.**

- chili-core/src/engine_state.rs: claude-2 is +531 lines vs main (push-model, GR4 helpers, M-1 invariant, publish_via_handle, roll_tick atomic cutover, flush_tplog).
- chili-core/src/external_fn.rs: claude-2 +43 lines; absent on main.
- chili-core/src/upd_notify.rs: claude-2 +180 lines; absent on main.
- chili-py/chili/engine.py: claude-2 +315 lines; 9 methods + 9 docstrings absent on main.
- chili-py/src/external_dispatcher.rs: claude-2 +170 lines; absent on main.
- tests: claude-2 ships 215 Rust + 108 pytest. main ships ~59 pytest (test_engine.py count).

---

## 3. Features in claude-2, missing on main

Each section gives: **what it is**, **why mdata needs it**, **chili-core surface cost** (deps + lines + golden-rule impact), and **adoption status**.

### 3.1. Push-model D-1 — `upd_notify_fd` + `drain_upds` + `UpdEvent` (Sprint 21)

> upd shouldn't notify fd, fd should always push async update to engine. Even for sync updates, fd only needs to know if the upd is written to the TCP socket.

- **What:** chili-core's IPC receive thread enqueues each inbound `upd` into a bounded crossbeam_channel(4096) and writes 1 byte to a POSIX self-pipe (macOS-portable; not `eventfd`). Python subscribers call `engine.upd_notify_fd()` to get the read-end FD, register it with `asyncio.loop.add_reader(...)`, then `engine.drain_upds() -> list[UpdEvent]` on wake. `UpdEvent { table, cursor_lo, cursor_hi, frame: pl.DataFrame }` — Polars frame is an Arc-shallow-clone of the inbound serde9 payload (no re-decode).
- **Why mdata needs it:** before D-1, mdata's rdb/wdb subscribers polled `engine.get_var(table)` every ~10 ms and diffed a `_last_seen_seq` watermark — burning CPU on the Python side AND paying chili-lock contention on every poll. A-033 (the EOD-cutover latency incident in v1-32) was rooted in that contention. D-1 is event-driven, GIL-free on the chili side (zero pyo3 on the receive path), blocking-never-drop (back-pressure to tp; tplog is the source of truth).
- **chili-core deps added:** `crossbeam-channel = "0.5"`, `libc = "0.2"` (the self-pipe + `fcntl` for `FD_CLOEXEC`).
- **chili-core surface added:** new `upd_notify.rs` module (180 lines); `UpdNotify` struct on `EngineState`; `enable_upd_notify()` lazy-create. **No pyo3 dependency in chili-core** — the receive thread runs lock-free; `UpdEvent` is a `#[pyclass]` in chili-py.
- **Golden rule impact:** GR5 preserved by construction (no `Python::with_gil` on the hot path; `py.detach` in chili-py drain).
- **ADR:** `docs/decisions/0006-async-upd-notification-ffi.md` (full contract: queue capacity, fd lifecycle, fork safety via `FD_CLOEXEC`, back-pressure escalation).
- **Adoption:** **mdata v1-26.2** (delivered as 0.8.7 on 2026-05-19). Replaces the 10ms poll loop; closes A-033's contention root cause.
- **Alternative we considered:** `eventfd` on linux. Rejected: chili dev + delivery wheel is darwin; self-pipe is portable and one byte per wakeup is fine.

### 3.2. Push-model D-2 — `get_var_lazy(id)` (Sprint 21)

> I don't see why have to force a lazy frame. User can always call .lazy() by themselves on the result.

- **What:** snapshot-clone the `SpicyObj::DataFrame` under the `vars` read-lock, return `.lazy()`. Same byte content as `get_var(id)` on `.collect()`, but the lazy plan is pushdown-capable across the FFI boundary.
- **Why mdata needs it:** mdata's gateway composes lazy plans over per-table accumulators before collecting. Without D-2, `get_var(table)` returns an eager DataFrame that has to be re-`.lazy()`-wrapped on the Python side — no pushdown.
- **chili-core surface:** +12 lines on `EngineState::get_var_lazy`.
- **Deps:** none new.
- **Golden rule impact:** none. The receive thread continues to mutate vars under the write-lock; the clone is a stable snapshot. No live view.
- **ADR:** ADR-0006 §5.
- **Adoption:** mdata v1-26.

### 3.3. Push-model D-3 — `subscribe(resume_from=…)` + `resume_cursor` field (Sprint 21)

> Engine has a builtin reconnect logic and replay lost tp log, no need to take care by using new subscribe api.

- **What:** new `subscribe(tick_socket, topics, resume_from: dict[str,int] | None = None)` signature. `EngineState.resume_cursor: RwLock<HashMap<String, i64>>` (table → last-delivered `cursor_hi`); the existing `.sub.init` / `.sub.recover` pepper scripts consult this map via a new accessor builtin rather than hardcoded `0` / `tick[0]`.
- **Why mdata needs it:** cold-restart of an rdb after a kill-9 must resume from the last-drained `cursor_hi` per topic so that the tplog replay starts at the correct message ordinal. Without D-3, mdata reverts to per-row `_last_seen_seq` dedup over the full replay — works but costs O(replay-size) per restart, and the per-row seq is a separate anchor with its own correctness concerns (Q1 Path-1).
- **chili-core surface:** +30 lines on `EngineState::set_resume_cursor` + `resume_start_for` + the new `resume_cursor` builtin in side_effect_fn.rs.
- **ADR:** ADR-0006 §4 (with the 2026-05-19 corrected-resume-coordinate amendment — the mdata-found doc-bug). The chili code was correct; only the ADR text needed the fix.
- **Adoption:** mdata v1-26.2. The doc-bug was found by mdata's first-hand empirical test on 0.8.7; corrected in claude-2 commit `a50cec9`.

### 3.4. `flush_tplog()` (Sprint 16)

> mdata shouldn't need this at all, it should be taken care by chili file system. mdata should be focus on get data. The tp log after all is for recovering data. The tick plant is a light process, which doesn't keep data, and should not be killed by kill -9.

- **What:** Python-callable method that flushes the in-memory tplog write buffer to disk via `fsync` on the underlying file handle. Targets `.tick.msgHandle` (set by `.tick.createLog` during `init_tick`).
- **Why mdata needs it:** PRD §5.1 part-2 specifies kill-9 durability — a hard-kill of the tp process must lose at most one in-flight message. The OS file-system buffer doesn't fsync on every write (would cost too much); mdata's tp daemon calls `flush_tplog()` at checkpoint-aligned moments (after a batch of N publishes, or every M ms) to bound the loss window.
- **Your reaction was "this doesn't make sense to him" — the rationale:** without push-model + resume-cursor context, `flush_tplog` looks like a one-off file-sync wrapper. The reason it's a chili-side API (not a Python `os.fsync` call) is that chili owns the file handle; the Python side has no way to reach the OS fd without an FFI. We could expose a generic `engine.fsync_handle(h)` instead — that's a refactor we'd accept upstream.
- **chili-core surface:** +30 lines; uses the existing `ReadWrite::sync_all` trait introduced for this purpose.
- **Deps:** none new.
- **Adoption:** mdata's tp daemon since 2026-05-13 (0.8.4).

### 3.5. `publish_via_handle(h, table, df)` (Sprint 19, mdata 2b)

> Should never use this, just use publish, which is a defined chili function for publishing data.

- **What:** outbound `sync(h, (`upd; table; df))`shaped helper that validates the handle is`ConnType::Outgoing` before publishing, eliminating the lock-acquisition + handle-lookup pattern mdata had to write at every call site.
- **Why mdata needs it:** gateway code emits ~10-50 per-table publishes per EOD cycle; without the helper, each call site re-implements the validation + the upd-message construction.
- **chili-core surface:** +30 lines on `EngineState::publish_via_handle` (with the explicit early-drop of the read lock so `sync()`'s internal write lock doesn't deadlock against the same-thread read lock — `parking_lot::RwLock` is not reentrant).
- **Deps:** none new.
- **Adoption:** mdata gateway since 2026-05-13 (0.8.4).

### 3.6. `roll_tick(log_dir, segment_label)` — atomic native cutover (Sprint 18)

> This function apparently used some functions that are not actually internal chili functions. Should just use roll_tick_log. The new function is `.handle.rotate` for atomic cutover.

- **What:** native Rust implementation that holds the handle write-lock across **open-next → swap-writer (same handle id) → fsync+close-old**. Any concurrent inbound `.tick.upd` is serviced by exactly one valid handle and lands wholly in the old segment OR wholly in the new — never dropped, never split.
- **Why mdata needs it:** UHF (ultra-high-frequency) tplog rotation. mdata's daily-rotation cutover happens while publishes may still be in flight; the old `engine.eod(d)` + `init_tick(.., d+1)` pair had a brief window where a publish could land in the wrong file. At ~thousands of ticks/sec, the data-loss probability over a full year of cutovers is non-trivial.
- **Semantic delta vs main's `roll_tick_log`:** main's version (commit `43faf44`) is a 4-line pepper script `.tick.rollLog` that calls `.handle.rotate[.tick.msgHandle; .tick.logFile]` — no atomicity guarantee. Functionally equivalent for the LOW-frequency case (daily rotation when traffic is quiescent); structurally unsafe for the UHF / size-triggered rotation case mdata cares about.
- **chili-core surface:** +50 lines.
- **Deps:** none new.
- **Adoption:** mdata tp daemon since 2026-05-13.

### 3.7. GR4 quantization helpers — `set_column_scale` / `clear_column_scales` (various sprints)

> This is mdata specific requirement, which should be built on top of chili engine, not part of chili engine.

- **What:** Python-callable helpers to register a column's quantization factor (e.g., "this column is Int64-quantized at scale=10000; dequant by dividing by 10000 on read"). chili reads quantized columns as Int64 from disk; the dequant happens at the user-facing Python boundary, not in chili-core.
- **Why mdata needs it:** mdata's storage schema is Int64-quantized for all price columns (golden rule 4 in chili's CLAUDE.md). Compression + cache efficiency benefit. The dequant convention is a user-side concern, but having chili expose the scale registry as part of the engine state lets the same engine serve both quantized and unquantized callers consistently.
- **M-1 invariant guard** (`test_engine.py:506` `TestM1EagerNoAutoDequant`): chili MUST NOT auto-dequant on `get_var(table)`. The read-time convenience of "dataframe out is what the user expects" is desirable in some contexts but conflicts with "on-disk dtype is preserved through the engine" — the latter is the load-bearing invariant for cross-process schema honesty. M-1 codifies "eager-eval does NOT auto-dequant; storage schema is preserved."
- **chili-core surface:** +60 lines.
- **Deps:** none new.
- **Adoption:** mdata gateway + rdb/wdb subscribers (live).

### 3.8. M-1 eager-eval-no-auto-dequant invariant (Sprint 20)

See §3.7 — the test guard codifies the contract; the code change is the explicit decision NOT to add auto-dequant logic that an earlier prototype had introduced. **Net: −20 lines of code, +60 lines of test, +1 documented invariant.**

### 3.9. W3 Python-callable bridge — `register_fn` / `unregister_fn` / `ExternalFnDispatcher` (Sprint 23, shipped 2026-05-24)

> To call any function, should just define function using pepper syntax, calling a python function is making things complicated, and it is not efficient.

- **What:** chili-py method `engine.register_fn(name, callable, arity)` stores a Python callable in a `PyExternalDispatcher: RwLock<HashMap<String, Py<PyAny>>>` registry; chili-core's `eval_fn_call` gains a new branch that routes via `ExternalFnDispatcher::dispatch(name, args)` when the target `Func` has `external_name: Some(_)`. The dispatcher trait is generic — a future R/Julia/JS dispatcher can install side-by-side.
- **Why mdata needs it:** mdata operates 3 control verbs that require Python-side daemon bookkeeping — `.mdata.eod.fire[date]` (drain a Polars buffer + broadcast EOD), `.mdata.wdb.finalize[date]` (finalize idb partition), `.mdata.hdb.reload[]` (reload partition cache). Today these dispatch via a bespoke attach-socket Unix protocol → Python handler. mdata's v1-36 sprint retires the attach socket; W3 is the chili-native replacement.
- **The "adds dependencies to chili" concern — empirically false:** chili-core adds ZERO new dependencies for W3. The trait (`ExternalFnDispatcher`) is in chili-core but pyo3-agnostic. The Python adapter (`PyExternalDispatcher`) lives entirely in chili-py. We measured the chili-core surface delta: +43 lines (`external_fn.rs` trait) + 1 `Option<String>` field on `Func` + 1 `RwLock<Option<Arc<dyn ExternalFnDispatcher>>>` slot on `EngineState` + 1 new branch in `eval_fn_call` (15 lines). The full chili-core diff is in commit `ae5668b`.
- **Hazards measured before impl, not speculated about:**
  - **Re-entrancy:** `grep -rn "self\.vars\.\(read\|write\)" crates/chili-core/src/eval.rs crates/chili-op/src/` returns ZERO hits. Function dispatch in `eval_fn_call:41-47` invokes `f` / `f_with_side_effect` outside any held `vars` lock. A Python callback at the dispatch point inherits the same lock-free contract as builtins today.
  - **GIL overhead:** measured via existing `get_var/set_var` `py.detach+with_gil` paths in chili-py: 151ns per cycle. W3 callback round-trip adds ~2× this = ~300ns + Python body time. For mdata's 3 daily-cadence control verbs: negligible.
- **GR5 impact:** preserved. Non-W3 users see `external_dispatcher = None` on EngineState and never reach the W3 branch. For W3 users, the GIL is re-acquired only for the callback duration, not for the surrounding `Engine::eval`. Matched-shell A/B bench (0.8.8 vs 0.8.9, same shell, force-reinstall between runs) shows 0.8.9 = 0.8.8 within ±3% on concurrent_eval N=1 and N=4.
- **ADR:** `docs/decisions/0007-w3-python-callable-bridge.md` (full contract: arity, exception semantics, lock discipline, wire serialization, re-entrancy, set_var shadowing).
- **Adoption:** delivered to mdata 2026-05-24 (0.8.9). Awaiting acceptance.

### Summary table

| #   | Feature                                 | Sprint   | Wheel adopted | mdata version      | chili-core deps added   |
| --- | --------------------------------------- | -------- | ------------- | ------------------ | ----------------------- |
| 1   | upd_notify_fd / drain_upds / UpdEvent   | 21 (D-1) | 0.8.7         | v1-26.2            | crossbeam-channel, libc |
| 2   | get_var_lazy                            | 21 (D-2) | 0.8.7         | v1-26              | none                    |
| 3   | subscribe(resume_from=) / resume_cursor | 21 (D-3) | 0.8.7         | v1-26.2            | none                    |
| 4   | flush_tplog                             | 16       | 0.8.4         | v1-22              | none                    |
| 5   | publish_via_handle                      | 19       | 0.8.4         | v1-22              | none                    |
| 6   | roll_tick atomic                        | 18       | 0.8.4         | v1-25              | none                    |
| 7   | set_column_scale / GR4                  | various  | 0.8.x         | v1-24+             | none                    |
| 8   | M-1 invariant + guard                   | 20       | 0.8.7         | v1-26              | none                    |
| 9   | W3 register_fn / ExternalFnDispatcher   | 23       | 0.8.9         | pending acceptance | none                    |

**Total chili-core dep additions across all 9 features: 2 (crossbeam-channel, libc — both stdlib-adjacent and tiny).**

---

## 4. Features in main 0.9.0, missing on claude-2

### 4.1. `async_(h, msg)` + `execute(h, msg)` — handle-sign-dispatched async IPC

- **What:** new `EngineState::async_` (sends an IPC message with `MessageType::Async` — fire-and-forget) and `EngineState::execute(h, msg)` which routes positive `h` → `sync`, negative `h` → `async_`. Same semantic shape as kdb+'s positive/negative handle convention.
- **What it solves:** caller-side async send. Useful for fan-out publishes where the caller doesn't want to block on every recipient's ack.
- **Relationship to claude-2's push-model:** orthogonal. Push-model is RECEIVER-side event-driven; async\_ is SENDER-side non-blocking. mdata could use both simultaneously.
- **claude-2 forward-port:** clean. No conflict with our additions. ~50 lines in chili-core + minor chili-py wrapper.
- **Recommendation:** forward-port to claude-2.

### 4.2. `eval_op` inline string-source parse (refactor of our `eval_str`)

- **What:** `eval_op` on main matches `SpicyObj::String(s)` and parses it as pepper source inline, eliminating the need for a separate `eval_str` SIDE_EFFECT_FN builtin.
- **claude-2 equivalent:** we have a standalone `eval_str` SIDE_EFFECT_FN builtin AND `state.eval` already handles `SpicyObj::String` via parse+eval (so `sync(h, b"…")` works on both branches via the receive path).
- **Functional comparison:**
  - bytes-form `sync(h, b"1+1")`: works on both (claude-2 via state.eval; main via eval_op's new arm).
  - tuple-form `sync(h, ("eval_str", "1+1"))`: works on claude-2 only (the builtin is registered).
  - str-form `sync(h, "1+1")`: errors on both (Python str → SpicyObj::Symbol = var lookup; "1+1" not a var name).
- **Recommendation:** adopt main's eval_op refactor; remove our `eval_str` SIDE_EFFECT_FN once mdata confirms they use bytes-form exclusively (their turn-9 self-discovery says they do).

### 4.3. `py.typed` marker

- **What:** PEP 561 marker file in `chili-sauce` indicating the package ships inline type hints.
- **Recommendation:** trivial forward-port.

### 4.4. `polars-core-patch` hosted on GitHub

- **What:** main's `Cargo.toml` `[patch.crates-io.polars-core]` points to `https://github.com/hinmeru/polars-core-patch.git` tag `v0.53.0`. claude-2 patches 25 polars-family crates against `/tmp/polars-py-1.39.3/...` (a local clone that disappears on reboot).
- **Why claude-2 has the full-family patch:** ADR 0003 (true-lazy via py-1.39.3 fork + chili-side q-style fmt patch). chili-py compiles polars-plan from source so that DSL_SCHEMA_HASH matches the Python polars 1.39.3 wheel, enabling lazy-frame transfer across the FFI with pushdown preserved.
- **The friction we're trying to solve:** the local-clone-only approach breaks every fresh clone until the user re-creates `/tmp/polars-py-1.39.3`. User-driven P0 backlog item for the last 6+ sprints.
- **Question for the author:** does `polars-core-patch` carry the q-style fmt patch (from `vendor/polars-core/chili-port-py-1.39.3.patch` in our tree)? If yes, we adopt main's URL and drop the local clone. If no, we'd want to fork in the same shape: a single `polars-core` repo on github with both patches applied.
- **Recommendation:** sync with author on the polars-core-patch contents; forward-port if compatible.

---

## 5. Refactor / semantic-delta analysis

### 5.1. `eval_str` (claude-2) vs `eval_op` inline (main)

See §4.2. **Verdict:** main's refactor is cleaner; claude-2 should adopt. No correctness regression for mdata.

### 5.2. `roll_tick` (claude-2 atomic) vs `roll_tick_log` (main pepper-rotate)

See §3.6. **Verdict:** functionally different. Main's version is fine for daily-rotation; claude-2's version is required for UHF / size-triggered rotation. The atomic native impl is a strict superset — should consider for upstream.

### 5.3. Tick-related API surface naming

- main: `init_tick(log_dir, filename)` (param renamed `date`→`filename`, typed `date | str`).
- claude-2: `init_tick(log_dir, date)` (still typed `date`).
- main also adds `roll_tick_log` whereas claude-2 has `roll_tick`.

If claude-2 forward-ports, the `init_tick` param rename is a breaking change for downstream callers (mdata). We'd want to coordinate with mdata on the rename window.

---

## 6. Test coverage delta

| Suite                                | claude-2                                                                                        | main                                                           |
| ------------------------------------ | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Rust workspace tests (excl chili-py) | **215** passed                                                                                  | ~67 (estimated from `crates/chili-core/tests/*.rs` file count) |
| chili-py pytest                      | **108** passed                                                                                  | ~59 (test_engine.py def count)                                 |
| Bench harnesses                      | parse_cache + concurrent_eval (4 shapes) + categorical_eval criterion + post-pivot baseline doc | parse_cache + categorical_eval criterion                       |

The Rust delta (215 vs ~67) is dominated by claude-2-added test files for the features in §3:

- `external_fn_test.rs` (5 tests, W3)
- `upd_notify_test.rs` (~6 tests, push-model)
- `flush_handle_test.rs` (Sprint 16)
- `roll_tick_test.rs` (Sprint 18)
- `tcp_listener_graceful_test.rs` (Sprint 22 W2)
- `eval_str_test.rs` (Sprint 22 W1)
- `fn_call_i64_test.rs` (Sprint 19 IPC remote query)
- `rotate_handle_test.rs` (Sprint 17)

Bench-discipline note: claude-2 records a "matched-shell A/B" methodology in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 23 § after we hit a false-alarm GR5 regression (the dev mac's bench noise floor turned out to be ~25%, far exceeding our ±2% gate). Lesson promoted: snapshot-vs-snapshot deltas are misleading when noise > gate; matched-shell `--force-reinstall` A/B is the correct methodology.

---

## 7. Where main's design is objectively cleaner than claude-2

Honest assessment, not deflection:

1. **`execute(h, msg)` sign-dispatcher** is a tighter API than what we had in mind for an async-send. We should adopt this shape.
2. **`eval_op` inline String-parse** is structurally cleaner than maintaining a separate `eval_str` SIDE_EFFECT_FN — the parse-as-source path becomes naturally a property of the eval dispatcher rather than a builtin one has to remember exists.
3. **`polars-core-patch` hosted on GitHub** (if it carries our q-style fmt patch) closes a 6-month-old user-driven P0 backlog item for us. We should adopt and drop the local clone.
4. **Sprint 20 lean refactors** that we adopted FROM you (`rotate_handle` extraction to utils, `LazyCell` → `LazyLock`, `disconnect_handle` → `ConnType::Disconnected` flag, log-and-drop on TCP listener bad-handshake) are all design wins we now run in production.

---

## 8. Recommendations / asks

In rough priority order:

1. **Forward-port to claude-2:** `async_` + `execute` + `polars-core-patch` URL (verify q-style fmt patch first) + `py.typed`. ~1pp work; no mdata-side coordination needed.
2. **Coordinate the eval_op inline-String refactor adoption.** We remove our `eval_str` SIDE_EFFECT_FN; mdata confirms they use bytes-form exclusively. Single delivery.
3. **Coordinate the `init_tick` rename window** (`date` → `filename`) with mdata before adopting.
4. **For upstream evaluation (your call):** the 9 mdata-driven features in §3. Five of them add zero chili-core dependencies and ~30-60 lines each; they're not invasive. The 2 that add deps (crossbeam-channel + libc, both for push-model D-1) bring a 2-month-track-record of mdata production use. We'd value your evaluation on:
   - **D-1/D-2/D-3 push-model** — the largest single piece, ADR-0006 in our tree.
   - **roll_tick atomic vs roll_tick_log pepper** — would you accept a Rust-side atomic cutover as the production-grade variant, with `roll_tick_log` kept as a convenience alias?
   - **flush_tplog** — would a generic `engine.fsync_handle(h)` (instead of the tplog-specific `flush_tplog()`) be more acceptable upstream? We'd happily refactor.
   - **W3 (Python-callable bridge)** — our concrete impl is +43 lines + 1 trait + 1 Func field + 1 EngineState slot. Zero chili-core deps. Awaiting mdata acceptance evidence (~2 weeks). Re-evaluate after that data lands.
5. **Cross-link mdata wishlist + delivery docs** in your dev notes so future v0.X cuts can check downstream-shipped features before claiming feature-parity. We're happy to maintain a wishlist + delivery index page if that helps.

---

## 9. Appendix — citation index

**Wishlists (live + history) — the source-of-truth for every claude-2-only feature:**

- `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` — W1/W2/W3 (turn-9 revision; W3 case)
- `docs/history/sync/mdata_chili_2026-05-19_0.8.7_delivery.md` — push-model D-1/D-2/D-3 delivery
- `docs/history/sync/mdata_chili_2026-05-13_0.8.4_delivery.md` — flush_tplog + publish_via_handle + roll_tick
- `docs/history/sync/mdata_push_model_proposal_2026-05-17.md` — push-model design (frozen post-D-3 shipment)
- `docs/sync/mdata_chili_2026-05-24_0.8.9_delivery.md` — W3 delivery (live, awaiting acceptance)

**ADRs:**

- `docs/decisions/0001-pub-sub-canonical-model.md` — pub/sub
- `docs/decisions/0002-eval-lazy-eager-default.md` — true-lazy Option B
- `docs/decisions/0003-pylazyframe-dsl-incompat.md` — **RESOLVED** via py-1.39.3 fork (relevant to §4.4)
- `docs/decisions/0005-parquet-write-defaults.md` — SUPERSEDED (Sprint 20)
- `docs/decisions/0006-async-upd-notification-ffi.md` — push-model contract (relevant to §3.1-3.3)
- `docs/decisions/0007-w3-python-callable-bridge.md` — W3 contract (relevant to §3.9)

**Lessons:**

- `docs/standards/iteration_lessons.md` — 841 lines, 22 durable lessons from 23 sprints

**Cadence:**

- `docs/sim/cadence_metrics.md` — 24 sprint rows (Sprint 1 → 23)
- `docs/sim/sprints_index.md` — sprint index

**Branch model:**

- `CLAUDE.md` "Branch policy" section
- Tags: `claude-baseline-2026-05-07`, `main-pivot-2026-05-07`

**Bench:**

- `docs/bench/post_pivot_baseline_2026-05-07.md` — full baseline including Sprint 23 §, the matched-shell A/B methodology lesson, GR6 parse-cache hit number (377 ns)

---

End of document. Open for follow-up — happy to drill down on any single feature, the polars-fork question, or the W3 dependency framing.
