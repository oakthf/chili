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

If you want only one section: skip to §3 (the 9 mdata-driven features) — that's the case for upstream adoption. §4 (the 3 main-only features) is the case for claude-2 forward-porting. **§10 lists 5 design-topic asks from mdata's first-party perspective** (their handoff doc at `docs/sync/mdata_architecture_handoff_2026-05-24.md` is recommended pre-reading — has the production-deployment topology + two mermaid diagrams).

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

**Reply (chili-team, 2026-05-25):** there's a mental-model mismatch here. The fd in D-1 is **NOT** an outbound TCP socket fd — it's a POSIX self-pipe used as a *receiver-side* wake-up signal for the Python asyncio loop. When chili's IPC receive thread accepts an inbound `upd`, it (a) applies the upd to `vars` and (b) writes 1 byte to the self-pipe. A Python subscriber that's `await`-ing in `asyncio.loop.add_reader(fd)` then wakes up and calls `drain_upds()` to retrieve the new events. Without this, the Python subscriber would have to poll `get_var(table)` every ~10ms to detect new data (which is what mdata did pre-Sprint-21 and what caused the A-033 contention incident). The fd is internal to the same process — there is no TCP socket between chili-core and the Python subscriber when they're embedded in the same daemon. See ADR-0006 §1-2 for the full primitive choice rationale.

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

**Reply (chili-team, 2026-05-25):** **conceded — you are correct.** We re-read both bodies. `get_var_lazy` does `df.clone().lazy()` server-side; `engine.get_var(id).lazy()` does the same on the Python side. Both produce a single-node `DslPlan::DataFrameScan` over an in-memory frame; neither enables actual scan-pushdown (ADR-0006 §5 even acknowledges this — pushdown happens "over the in-memory frame," not at the scan). The FFI marshalling cost is roughly the same (PyDataFrame and PyLazyFrame both wrap Arc'd internals). The API is gratuitous surface. **claude-2 will deprecate `get_var_lazy` in the next sprint** — mdata can migrate to `engine.get_var(id).lazy()` with no functional change.

- **What:** snapshot-clone the `SpicyObj::DataFrame` under the `vars` read-lock, return `.lazy()`. Same byte content as `get_var(id)` on `.collect()`, but the lazy plan is pushdown-capable across the FFI boundary.
- **Why mdata needs it:** mdata's gateway composes lazy plans over per-table accumulators before collecting. Without D-2, `get_var(table)` returns an eager DataFrame that has to be re-`.lazy()`-wrapped on the Python side — no pushdown.
- **chili-core surface:** +12 lines on `EngineState::get_var_lazy`.
- **Deps:** none new.
- **Golden rule impact:** none. The receive thread continues to mutate vars under the write-lock; the clone is a stable snapshot. No live view.
- **ADR:** ADR-0006 §5.
- **Adoption:** mdata v1-26.

### 3.3. Push-model D-3 — `subscribe(resume_from=…)` + `resume_cursor` field (Sprint 21)

> Engine has a builtin reconnect logic and replay lost tp log, no need to take care by using new subscribe api.

**Reply (chili-team, 2026-05-25):** the existing `.sub.recover` does replay-from-`tick[0]` (current count) — for a process that has been running, that's correct; for a COLD-RESTART (process was killed, comes back with `tick[0]=0`), it implicitly means "replay nothing past current," which would skip messages already drained-and-acked by the consumer. mdata's actual deployment: rdb consumes ~10M rows/day; tplog accumulates to ~50-100 GB over the rolling window. After kill-9 + restart, the consumer needs to resume from the LAST-DRAINED cursor per-table, not from 0 (which would re-process everything) and not from `tick[0]` (which would skip everything already in tplog ahead of cursor). The `resume_cursor` map is small (one i64 per table) and lives in EngineState; the consumer persists its cursor durably (out-of-band). Without D-3, mdata reverts to per-row `_last_seen_seq` dedup over a full replay — works but is O(replay-size) per restart and the per-row seq is a separate anchor (Q1 Path-1 in ADR-0006 §4). For UHF + multi-day retention, the cursor matters. For typical short-replay scenarios, the existing reconnect logic is adequate.

- **What:** new `subscribe(tick_socket, topics, resume_from: dict[str,int] | None = None)` signature. `EngineState.resume_cursor: RwLock<HashMap<String, i64>>` (table → last-delivered `cursor_hi`); the existing `.sub.init` / `.sub.recover` pepper scripts consult this map via a new accessor builtin rather than hardcoded `0` / `tick[0]`.
- **Why mdata needs it:** cold-restart of an rdb after a kill-9 must resume from the last-drained `cursor_hi` per topic so that the tplog replay starts at the correct message ordinal. Without D-3, mdata reverts to per-row `_last_seen_seq` dedup over the full replay — works but costs O(replay-size) per restart, and the per-row seq is a separate anchor with its own correctness concerns (Q1 Path-1).
- **chili-core surface:** +30 lines on `EngineState::set_resume_cursor` + `resume_start_for` + the new `resume_cursor` builtin in side_effect_fn.rs.
- **ADR:** ADR-0006 §4 (with the 2026-05-19 corrected-resume-coordinate amendment — the mdata-found doc-bug). The chili code was correct; only the ADR text needed the fix.
- **Adoption:** mdata v1-26.2. The doc-bug was found by mdata's first-hand empirical test on 0.8.7; corrected in claude-2 commit `a50cec9`.

### 3.4. `flush_tplog()` (Sprint 16)

> mdata shouldn't need this at all, it should be taken care by chili file system. mdata should be focus on get data. The tp log after all is for recovering data. The tick plant is a light process, which doesn't keep data, and should not be killed by kill -9.

**Reply (chili-team, 2026-05-25):** the disagreement is about whether "don't kill -9" is an enforceable contract. mdata's production reality is that it's not — OOM kills happen, container orchestrators (k8s/nomad) hard-kill on liveness-probe failure, and ops occasionally `kill -9` by hand during incident response. mdata's PRD §5.1 part-2 treats kill-9 durability as a hard requirement; the contract is "at most one in-flight message lost." The OS file-system buffer doesn't satisfy that on its own — without an explicit `fsync` at checkpoint-aligned moments, a kill-9 can lose seconds of writes. **Counter-proposal:** rather than expose the tplog-specific `flush_tplog()`, expose a generic `engine.fsync_handle(h)` (any handle, not just `.tick.msgHandle`). That puts the durability primitive at the file-system layer (where you suggest it belongs), is opt-in (user calls it at the cadence they need), and lets mdata + any other downstream user implement their own durability policy. We're happy to refactor in that direction.

- **What:** Python-callable method that flushes the in-memory tplog write buffer to disk via `fsync` on the underlying file handle. Targets `.tick.msgHandle` (set by `.tick.createLog` during `init_tick`).
- **Why mdata needs it:** PRD §5.1 part-2 specifies kill-9 durability — a hard-kill of the tp process must lose at most one in-flight message. The OS file-system buffer doesn't fsync on every write (would cost too much); mdata's tp daemon calls `flush_tplog()` at checkpoint-aligned moments (after a batch of N publishes, or every M ms) to bound the loss window.
- **Your reaction was "this doesn't make sense to him" — the rationale:** without push-model + resume-cursor context, `flush_tplog` looks like a one-off file-sync wrapper. The reason it's a chili-side API (not a Python `os.fsync` call) is that chili owns the file handle; the Python side has no way to reach the OS fd without an FFI. We could expose a generic `engine.fsync_handle(h)` instead — that's a refactor we'd accept upstream.
- **chili-core surface:** +30 lines; uses the existing `ReadWrite::sync_all` trait introduced for this purpose.
- **Deps:** none new.
- **Adoption:** mdata's tp daemon since 2026-05-13 (0.8.4).

### 3.5. `publish_via_handle(h, table, df)` (Sprint 19, mdata 2b)

> Should never use this, just use publish, which is a defined chili function for publishing data.

**Reply (chili-team, 2026-05-25):** **partially conceded.** You're right that `publish_via_handle(h, table, df)` is sugar over `sync(h, ("upd", table, df))` with one guard (validates the handle is `Outgoing`). The use case it addresses is "publish to a SPECIFIC handle" (point-to-point) vs `publish(table, data)`'s "broadcast to all subscribers of `table`" — different semantics, but the point-to-point case can be expressed directly via `sync(h, ...)`. **claude-2 will deprecate `publish_via_handle` in the next sprint**; mdata's call sites migrate to the direct sync form. The handle-type guard becomes a one-time assertion at the call site (or a defensive check inside mdata's own publisher wrapper). Net: −30 lines from chili-core.

- **What:** outbound `sync(h, (`upd; table; df))`shaped helper that validates the handle is`ConnType::Outgoing` before publishing, eliminating the lock-acquisition + handle-lookup pattern mdata had to write at every call site.
- **Why mdata needs it:** gateway code emits ~10-50 per-table publishes per EOD cycle; without the helper, each call site re-implements the validation + the upd-message construction.
- **chili-core surface:** +30 lines on `EngineState::publish_via_handle` (with the explicit early-drop of the read lock so `sync()`'s internal write lock doesn't deadlock against the same-thread read lock — `parking_lot::RwLock` is not reentrant).
- **Deps:** none new.
- **Adoption:** mdata gateway since 2026-05-13 (0.8.4).

### 3.6. `roll_tick(log_dir, segment_label)` — atomic native cutover (Sprint 18)

> This function apparently used some functions that are not actually internal chili functions. Should just use roll_tick_log. The new function is `.handle.rotate` for atomic cutover.

**Reply (chili-team, 2026-05-25):** we re-read `EngineState::rotate_handle` (`engine_state.rs:754` on main, the impl behind `.handle.rotate`) and compared to `EngineState::roll_tick` (`engine_state.rs:931` on claude-2). They are **not equivalent** for the crash-recovery case mdata cares about. Four concrete differences:

1. **`rotate_handle` refuses non-empty target files** (`if conn_type != ConnType::New { return Err("file is not empty") }`). After a crash mid-roll, the next segment may already exist as a partial file — `rotate_handle` can't recover from that state. `roll_tick` runs `.broker.validateSeq` first to walk the seq tail and truncate any torn record via `set_len`.
2. **`rotate_handle` does NOT fsync the OLD writer before swapping.** `roll_tick` calls `old.flush() + old.sync_all()` under the handle write-lock, so the old segment is durable before it stops being the live writer.
3. **`rotate_handle` has no idempotent short-circuit.** A retry of `roll_tick` on the same target is a cheap no-op (checks `entry.uri == next_uri` first); `rotate_handle` would error on retry because the target now exists.
4. **`roll_tick` calls `validateSeq` OUTSIDE the handle write-lock**, keeping the lock window minimal — important under concurrent `.tick.upd` traffic.

The "non-chili-internal functions" your comment flags are `.broker.validateSeq` (a pepper-side builtin in `broker.pep`) and `prepare_file_writer` (a `utils.rs` helper introduced Sprint 17 for `rotate_handle` itself). Both ARE internal chili functions; `validateSeq` ships in chili's bundled `broker.pep`. Happy to send pointer-references if useful.

For mdata's daily-rotation use case at quiescent traffic, `roll_tick_log` is adequate. For UHF + crash-recovery, the explicit atomicity contract matters. We'd ask you to consider keeping `roll_tick_log` as the simple-case API + `roll_tick` (or an equivalent crash-safe variant) as the production-grade one.

- **What:** native Rust implementation that holds the handle write-lock across **open-next → swap-writer (same handle id) → fsync+close-old**. Any concurrent inbound `.tick.upd` is serviced by exactly one valid handle and lands wholly in the old segment OR wholly in the new — never dropped, never split.
- **Why mdata needs it:** UHF (ultra-high-frequency) tplog rotation. mdata's daily-rotation cutover happens while publishes may still be in flight; the old `engine.eod(d)` + `init_tick(.., d+1)` pair had a brief window where a publish could land in the wrong file. At ~thousands of ticks/sec, the data-loss probability over a full year of cutovers is non-trivial.
- **Semantic delta vs main's `roll_tick_log`:** main's version (commit `43faf44`) is a 4-line pepper script `.tick.rollLog` that calls `.handle.rotate[.tick.msgHandle; .tick.logFile]` — no atomicity guarantee. Functionally equivalent for the LOW-frequency case (daily rotation when traffic is quiescent); structurally unsafe for the UHF / size-triggered rotation case mdata cares about.
- **chili-core surface:** +50 lines.
- **Deps:** none new.
- **Adoption:** mdata tp daemon since 2026-05-13.

### 3.7. GR4 quantization helpers — `set_column_scale` / `clear_column_scales` (various sprints)

> This is mdata specific requirement, which should be built on top of chili engine, not part of chili engine.

**Reply (chili-team, 2026-05-25):** **conceded.** You're right that `set_column_scale` / `clear_column_scales` are opinionated about "what columns mean" — that's user-side semantics, not engine semantics. A pure-Python facade can hold the column-scale registry + apply dequant at the user-facing boundary; nothing chili-side needs to know about scales. **claude-2 will lift these helpers OUT of chili-core into a pure-Python `chili.scale` module in the next sprint.** The M-1 invariant (eager-eval does NOT auto-dequant; preserves on-disk dtype) stays as a chili-side property — that's about engine honesty, not about quantization specifically.

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

**Reply (chili-team, 2026-05-25):** this is where the architectural-model mismatch is sharpest — see §9 below. The pepper-only stance assumes the function bodies CAN be expressed in pepper. mdata's 3 control verbs cannot:

- `.mdata.eod.fire[date]`: drains a Polars-managed in-memory buffer, atomically renames the partial parquet to its final EOD path, broadcasts EOD to downstream daemons via mdata's own pub/sub mesh (not chili's), updates a partition-index file via pyarrow. **None of these primitives exist in pepper.** Polars LazyFrame manipulation, pyarrow IPC, `os.rename` + `os.fsync` of arbitrary paths, mdata-pub/sub — these are Python ecosystem features.
- `.mdata.wdb.finalize[date]`: writes the idb partition to disk via pyarrow, validates schema parity with hdb, atomically transitions the partition into the hdb table tree.
- `.mdata.hdb.reload[]`: drops chili's partition cache (via `engine.clear_partitioned_df()`), reloads via pyarrow scans, invalidates mdata's own per-process query cache.

"Efficiency" framing: at the per-call level, W3 adds ~300ns/round-trip (measured against `get_var/set_var` `py.detach+with_gil` paths). mdata's 3 verbs fire at most ~3 times/day per daemon. Total annual W3-overhead per daemon: ~10µs. Not a hot path.

"Complicated" framing: chili-core surface delta is +43 lines (`external_fn.rs` trait) + 1 `Option<String>` field on `Func` + 1 slot on `EngineState` + 1 dispatch branch in `eval_fn_call` (15 lines). Zero new chili-core dependencies. The Python adapter lives entirely in chili-py. We'd argue this is structurally simple — but agree the conceptual coupling (chili-core knows about an "external dispatcher" trait) is novel.

**The deeper question is positioning** (§9): if chili is positioned as a self-contained pepper-first system, W3 doesn't belong. If chili is positioned as an embedded analytics engine inside a Python (or future R / Julia) host, W3 is the standard FFI-extension primitive that every embedded runtime offers (kdb+'s `dlopen`/foreign-fn registration, Lua's `lua_register`, Python's C-extension API, etc.). For mdata, the embedded model is non-negotiable — we can't move the buffer-management and pyarrow logic into pepper. So claude-2 keeps W3 even if upstream rejects it.

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

**Revised 2026-05-25 after chili-author's inline comments.** Three of our 9 claude-2-only features become **claude-2-side refactors** (we agree with you); four remain **upstream-evaluation asks**; one is **technical correction** (we believe your comment was based on incorrect information); §9 below frames the underlying architectural question.

### claude-2-side refactors (we conceded to your comments — work happens on our side, requires coordinated migration with mdata)

**Migration footprint check (against mdata's `docs/sync/mdata_architecture_handoff_2026-05-24.md` §2 call-site inventory, 2026-05-24).** The three concessions below all have non-trivial mdata-side call sites in production. We will deprecate, **but the timing must be coordinated with mdata's sprint cadence** — they are currently running 0.8.8 in production with a 24h Pipeline X soak in flight (completes ~2026-05-25 morning local); deprecation-bearing wheels can land only after the soak passes and a dedicated migration sprint is scheduled.

1. **Forward-port to claude-2:** `async_` + `execute` + `polars-core-patch` URL (verify q-style fmt patch first) + `py.typed`. ~1pp work; no mdata-side coordination needed.

2. **Deprecate `get_var_lazy`** (D-2). Conceded per §3.2 reply. **Migration footprint:** mdata does not enumerate call sites in their handoff doc, but their `MultiRdbRouter` (`src/mdata/common/remote_client.py:225`) and gw query path likely consume the lazy-frame return. **Plan:** chili emits a `DeprecationWarning` for one release (0.8.10), mdata migrates to `engine.get_var(id).lazy()` in their next sprint, removal in 0.9.x. **Not single-sprint.**

3. **Deprecate `publish_via_handle`** (mdata 2b). Conceded per §3.5 reply. **Migration footprint: 10 cross-process call sites in mdata's fh→tp path** (per their §2 inventory) — this is mdata's "canonical chili usage pattern" (their §3 wording), the entire write path of the warehouse. Throughput in production: 6944 msg/sec sustained (their v1-32 6h soak result). **Plan:** chili emits `DeprecationWarning` in 0.8.10, mdata migrates their 10 sites + the `RemoteTpClient` wrapper to `engine.sync(h, ("upd", table, df))` over a coordinated sprint, removal in 0.9.x. **NOT a Sprint 24 deletion** — premature removal breaks the entire production write path.

4. **Lift GR4 helpers out of chili-core** (`set_column_scale` / `clear_column_scales`) into a pure-Python `chili.scale` module. Conceded per §3.7 reply. **Migration footprint:** mdata's `StorageEngine` wrapper (`src/mdata/db/storage.py`) is the integration point — it handles partition-aware Parquet helpers, schema enforcement on write, and integration with mdata's audit columns + Int64-quantized columns. **Plan:** lift the helpers to a pure-Python `chili.scale` module that `StorageEngine` consumes via composition; M-1 invariant stays in chili-core (engine-honesty, not quantization). Migrating mdata is single-site (StorageEngine) but the wheel-API for GR4 changes — coordinate with mdata before removing the chili-py shims. Sprint 25 work.

5. **Coordinate the eval_op inline-String refactor adoption.** We remove our `eval_str` SIDE_EFFECT_FN; mdata confirms they use bytes-form exclusively (their handoff §2 — "0 active mdata uses yet" for `eval_str` builtin). Single delivery, no migration cost.

6. **Coordinate the `init_tick` rename window** (`date` → `filename`) with mdata before adopting.

Net result after the full migration cycle: **claude-2's chili-core surface shrinks by ~80-100 lines** (publish_via_handle removed; get_var_lazy removed) + chili-py shrinks by ~50 lines (GR4 helpers lifted to Python). We end up closer to your codebase. **End-state arrival: estimated 2-3 mdata-sprints from today** (post 24h soak + post 0.8.9 W3 install + dedicated migration sprint).

### Upstream-evaluation asks (we believe these belong upstream; architectural disagreement; your call)

7. **D-1 push-model** (upd_notify_fd / drain_upds / UpdEvent) — see §3.1 reply for the receive-side wake-up clarification. ADR-0006. 2-month production track record at mdata.
8. **D-3 resume_from cursor** — see §3.3 reply for the UHF crash-recovery case. Critical for tplog tails > 10GB.
9. **`fsync_handle(h)` durability primitive** (replacing tplog-specific `flush_tplog`) — see §3.4 reply for the kill-9 contract framing. We propose the generic shape as an upstream-friendly compromise.
10. **W3 Python-callable bridge** — see §3.9 reply and §9 below for the embedded-runtime positioning argument. Net chili-core surface: +43 lines + 1 trait + 1 Func field + 1 EngineState slot. Zero new chili-core deps. Awaiting mdata acceptance evidence (~2 weeks); re-evaluate then.

### Technical correction

11. **`roll_tick` is not equivalent to `roll_tick_log` / `.handle.rotate`.** See §3.6 reply: `rotate_handle` lacks (a) seq-tail validation, (b) old-writer fsync, (c) idempotent retry, (d) non-empty-file recovery. For mdata's UHF crash-recovery case these matter; for the daily-rotation case they don't. We'd ask you to consider keeping `roll_tick_log` as the simple case + `roll_tick` (or an equivalent crash-safe variant) as the production-grade case.

### Process

12. **Cross-link mdata wishlist + delivery docs** in your dev notes so future v0.X cuts can check downstream-shipped features before claiming feature-parity. We're happy to maintain a wishlist + delivery index page if that helps.

---

## 9. Architectural model: standalone vs embedded — naming the disagreement

Your 8 inline comments in §3 are internally consistent: they reflect a coherent model where chili is a **standalone, pepper-first analytics system**. Python is a REPL / convenience layer; first-class users write pepper; subscribers register as pepper functions; control verbs live in pepper.

mdata's deployment fits a different model: **chili as an embedded analytics engine inside a Python (or future R / Julia) host daemon**. The Python (host) layer owns: process lifecycle, async I/O (pyarrow + polars + asyncio), pub/sub to mdata's own mesh, control verbs that touch buffers/disk via Python ecosystem primitives. chili owns: pepper-callable analytics + tplog + IPC.

The two models lead to different conclusions on every feature we discussed:

| Question | Standalone-first answer | Embedded-first answer |
|---|---|---|
| Where do subscribers live? | Pepper functions registered via `.broker.subscribe` | Python coroutines woken by D-1's self-pipe |
| Who owns durability? | OS file system + "don't kill -9" contract | The host process — explicit `fsync_handle(h)` at checkpoints |
| Where do control verbs live? | Pepper functions in user-bundled `.pep` files | Python handlers registered via W3's `register_fn` |
| How is data exchanged Python↔chili? | Out-of-band over chili-IPC (TCP) between processes | Same-process FFI; D-1 wakes Python directly |
| What's the engine size budget? | Minimal — pepper-first, opinionated, small dep tree | Whatever the host can absorb; trait-based extension points are fine |

**Both models are legitimate.** kdb+ has gone hard on standalone; numpy + Polars + scikit-learn are firmly embedded. There's no objective "right answer" — it depends on who chili's primary user is.

mdata committed to the embedded model in 2026-04 (predates our claude-2 fork). For us, the embedded model is non-negotiable — we can't move Polars-buffer management, pyarrow I/O, and mdata-mesh-pub/sub into pepper.

**What this means for upstreaming:**

- If chili's positioning stays standalone-first, the 4 upstream-evaluation asks above (D-1, D-3, fsync_handle, W3) probably don't fit. We accept that and continue maintaining the claude-2 fork. The 3 claude-2-side refactors still happen because we agree with you on those.
- If chili can accommodate an "embedded extension surface" (optional, opt-in, zero-dep-when-unused), the 4 asks might fit — W3's trait shape is explicitly designed to be opt-in, and D-1's fd-notification is gated by `enable_upd_notify()`.

We don't need you to commit to a positioning shift today. The frame is "here's where the seams are; tell us which seams you're comfortable carrying upstream." We'll plan claude-2's next 2-3 sprints around your answer.

---

## 10. Discussion topics from mdata's first-party perspective

mdata-team drafted their own architecture handoff doc the same day this gap analysis went out (`docs/sync/mdata_architecture_handoff_2026-05-24.md`, mirror in this repo). It's an authoritative production-deployment snapshot — 9 daemons per pipeline, 7 embed `ChiliEngine`, two mermaid diagrams of the data-flow topology and the today-vs-post-cutover IPC shape. Recommended pre-reading before responding to this gap analysis.

Their handoff §4.3 lists 5 discussion topics they would value your input on, in their priority order. We're surfacing them here because they're substantive design questions that overlap our §8 asks but come from the deployment-side rather than the chili-team-side. Together they cover the next 2-3 sprints of mdata's chili integration roadmap.

### 10.1. IPC cutover design review (HIGH PRIORITY — time-sensitive)

mdata has authored an IPC-cutover proposal at `~/code/mdata/docs/proposals/ipc_cutover_remove_unix_socket_2026-05-24.md` — Option A' migrates all 6 AF_UNIX attach-socket surfaces to chili-IPC using `register_fn` + tuple-form `sync(h, (name, *args))`. Before mdata commits **~10pp of v1-36+** to retire attach-socket, they want chili-author input:

- Is `register_fn` + tuple-form dispatch the right primitive for "evaluate this pepper expression on a remote engine and return the result," or should chili-IPC have a higher-level "remote pepper eval" surface?
- mdata's current attach-socket use is mostly "evaluate this bytes source string, give me the result back" — that's what `engine.sync(h, b"<src>")` already does over chili-IPC (W1 self-discovered bytes-form). Is the W3 register_fn path strictly better, or are there cases where bytes-form sync is preferable?

**Why time-sensitive:** mdata's next sprint after the in-flight Pipeline X soak is either v1-36 (IPC cutover) or v1-37 (LTP adapter) — principal's call. If you have an opinion on the cutover shape, it's most valuable BEFORE v1-36 starts.

### 10.2. Async surface roadmap

The A-033 incident (mdata v1-32) — asyncio event loop saturation at 6944 msg/sec sustained writer load — cost mdata ~12pp over an 8-commit fix arc (F1–F9). They mitigated mdata-side with `asyncio.to_thread` executor dispatch + executor-bounded `__ping__` fast-path. The underlying chili-side observations stand:

- `flush_tplog()` is sync; holds GIL; blocks the event loop. **Ask:** `flush_tplog_async()` (or our proposed generic `fsync_handle_async(h)`) on the chili roadmap?
- Reader fairness under sustained writer load — mdata observed `RwLock` reader starvation during A-033 at 6944 msg/sec writer. **Ask:** tunable reader-writer-fairness knob? Default-fair? Or is the v1-32 mitigation (executor-dispatch + fast-path) the right shape?

The full wishlist is at mdata-side `docs/sync/chili_wishlist_2026-05-22_async-surface.md`. Non-blocking but on radar.

### 10.3. Reader-writer fairness defaults

Sub-bullet of 10.2 but worth separating: at 6944 msg/sec sustained writes, mdata observed reader starvation on `parking_lot::RwLock`. They worked around it with `__ping__` fast-path + executor-bounded reads. **Ask:** is there a chili-side configuration knob (e.g., `parking_lot::RwLock` with `RawRwLock::new_fair()` or equivalent) we missed? Or is "be reader-fair by default" something to consider for a future release?

### 10.4. Pepper query-result serialization

mdata serializes pepper query results manually over attach-socket today — custom protocol in `src/mdata/common/attach_socket.py`. With W3 register_fn shipping, can chili-IPC carry the result directly (no custom serializer)? If so:

- **Ask:** what's the wire format chili uses for `sync(h, (name, *args))` return values? Is it well-documented for downstream embedders?
- **Ask:** does chili guarantee schema stability for query results (e.g., a `DataFrame` returned today has the same wire shape next release)?

This question gates how cleanly the IPC cutover (10.1) can drop the custom protocol.

### 10.5. `StorageEngine` wrap pattern — upstreamable?

mdata's `src/mdata/db/storage.py` is a `ChiliEngine` wrapper that adds:

- Partition-aware Parquet load helpers (slices the canonical hdb tree by date / table)
- Schema enforcement on write (per-table schema registry; type-mismatch errors before chili sees the rows)
- Integration with mdata's audit columns (`seq`, `ingest_ts`, `schema_version`) — auto-stamped on publish

mdata's question: is this a pattern other chili embedders would want? If yes, a subset of `StorageEngine` could upstream into chili-py as `chili.storage.StorageWrapper` or similar — saving every downstream embedder from re-inventing it.

mdata is happy to upstream the pattern if you'd accept the abstraction. If you'd rather chili-core stay minimal (per the §9 standalone-first model), they'll keep it mdata-side.

### Cross-reference table

| mdata-side doc | Topic |
|---|---|
| `docs/sync/mdata_architecture_handoff_2026-05-24.md` (mirror in this repo) | Full architecture handoff + 2 mermaid diagrams + invariant list |
| `~/code/mdata/docs/proposals/ipc_cutover_remove_unix_socket_2026-05-24.md` | IPC cutover Option A' (gates §10.1) |
| `~/code/mdata/docs/sync/chili_wishlist_2026-05-22_async-surface.md` | Async surface wishlist (gates §10.2 + §10.3) |
| `~/code/mdata/docs/sync/v1_32_a033_step1_findings_2026-05-21.md` | A-033 incident + F1-F9 fix arc |
| `~/code/mdata/docs/standards/chili_capability_inventory.md` | mdata's catalogue of chili APIs in production |

---

## 11. Appendix — citation index

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
