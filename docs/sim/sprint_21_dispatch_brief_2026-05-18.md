# Sprint 21 dispatch brief — mdata push-model: D-1 event-driven delivery + D-3 resumable cursor (one surface) + D-2 lazy accessor (parallel)

**Kickoff:** 2026-05-18 — Sprint 20 ratified; mdata gates Q1–Q5 locked (`docs/sync/mdata_push_model_proposal_2026-05-17.md`).
**Owner:** coordinator + **mandatory 3-agent pre-execution audit** (Explore + code-reviewer + planner) per `~/.claude/rules/self-audit-on-plans.md`.
**Type:** feature — new async Rust→Python notification FFI surface (ADR-0002-class → **ADR-0006**). Touches the IPC receive thread + `chili-py` FFI + `sub.pep` → dispatch brief mandatory per `.claude/rules/sprint-cadence.md`.
**Predicted pp:** 10–18 (mid ~14, pre-audit). Calibration: mdata-accepted sizing D-1 ≈ 1.5–2× roll_tick (Sprint-18 roll_tick ran ~ mid-sprint); D-3 ~60% pre-built; D-2 small. Push-model is net-new primitive (no auto-merge-cascade risk, unlike Sprint 20) but spans Rust thread + FFI + pepper + 3 committed test surfaces.
**Plan reference:** `docs/sync/mdata_push_model_proposal_2026-05-17.md` (chili evaluation + 3-agent audit + reply + mirrored mdata Q1–Q5).
**ADR references:** new **ADR-0006** (async notification FFI contract) — drafted this sprint. ADR-0002/0003 (true-lazy) unaffected; ADR-0001 (pub/sub) cross-ref.

---

## Sprint objective

Let mdata delete its rdb/wdb subscriber poll-loop + `_last_seen_seq` dedup + dual-buffer by adding a **GIL-free outbound Rust→Python upd notification** (D-1: fd + drain), a **resumable-from-caller-cursor subscription** (D-3), and a **lazy state accessor** (D-2) — without relocating any safeguard into pepper. **Binary success criterion (all hold):**

1. `engine.upd_notify_fd() -> int` returns an `asyncio.add_reader`-able fd; `engine.drain_upds() -> list[UpdEvent]` non-blocking; the IPC receive thread does **enqueue+signal only** (no GIL on the receive hot path — verified by inspection: no `Python::with_gil`/`Py` touch in the signal path).
2. `engine.get_var_lazy(id) -> pl.LazyFrame` returns a snapshot-clone `.lazy()`; projection/predicate pushdown appears in the lazy plan over the in-memory frame; `.collect()` byte-identical to `get_var(id)`.
3. `engine.subscribe(tick_socket, topics, resume_from={table: seq})` replays only seq > cursor; kill+restart-with-cursor stream is per-table contiguous, gap-free, zero dup (mdata's own row-`seq`, Q1 Path-1).
4. Back-pressure: bounded queue; when Python drains slower than arrival the receive thread **blocks** (never drops — tplog is source of truth). Contract documented in ADR-0006.
5. Full pre-commit gate green (`cargo fmt`/`clippy -D warnings`/`cargo test --workspace --exclude chili-py` + `maturin develop` + `pytest`); 3 committed e2e guards land (D-1 fd+drain round-trip, D-3 resume-after-kill, D-2 lazy-plan).
6. **No chili-py version bump, no wheel delivered** (per Sprint-20 G2: 0.8.7 cut from claude-2 HEAD *after* this sprint; single combined mdata delivery).

---

## Why now

- Sprint 20 ratified; the substrate the evaluation assumed is preserved + verified (claude-2 `roll_tick`, true-lazy ADR-0002/0003, the `replay` cursor primitive, the pure-Rust GIL-free `handle_chili_conn`).
- mdata's 5 gate answers are locked — no premise risk: Q1 **Path 1** (mdata owns a per-row `seq` UInt64 in every schema; chili's per-handle ordinal is *only* a monotonic delivery cursor — chili does NOT build per-table seq), Q2 chili discretion → **message-shape interception (a)**, Q3 **raw delta as sent** (no `upsert` reorder; mdata's accumulation is unkeyed so raw==post-upsert anyway), Q4 **per-drain-ack durability confirmed** (loss past last-drained recovered via D-3 replay; no disk-backed queue), Q5 **no fork** + defensive close-on-exec clause requested.
- 0.8.7 (post-sprint) is the single mdata delivery; getting D-1/D-2/D-3 in one sprint keeps that delivery coherent.

---

## Scope — Part A: D-1 + D-3 (one surface)

### A.1 Surface additions

```python
# chili-py FFI (crates/chili-py/src/lib.rs PyO3 + crates/chili-py/chili/engine.py wrapper)
engine.upd_notify_fd() -> int            # readable eventfd/self-pipe; O_CLOEXEC; kqueue/asyncio-able
engine.drain_upds() -> list[UpdEvent]    # non-blocking; drains the bounded queue
engine.subscribe(tick_socket, topics, resume_from: dict[str,int] | None = None)  # D-3 cursor arg
```
`UpdEvent` (new Python-visible type — `#[pyclass]`): `{table: str, cursor_lo: int, cursor_hi: int, frame: pl.DataFrame}`.

**Field-naming decision LOCKED (audit#2 C6 / Q1 Path-1):** the ordinal fields are named **`cursor_lo`/`cursor_hi`**, NOT `seq_lo/seq_hi` — they carry chili's **per-handle `tick_count` delivery ordinal**, explicitly NOT mdata's per-row `seq` column. Docstring + ADR-0006 must state: "per-handle monotonic delivery cursor; per-table contiguity is the caller's own `seq` column (Q1 Path-1)." This prevents the documented seq-collision confusion.

### A.2 Implementation hints (grounded in verified code state)

- **Receive thread:** `handle_chili_conn` (`crates/chili-core/src/utils.rs:307`) — pure `std::thread::spawn` (engine_state.rs:1097/2497), GIL-free, calls generic `state.eval` (utils.rs:351). **No dedicated upd-apply site** — `upd` is pepper (`crates/chili-py/chili/src/sub.pep:1`: `upd: {[table; data] table upsert data; tick[this.h;1]; }`).
- **D-1 hook (Q2 (a) — message-shape interception):** after a *successful* non-Sync `state.eval` in `handle_chili_conn`, inspect whether the inbound `SpicyObj` was an `(`upd; table; data)` shape; if so enqueue `(table, frame=data, cursor=post-eval tick_count[handle])` to a bounded `crossbeam`/`std::sync::mpsc::sync_channel`-style queue (net-new `EngineState` field, e.g. `upd_notify: Option<Arc<UpdNotify>>` holding the bounded sender + the eventfd write end) and `write(1)` the eventfd. **Never** acquire the GIL here.
- **seq/cursor:** `tick_count: RwLock<Vec<i64>>` per-handle (`engine_state.rs:2172` `fn tick`); `cursor_hi` = value after `tick[this.h;1]`, `cursor_lo` = prior. Per-handle, monotonic, in-memory.
- **Zero-copy frame:** the inbound `data` is already a deserialized `SpicyObj`/`DataFrame` via `serde9` (`crates/chili-core/src/serde9.rs:13` Arrow IPC) — hand it out without re-encode.
- **Back-pressure:** bounded `sync_channel(N)`; `handle_chili_conn` is already a blocking socket read-loop, so a blocking `send` naturally back-pressures the upstream tp (kdb+-like). Never `try_send`-drop.
- **fd lifecycle (Q5):** create the eventfd/self-pipe with `O_CLOEXEC` (close-on-exec — mdata's defensive request even though it doesn't fork); document that the fd must not be used across `os.fork` without re-creation; `check_fork()` (`lib.rs:283`) still guards method calls.
- **D-3 (~60% pre-built):** `replay`/`replay_chili` already takes an i64/timestamp start cursor (`side_effect_fn.rs:180`; `replay_chili_msgs_log` `engine_state.rs:605` skips `i < start`). `.sub.recover` (`sub.pep`) already replays from `tick[0]`. Gap = thread a caller-supplied `resume_from` through `subscribe()` → `.sub.init` (today hardcodes `replay[info[0]; 0; …]`) + a small accessor to seed/pass the per-table cursor. Make the reconnecting-handle index explicit (do not inherit `.sub.recover`'s hardcoded handle-0).

### A.3 Storage / schema

No on-disk format change. tplog is the source of truth (Q4). `tick_count` semantics unchanged. mdata owns per-row `seq` (Q1) — chili stays seq-agnostic.

### A.4 Tests (committed, mandatory — Sprint-19 lesson #1)

- D-1: Rust unit — bounded-queue back-pressure (sender blocks at capacity, no drop) + GIL-free-signal assertion (signal path has no `Py`/`with_gil`). Python integration — `loop.add_reader(upd_notify_fd(), drain)`; assert ≥1 `UpdEvent` per applied upd batch, frame == applied delta, zero steady-state `get_var` polls.
- D-3: Python — kill+restart subscriber with persisted cursor; assert post-restart per-table contiguous, gap-free, zero dup, no Python dedup.
- Unclean-`kill -9` test: loss past last drained recovered via D-3 replay (Q4 contract).
- D-2: lazy-plan inspection (pushdown present) + `.collect()` byte-identical to `get_var`.

## Scope — Part B: D-2 (small, independent, parallel)

### B.1 Surface
```python
engine.get_var_lazy(id: str) -> pl.LazyFrame
```
### B.2 Hints
`get_var` (`engine_state.rs:290`) is an eager clone under `vars.read()`; the var is an in-memory accumulated `DataFrame` (NOT a Parquet scan). `get_var_lazy` = snapshot-clone then `.lazy()` (sound: receive thread mutates under write-lock — no live view). ADR-0002/0003 true-lazy plumbing already proven (the `query_plan` lazy path). **Acceptance criterion (mdata-accepted reword):** "projection/predicate pushdown appears in the lazy plan over the in-memory frame" — NOT "reaches the scan."

---

## Out of scope (defer)

- **D-4 `evict_before`** — mdata-confirmed deferred (ergonomic sugar; `eval("delete from t where seq<n")` suffices; mdata owns the `seq` boundary).
- **chili-py version bump / wheel cut / mdata delivery** — post-sprint: 0.8.7 from claude-2 HEAD after this ratifies (Sprint-20 G2 single-delivery model). Brief Part-B does NOT bump.
- `async for`/callback sugar atop fd+drain — optional, only if pp budget remains; fd+drain is the load-bearing primitive.
- The full-family polars GitHub-host P0 — unchanged standing backlog.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `EngineState` bounded upd-notify queue + eventfd (net-new field + signal in `handle_chili_conn`) | new |
| 2 | `upd_notify_fd()` / `drain_upds()` / `UpdEvent` `#[pyclass]` — PyO3 + engine.py wrappers | new |
| 3 | `subscribe(resume_from=…)` + `.sub.init` cursor threading + accessor | new/edit |
| 4 | `get_var_lazy()` — PyO3 + engine.py | new |
| 5 | ADR-0006 (async notification FFI contract: fd semantics, back-pressure, `UpdEvent` schema, cursor contract, close-on-exec) | new |
| 6 | committed e2e guards (D-1 fd+drain, D-3 resume-after-kill, kill-9, D-2 lazy-plan) | new |
| 7 | `docs/sim/sprint_21_retro.md` + `cadence_metrics.md` row | new (post-sprint) |

---

## Lead allocation

Coordinator owns the load-bearing Rust thread/queue + FFI personally (do NOT delegate the GIL-free signal path or the back-pressure contract). Optional impl subagent for D-2 (small, independent) in parallel. Mandatory 3-agent pre-execution audit before any implementation. Budget ~2pp audit, ~8–16pp impl+tests+ADR.

## Mid-checkpoint plan

At ~50% predicted-pp (≈ D-1 queue+fd+signal landed, pre-Python-integration):
- Is the signal path provably GIL-free (no `Py`/`with_gil` in the `handle_chili_conn` enqueue+signal)?
- Does back-pressure block (not drop) at queue capacity?
- Is `cursor_lo/hi` wired to `tick_count` correctly (per-handle, monotonic)?
- ETA to gate.

Halt-and-escalate: (1) scope-blow >150% pred; (2) plan-pivot — the message-shape interception (a) can't cleanly capture `(`upd;table;data)` without a GIL touch or a pepper change (would relocate safeguard into pepper — violates the core constraint → escalate); (3) user-decision — reversible architectural choice not in this brief; (4) watchdog 5h ≥ 80% AND remaining > 15pp.

## Wrap (per ceremony)

- Full pre-commit gate green (incl. `maturin develop` + `pytest`).
- 3 committed e2e guards green; test-count delta documented.
- ADR-0006 committed; CLAUDE.md ADR list + state updated.
- Bench: confirm parse-cache golden-rule-6 untouched (push-model doesn't touch the parse hot path — confirm by inspection); D-1 throughput sanity (mdata's ≥363k msg/s acceptance is mdata-side post-delivery).
- `docs/sim/sprint_21_retro.md` + `cadence_metrics.md` row. Brief → `docs/history/sprints/` post-ratification. HALT until user ratifies.

## Pp accounting reference

| Item | Pred pp |
|---|---|
| Brief + 3-agent audit + appendix | ~2 |
| D-1: bounded queue + eventfd + GIL-free signal in handle_chili_conn | ~4–6 |
| D-1: PyO3 `upd_notify_fd`/`drain_upds`/`UpdEvent` + engine.py | ~2–3 |
| D-3: subscribe(resume_from) + sub.pep cursor threading + accessor | ~2–3 |
| D-2: get_var_lazy (small parallel) | ~1 |
| ADR-0006 + committed e2e guards + retro/cadence | ~2–3 |
| **Total** | **~10–18 (mid ~14)** |

Compare vs `cadence_metrics.md`: Sprint-18 roll_tick (the sizing anchor mdata accepted for ×1.5–2). Net-new primitive — no auto-merge-cascade risk (unlike Sprint 20), but spans thread+FFI+pepper+3 test surfaces; expect mid-band unless the GIL-free-signal or back-pressure design needs iteration (halt-trigger-2).

---

## Appendix — Independent audit (2026-05-18)

3 agents (Explore code-state · code-reviewer design · planner sequencing) re-verified against the **post-Sprint-20 merged tree** (HEAD `8ce4218`). **Convergent CONFIRM** (clean, cited): GIL-free signal achievable (`utils.rs` no pyo3, `Arc<EngineState>`, `state.eval` pure-Rust); message-shape interception works without a pepper change (post-`state.eval` the inbound `SpicyObj::MixedList` `(`upd`,table,df)` is still in scope, not consumed; `serde9.rs:384`); D-3 replay *mechanism* ~60% pre-built (`replay_chili_msgs_log` `engine_state.rs:605` takes `start`, skips `i<start`); `get_var_lazy` snapshot-clone sound (`DataFrame::lazy()`→`DataFrameScan` pushdown); `UpdEvent` `#[pyclass]`+`PyDataFrame` precedented (`lib.rs:49`/`:752`); back-pressure blocks without deadlock (receive=Rust thread, drain=Python thread, distinct). Sprint-20 merge did not alter the IPC/`tick`/`sub.pep` path.

### Material corrections (apply before/at impl)

1. **BLOCKER — macOS has no `eventfd(2)` (all 3 agents).** Dev env is darwin; `eventfd` is Linux-only. **Implementation = POSIX self-pipe** (`pipe2(fds, O_CLOEXEC)`). Neither `libc` nor `nix` is in chili-core/chili-py `Cargo.toml` → **net-new `libc` dependency** (add to scope + deliverables + clippy). Optional `#[cfg(target_os="linux")] eventfd` only if a real need arises; self-pipe is cross-platform and the default. State the delivered-wheel target platform.
2. **BLOCKER — ADR-0006 committed BEFORE impl, not co-drafted (planner).** It must lock: (a) **bounded-queue capacity `N`** — a concrete power-of-two **`N = 4096`** (tunable later; mdata's capacity acceptance test needs a deterministic value); (b) `UpdEvent` schema (`{table, cursor_lo, cursor_hi, frame}`, `cursor_*` = per-handle delivery ordinal, NOT mdata row-`seq`); (c) blocking-send-never-drop back-pressure contract + the timeout/disconnect escalation (see new halt-trigger); (d) close-on-exec. ADR-0006 is deliverable #1 and gates D-1 impl.
3. **MAJOR — `.sub.recover` cursor-persistence is NEW surface, not "~60% pre-built" (planner).** The replay *mechanism* is ~60% pre-built, but threading `resume_from` through `.sub.init` (sub.pep:10, ~0.5pp trivial) AND persisting the per-table cursor across disconnect→`.sub.recover` (sub.pep:18 hardcodes `tick[0]`) is genuine new design — **where is the cursor stored between disconnect and reconnect?** Decide in ADR-0006 (engine-state-held per-table cursor map vs closure arg). Cost it separately from the trivial `.sub.init` arg.
4. **MAJOR — pp band under-anchored.** Sprint-18 roll_tick actual = **~16pp** (verified, cadence_metrics row 18). Sprint 21 is materially broader (net-new queue+self-pipe+`#[pyclass]`+2 FFI methods+pepper×2+recover-persistence+ADR+3 committed guards+`libc` dep). **Revised band: 14–22 pp, mid ~18** (was 10–18/14; 10 unreachable).
5. **MINOR — "zero-copy" is loose** (code-reviewer): extracting `data` from the MixedList is a Polars `DataFrame::clone()` = shallow Arc-clone of column buffers (no re-encode/re-serialize). Reword "zero-copy" → "Arc-shallow-clone, no re-serialize."
6. **MINOR — line nit:** `check_fork()` is `lib.rs:288` (brief said ~283).

### Missing work categories (fold into deliverables)

`libc` Cargo.toml dep; the unclean-`kill -9` test (was in A.4, absent from deliverables table — add to #6); **Q5 close-on-exec as a committed pytest** (fork→assert child can't read the fd / EBADF — mdata uses `multiprocessing`, so a real guard, not a doc note); a cross-thread lock-ordering review note (new `upd_notify` field vs `vars`/`tick_count`/handle write-lock — confirm Send+Sync, no Mutex contention with drain).

### New halt-trigger (add to Mid-checkpoint)

**(2b) back-pressure-deadlock / upstream-tp-disconnect:** if blocking back-pressure holds the receive thread long enough that the upstream tp times out / drops the chili connection, the never-drop contract needs a timeout/drop-mode decision → **escalate** (user-decision; do not silently add a drop path — that would violate the tplog-is-source-of-truth invariant without sign-off).

### Revised sequencing (supersedes the brief's order)

**Step 0 (pre-impl POC, ~0.2pp):** 10-line darwin `os.pipe2`-equiv → `asyncio.add_reader` smoke to de-risk fd portability before wiring Rust. → **ADR-0006 (N=4096, UpdEvent schema, self-pipe, close-on-exec, back-pressure+escalation, recover-cursor-storage) committed** → D-1 (libc self-pipe + bounded `crossbeam`/`sync_channel` + GIL-free signal in `handle_chili_conn` + PyO3) → D-3 (`.sub.init` arg + `.sub.recover` persistence) → D-2 (parallel) → committed guards (incl. kill-9 + close-on-exec fork test) → gate. crossbeam 0.8.4 already in Cargo.lock (queue primitive available); only the fd side needs the new `libc` dep.

### Sprint sizing

**Revised 14–22 pp, mid ~18.** Drivers: the `libc`-dep + self-pipe platform abstraction, the `.sub.recover` cursor-persistence design (under-counted in the original "~60%"), and 4 committed test surfaces (D-1 round-trip, D-3 resume-after-kill, kill-9, close-on-exec-fork). No auto-merge-cascade risk (greenfield primitive). Upper edge if back-pressure/escalation contract or the GIL-free signal needs iteration.
