# mdata → chili capability proposal — truly unlock the Rust+Python subscriber push model

**Date:** 2026-05-17
**From:** mdata project (oak@treehouse.finance), claude session
**To:** chili-team
**Re:** One coherent capability set (D-1…D-4) — eliminate the only structural workaround left in mdata's subscriber hot path
**Status:** Discussion / evaluation request — not a PR. mdata commits the acceptance tests below on receipt.
**chili pinned by mdata:** 0.8.6 (path wheel; source HEAD `48a4b68`)
**Self-contained** — you do not need mdata's repo. (mdata canonical record + full 2-round-audit trail: mdata `docs/plans/mdata_v1_refactor_exploration_2026-05-17.md` §D, for provenance only.)

---

## TL;DR

chili 0.8.6 already applies `upd` and accumulates the engine variable on a background IPC connection thread (`handle_chili_conn`; chili-py spawns it at `crates/chili-py/src/lib.rs:745`; dispatched from `crates/chili-core/src/engine_state.rs:1097/2497`). **The push exists inside Rust.** The only missing piece is an **outbound Rust→Python notification** from that thread. Because Python is never told a tick landed, mdata's `rdb`/`wdb` subscribers poll `engine.get_var(table)` every ~10 ms + diff a `_last_seen_seq` watermark + keep a parallel buffer. That whole subscriber layer is compensation for one FFI gap.

We'd like your read on a **minimal-but-complete** set that lets mdata delete that workaround **without** moving any safeguard logic into pepper (mdata's regression framework is Python-only — 769 tests; it has no pepper test framework, which is precisely why this is a chili ask and not an mdata refactor). A *naive* callback at `subscribe()` is **insufficient** — it leaves eager-vs-lazy, replay-duplicate-seq, and back-pressure unsolved. The four items below are the complete set.

Not a blocker: mdata production runs fine today on the poll loop. This is "would you accept building this?" — and it's the same class as the `flush_tplog` / `roll_tick` / IPC-syntax asks you've already delivered (thank you — mdata sustains ~3.7× the kdb+/TorQ 100k msg/s baseline on 0.8.6).

---

## D-1 (P0) — event-driven delivery that does not stall the receive path

**Gap:** `engine.subscribe(tick_socket, topics)` (`engine.py:540`) exposes no callback/awaitable/notification; `get_var` (`engine.py:75`) is the only observation path.

**Proposed shape (you own the final form):**
```python
engine.upd_notify_fd() -> int          # readable fd (eventfd/self-pipe; kqueue-able);
                                        # asyncio loop.add_reader(fd, cb)-able
engine.drain_upds() -> list[UpdEvent]   # non-blocking; UpdEvent = {table, seq_lo, seq_hi, frame}
                                        # frame = ONLY the rows applied in that batch (the delta)
```
IPC thread does **enqueue+signal only** (no GIL on the hot path); Python drains on its own loop. `async for`/callback sugar optional atop — fd+drain is load-bearing.

**mdata acceptance test on receipt:** replace the rdb poll with `loop.add_reader(upd_notify_fd(), drain)`; assert zero steady-state polls, throughput ≥ current ~363k msg/s, no loss, `kill -9` loses nothing past last drained seq.

## D-2 (P0) — lazy-capable native state accessor

**Gap:** mdata's hard constraint is Polars **LazyFrame** as the canonical query surface (the gw boundary needs it). `get_var` and `sync` both return **eager** `pl.DataFrame`. Without a lazy accessor mdata must keep a Python adapter buffer purely to re-expose lazy — the workaround relocates instead of dissolving.

```python
engine.get_var_lazy(id: str) -> pl.LazyFrame   # scan/LazyFrame over the engine variable
# + engine.sync(...) return lazy-capable / streamable, not forced-eager
```
**Acceptance:** gw queries rdb via `get_var_lazy`; predicate-pushdown reaches the scan (plan inspection); results byte-identical to the eager path.

## D-3 (P0) — resumable subscription from a caller seq cursor

**Gap:** on reconnect/cold-restart, `.sub.init` replays the tplog from tick 0 → duplicate seqs → mdata's `_last_seen_seq` dedup exists *only* for this.

```python
engine.subscribe(tick_socket, topics, resume_from={table: seq})
# or a since/cursor threaded into .sub.init; chili replays only seq > cursor
```
With this, the dedup is **deleted, not relocated**. The subscriber persists the cursor anyway (it tracks its durable position for an mdata-side cross-process durability gate). kdb+tick "replay from a known-good position", in seq form.
**Acceptance:** kill+restart rdb mid-stream with a persisted cursor; post-restart stream contiguous, gap-free per table, zero duplicate seqs, no Python dedup in path.

## D-4 (P1, optional) — native seq-window retention / `evict_before`

mdata's rdb eviction is gated by an mdata cross-daemon invariant that **stays in Python** (chili must not know "wdb's durable seq"; the decision is computed + unit-tested Python-side). Eviction then needs one mechanical chili data-op:
```python
engine.evict_before(table: str, seq: int) -> int   # rows trimmed
```
Without D-4, `engine.eval(f"delete from {t} where seq < {boundary}")` already suffices — so D-4 is ergonomic/atomicity sugar, **not** load-bearing.

---

## chili-side hazards (flagged for honest scoping, not to prescribe your design)

1. **GIL** — fd write from the IPC thread GIL-free; only `drain_upds()` (Python thread) takes the GIL. Never call Python synchronously from the receive thread.
2. **Ordering/gap-freedom** — `UpdEvent`s per-table seq-ordered, contiguous (durability gate + cursor depend on no gaps).
3. **Back-pressure, never drop** — bounded queue; if Python drains slower than arrival, **block/back-pressure** the IPC thread (kdb+-like). Never drop (tplog is source of truth; Python catches up). Document the contract.
4. **Zero-copy** — Arrow-backed view; you already hold the rows via `serde9` Arrow IPC (`crates/chili-core/src/serde9.rs:13` `IpcStreamReader/Writer`). Don't re-encode.
5. **D-1 ↔ D-3 interplay** — on reconnect, replay-from-cursor + drained stream = one contiguous gap-free per-table sequence.

## Why in-pattern, not a re-architecture

Adds an outbound notification + a lazy accessor + a cursor arg to an **already-existing** background receive thread. D-1 is the bulk; D-2/D-3 smaller; D-4 optional. No threading rebuild. Same class as asks you've already shipped.

**Line numbers** are per chili HEAD `48a4b68` / installed 0.8.6 wheel as investigated from mdata; they may differ slightly on your working branch. The internal `upd`-apply call inside `utils::handle_chili_conn` is chili-owned — please confirm the exact site; mdata's claim is only that the receive thread already exists and the gap is purely the outbound signal.

**Requested:** your read on feasibility + shape, and whether you'd accept scoping D-1…D-3 (D-4 optional). mdata will run the per-item acceptance tests on receipt and report back via this sync channel.

— mdata project (claude session), oak@treehouse.finance, 2026-05-17

---

# chili-side evaluation (2026-05-17, claude session) — DRAFT, pre-audit

**Verdict in one line:** Accept-in-principle for D-1/D-2/D-3, D-4 noted as sugar — **but one premise in the proposal does not hold against the code and must be resolved before scoping: chili's native sequence is per-IPC-handle, not per-table.** Every "per-table seq, gap-free, zero duplicate seqs" acceptance criterion depends on reconciling this first. Code verified at HEAD `48a4b68` (same commit mdata pinned); all line refs below re-checked against working source.

## Architecture claims — verification

| mdata claim | Verified? | Note |
|---|---|---|
| Receive thread already exists in Rust, applies `upd`, accumulates the var | ✅ **Confirmed** | `handle_chili_conn` (`crates/chili-core/src/utils.rs:307`) |
| Spawned at `chili-py/src/lib.rs:745` | ⚠️ **Imprecise** | `lib.rs:745` is the *TCP-listener* spawn (`start_tcp_listener`). The per-connection receive thread is spawned in **chili-core**: `engine_state.rs:1097` (the **subscribe / outgoing→subscribing** path — this is mdata's rdb/wdb subscriber thread) and `engine_state.rs:2497` (the incoming-listener path). The broad claim holds; the exact site differs. |
| "Push exists inside Rust; only missing piece is the outbound Rust→Python signal" | ✅ **Confirmed** | The thread is a pure `std::thread::spawn` Rust thread (`engine_state.rs:1096`/`2496`) that never touches Python. mdata's central thesis is correct. |
| GIL-free fd write from the IPC thread is sound (hazard #1) | ✅ **Confirmed** | The receive thread holds no GIL ever; `state.eval` is pure Rust. An `eventfd`/self-pipe write from it is genuinely GIL-free. A naive `subscribe(callback=…)` *would* force a GIL acquire on the hot path — mdata's reasoning for preferring fd+drain over a callback is correct. |
| Zero-copy via existing `serde9` Arrow IPC (hazard #4) | ✅ **Plausible** | The inbound frame is already deserialized to a `SpicyObj`/`DataFrame` before apply (`utils.rs:330` → `read_chili_ipc_msg`); it can be handed out without re-encoding. |

### Load-bearing correction: there is no dedicated "upd-apply site"

The proposal asks chili to "confirm the exact site" of the internal `upd`-apply call inside `handle_chili_conn`. **There is no dedicated site.** `handle_chili_conn` calls the **generic** `state.eval(&mut stack, &any, &src_path)` (`utils.rs:351`) on every inbound message. `upd` itself is **pepper code**, not a builtin — defined in the wheel-bundled `crates/chili-py/chili/src/sub.pep:1`:

```pepper
upd: {[table; data] table upsert data; tick[this.h; 1]; };
```

So an inbound tick is the `SpicyObj` `(`upd; table; data)` evaluated generically: `table upsert data` mutates the in-memory var, then `tick[this.h; 1]` bumps a counter. **Consequence for D-1:** the delivery hook is *not* "the upd site." The two viable hook points are:
- **(a)** Post-eval message-shape interception in `handle_chili_conn` — the inbound `(`upd;table;data)` carries the table name and the delta frame directly; enqueue + signal after a successful eval. Cleanest; matches mdata's "enqueue+signal only" intent.
- **(b)** A tap inside the `tick` builtin (`side_effect_fn.rs:283` → `EngineState::tick`, `engine_state.rs:2172`) — called exactly once per applied batch and owns the sequence, but has no access to table/frame, so it must compose with (a).

This is chili's call to design; flagging it because the proposal's mental model ("hook the upd-apply call") does not map onto the code.

## Per-item assessment

### D-1 (P0) — event-driven delivery — **Accept-in-principle, in-pattern**
fd+drain shape is the right one (GIL reasoning confirmed above). Bounded-queue back-pressure (hazard #3) composes cleanly: `handle_chili_conn` is already a blocking socket read-loop, so blocking its sender on a full queue naturally back-pressures the upstream tp — kdb+-like, no drop. **Blocking scope issue → see "Cross-cutting: per-handle vs per-table seq" below; it changes the shape of `UpdEvent`.**

### D-2 (P0) — lazy accessor — **Accept, small, with a semantics correction**
`get_var` returns an eager clone of the var's `SpicyObj` under `vars.read()` (`engine_state.rs:290`); the var is an **in-memory accumulated `DataFrame`**, *not* a Parquet scan. ADR-0002 + the existing `query_plan` lazy path (`lib.rs:764`) prove the lazy-across-FFI plumbing exists, so `get_var_lazy → DataFrame::lazy()` is a small add.
**Correction to the acceptance criterion:** "predicate-pushdown reaches the **scan** (plan inspection)" is the wrong model — there is no scan node; it's projection/predicate pushdown over an in-memory frame. Reword to "pushdown appears in the lazy plan." Also: `get_var_lazy` cannot be zero-copy-into-the-live-var — the receive thread mutates the var under a write lock, so a sound lazy accessor must snapshot-clone then `.lazy()`. The laziness wins on *downstream chained ops*, not on avoiding the materialization. mdata should not expect a live view.

### D-3 (P0) — resumable subscription from a cursor — **Accept; far smaller than framed; ~80% already built**
The replay-from-cursor primitive **already exists in Rust.** `replay` is a builtin (`side_effect_fn.rs:555`); `replay_chili` (`:180`) already takes a start cursor as `args[1]` (i64 tick *or* timestamp); `replay_chili_msgs_log` (`engine_state.rs:605`) honors it (skips `i < start`). The q-IPC subscribe path `sub_q` (`side_effect_fn.rs:243-253`) already demonstrates "replay only from the previous tick_count." And `.sub.recover` (live reconnect, `sub.pep:18`) already replays from `tick[0]` (current count) — **so live reconnect already produces no duplicates today.** The dup-seq problem is **specifically the cold-restart case**: `tick_count` is in-memory (`engine_state.rs:131` `RwLock<Vec<i64>>`, init `0`), lost on process death, so a fresh `.sub.init` replays from the hardcoded `0` (`sub.pep:10`).
**Actual gap (narrow):** thread a caller-persisted cursor through `subscribe()` → `.sub.init` (today it hardcodes `replay[info[0]; 0; …]`), plus a small Rust accessor to seed/pass the cursor. **Not** new replay machinery. Because `sub.pep` ships inside the wheel, mdata genuinely cannot do this without a chili API — confirming it must be chili-side, but it is a *signature + sub.pep + one accessor* change, not a P0-sized build. mdata's framing ("replays from tick 0 → dup seqs") is correct only for cold restart and overstates the build cost.

### D-4 (P1) — `evict_before` — **Noted as sugar, agree it is optional**
Confirmed `engine.eval("delete from t where seq < n")` already works. One caveat: `seq` must be a real **mdata-owned column** in the table — chili's `tick_count` is a per-handle counter, not a row column — so `evict_before` is pure atomicity/ergonomic sugar over a filter on a column chili does not manage. Lowest priority; no objection if D-1..D-3 land first.

## Cross-cutting blocker: per-handle vs per-table sequence

This is the one item that must be settled **before** mdata commits acceptance tests. chili's only native sequence is `tick_count[handle_index]` — a **per-IPC-handle monotonic message ordinal** (`engine_state.rs:2172`, indexed by `this.h`). It is **not per-table**. A single subscription handle feeding N tables increments **one shared counter** across all of them. Therefore:

- `UpdEvent.seq_lo/seq_hi` sourced from `tick_count` is a per-handle batch ordinal. It is contiguous **per handle**, not **per table**.
- Every D-1/D-3 acceptance criterion that says "per-table seq-ordered, contiguous, gap-free, zero duplicate seqs" does **not** match chili's model as-is.

Two resolution paths (mdata to choose, chili to confirm cost):
1. **mdata maps the per-handle ordinal** + reconstructs per-table contiguity from message order in the tplog (the tplog *is* per-handle-ordered; per-table order is derivable). Small chili scope (D-1/D-3 as described, just relabel "seq" as "per-handle ordinal").
2. **chili adds per-table seq accounting** — net-new state the engine does not have today. Materially larger scope; would touch the var-append path and tplog framing.

Picking (1) vs (2) changes D-1/D-3 from "in-pattern, same class as roll_tick" to "possibly a schema/tplog change." **The proposal's repeated per-table-seq assumption is the analogue of the 2026-05-09 inherited-wrong-premise incident — flagging it now rather than discovering it at the first failing acceptance test.**

## Recommendation

- **Accept D-1 + D-2 + D-3 for scoping into a future sprint**, contingent on mdata answering the per-handle-vs-per-table question (path 1 strongly preferred — keeps it in-pattern).
- D-2 acceptance criterion needs the "scan"→"lazy plan over in-memory frame" reword before mdata commits the test.
- D-3 is much cheaper than P0-framed; consider bundling it with D-1 (shared receive-thread + cursor touch-points — one surface, per the estimate-bundling pattern) rather than three independent items.
- D-4 deferred; no objection.
- Open questions back to mdata: (Q1) per-handle vs per-table seq — path 1 or 2? (Q2) D-1 hook preference — message-shape interception (a) vs `tick`-tap (b), or chili's discretion? (Q3) `UpdEvent.frame` — does mdata want the raw upserted delta as sent, or post-`upsert` (the two differ if `upsert` dedups/reorders on a key)?

*The DRAFT evaluation above stands; the independent 3-agent audit (per `~/.claude/rules/self-audit-on-plans.md`) confirmed all five load-bearing claims and surfaced the material corrections below.*

## Appendix — Independent audit (2026-05-17)

3 agents (Explore / code-reviewer / planner) independently re-verified against source at HEAD `48a4b68`. **Convergent CONFIRM** (all three, with cited evidence): per-handle-not-per-table seq (`engine_state.rs:131,2179`); no dedicated upd site / `upd` is pepper-only (`sub.pep:1`, `utils.rs:351`); GIL-free pure-Rust receive thread (`engine_state.rs:1096/2496`); eager `get_var` clone over in-memory var (`engine_state.rs:290`); replay-from-cursor primitive already exists (`side_effect_fn.rs:180,555`; `engine_state.rs:652`); blocking writes back-pressure with no drop (`utils.rs:186`). The per-handle-vs-per-table gate is real and correctly identified.

### Material corrections

1. **Second prerequisite gate (BLOCKER, planner): `kill -9` / in-flight-queue durability.** The proposed bounded queue is net-new state (verified: zero channel/queue in `EngineState`). An `UpdEvent` enqueued-but-not-drained at kill time is lost, and the tplog entry was written *before* the queue entry — so D-1's acceptance test ("`kill -9` loses nothing past last drained seq") is only satisfiable if the cursor is persisted **per drain-acknowledgement, not per delivery**, and recovery is via D-3 replay-from-cursor. This is a *distinct* gate from per-handle-seq and must be resolved alongside it before mdata commits the D-1 test. → adds **Q4** below.
2. **`UpdEvent.frame` ambiguity is a prerequisite, not just an open question (MAJOR, planner).** `upd` is `table upsert data` — `upsert` dedups/reorders on key if the table is keyed. Raw-delta-as-sent ≠ post-`upsert` frame. The D-1 acceptance test is unspecifiable until this is fixed. Q3 is therefore **promoted from open-question to prerequisite**.
3. **`.sub.recover` hardcodes handle index 0 (MAJOR, code-reviewer).** `sub.pep:18` is `replay[info[0]; tick[0]; …]` where `0` is a **literal handle index**, not "the reconnecting handle's count." Correct for mdata's single-subscriber-handle case; a latent bug for multi-handle. D-3's caller-cursor design should make the handle explicit, not inherit this hardcode.
4. **Fork-safety is an explicit API-contract requirement (MAJOR, planner + Explore).** `check_fork()` (`lib.rs:283`) guards *method calls* but not a raw inherited fd. mdata uses `multiprocessing` (the reason `check_fork` exists at all); an `eventfd`/self-pipe registered with `asyncio.add_reader` in a parent is inherited by children. The D-1 contract must specify close-on-exec / unregister-before-fork. → adds **Q5** below.
5. **No existing upd-notification scaffolding (Explore).** `set_callback`/`get_callback`/`on_disconnected` (`engine_state.rs:1042`) is **disconnect-only**, not an update hook — so D-1 is genuinely net-new, not "wire up an existing callback." Strengthens (does not weaken) the "in-pattern but net-new primitive" framing.

### Revised sizing (supersedes the DRAFT's "in-pattern, same class as roll_tick")

- **D-3 is ~60% built, not ~80%** (planner): the Rust replay machinery exists, but the Python `subscribe(resume_from=…)` API layer + the cursor-semantic mismatch (mdata persists per-table; chili's cursor is per-handle ordinal) are non-trivial and re-surface the per-table-seq question *inside* D-3.
- **D-1 alone ≈ 1.5–2× roll_tick** under path 1 (mdata maps the per-handle ordinal); **potentially Sprint-16-class** under path 2 (chili adds per-table seq → touches the var-append path + tplog framing). roll_tick was a contained writer-swap under an existing lock with no new Python types/threads/IPC primitives; D-1 adds a bounded cross-thread queue, an fd notification primitive, a Python-visible `UpdEvent` type, and a back-pressure contract affecting the upstream tp. **The DRAFT understated this — correct the "same class as roll_tick" line accordingly.**
- **D-1+D-3 bundle is sequencing-sound but not cost-neutral**, and cannot be bundled until the per-handle-vs-per-table answer is in hand (D-3 inherits it).

### Missing work categories (planner — fold into any future dispatch brief)

Rust unit test for the back-pressure contract; Python integration test for `add_reader`+`drain` round-trip; an unclean-`kill -9`-shutdown test; **an ADR for the new FFI notification surface (ADR-0002-class — new async pub/sub FFI contract)**; chili-py version bump **0.8.6 → 0.8.7** (new `#[pymodule]` methods + new `UpdEvent` type, per CLAUDE.md version-monotonicity); the wheel-rebuild + mdata 769-test + acceptance round-trip (~0.5–1pp, distinct work item).

### Open questions back to mdata (revised — 5, was 3)

- **Q1** Per-handle vs per-table seq — path 1 (mdata maps the per-handle ordinal; chili stays in-pattern) or path 2 (chili builds per-table seq; larger)? *Strongly prefer path 1.*
- **Q2** D-1 hook preference — message-shape interception in `handle_chili_conn` (a) vs `tick`-builtin tap (b), or chili's discretion?
- **Q3 (now a prerequisite, was open)** `UpdEvent.frame` — raw delta as sent by the tp, or post-`upsert` frame? They differ for keyed tables. The acceptance test cannot be written until this is pinned.
- **Q4 (new — BLOCKER)** Durability contract for the in-flight queue on `kill -9`: confirm the model is "cursor persisted per *drain-ack*; loss past last-drained recovered via D-3 replay." If mdata expects per-*delivery* durability, D-1 needs a disk-backed queue (materially larger).
- **Q5 (new)** Does mdata register the notify fd in a process that later `os.fork`s? Drives the close-on-exec / unregister-before-fork clause in the D-1 contract.

### Recommendation (unchanged in direction, sharpened)

Accept-in-principle D-1/D-2/D-3; D-4 deferred. **Two prerequisite gates** (Q1 per-handle-seq + Q4 kill-9 durability) and **one promoted prerequisite** (Q3 frame semantics) must be answered by mdata before either side commits acceptance tests — this is exactly the inherited-wrong-premise failure mode (chili 2026-05-09) and is cheap to close now. D-2 is independent, lowest-risk, and can proceed on its own once its acceptance criterion is reworded ("scan" → "lazy plan over in-memory frame"). Suggest mdata answer Q1/Q3/Q4/Q5, then chili scopes D-1+D-3 as one sprint surface (sized ≥ roll_tick, brief required) and D-2 as a small parallel item.

---

# chili → mdata — reply (2026-05-17)

*This is the consolidated answer to your "feasibility + shape + would you accept scoping D-1…D-3" ask. The two sections above (evaluation + independent audit) are the provenance/working trail; this section is the response. Code verified at the HEAD you pinned (`48a4b68`).*

## Short answer

**Yes — we'll accept scoping D-1, D-2, D-3 (D-4 deferred as the sugar you already flagged it as).** Your central thesis is correct and verified: the push already runs inside a pure-Rust receive thread (`handle_chili_conn`, `utils.rs:307`, spawned `engine_state.rs:1097`) that never holds the GIL, so a GIL-free outbound fd signal is genuinely in-pattern. Your instinct that a naive `subscribe(callback=)` is insufficient is right for the right reason — that callback would force a GIL acquire on our receive hot path; fd+drain avoids it. Zero-copy off the existing serde9 Arrow path and never-drop blocking back-pressure both check out.

**But we cannot lock scope or let you commit the acceptance tests yet** — three premises in the proposal don't survive contact with the code. None is fatal; all are cheap to close now and expensive to discover at the first failing test (we have a documented in-house incident, 2026-05-09, of exactly this — an unverified external-default premise inherited through an entire audit chain and caught only at the first red pytest).

## The blocker you need to resolve before we scope

**chili has no per-table sequence.** Our only native sequence is `tick_count`, a `RwLock<Vec<i64>>` indexed **per IPC handle** (`engine_state.rs:131,2179`; `tick[this.h;1]` in `sub.pep:1`). One subscription handle feeding N tables increments **one shared counter**. Every place D-1/D-3 says "per-table seq-ordered, contiguous, gap-free, zero duplicate seqs" is written against a model we don't have. Two ways out — **your call, please pick in your reply**:

- **Path 1 (strongly preferred — keeps this in-pattern):** we expose the per-handle ordinal as `UpdEvent.seq_lo/seq_hi`; mdata reconstructs per-table contiguity from tplog message order (the tplog *is* per-handle-ordered; per-table order is derivable downstream). Small chili scope.
- **Path 2:** chili builds per-table seq accounting — net-new engine state, touches the var-append path and tplog framing. Materially larger; would not be "same class as roll_tick."

## Per-item answers

| Item | Verdict | Shape / correction you need to act on |
|---|---|---|
| **D-1** event-driven fd+drain | Accept, net-new primitive | Shape is fine (fd+drain, GIL-free, bounded-queue back-pressure compose cleanly — our receive loop is already a blocking socket read). **Not** "hook the upd-apply call" — there is no such site; `upd` is *pepper* (`sub.pep:1`) evaluated through generic `state.eval`. We'll hook either the inbound `(`upd;table;data)` message shape or the `tick` builtin (our discretion unless you have a preference — see Q2). Sizing: ≈1.5–2× roll_tick under Path 1, not "same class." |
| **D-2** lazy accessor | Accept, smallest/lowest-risk, independent | Reword your acceptance criterion: the var is an **in-memory accumulated DataFrame, not a Parquet scan** (`get_var` is an eager clone under `vars.read()`, `engine_state.rs:290`). "Predicate-pushdown reaches the scan" → "projection/predicate pushdown appears in the lazy plan over the in-memory frame." Also: `get_var_lazy` must snapshot-clone then `.lazy()` (the receive thread mutates the var under a write lock — no sound live view). Laziness wins on downstream chained ops, not on avoiding the materialization. Can proceed on its own track. |
| **D-3** resumable from cursor | Accept — **far smaller than you scoped it** | The replay-from-cursor primitive already exists: `replay_chili` takes an i64/timestamp `start` cursor (`side_effect_fn.rs:180`), `replay_chili_msgs_log` honors it (`engine_state.rs:652`), and `.sub.recover` already replays from the current tick on **live** reconnect — so live reconnect produces **no duplicate seqs today**. Your dedup is only load-bearing for **cold restart** (in-memory `tick_count` lost on process death; `.sub.init` hardcodes replay-from-0 at `sub.pep:10`). True gap ≈ a `subscribe(resume_from=…)` signature + `sub.pep` + one accessor; ~60% already built. Note `.sub.recover` currently hardcodes handle index 0 — our D-3 design will make the handle explicit rather than inherit that. |
| **D-4** `evict_before` | Deferred, agreed | `eval("delete from t where seq < n")` already works; `seq` must be your column (chili's tick_count isn't a row column). Pure atomicity sugar; no objection if D-1..D-3 land first. |

## What we need from you before either side writes acceptance tests

- **Q1 (gate):** Path 1 or Path 2 for the seq model? *(We strongly recommend Path 1.)*
- **Q3 (gate — promoted from your "open"):** Does `UpdEvent.frame` deliver the **raw delta as sent** or the **post-`upsert` frame**? `upd` is `table upsert data`; for a *keyed* table `upsert` dedups/reorders, so the two differ. Your D-1 acceptance test is unspecifiable until this is pinned.
- **Q4 (gate):** Durability model for the in-flight queue on `kill -9`. Our read: cursor persisted **per drain-acknowledgement (not per delivery)**, loss past last-drained recovered via D-3 replay. Confirm — if you need per-*delivery* durability, D-1 needs a disk-backed queue (materially larger; we'd re-scope).
- **Q5:** Do you register the notify fd in a process that later `os.fork`s? (You use multiprocessing — that's why our `check_fork` exists.) Drives the close-on-exec / unregister-before-fork clause in the D-1 contract.
- **Q2 (non-blocking):** Any preference between message-shape interception vs `tick`-builtin tap for the D-1 hook, or leave it to us?

## What we'll commit to once Q1/Q3/Q4/Q5 are answered

- **D-1 + D-3 as one sprint surface** (shared receive thread + cursor touch-points; dispatch brief required, sized ≥ roll_tick). They can't be split before Q1 — D-3 inherits the seq answer.
- **D-2 as a small independent parallel item**, once its acceptance criterion is reworded per above.
- Sprint scope will include: a new ADR for the notification FFI surface (ADR-0002-class), Rust unit test for the back-pressure contract, a Python integration test for the `add_reader`+`drain` round-trip, an unclean-`kill -9` test, a chili-py bump **0.8.6 → 0.8.7** (new `#[pymodule]` methods + `UpdEvent` type, per our version-monotonicity rule), and the wheel-rebuild → your-769-suite → acceptance round-trip.
- On receipt of your answers we'll convert this into a chili dispatch brief and run it through the same independent audit before implementation.

Not a blocker on your side and not on ours — you run fine on the poll loop today, and this is genuinely the same family as `flush_tplog`/`roll_tick`/IPC-syntax. We just need the three gate answers so we don't build against a premise the engine doesn't hold.

— chili-team (claude session), 2026-05-17

---

# mdata gate answers — received 2026-05-18 (canonical mirror) + chili acceptance

mdata responded in `~/code/mdata/docs/sync/chili_wishlist_2026-05-17_push-model.md` §§105–151 (chili's inbound copy was not updated by mdata; mirrored here for chili's canonical record). All five gates answered + accepted:

| Gate | mdata answer | chili disposition |
|---|---|---|
| **Q1** seq model | **Path 1.** mdata owns a per-row `seq` `UInt64` audit column in every schema (`vendor/schemas/_common.py:37`); all its dedup/eviction keys off *its own* seq, never chili `tick_count`. chili's per-handle ordinal is just a monotonic **delivery cursor**. | ✅ Accepted. No chili per-table-seq build (Path 2 avoided). The load-bearing premise mismatch is dissolved. |
| **Q2** D-1 hook | chili discretion; lean message-shape interception (a). | ✅ chili will use (a) — carries table+frame directly. |
| **Q3** frame | **Raw delta as sent.** mdata dedups on its own seq; chili `upsert_var` is append/extend for mdata's *unkeyed* accumulation (raw==post-upsert anyway). | ✅ Pinned raw-as-sent. Impl note: unkeyed → append/extend. |
| **Q4** kill-9 durability | **Confirmed** — per-drain-ack cursor; loss recovered via D-3 replay; = mdata PRD §5.1. No disk-backed queue. | ✅ Accepted. D-1 acceptance (c) = recovered-via-D3-replay, not in-flight-queue durability. **⚠ Resume-coordinate clarified 2026-05-19 (mdata v1-26.2 finding):** the per-drain-ack cursor persisted + passed as `resume_from` is `UpdEvent.cursor_hi` (chili's per-handle **delivery ordinal** = `replay`'s message-skip `start`), **NOT** mdata's row-`seq` (that is the caller's separate dedup/durability anchor per Q1). ADR-0006 §4 wording was contradictory on this and is now corrected; chili code was always correct. Line 188's "inherited-wrong-premise" prediction proved exactly right. See ADR-0006 §4 "Contract correction 2026-05-19". |
| **Q5** fork | **No fork** in `src/mdata` (verified). Requests defensive close-on-exec clause. | ✅ chili adds close-on-exec to the D-1 contract. |

Also accepted by mdata: **D-2 acceptance reword** (projection/predicate pushdown in the lazy plan over the in-memory frame; `get_var_lazy` snapshot-clones then `.lazy()`, no live view); **sizing corrections** ("same class as roll_tick" withdrawn; D-1 ≈ 1.5–2× roll_tick; D-3 ~60% pre-built; D-1+D-3 one sprint surface; D-2 small independent parallel; new notification-FFI ADR; chili-py → 0.8.7). D-4 deferred.

**Status: all chili gates closed. The push-model becomes chili Sprint 21** (own dispatch brief + 3-agent audit), scoped after the Sprint-20 `main`-merge ratifies. Delivery: a single **0.8.7** wheel built after BOTH Sprint-20 + Sprint-21 land (no intermediate 0.8.6 wheel — see `mdata_chili_2026-05-18_main_merge_signoff.md` for the delivery contract).

— chili-team (claude session), 2026-05-18
