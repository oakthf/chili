# ADR-0006 — Async upd-notification FFI contract (mdata push-model D-1/D-3)

**Date:** 2026-05-18 (Sprint 21).
**Status:** **Accepted** (committed before implementation, per Sprint-21 audit BLOCKER-2 — the queue capacity, `UpdEvent` schema, and back-pressure/escalation contract must be fixed before impl + acceptance tests). **Implemented + gate-green Sprint-21 commit `4c3fe0c` (Rust 201 / pytest 97, 0 failed); awaiting user ratification.** Impl-time corrections to this ADR are inline (crossbeam version §3; deps-in-chili-core §Consequences; cross-thread no-deadlock review §Consequences). Field-naming resolved: `cursor_lo`/`cursor_hi` (per-handle delivery ordinal), NOT `seq_*` (audit#2 C6).
**Cutover:** None for existing surface — purely additive (`upd_notify_fd`/`drain_upds`/`UpdEvent`/`subscribe(resume_from=)`/`get_var_lazy`). No on-disk or wire-format change. tplog remains the source of truth.
**Supersedes:** None.
**Related:** `docs/history/sync/mdata_push_model_proposal_2026-05-17.md` (evaluation + 3-agent audit + mirrored mdata Q1–Q5; swept to history 2026-05-19 — satisfied by this ADR + 0.8.7 delivery); ADR-0001 (pub/sub canonical — this is the *subscriber-side delivery notification*, not a new pub/sub channel); ADR-0002/0003 (true-lazy — `get_var_lazy` reuses, unaffected); Sprint-21 dispatch brief + audit appendix.

---

## Context

mdata's rdb/wdb subscribers poll `engine.get_var(table)` every ~10 ms + diff a `_last_seen_seq` watermark + keep a parallel buffer — solely because chili's pure-Rust IPC receive thread (`handle_chili_conn`, `utils.rs:344`) applies `upd` but never tells Python. This ADR fixes the contract for an outbound GIL-free notification so that workaround layer is deletable. mdata's gate answers are locked (Q1 Path-1, Q2 (a), Q3 raw-as-sent, Q4 per-drain-ack, Q5 no-fork+close-on-exec).

## Decision

### 1. Notification primitive — POSIX self-pipe (NOT `eventfd`)

`eventfd(2)` is Linux-only; chili dev + the delivered wheel target macOS (darwin). The notification fd is a **POSIX self-pipe** (`libc::pipe` + `O_NONBLOCK` on the read end), portable to linux+darwin. `libc` is added to `chili-py` deps. Step-0 POC (2026-05-18) confirmed self-pipe + `asyncio.loop.add_reader` + `FD_CLOEXEC` works on darwin. A `#[cfg(target_os="linux")] eventfd` fast-path is explicitly **out of scope** (self-pipe is sufficient; one byte per wakeup, coalesced).

Both pipe ends are created **`FD_CLOEXEC`** (Q5: mdata uses `multiprocessing`; the fd must not survive `exec`/leak into children). `check_fork()` (`lib.rs:288`) still guards method calls; the close-on-exec is the defensive belt mdata requested. Contract: **the notify fd must not be used across `os.fork` without re-creation** — committed-tested.

### 2. `UpdEvent` schema (Python-visible `#[pyclass]`)

```
UpdEvent { table: str, cursor_lo: int, cursor_hi: int, frame: pl.DataFrame }
```

- `cursor_lo`/`cursor_hi` = chili's **per-handle `tick_count` delivery ordinal** (value before/after the batch's `tick[this.h;1]`). **NOT** mdata's per-row `seq`. Q1 Path-1: per-table contiguity is the caller's own `seq` column; chili's cursor is only a monotonic delivery position. Field names are deliberately `cursor_*` (not `seq_*`) to prevent the documented seq-collision confusion (Sprint-21 audit#2 C6).
- `frame` = the **raw delta as sent by the tp** (Q3), delivered as a Polars `DataFrame` Arc-shallow-clone of the inbound `serde9` MixedList payload — **no re-serialize / no re-decode** (not literally zero-copy: `DataFrame::clone()` is a shallow Arc-clone of column buffers). mdata's accumulation is unkeyed so raw == post-`upsert`; pinned raw-as-sent to keep mdata's `seq` authoritative.

### 3. Bounded queue + back-pressure

- Bounded **`crossbeam_channel::bounded(N)`** with **`N = 4096`**. `N` is a fixed constant this sprint; tunability is a future-sprint concern (documented, not built). **Version correction (Sprint-21 impl, verify-before-claim):** the brief audit + an earlier draft of this ADR said "crossbeam 0.8.4 already in Cargo.lock" — wrong (corrected from 0.8.4 to **0.5.15** per `grep -A1 '^name = "crossbeam-channel"' Cargo.lock` → `version = "0.5.15"`). `libc` is likewise present transitively at `0.2.186`. Both are promoted transitive→direct in `crates/chili-core/Cargo.toml` (`crossbeam-channel = "0.5"`, `libc = "0.2"`) — no `Cargo.lock` churn. The 0.5 API used here (`bounded`, blocking `send`, `try_recv`) is identical to 0.8; the design is version-independent.
- The IPC receive thread (`handle_chili_conn`, a dedicated `std::thread`) does **enqueue + 1-byte self-pipe `write` only**, GIL-free (no `Py`/`Python::with_gil` anywhere on this path — committed-asserted). `drain_upds()` runs on the Python caller thread (distinct thread) and takes the GIL only there.
- **Back-pressure = blocking send, never drop.** At capacity the receive thread **blocks** in `send`, which back-pressures the upstream tp's blocking write (kdb+-like). The tplog is the source of truth; Python catches up. There is **no drop path**.
- **Escalation contract (Sprint-21 audit new halt-trigger 2b):** if blocking back-pressure ever holds long enough that the upstream tp times out / drops the chili connection, that is a *contract* tension, not a code bug. Resolving it (adding a timeout/drop-mode) is a **user-sign-off decision** — it would weaken the tplog-is-source-of-truth invariant and must not be added silently. Until such a decision, blocking-never-drop stands; recovery from any disconnect is via §4 replay-from-cursor.

### 4. Resumable subscription (D-3) + cursor persistence

- `subscribe(tick_socket, topics, resume_from: dict[str,int] | None = None)`. The replay *mechanism* is pre-built (`replay_chili_msgs_log` `engine_state.rs:605` takes `start`, skips `i < start`).
- **Cursor storage model (audit MAJOR-3 — the genuinely-new surface):** the per-table resume cursor is held in **engine state** — a new `EngineState` field `resume_cursor: RwLock<HashMap<String,i64>>` (table → last-delivered cursor), seeded from `subscribe(resume_from=)`. `.sub.init` (sub.pep:10, currently `replay[info[0]; 0; …]`) and `.sub.recover` (sub.pep:18, currently `tick[0]`) both consult this map via a new accessor builtin rather than the hardcoded `0`/`tick[0]`. The reconnecting-handle index is passed explicitly (do not inherit `.sub.recover`'s hardcoded handle-0).
- **Resume coordinate (CONTRACT — read with §2-L29):** the value the caller persists and passes back as `resume_from[table]` is **chili's per-handle delivery ordinal — `UpdEvent.cursor_hi`** — NOT mdata's per-row `seq`. `cursor_hi` and `replay`'s `start` are the *same* coordinate (a tplog **message** ordinal): `cursor_hi = get_tick_count(handle)` (`utils.rs:457`, `tick_count` += 1 per applied `upd`) flows unchanged through `set_resume_cursor` → the `resume_cursor` map → `resume_start_for` → `replay`'s `start`, which skips by **message count** (`engine_state.rs:578 .take(start)`, `:688 i < start`). mdata's own row-`seq` is a *separate* anchor (dedup / eviction / bounded-over-replay de-overlap / durability) per §2-L29 and Q1 Path-1 — it is **never** passed as `resume_from`. Passing row-`seq` (monotone across rows; a 50-row batch = +50 seq but 1 message) as `resume_from` is misread as a 50-message skip → silent total loss.
- Q4 durability: "kill -9 loses nothing past last drained" = recovered via replay-from-cursor (resume from the last-drained `cursor_hi`), NOT in-flight-queue durability. No disk-backed queue. A subscriber resumes from the **post-cursor tail**; chili does not re-deliver pre-cursor messages (`i < start` skipped) — a recent-window consumer (e.g. an rdb cache) gets exactly the tail by design, while a full-history consumer recovers all rows by resuming from `cursor_hi = 0` (or its own cross-shard `seq` dedup over the full replay). Intended behaviour, not a gap.
- **Contract correction 2026-05-19 (mdata-found, v1-26.2 D-3 adoption; `chili_wishlist_2026-05-17_push-model.md` commit `919e880`).** The prior §4 line — *"the caller persists its durable position as mdata's own row-`seq`"* — was internally contradictory with §2-L29 and §4-L41 and, if followed, reintroduces silent total data loss. Corrected from "persist row-`seq` as the resume position" → "persist `UpdEvent.cursor_hi` as `resume_from`; row-`seq` is the caller's separate dedup/durability anchor" per first-hand verification: `grep -n "i < start\|take(start" engine_state.rs` → `:578 .take(start as usize)`, `:688 i < start` (message-skip), and `cursor_hi = state.get_tick_count(handle)` `utils.rs:457`. The chili **code was already correct**; only this normative line was wrong. The error was an inherited-wrong-premise from the Q4 gate answer that survived the brief, the 3-agent pre-impl audit, ADR drafting, implementation, and the committed D-3 guards (whose `resume_from` test values were message-ordinals by luck, never exercising the row-`seq` confusion) — caught only by mdata's first-hand empirical test on the delivered 0.8.7 wheel. See `~/.claude/rules/verify-before-claim.md` + `docs/standards/iteration_lessons.md`.

### 5. D-2 lazy accessor (independent)

`get_var_lazy(id) -> pl.LazyFrame` = snapshot-clone under `vars.read()` then `.lazy()` (sound: receive thread mutates under the write-lock; no live view). Acceptance: projection/predicate pushdown appears in the lazy plan over the in-memory frame (NOT "reaches the scan"); `.collect()` byte-identical to `get_var`.

## Consequences

- mdata deletes its poll-loop + `_last_seen_seq` dedup + dual-buffer; no safeguard relocates into pepper.
- New `EngineState` fields (`upd_notify`, `resume_cursor`) are `Send + Sync` and introduce no lock-order inversion. **Cross-thread review (Sprint-21 impl, committed):**
  - `Send + Sync`: `UpdNotify { tx/rx: crossbeam Sender/Receiver, pipe_r/w: RawFd }` — crossbeam channel ends are `Send + Sync`, `RawFd` is `i32` → `UpdNotify: Send + Sync`; `RwLock<Option<Arc<UpdNotify>>>` / `RwLock<HashMap<String,i64>>` likewise. Compiler-enforced (`EngineState` is used as `Arc<EngineState>` across the receive + Python threads; it compiles).
  - **Load-bearing no-deadlock invariant:** the IPC receive thread acquires `upd_notify.read()` *only* to clone the `Arc` out, then **drops the guard before** calling `enqueue`. So when the bounded queue is full the receive thread blocks in `crossbeam send` while holding **no `EngineState` RwLock** (`vars`/`tick_count`/`handle`/`upd_notify`/`resume_cursor` all released). `drain_upds` on the Python thread can therefore always make progress and drain the queue → back-pressure can never deadlock. The pre-eval cursor snapshot, `state.eval`, and the post-eval enqueue are **sequential, never nested** lock acquisitions.
  - `resume_cursor` is written only by `set_resume_cursors` (Python subscribe thread, *before* `.sub.init` eval) and read by the `resume_cursor` builtin during `.sub.init`; the lock is held alone and briefly, never nested under `vars`/`tick_count`/the Sprint-18 handle write-lock, and there is no reverse path that holds `resume_cursor` while acquiring those.
- `libc` + `crossbeam-channel` are added to **chili-core** (the pure-Rust self-pipe + bounded queue live there); **chili-py gains no new direct dependency** — it consumes the chili-core public API only. (Earlier ADR drafts said "chili-py gains a libc dependency" — corrected: the dep landed in `crates/chili-core/Cargo.toml`.)
- Delivered as a single **0.8.7** wheel cut from claude-2 HEAD *after* Sprint-21 ratifies (Sprint-20 G2 single-delivery model); no intermediate wheel.
- ADR-0001 unaffected (this is subscriber-side delivery notification, orthogonal to the pub/sub broadcast path).
