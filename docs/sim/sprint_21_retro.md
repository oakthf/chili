# Sprint 21 retro — mdata push-model D-1/D-2/D-3 (GIL-free upd notification + resumable cursor + lazy accessor)

**Wrap:** 2026-05-19 (gate-green; awaiting user ratification)
**Predicted:** 14–22 pp (mid ~18, post-3-agent-audit; pre-audit 10–18/14)
**Actual:** ~16–18 pp (best-effort; spans a `/compact` boundary so not cleanly rate-timeline-anchored — mid-band, no halt-trigger, greenfield so no auto-merge-cascade; one regression caught+fixed by the gate in <1pp)
**Variance:** ≈ mid-band (within the audit's revised 14–22 band; below the 22 upper edge because the GIL-free-signal + back-pressure design did not need iteration — halt-trigger-2/2b never fired)
**Owner:** coordinator-solo (load-bearing Rust thread/queue + FFI owned personally per brief; no impl subagent needed — D-2 was trivial inline) + the prior 3-agent pre-execution audit (folded into ADR-0006 + brief appendix at kickoff `4017674`)
**Plan reference:** `docs/history/sprints/sprint_21_dispatch_brief_2026-05-18.md` (moves post-ratification)

---

## Scope shipped

Implementation commit `4c3fe0c` (kickoff brief+ADR `4017674`):

- **D-1 — GIL-free outbound upd notification** (`4c3fe0c`): new `chili-core/src/upd_notify.rs` (POSIX self-pipe — macOS has no `eventfd`/`pipe2`, so `pipe`+`fcntl` with `O_NONBLOCK`+`FD_CLOEXEC` both ends — + bounded `crossbeam_channel(4096)`, blocking-send/never-drop). GIL-free message-shape interception in `handle_chili_conn` (`utils.rs`): post-successful-async-eval the borrowed `(`upd;table;data)` MixedList is still in scope → enqueue + 1-byte wakeup, zero pyo3 on the path (chili-core has **no** pyo3 dep — structural invariant, not a runtime assertion). `cursor_lo/hi` = per-handle `tick_count` snapshot before/after eval (read empirically, not assuming +1). PyO3 `UpdEvent` `#[pyclass]` + `upd_notify_fd()`/`drain_upds()` (drain pipe-then-queue, edge-safe) + engine.py wrappers.
- **D-3 — resumable subscription** (`4c3fe0c`): new `EngineState.resume_cursor` map + `resume_start_for` (min over subscribed topics; 0 ⇒ full replay — mdata owns per-row dedup via its own `seq`, Q1 Path-1) + `resume_cursor` pepper builtin + `subscribe(resume_from=)`/`set_resume_cursors` FFI + sub.pep `.sub.init`/`.sub.recover` rewired off the hardcoded `0`/latent `tick[0]`.
- **D-2 — lazy accessor** (`4c3fe0c`): `get_var_lazy()` — snapshot-clone under `vars.read()` then `.lazy()`; `.collect()` == `get_var`, pushdown over the in-memory frame.
- **ADR-0006** (`4c3fe0c`): impl-time corrections — crossbeam version `0.8.4`→`0.5.15` (verify-before-claim, `grep Cargo.lock`), `libc`/`crossbeam-channel` land in **chili-core** not chili-py, + a committed cross-thread lock-ordering review (no inversion; the receive thread holds **no** `EngineState` lock while blocked on the bounded send → back-pressure cannot deadlock).
- **Out of scope, as planned:** D-4 `evict_before` (mdata-deferred); version bump / wheel cut (post-ratification, Sprint-20 G2); `async for` sugar.

Tests: **+10** (3 Rust integration `upd_notify_test.rs`: back-pressure-blocks/never-drops+FIFO, fd signal→quiesce, `FD_CLOEXEC`+`O_NONBLOCK`; 7 Python `test_push_model.py`: D-1 real-IPC fd+drain round-trip, D-3 resume-skip + unclean-restart-recovery (Q4), D-2 lazy-pushdown+equivalence, Q5 close-on-exec-fork). Rust 198→**201**, pytest 90→**97**.

Bench delta: none — push-model does not touch the parse-cache / scan / eval hot paths (golden rule 6 untouched, confirmed by inspection: the enqueue is off the parse path entirely).

---

## Lessons (durable)

### 1. A tolerant shared extractor's strict variant is a regression trap when reused in a new always-on path

**Rule.** Before calling a shared "extract X from a pepper value" helper (`to_str_vec`, `to_i64`, …) on a **new always-executed** path, check every refutable arm of that helper against the *actual runtime value shape*, not the intuitive one. `to_str_vec` returns `Ok` for an *empty* `MixedList` but **errors** on a *non-empty* one — so `resume_cursor[topics]` hard-failed `.sub.init` for the common `["trade"]` subscribe, regressing 7 tests including 4 pre-existing subscriber tests. The fix was a path-local tolerant extractor (filter_map over the MixedList; `unwrap_or_default()` fallback), not a change to the widely-used shared helper (blast-radius discipline).
**Why.** Sprint-21: `resume_cursor` builtin is called on *every* subscribe (`.sub.init`), resume cursor or not. Inheriting `to_str_vec`'s strict non-empty-MixedList arm broke the no-`resume_from` path that must behave exactly like the old `replay[...;0;...]`. The gate caught it (7 pytest failures incl. `test_tick_sub`); a structural pre-impl read of `to_str_vec`'s arms would have caught it earlier.
**Apply where.** Any sprint that inserts a new builtin/accessor into an existing always-run pepper path (`.sub.*`, `.tick.*`, `upd`) and reuses a shared `SpicyObj` extractor.
**Cost saved.** ~1 pp this sprint (gate-caught, fixed in one edit); would have shipped a broken subscribe to mdata if the committed Python guards hadn't exercised the no-`resume_from` path. Promote candidate — single incident, but high-cost class (silently breaks the load-bearing subscriber path). Promote on confirmed recurrence; held here pending a 2nd occurrence per the promotion bar.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| (ADR + 3-agent audit — landed at kickoff `4017674`) | ~2 | (prior) |
| D-1: self-pipe + bounded queue + GIL-free signal in handle_chili_conn | ~4–6 | ~5 |
| D-1: PyO3 UpdEvent + upd_notify_fd/drain_upds + engine.py | ~2–3 | ~2 |
| D-3: subscribe(resume_from) + resume_cursor builtin + sub.pep + FFI | ~2–3 | ~3 (incl. the to_str_vec regression catch+fix <1) |
| D-2: get_var_lazy | ~1 | ~1 |
| Committed guards (3 Rust + 7 pytest) + cross-thread review | ~2–3 | ~4 (real-IPC guards + the regression cycle drove the count) |
| Part B docs (retro, cadence, CLAUDE.md, ADR finalize) | ~1.5 | ~1.5 |
| **Total** | **14–22 (mid ~18)** | **~16–18 (best-effort)** |

Mid-band. No halt-trigger fired (no scope-blow, no GIL/pepper-pivot — interception is provably GIL-free by construction; no back-pressure-deadlock — the no-lock-held-while-blocked invariant holds). The single deviation was the `to_str_vec`-on-non-empty-MixedList regression, caught by the committed pytest guards on the very first gate run and fixed in one localized edit (the verifier-must-run discipline working as intended — the guards exercised the no-`resume_from` path, not just the happy path).

---

## What surprised

- `to_str_vec` is asymmetric: `Ok(vec![])` for an *empty* MixedList but `Err` for a *non-empty* one. Counter-intuitive; the empty-list special case masked the general non-empty failure during design reasoning. (→ Lesson 1.)
- Replay does **not** flow through the D-1 hook: `replay_chili_msgs_log` calls `self.eval` directly, not `handle_chili_conn`, so subscribe-time replay applies upds *without* notification — only live post-subscribe publishes notify. This made the D-1 round-trip test *more* deterministic than feared (events == the live batch, not replay+live).
- The pre-existing `engine_state.rs:2562` `stream.shutdown().unwrap()` TCP-listener-teardown panic (a `NotConnected` race) is unrelated background noise; verified against the changed-surface set, not assumed (verify-before-claim).

---

## Cross-references

- Plan: `docs/history/sprints/sprint_21_dispatch_brief_2026-05-18.md` (post-ratification)
- ADR: `docs/decisions/0006-async-upd-notification-ffi.md` (Accepted; impl-finalized this sprint)
- Cadence metrics row: `docs/sim/cadence_metrics.md`
- mdata contract: `docs/sync/mdata_push_model_proposal_2026-05-17.md` (chili evaluation + 3-agent audit + mirrored Q1–Q5)
- Companion: `docs/sim/sprint_20_retro.md` (the merge that preserved the substrate this sprint built on)
