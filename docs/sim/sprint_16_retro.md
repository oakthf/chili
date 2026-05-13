# Sprint 16 retro — mdata wishlist v1 bundle (P0 + P3 + P2)

**Wrap:** 2026-05-13
**Predicted:** 10.7–18.2 pp (post-audit lock-in band; original audit 10.5–21.5)
**Actual:** _TBD on wrap commit_
**Variance:** _TBD_
**Owner:** coordinator-solo (no subagent fan-out; Part C lead changed from `tester` to coordinator-solo per audit NIT)
**Plan reference:** `docs/history/sprints/sprint_16_dispatch_brief_2026-05-13.md`

---

## Scope shipped

### Part A — `engine.flush_tplog()` (mdata P0)

- `trait ReadWrite` extended with `sync_all` default no-op + `Box<T>` forwarding impl + explicit `impl ReadWrite for fs::File` + `impl ReadWrite for TcpStream {}`; blanket impl removed (Option β chosen — single-line struct change to `Handle` for `bytes_since_flush: AtomicU64` only; `Handle.rw` shape unchanged).
- `EngineState::flush_handle(h) -> SpicyResult<i64>` (returns bytes-since-last-flush as i64, errors for non-file:// conn types).
- `bytes_since_flush` write-tracking wired into `sync()` New / File / Sequence branches (the three file:// conn types).
- `EngineStatePy::flush_tplog()` PyO3 binding (looks up `.tick.msgHandle`, calls flush_handle, GIL released around the fsync syscall).
- `ChiliEngine.flush_tplog()` Python wrapper.
- Tests: 1 Rust integration (`crates/chili-core/tests/flush_handle_test.rs`, 2 cases) + 3 chili-py pytest (`tests/test_tplog_flush.py`).

### Part B — `engine.add_at_time()` (mdata P3)

- `EngineStatePy::add_at_time(fn_name, start_time, description)` PyO3 binding (passes through to `.job.addAtTime` via fn_call).
- Local-offset adjustment in PyO3 binding (Python tz-aware datetime → UTC ns; chili scheduler compares against local-wall-clock-as-UTC-ns; offset added).
- `ChiliEngine.add_at_time(fn_name, start_time, description="")` Python wrapper.
- **Pre-existing chili bug fix:** `crates/chili-core/src/job.rs:96` `add_at_time` set `next_run_time: 0` causing jobs to fire on the very next poll instead of at `start_time`. Changed to `next_run_time: start_time`; one-shot semantics preserved by `interval: 0` + `end_time == start_time`.
- Tests: 4 chili-py pytest (`tests/test_add_at_time.py`).

### Part C — `::` null-literal disambiguation (mdata P2)

- `crates/chili-parser/src/expr.rs` — added `Op("::")` → `Expr::Nil(span)` as an `.or` branch on the `lit` production. Applied to both `parser_chili` and `parser_pepper`.
- Tests: 4 chili-py pytest (`tests/test_pepper_syntax.py`) — includes the exact mdata wishlist form `.sub.eod.fired: ::; eod: {[msg] .sub.eod.fired: msg};`.

### Wheel cut + handoff

- chili-py version 0.8.3 → 0.8.4 (`Cargo.toml` + `pyproject.toml`).
- `dist/chili_sauce-0.8.4-cp310-abi3-macosx_11_0_arm64.whl` built (sha256 TBD on wrap commit).
- Handoff doc: `docs/sync/mdata_chili_2026-05-13_0.8.4_delivery.md`.

### Test count delta

- Rust: 170 → 172 (+2 from `flush_handle_test.rs`)
- chili-py pytest: 72 → 83 (+11; 3 P0 + 4 P3 + 4 P2)

---

## Lessons (durable)

### 1. mdata-wishlist-driven sprint cadence catches premise drift early

**Rule.** For sprint scope driven by an external project's wishlist, dispatch a `verify-before-claim` pass + parallel 3-agent audit BEFORE drafting the brief, then a clarification-question response BEFORE kickoff. This Sprint 16 used both gates and caught: P2 was diagnosed wrong in mdata's text (general `;` not the bug; `::` ambiguity was), P1 publish_remote scope reversed (mdata changed preference once chili showed the pp tradeoff), P0 par_df-vs-tplog scope clarified (par_df is wdb's responsibility, not chili's), and P3 had a hidden chili-side bug (`next_run_time: 0`) that only surfaced when the binding's pytest exercised the scheduler.

**Why.** Sprint 16's pre-audit pp band was 10.5–21.5. Post-mdata-clarification it tightened to 10.7–18.2 (Part C dropped from 1–6 to 2–4; Sprint 17 P1-publish dropped from 15–25 to ~8). Without the clarification pass, Sprint 17 would've been ~10pp larger than necessary. The audit-then-clarify cadence is structurally cheaper than letting the impl run discover the drift.

**Apply where.** Any sprint sourced from `docs/sync/<wishlist>.md` or `~/code/<downstream>/docs/sync/`. Mandatory if the wishlist is ≥ 4 items or names ≥ 5 code artifacts.

**Cost saved.** Sprint 16 saved ~3pp (Part C scope tightening + audit-flagged trait-coherence fix). Sprint 17 will save ~10pp on P1-publish (Option B vs A reversal). Total ~13pp avoided over 2 sprints.

### 2. Test-driven discovery of chili-time convention bug

**Rule.** When binding a Rust function to Python, write a fires-end-to-end pytest that exercises the FULL scheduler/thread/IPC stack — not just the syntactic surface. Drift between Python wall-clock semantics and chili's internal time model surfaces only at execution, not at API-surface level.

**Why.** Part B's `test_fires_near_target_time` revealed TWO bugs in chili-core that compile + clippy + the "does the binding return a job_id?" tests all missed: (a) `next_run_time: 0` made jobs fire immediately on first poll regardless of `start_time`, and (b) chili's `get_local_now_ns()` returns local-wall-clock-as-UTC-ns while pyo3-chrono's `DateTime<Utc>` returns true UTC ns — a timezone-offset gap that meant scheduled jobs never fired in any non-UTC timezone. Only the e2e pytest caught both.

**Apply where.** Any Python-binding sprint that exposes chili's internal scheduler, async, or time-dependent surfaces.

**Cost saved.** Caught two latent bugs in Sprint 16 instead of Sprint 17+. Both would have surfaced in mdata's acceptance test on receipt of the 0.8.4 wheel; catching them here avoided a wheel re-cut cycle.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| K1+K2 pre-kickoff gates | 0.2 | ~0.2 (both verified, /tmp clone re-cloned, rustc 1.95 verified) |
| Part A trait coherence design + impl + tests | 5–8 | _TBD_ |
| Part B add_at_time + 2 chili-side bug fixes + tests | 2–3 | _TBD (likely 3–4 with the bug fixes)_ |
| Part C `::` disambiguation + tests | 2–4 | _TBD (likely ~2; single-line + 4 tests)_ |
| 0.8.4 wheel cut + handoff doc | 0.5–1 | _TBD_ |
| Wrap + retro + housekeeping | 1–2 | _TBD_ |
| **Total** | **10.7–18.2** | _TBD on wrap commit_ |

---

## What surprised

- **The trait-coherence audit finding turned out to be the easy part of Part A.** The trait surgery itself was ~10 LOC; the harder design choice was where to track `bytes_since_flush` (per-handle AtomicU64 with write-tracking in each `sync()` branch). Option α's enum approach was tempting from the audit perspective but actually more invasive than Option β.
- **Part B uncovered TWO pre-existing chili bugs** that nobody noticed because Python callers haven't exercised the scheduler API end-to-end before. The wishlist's "just expose the binding" framing understated the work; the actual binding requires `next_run_time` fix + tz-offset conversion. Net: still ~3pp, but for different reasons than the brief predicted.
- **Part C was a 1-line parser change.** The mdata Q2 clarification narrowed the scope correctly. The audit finding "wrong fix site at line 559" was right — the actual change was in the literal-production area at line 122/583. Both `parser_chili` and `parser_pepper` were edited to keep them parallel.
- **`Box<dyn ReadWrite>` doesn't satisfy `ReadWrite` without an explicit `impl<T: ReadWrite + ?Sized> ReadWrite for Box<T>`** when the blanket impl is removed. Caught at first compile; one-line fix.
- **macOS `/tmp` survived to kickoff.** Per Sprint 15's incident, every fresh session should K1-gate the polars fork before doing anything. The CLAUDE.md elevation + `vendor/polars-core/` reference patches paid off — recovery path is now documented.

---

## Cross-references

- Plan: `docs/history/sprints/sprint_16_dispatch_brief_2026-05-13.md` (will be moved post-ratification)
- mdata wishlist source: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md`
- mdata reply lock-in: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`
- chili response draft (untracked): `docs/sync/chili_wishlist_2026-05-13_response_draft.md`
- 0.8.4 delivery doc: `docs/sync/mdata_chili_2026-05-13_0.8.4_delivery.md`
- Pre-existing chili bug fix in `job.rs:96` — `next_run_time: 0` → `next_run_time: start_time`; future ports of `.job.addAtTime` should preserve this fix.
- Pre-existing chili-time convention quirk: `get_local_now_ns()` returns local-wall-clock-as-UTC-ns. Worked around in chili-py's `add_at_time` PyO3 binding; a future Sprint 18+ could refactor the chili-core scheduler to use UTC ns natively (would need to confirm no pepper-callers depend on the local-as-UTC convention).
