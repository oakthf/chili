# mdata ← chili 0.8.4 wheel delivery

**Date:** 2026-05-13
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.8.4-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `6e724eef6b526372d82b14fb2c7f6ae0eafb482e2067005f9ba79f3839451f87`
**Replaces:** 0.8.3 (sha256 `6345fcac6eb2e9905bed40d5839fdcf90d0c03bde98ee2e3cd615bc48e490c47`)

---

## TL;DR

Sprint 16 closes the three smallest items from the 2026-05-13 wishlist as
chili-sauce 0.8.4:

- **P0** `engine.flush_tplog()` — durability hook for PRD §5.1 part-2.
- **P3** `engine.add_at_time()` — Python binding for `.job.addAtTime`.
- **P2** Pepper `::` null-literal disambiguation — `x: ::; y: 1` parses now.

The two P1 items (`publish_via_handle` + subscriber `eod` dispatch) are
Sprint 17, drafting now per the Q3 + Q4 lock-ins. No timeline commitment
yet on Sprint 17 wheel.

mdata committed: flip 3 xfailed acceptance tests to strict-pass within 1
sprint of receipt + rerun stress benchmarks.

---

## Install

Replace your pinned wheel hash:

```bash
# uninstall the prior version
uv pip uninstall chili-sauce

# install the new wheel (pin the sha256 to detect re-builds)
uv pip install --no-deps \
  /Users/oakadmin/code/chili/dist/chili_sauce-0.8.4-cp310-abi3-macosx_11_0_arm64.whl
```

Update your `pyproject.toml` lockfile pin to the new sha256 above. Cross-
verify by running `shasum -a 256` on the wheel after install.

---

## API additions (full surface)

### 1. `engine.flush_tplog() -> int` (P0)

Flushes user-space buffers and `fsync`s the kernel buffers for the active
tplog handle (`.tick.msgHandle`, set by `init_tick` / `.tick.createLog`).
Returns payload bytes-since-last-flush as an `int` — replaces the
`log_path.stat().st_size` proxy with a precise monitor probe.

```python
tp.boot(today)
tp.publish("trades", df_50_rows)
bytes_flushed = engine.flush_tplog()   # returns positive int
# Now safe to os.kill(os.getpid(), SIGKILL) — tplog won't lose those rows.
```

Raises `RuntimeError` if `init_tick` hasn't been called yet
(`.tick.msgHandle` is unset).

GIL released around the `fsync` syscall, so the calling Python thread
doesn't block other workers.

### 2. `engine.add_at_time(fn_name, start_time, description="") -> int` (P3)

Schedule a **nullary** pepper function to fire once at `start_time` on the
chili scheduler thread. Returns the job ID. Use `engine.cancel_job(id)` to
revoke before fire.

```python
from datetime import datetime, timedelta, timezone

engine = ChiliEngine(pepper=True, job_interval=50)   # scheduler must run
engine.eval(".tick.eod_runner: {[] .tick.eod[today[]]}")  # nullary form
target = datetime.now(timezone.utc) + timedelta(minutes=5)
job_id = engine.add_at_time(".tick.eod_runner", target, "EOD timer")
```

**Important caveats:**

- The chili scheduler must be active. Construct `ChiliEngine` with
  `job_interval > 0` (milliseconds). Default is 0 (disabled).
- **Functions must be nullary.** chili's `execute_jobs` invokes them as
  `fn_name[]` (no args). For time context inside the handler, use
  `today[]` / `now[]` or pre-set engine variables.
- `start_time` must be **timezone-aware**; naive datetimes raise
  `TypeError`. Attach `timezone.utc` explicitly if needed.

**Implementation note** (internal, may move in a future sprint): chili's
internal scheduler clock is local-wall-clock-as-UTC-ns. The chili-py
binding converts your tz-aware UTC datetime to that convention
automatically. Callers don't need to compensate manually.

**Two pre-existing chili bugs were fixed in this sprint as part of P3:**

- `.job.addAtTime` set `next_run_time: 0`, causing jobs to fire
  immediately on the next scheduler poll instead of at `start_time`.
  Fixed to seed `next_run_time = start_time`.
- pyo3-chrono extracts `DateTime<Utc>` as true UTC ns; chili's scheduler
  compares against local-wall-clock-as-UTC-ns. Without conversion, jobs
  never fired in non-UTC host timezones. Fixed in chili-py binding.

### 3. Pepper `::` null-literal disambiguation (P2)

Pepper now correctly parses `::` as a standalone null literal when it
appears at the end of an expression (e.g., RHS of assignment followed by
`;`). The grammar previously consumed `::` as a binary operator awaiting
RHS args.

**Cases that now parse:**

```pepper
.sub.eod.fired: ::                                   // single statement
.sub.eod.fired: ::; eod: {[msg] .sub.eod.fired: msg};   // mdata's wishlist form
x: ::                                                // bare null assignment
```

Your daemon-boot code can collapse the work-around (one `eval` call per
statement) back to a single `eval` call with `;` separators — if it's
worth the change. The work-around still works; nothing forced.

Acceptance test on chili side: `crates/chili-py/tests/test_pepper_syntax.py`
covers the exact wishlist form + a standalone `::` + the minimal repro
+ the general non-`::` regression case.

---

## Acceptance tests to flip xfail → strict-pass

Per the wishlist commitment:

| mdata test path | Tests | chili API |
|---|---|---|
| `tests/tp/test_tp_durability.py::test_kill_9_durability` | 1 | `engine.flush_tplog()` |
| `tests/tp/test_eod_scheduler.py::test_chili_side_eod_timer` (or similar) | 1 | `engine.add_at_time()` |
| `tests/.../test_pepper_syntax.py::test_null_literal_semicolon_disambiguation` (NEW per Q2 reply) | 1 | parser `::` |

Run + report after install. If any fail unexpectedly, the chili-side test
counterparts at `crates/chili-py/tests/test_*` are the reference.

---

## Stress benchmark request

Per your wishlist commitment, please rerun `test_tp_stress.py` (v1-14
canonical) against 0.8.4 and report:

- tp burst msgs/s (0.8.3 baseline: 370,664)
- rdb e2e msgs/s (0.8.3 baseline: 363,027)
- wdb e2e msgs/s (0.8.3 baseline: 360,371)
- p99 publish-to-tplog latency

We don't expect regressions. The only change to write-path is `Handle`
now carries an `AtomicU64` field bumped per write; AtomicU64 fetch_add
is single-instruction on Apple Silicon and isn't on the hot path's bottleneck.

If you observe regressions > 5%, flag and we'll profile.

---

## Out of scope (Sprint 17 candidates)

| # | Wishlist | Status |
|---|---|---|
| P1 | `engine.publish_via_handle(h, table, df)` (Option B per Q3 reply) | Sprint 17 Part B drafting now |
| P1 | Subscriber-side `eod` dispatch | Sprint 17 Part A; will port your `test_subscriber_eod_shim_triggered_by_publisher_eod` source |

If you have additional asks surfacing from v1-21 (vendor lift), feel free to
append to your wishlist — we'll fold them into Sprint 18+ scoping.

---

## Cross-references

- Sprint 16 dispatch brief (audited): `~/code/chili/docs/sim/sprint_16_dispatch_brief_2026-05-13.md`
- Sprint 16 retro: `~/code/chili/docs/sim/sprint_16_retro.md`
- Sprint 16 source changes:
  - `crates/chili-core/src/engine_state.rs` — `ReadWrite` trait + `flush_handle` + `bytes_since_flush`
  - `crates/chili-core/src/job.rs:96` — `next_run_time` fix
  - `crates/chili-parser/src/expr.rs` — `Op("::")` → `Expr::Nil` literal production
  - `crates/chili-py/src/lib.rs` — `EngineStatePy::flush_tplog` + `EngineStatePy::add_at_time`
  - `crates/chili-py/chili/engine.py` — `flush_tplog` + `add_at_time` wrappers
- Sprint 16 test additions:
  - `crates/chili-core/tests/flush_handle_test.rs`
  - `crates/chili-py/tests/test_tplog_flush.py`
  - `crates/chili-py/tests/test_add_at_time.py`
  - `crates/chili-py/tests/test_pepper_syntax.py`
- Wishlist source: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md`
- Wishlist reply: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`
- ADR 0005 (Parquet write defaults — unaffected by 0.8.4): `docs/decisions/0005-parquet-write-defaults.md`

---

## Thanks

This sprint's audit-then-clarify cadence (drafted Sprint 16 brief →
3-agent audit → response with 4 clarification Qs → mdata reply →
locked-in scope refinement → implementation) caught three premise drifts
that would otherwise have cost a wheel re-cut cycle:

1. Part C `::` scope tightened (general `;` works; only `::` ambiguity fails)
2. P1 publish_remote ownership reversed (mdata owns RemoteTpClient class)
3. P3 hidden chili bugs (next_run_time + tz convention) surfaced before mdata's acceptance test would have flagged them post-receipt

Net: ~13pp saved across Sprint 16 + Sprint 17 vs the unaudited path.
