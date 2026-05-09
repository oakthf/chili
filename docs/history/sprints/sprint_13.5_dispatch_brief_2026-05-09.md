# Sprint 13.5 dispatch brief — bench infrastructure + state audit (pre-Sprint-14 measurement)

**Kickoff:** 2026-05-09 — user-ratified post Sprint 13 retro + post audit-revised plan.
**Owner:** coordinator-solo (main Claude); no code-reviewer dispatch (no chili source code changes; bench files + docs only).
**Type:** measurement / scaffold (NOT implementation; no chili source touched).
**Predicted pp:** 8–12.
**Plan reference:** Sprint 13 retro hand-off + audited proposal `docs/proposals/perf_alternatives_post_mimalloc_2026-05-08.md` §A.4 with post-audit revisions per user direction (2026-05-09): A.2.2 descoped, A.2.4 → Sprint 15, P3.4 deferred pending Sprint 13.5 evidence, ADR 0005 dropped.
**ADR references:** none new this sprint. ADR 0005 (Categorical cache invalidation) deferred until P3.4 fate decided post-wrap.

---

## Sprint objective

**Establish the measurement evidence base for Sprint 14 (P3.2b GIL release).** Apply Sprint 13 lesson 2 ("speculative optimization claims need profile-evidence verification"): before Sprint 14 changes any FFI surface, Sprint 13.5 must produce (a) bench harnesses that measure the concurrent throughput shape Sprint 14 targets, (b) baseline numbers on the shipped 0.8.2 wheel + current claude-2 HEAD, (c) profile evidence that GIL hold IS the dominant cost on the concurrent harness, and (d) a state-audit doc verifying `load_par_df` is safe to call with GIL released.

**Binary success criterion:** all 5 deliverables (concurrent harness + Categorical bench + baselines + profile + state audit) committed and ratified, AND the profile shows GIL hold ≥ 40% of wall time on the concurrent-load shape (= Sprint 14's premise is empirically supported). If GIL hold < 40%, sprint succeeds in producing measurement infra but Sprint 14 scope must pivot.

---

## Why now

- **Sprint 13 reverted** — 0pp gain from chili-side allocation reductions. Lesson: don't optimize without profile evidence on the same bench shape we target.
- **Sprint 14's premise needs verification.** P3.2b assumes GIL hold is the dominant cost on concurrent `load_par_df` calls. No profile measures this today. Sprint 13.5 closes that gap before Sprint 14 commits FFI changes.
- **No blockers.** All 5 deliverables are chili-side or test-harness work; no user-driven P0 needed; no upstream version dep.
- **0.8.2 wheel exists** at `dist/chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl` — usable as a clean, reproducible baseline for any future bench A/B (Sprint 14 will compare its post-change numbers against Sprint 13.5's 0.8.2 baseline).
- **CLAUDE.md `tests/bench_concurrent.py` reference is stale.** The file does not exist. Sprint 13.5 creates it; CLAUDE.md update lands in retro.

---

## Scope — Part A: Bench harness creation

### A.1 NEW `crates/chili-py/tests/bench_concurrent.py`

Python harness for concurrent throughput measurement. Three shapes:

- **Shape 1 — concurrent eval**: N worker threads each calling `engine.eval(query, lazy=False)` on different small queries, measure aggregate throughput (calls/sec). N ∈ {1, 2, 4, 8}.
- **Shape 2 — concurrent load_par_df**: N worker threads each calling `engine.load_par_df(hdb_path)` on a shared HDB. Measures contention on `par_df.write()` lock + GIL hold during Phase 1 parallel build.
- **Shape 3 — single-thread eval baseline**: 1 thread, same eval queries. Reference for "what does GIL release cost on single-thread."

Harness uses `ThreadPoolExecutor`. Runs against installed `chili` package (= 0.8.2 wheel installed in clean venv per the §B procedure). Outputs JSON-line per shape × N: `{shape, n_workers, total_seconds, total_calls, calls_per_sec, p50_ms, p99_ms}`.

### A.2 NEW `crates/chili-op/benches/categorical_eval.rs`

Rust criterion bench, forward-looking evidence for P3.4 fate decision. Two shapes:

- **`categorical_filter_repeated`**: build a 100-symbol Categorical column, run 1000 iterations of `select * where symbol = "<S>"` for varying S. Measures Categorical mapping rebuild cost.
- **`categorical_filter_distinct`**: same shape but each iteration filters a DIFFERENT symbol. Stresses the not-yet-cache path.

Mirrors the structure of existing `crates/chili-op/benches/eval.rs`. Bench is just measurement; no chili changes.

### A.3 (no — A.2.2 descoped per user direction; no upsert microbench)

Per Sprint 13.5 brief revisions (2026-05-09): the upsert/insert vars-lock release optimization (A.2.2 in the audited proposal §A.2.2) was descoped because the only feasible implementation is clone-then-swap, which has a 200–500 MB allocation cost on 5M-row tables and may regress single-thread upsert throughput. **Reopen only if profile evidence shows lock contention is dominant on a representative workload.** Captured in Sprint 13.5 retro.

---

## Scope — Part B: Baseline capture

### B.1 0.8.2 wheel baseline (clean venv)

Procedure (committed as `tests/install_0_8_2_for_bench.sh` or inline in retro):

```bash
TEMPDIR=$(mktemp -d) && cd "$TEMPDIR"
uv venv --python 3.12 .venv
VIRTUAL_ENV="$TEMPDIR/.venv" uv pip install \
  /Users/oakadmin/code/chili/dist/chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl \
  'polars==1.39.3' 'pyarrow==24.0.0'
.venv/bin/python -c "import chili; print(chili.__file__)"  # confirm site-packages, not editable
.venv/bin/python /Users/oakadmin/code/chili/crates/chili-py/tests/bench_concurrent.py \
  > /tmp/sprint_13.5_baseline_0_8_2_concurrent.json
```

Run all three concurrent shapes from A.1 against the 0.8.2 wheel. Capture JSON output.

### B.2 claude-2 HEAD baseline (release wheel from current source)

Build current `claude-2` HEAD as a wheel via `uv run --no-sync maturin build --release --out /tmp/sprint_13.5_head_dist`. Install in second clean venv. Run same A.1 + A.2 benches.

This identifies whether claude-2 HEAD has drifted from 0.8.2 (it shouldn't — no chili source changes since the 0.8.2 cut commit `b660a50`-era tree, but verify).

### B.3 Rust criterion bench against claude-2 HEAD

Run from workspace:
- `cargo bench -p chili-op --bench load_par_df` (existing — reference numbers carry forward)
- `cargo bench -p chili-op --bench categorical_eval` (NEW from A.2)
- `cargo bench -p chili-core --bench parse_cache` (existing — golden rule 6 gate ≤ 400 ns)

Document all numbers in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 13.5 section, formatted as a table with columns: bench / shape / 0.8.2 / claude-2 HEAD / Δ.

### B.4 Lesson-8 compile cost reservation

Per `docs/standards/iteration_lessons.md` lesson 8: release-profile compile on the polars 0.53 dep tree is 5–10 min wall per binary. Three new bench-binary compiles = ~15–30 min wall = ~2–4pp tokens. Budget reserved.

---

## Scope — Part C: Concurrent profile capture

### C.1 samply on concurrent-load shape

Pick the `concurrent load_par_df` shape with N=4 workers (smallest meaningful concurrency). Capture profile:

```bash
samply record --save-only -o /tmp/sprint_13.5_concurrent_load_profile.json \
  .venv/bin/python /Users/oakadmin/code/chili/crates/chili-py/tests/bench_concurrent.py \
    --shape concurrent_load --workers 4 --duration 30
```

### C.2 chili-side symbolization (per Sprint 12 + lesson 17)

Use `addr2line` (already installed per Sprint 12 P2 work) to resolve chili-side hot frames in the captured profile. Polars-internal frames remain unresolved (separate user-P0 issue; not in Sprint 13.5 scope).

### C.3 GIL-hold attribution

Identify frames matching `pyo3::Python::with_gil`, `pyo3::Python::detach`, or equivalent GIL-acquire/release calls. Sum their wall-time share.

**Halt threshold: GIL hold < 40% of wall time → escalate per halt criterion 2.** Sprint 14's P3.2b premise is contradicted; pivot Sprint 14 scope (likely to A.2.4 codec tuning earlier or new investigation).

If GIL hold ≥ 40% → premise confirmed; record finding + proceed to Part D.

### C.4 Documentation

Append to `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 13.5 section: profile artifact path, top 10 chili-side frames by wall-time share, GIL-hold percentage, halt-threshold verdict.

---

## Scope — Part D: State audit doc

### D.1 NEW `docs/sync/load_par_df_state_audit.md`

Standalone audit doc (per audit recommendation: keep separate, easier to point at if mdata asks "is this safe?"). Contents:

- **Shared state held during `load_par_df`**: enumerate every `Arc<RwLock<...>>` / `Mutex` / `&mut self` access. Source: `crates/chili-core/src/engine_state.rs::load_par_df` lines 1468–1506.
- **Send + Sync verification**: confirm `EngineState`'s inner type is `Send + Sync` (likely already proven by `lib.rs:558-561` TCP listener spawning `Arc<inner>` to another thread).
- **Concurrency hazards under GIL release**: what happens if two Python threads both call `load_par_df` simultaneously? Both call `clear_par_df` simultaneously? One calls `load`, other calls `eval`?
- **Verdict**: green / yellow / red on safety of `py.detach()` wrapper. Yellow/red blocks Sprint 14.

### D.2 (optional) Verification micro-test

If the audit verdict is green and time permits, a quick stress test: 4 threads call `load_par_df` concurrently for 30 seconds. Verify no panics, no deadlocks, no data corruption. Goes into D.1's appendix.

---

## Scope — Part E: Wrap

- Pre-commit gate green (no chili source touched, but bench files compile + tests still pass): `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- chili-py pytest unchanged (65/0).
- Author retro at `docs/sim/sprint_13.5_retro.md`. Include explicit A.2.2 descope note + reasoning so future Claude doesn't re-propose without profile evidence.
- Append row 13.5 to `docs/sim/cadence_metrics.md`.
- Update `docs/sim/sprints_index.md` Sprint 13.5 row.
- Move dispatch brief to `docs/history/sprints/` post-ratification.
- Update CLAUDE.md: remove stale `tests/bench_concurrent.py` reference (now exists, not stale; could keep or rephrase).
- HALT until user ratifies retro.

---

## Out of scope (defer)

| Item | Reason |
|---|---|
| **A.2.2 (vars-write-lock release)** | Descoped per user direction 2026-05-09 (clone-then-swap memory cost vs concurrent gain). Reopen only with profile evidence. |
| **A.2.4 (Parquet codec tuning)** | Moved to Sprint 15 per user direction. Needs new public API (`ParquetWriteConfig`) + mdata coordination. |
| **P3.4 (Categorical mapping cache)** | Deferred indefinitely. Sprint 13.5's A.2 Categorical bench gives forward-looking evidence; decision after wrap. |
| **ADR 0005 draft** | Dropped pending P3.4 fate. |
| **Polars-internal kernel optimization** (`0x450c` 93.1% multi-table worker time) | Blocked on user P0 (GitHub-host the polars fork). |
| **Sprint 14 P3.2b implementation** | Sprint 14 scope; Sprint 13.5 produces only the evidence + state audit. |
| **Code-reviewer dispatch** | Not needed this sprint (no chili source touched). Reviewer at Sprint 14 wrap per lesson 7. |

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-py/tests/bench_concurrent.py` | new |
| 2 | `crates/chili-op/benches/categorical_eval.rs` | new |
| 3 | `crates/chili-op/Cargo.toml` — `[[bench]] name = "categorical_eval"` entry | edit |
| 4 | `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 13.5 section (baselines + profile findings) | edit |
| 5 | `/tmp/sprint_13.5_baseline_0_8_2_concurrent.json` | new (artifact, /tmp only — not committed) |
| 6 | `/tmp/sprint_13.5_concurrent_load_profile.json` | new (artifact, /tmp only — not committed) |
| 7 | `docs/sync/load_par_df_state_audit.md` | new |
| 8 | `docs/sim/sprint_13.5_retro.md` | new (post-sprint) |
| 9 | `docs/sim/cadence_metrics.md` — row 13.5 | edit (post-sprint) |
| 10 | `docs/sim/sprints_index.md` — Sprint 13.5 row | edit |

---

## Lead allocation

**Coordinator-solo** for Parts A, B, C, D, E. No subagent dispatch this sprint:

- No code-reviewer (no chili source touched; lesson 7 doesn't bind).
- No Explore (codebase scan already done in pre-Sprint-13.5 audit).
- No planner (sequencing already audited and revised per user direction).

If Part C profile reveals an unexpected dominant cost (not GIL, not lock contention, but e.g., Polars rayon thread-pool init), `debugger` subagent dispatch becomes appropriate per template halt criterion 2.

No worktree (single sprint; no parallel execution).

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~5pp), post a status:

- Have A.1 concurrent harness + A.2 Categorical bench compiled and baseline-ready?
- Has Part B captured 0.8.2 baseline numbers?
- ETA to Part C profile capture (the load-bearing one for Sprint 14 readiness)?

Halt-and-escalate criteria:

1. **Scope-blowing bug** — if any new bench harness is structurally infeasible (e.g., concurrent harness deadlocks on installed wheel) and root-cause exceeds 2pp to fix, halt.
2. **Plan-pivot finding** — if Part C profile shows GIL hold < 40% of wall time, halt and surface to user. Sprint 14's premise is contradicted; new scope needed.
3. **User-decision needed** — if Part D state audit returns yellow/red verdict on `load_par_df` GIL-release safety, halt for explicit user direction. Sprint 14 cannot proceed without green verdict.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 6pp, halt per `~/.claude-team/rules/work-metrics.md`.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- chili-py pytest unchanged: 65/0.
- All bench numbers documented in `docs/bench/post_pivot_baseline_2026-05-07.md`.
- Profile artifact preserved at `/tmp/sprint_13.5_concurrent_load_profile.json` + path noted in retro for future debugging access (not committed).
- State audit verdict (green/yellow/red) explicit in retro; Sprint 14 readiness statement explicit (proceed / pivot / halt).
- Author retro at `docs/sim/sprint_13.5_retro.md`. Include A.2.2 descope note (per user direction).
- Append row 13.5 to `docs/sim/cadence_metrics.md`.
- Move dispatch brief to `docs/history/sprints/` post-ratification.
- HALT until user ratifies retro AND green-lights Sprint 14 (or Sprint 14 pivot).

---

## Pp accounting reference

Closest historical comparable sprints from `docs/sim/cadence_metrics.md`:

- **Sprint 8** (perf-pass-1, P3+P4 eval bench fix + A/B fill, P2 deferred) — predicted 6–12, actual ~4. Comparable shape: bench-files-only sprint with profile attempt. Sprint 13.5 has more deliverables (3 new files) but smaller per-item complexity; expect mid-band actual.
- **Sprint 9** (perf-pass-2, profile capture + symbolization-blocked) — predicted 5–10, actual ~2. Highly comparable: profile-capture + measurement-shaped sprint. Sprint 13.5 differs in that addr2line is now installed (no infra friction) + state audit doc is bounded scope.
- **Sprint 12** (perf-pass-3 + Iceberg eval) — predicted 6–12, actual ~3. Research-shape; not comparable to Sprint 13.5's measurement-implementation shape.

Sprint 13.5 expected at the **mid-to-low end** (8–10pp), capped above by:
- New bench-binary compile cost (~3pp per lesson 8) is a hard floor.
- A.1 concurrent harness from scratch is ~2-3pp; could overrun if installed-wheel patterns aren't well-documented for the threaded harness.

Capped below by: Part C profile producing a clear ≥40% GIL-hold finding fast (no halt), plus Part D state audit being short (likely green given existing TCP listener evidence).

---

## Cross-references

- **Audited proposal:** [`../proposals/perf_alternatives_post_mimalloc_2026-05-08.md`](../proposals/perf_alternatives_post_mimalloc_2026-05-08.md) §A (revised plan after independent audit).
- **Sprint 13 retro:** [`sprint_13_retro.md`](sprint_13_retro.md) — the lesson 2 ("speculative optimization claims need profile-evidence verification") that drove Sprint 13.5's existence.
- **Sprint 12 P2 partial symbolization:** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 12 P2 partial symbolization" — addr2line install evidence for Sprint 13.5 Part C.
- **Iteration lessons referenced:** lesson 7 (reviewer-before-retro — N/A this sprint), lesson 8 (bench-related sprint compile cost), lesson 15 (re-measure ±10% target — bench gates set AFTER 13.5 wrap, not before), lesson 17 (macOS samply autonomous-run profiling — addr2line resolution).
- **Hard constraints:** mimalloc removed in 0.8.2 (no `#[global_allocator]`); polars py-1.39.3 fork at `/tmp/polars-py-1.39.3` (P0 unresolved); golden rule 6 (parse_cache hit ≤ 400 ns).
- **Sprint 14 readiness gate:** Part C profile ≥ 40% GIL hold AND Part D state audit verdict green = Sprint 14 proceeds with P3.2b implementation. Otherwise pivot.
- **Cross-project (mdata):** none this sprint. Sprint 14's eventual P3.2b ship will include a delivery-doc note since it's FFI-visible.
