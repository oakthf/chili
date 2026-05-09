# Sprint 14 dispatch brief — release GIL on direct-FFI `load_par_df` + `clear_par_df` (P3.2b)

**Kickoff:** 2026-05-09 — user-ratified post Sprint 13.5 retro ("ratified
13.5 retro. let's move on").
**Owner:** coordinator-solo + `code-reviewer` subagent dispatch at Part C (lesson 7 binds — FFI surface change).
**Type:** implementation (FFI surface change; no engine-state behavior change).
**Predicted pp:** 5–9.
**Plan reference:** [`sprint_13.5_retro.md`](sprint_13.5_retro.md) §"Sprint 14 hand-off"
+ [`../sync/load_par_df_state_audit.md`](../sync/load_par_df_state_audit.md)
GREEN verdict (pre-Sprint-14 readiness gate, all 9/9 conditions met).
**ADR references:** none new this sprint (state audit is the load-bearing artifact).

---

## Sprint objective

Wrap `engine.load_par_df` and `engine.clear_par_df` (the direct-FFI
methods at `crates/chili-py/src/lib.rs:532` and `:539`) in `py.detach(...)`
to release the GIL during their execution. Achieves FFI-symmetry with
the GIL-releasing siblings (`eval`, `fn_call`, `get_var`,
`import_source_path`).

**Binary success criterion:** post-implementation `concurrent_load_direct`
shape at N=4 ≥ 12,000 calls/s on the chili-py bench fixture (Sprint 13.5
Part B baseline: **4,841 calls/s** flat across N ∈ {1,2,4,8} — perfectly
serialized by the GIL). Target ≥ 12K is set within ±10 % of the
`concurrent_load` (fn_call-path) ceiling at 13.1 K calls/s observed at
N=4 in Sprint 13.5 Part B; hitting the ceiling means the change reaches
the lock-contention boundary the fn_call path also hits.

---

## Why now

- Sprint 13.5 readiness gates **9/9 green** — bench infra committed,
  baseline captured, profile evidence shows 92.5 % kernel time on
  worker threads (GIL contention signature), state audit verdict GREEN.
- No blockers. Polars fork P0 not on the critical path for this change.
- Surface is well-bounded: 2 FFI methods, ~6 lines of code change,
  pattern already established in same file (`fn_call` line 527, `eval`
  line 362, `get_var` line 385, `import_source_path` line 445).

---

## Scope — Part A: implementation

### A.1 Surface additions / changes

No new public API. Two existing FFI method bodies wrapped in `py.detach`:

**`crates/chili-py/src/lib.rs:531-536`** — current:
```rust
fn load_par_df(&self, hdb_path: &str) -> PyResult<()> {
    self.check_fork()?;
    map_spicy_error(self.inner.load_par_df(hdb_path))?;
    Ok(())
}
```

Target shape (mirrors `fn_call` line 527 and `eval` line 362):
```rust
fn load_par_df(&self, py: Python<'_>, hdb_path: &str) -> PyResult<()> {
    self.check_fork()?;
    let path = hdb_path.to_owned();   // 'static String for Send closure
    py.detach(move || map_spicy_error(self.inner.load_par_df(&path)))?;
    Ok(())
}
```

**`crates/chili-py/src/lib.rs:538-543`** — `clear_par_df`. No closure
args; `Arc<Inner>` capture is enough.

### A.2 Implementation hints

- **`Send` closure constraint.** `py.detach` requires the closure to be
  `Send + 'static`. `&str` arg is not `'static`; clone to `String` (or
  `PathBuf`) before the move closure. `&self` capture works because
  `PyEngineState`'s `inner: Arc<EngineState>` is `Send + Sync` (state
  audit §2.1).
- **Error propagation.** Mirror `import_source_path` (line 445) which
  returns `obj?` outside the closure and lets `map_spicy_error` lift
  `SpicyError` into `PyErr`. The current pattern uses `map_spicy_error(...)?`
  inline — same shape works inside the closure: `py.detach(move ||
  map_spicy_error(...))?;`.
- **Don't change `EngineState::load_par_df` itself** (`crates/chili-core/src/engine_state.rs:1468`). The state audit established it's `Send + Sync` safe; no engine-state behavior changes are in scope.
- **Lifetime of `Python<'_>`.** Add `py: Python<'_>` parameter — pyo3
  injects it for free; `py.detach` is the entry point. Same pattern as
  `eval` (line 354), `fn_call` (line 520), `import_source_path` (line 438).

### A.3 Storage / schema

None. Pure FFI surface change. On-disk format unchanged.

### A.4 Tests

- Rust: existing 166 pass — the change touches only `chili-py` which is
  excluded from `cargo test --workspace --exclude chili-py`.
- chili-py pytest: existing 65 pass. No new tests required for this
  sprint — the bench-validation step (Part B) is the load-bearing
  verification.
- Bench harness already in place (Sprint 13.5 A.1).

---

## Scope — Part B: bench validation

### B.1 Local install

```sh
cd crates/chili-py
uv run --no-sync maturin develop  # installs the changed .so into the dev venv
```

(Lesson 8 expectation: ~3–5 min compile incremental. Sprint 13.5 P0 is
not blocking — the local maturin develop path uses the existing
`/tmp/polars-py-1.39.3` clone unchanged.)

### B.2 Full sweep

```sh
uv run --no-sync python tests/bench_concurrent.py --duration 5 \
  > /tmp/sprint_14_post_change_concurrent.json 2> /tmp/sprint_14_post_change_concurrent.stderr
```

Captures all 4 shapes at N ∈ {1, 2, 4, 8}.

### B.3 A/B comparison

Compare against Sprint 13.5 Part B.2 baseline at
`/tmp/sprint_13.5_baseline_head_concurrent.json` (claude-2 HEAD pre-change
wheel). Expected delta on `concurrent_load_direct`:

| N | Pre-Sprint-14 (Sprint 13.5 B.2) | Sprint 14 target |
|---|---:|---:|
| 1 | 4,857 calls/s | ≈ 4,800 calls/s (single-thread; GIL release adds tiny overhead) |
| 2 | 4,821 | ≥ 8,500 (≈ 1.8× scaling, matching `concurrent_load` shape) |
| 4 | 4,841 | **≥ 12,000** (binary success criterion; ceiling 13.1K) |
| 8 | 4,839 | ≥ 8,000 (Phase 2 lock-contention regression expected from N=4 → N=8 same as `concurrent_load`) |

Other shapes (`single_eval`, `concurrent_eval`, `concurrent_load`) should
be **unchanged within ±5 %** — the change does not touch their paths.

### B.4 Halt criteria

1. `concurrent_load_direct` N=4 < 8,000 calls/s post-change → halt and
   surface (something else is wrong; possibly Send-closure constraint
   error, possibly different bottleneck). Do NOT proceed to Part C.
2. Any non-target shape regresses > 5 % vs Sprint 13.5 B.2 → halt; the
   change touched something it shouldn't have.
3. New panic in any chili-py pytest → halt; rollback Part A.
4. parse_cache hit > 400 ns post-change → halt (golden rule 6
   regression — even though Part A doesn't touch parse_cache, run
   `cargo bench -p chili-core --bench parse_cache` once at wrap as a
   sanity check; the Send constraint sometimes triggers cross-crate
   inlining changes).

### B.5 Documentation

Append a "Sprint 14 — P3.2b implementation A/B" section to
`docs/bench/post_pivot_baseline_2026-05-07.md`. Table format mirrors
Sprint 13.5 B.2; include the per-shape delta and the verdict.

---

## Scope — Part C: code-reviewer dispatch

Per **lesson 7 (reviewer-before-retro cadence)**: dispatch
`code-reviewer` subagent BEFORE writing the retro. Lesson 7 binds
because Sprint 14 touches the FFI surface (`crates/chili-py/src/lib.rs`).

**Reviewer prompt should specifically check:**

1. `Send` constraint satisfied on the `py.detach` closure (no captured
   `Bound<'_, PyAny>`, no `&str` lifetimes leaking into the closure).
2. Error propagation matches existing patterns (`fn_call`, `eval`).
3. The `Python<'_>` parameter addition is consistent with sibling
   methods.
4. No accidental change to `EngineState::load_par_df` semantics
   (state audit's GREEN verdict is conditional on the engine-state body
   being unchanged).
5. Borrow checker: `&self.inner` capture into a `move` closure works
   because `inner: Arc<...>` is `Send + Sync` — verify the implicit
   capture path is correct.

Reviewer findings either fold cleanly into a follow-up commit on the
same sprint (preferred per lesson 7), or surface as a halt criterion
if the issue is structural.

---

## Scope — Part D: wrap

- **Pre-commit gate green:** `cargo fmt --all -- --check && cargo clippy
  --all-targets -- -D warnings && cargo test --workspace --exclude
  chili-py`. The `--exclude chili-py` is load-bearing per CLAUDE.md.
- **chili-py pytest unchanged:** 65/0 (no new tests; no behavior change).
- **Bench delta documented** in
  `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 14 section.
- **Author retro** at `docs/sim/sprint_14_retro.md`. Include reviewer
  findings + bench delta + any A.2.2/A.2.4/P3.4 update if implementation
  surfaced new evidence.
- **Append row 14** to `docs/sim/cadence_metrics.md`.
- **Update `docs/sim/sprints_index.md`** Sprint 14 row to "Wrapped".
- **Move dispatch brief** to `docs/history/sprints/sprint_14_dispatch_brief_2026-05-09.md`.
- **Update `CLAUDE.md` state line:** chili-py at 0.8.2 → 0.8.3 if a new
  wheel is cut (Sprint 14 wheel cut is OPTIONAL — only needed if mdata
  asks for it; the change is self-contained and not user-visible-broken
  on 0.8.2 either, just slower under direct-FFI concurrency).
- **HALT** until user ratifies retro.

---

## Out of scope (defer)

| Item | Reason |
|---|---|
| **Wheel cut for mdata delivery** | Optional; not on Sprint 14 critical path. mdata's `load_partitioned_df` already uses the GIL-released fn_call path (Sprint 13.5 lesson 2). The Sprint 14 change benefits direct-FFI callers; no urgency for a wheel re-cut. |
| **A.2.2 vars-write-lock release** | Descoped indefinitely per Sprint 13.5 retro (clone-then-swap memory cost vs unmeasured concurrent gain). |
| **A.2.4 Parquet codec tuning** | Sprint 15 — needs `ParquetWriteConfig` public API + mdata coordination. |
| **P3.4 Categorical mapping cache** | Deferred indefinitely per Sprint 13.5 categorical_eval bench (Δ 0.4 % within noise). |
| **Polars-internal kernel optimization** | Blocked on user-driven P0 (GitHub-host the polars fork). |
| **New chili-py FFI methods** | Out of scope — Sprint 14 is restricted to load_par_df + clear_par_df symmetry. |
| **`upsert` / `insert` GIL release** | Borrow-checker prevents clean wrap (state audit §3.8). Same shape as A.2.2 descope. |
| **`set_var` GIL release** | Independent of P3.2b's `par_df` lock; could be bundled in a future symmetry sprint, but not on the readiness-gate critical path. |

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-py/src/lib.rs:531-543` — `load_par_df` + `clear_par_df` py.detach wrap | edit |
| 2 | `/tmp/sprint_14_post_change_concurrent.json` — bench A/B output | new (artifact, /tmp only — not committed) |
| 3 | `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 14 section | edit |
| 4 | `docs/sim/sprint_14_retro.md` — retro | new (post-sprint) |
| 5 | `docs/sim/cadence_metrics.md` — row 14 | edit (post-sprint) |
| 6 | `docs/sim/sprints_index.md` — Sprint 14 row | edit |
| 7 | `CLAUDE.md` — state-line refresh | edit |

---

## Lead allocation

**Coordinator-solo for Parts A, B, D.** **`code-reviewer` subagent
dispatch for Part C** (lesson 7). Budget allocation:

- Part A implementation: ~2 pp (small surgical change).
- Part B bench validation: ~2 pp (incremental compile + 5 s × 13 runs +
  A/B writeup).
- Part C reviewer dispatch: ~1.5 pp (reviewer cost ~5–10 min wall).
- Part D wrap: ~1.5 pp.

No worktree (single sprint, no parallel execution).

---

## Mid-checkpoint plan

At ~50 % predicted-pp consumed (~3.5 pp), post a status:

- Has Part A compiled cleanly via `maturin develop`?
- Bench result: is `concurrent_load_direct` N=4 ≥ 12K calls/s?
- ETA to Part C reviewer dispatch?

Halt-and-escalate criteria:

1. **Send-constraint compile failure** — if pyo3 rejects the closure
   capture, root-cause exceeds 1.5 pp → halt; revisit closure shape.
2. **Bench halt criterion fires** — see Part B.4. Halt and surface.
3. **Reviewer finds structural issue** — Send/lifetime/error-prop
   issue not fixable in <1 pp → halt for user direction.
4. **Watchdog approaching** — 5h ≥ 80 % AND remaining work > 6 pp,
   halt per shutdown-protocol.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy
  --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- chili-py pytest unchanged: 65/0.
- All bench numbers documented in
  `docs/bench/post_pivot_baseline_2026-05-07.md`.
- Reviewer findings explicit in retro (one-line per finding;
  fold-or-defer column).
- Author retro at `docs/sim/sprint_14_retro.md`.
- Append row 14 to `docs/sim/cadence_metrics.md`.
- Move dispatch brief to `docs/history/sprints/`.
- HALT until user ratifies retro.

---

## Pp accounting reference

Closest historical comparable sprints from `docs/sim/cadence_metrics.md`:

- **Sprint 4** (additive feature port wave 2 — chili-py clippy unblock
  + ADR 0002 + bench harness validation) — predicted 9–14, actual ~9.
  Sprint 14 is narrower (single-file FFI change), expect lower actual.
- **Sprint 7 Part A** (ADR 0003 resolution + chili 0.8.1 wheel cut) —
  predicted 8–15, actual ~12. Comparable because Sprint 14 also touches
  the FFI surface, but Sprint 14 has no wheel-cut path; expect mid- to
  lower-band.
- **Sprint 13** (load_par_df hot path optimization — REVERTED) —
  predicted 9–13, actual ~3. Cautionary tale; Sprint 14 differs in that
  bench infrastructure + state audit + profile evidence all in place
  beforehand. Sprint 13.5's lesson 1 binds: bench-gate threshold here
  is set FROM Sprint 13.5 measurement, not pre-specified.

Sprint 14 expected at the **mid- to lower-band** (5–7 pp), capped above
by:
- Lesson 8: maturin develop incremental compile cost ~3 pp wall.
- Reviewer dispatch surfacing a follow-up commit or two.

Capped below by: bench A/B running clean on first attempt + reviewer
finding nothing of substance (lesson 7 says reviewer should always run,
not that it should always find issues).

---

## Cross-references

- **Sprint 13.5 retro:** [`sprint_13.5_retro.md`](sprint_13.5_retro.md) §"Sprint 14 hand-off"
- **State audit (Part D output of Sprint 13.5):** [`../sync/load_par_df_state_audit.md`](../sync/load_par_df_state_audit.md) — verdict GREEN; load-bearing for this sprint.
- **Bench evidence (Sprint 13.5 Part B+C):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 13.5"
- **Iteration lessons referenced:**
  - **Lesson 7** — reviewer-before-retro cadence binds Sprint 14 (FFI surface).
  - **Lesson 8** — maturin compile cost reservation (~3 pp wall).
  - **Lesson 14** — wheel-only install protocol (NOT triggered this sprint; no mdata wheel re-cut).
  - **Lesson 15** — re-measure within ±10 % of target before signing off.
  - **Sprint 13 lesson 1** (Sprint 13.5 retro) — bench-gate threshold set FROM measurement; this sprint does it correctly.
- **Hard constraints:**
  - mimalloc removed in 0.8.2 (no `#[global_allocator]`).
  - polars py-1.39.3 fork at `/tmp/polars-py-1.39.3` (P0 unresolved; not blocking this sprint).
  - golden rule 6 (parse_cache hit ≤ 400 ns) — sanity check at wrap.
- **Cross-project (mdata):** none required this sprint. mdata's
  `load_partitioned_df` already routes through the GIL-released
  `fn_call` path (Sprint 13.5 lesson 2); Sprint 14 is FFI-symmetry
  correctness, not a user-visible bug fix for them. If a wheel cut is
  added, the delivery doc should explicitly state this framing.
