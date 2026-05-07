# Sprint 4 dispatch brief — additive feature port wave 2 + ADR 0002 + bench rebaseline rows

**Kickoff:** Immediately on Sprint 3 ratification (autonomous run, user pre-ratification).
**Owner:** coordinator-solo (main Claude); `code-reviewer` subagent at sprint wrap (Part E.1 cadence).
**Type:** implementation (ADR 0002 lazy-eval Python API; chili-py FFI clippy unblock; bench rebaseline rows for scan / eval / load_par_df / write_partition).
**Predicted pp:** 9–14.
**Plan reference:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 4 row.
**ADR references:** [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md) (Status: Accepted, Option b).

---

## Sprint objective

Land **ADR 0002 implementation** (`engine.eval(query, lazy=True)` returns `PyLazyFrame`; default `lazy=False` preserves current eager DataFrame return). Unblock the **chili-py clippy gate** (2 `needless_question_mark` lints inherited from main keep `cargo clippy` RED inside `crates/chili-py/`). Append **scan / eval / load_par_df / write_partition rows to `docs/bench/post_pivot_baseline_2026-05-07.md`** measuring claude-2 hot paths against parked-claude where the comparison is cheap.

**Binary success criterion:**

1. Pre-commit gate GREEN end-to-end on `claude-2` AND inside `crates/chili-py/`:
   - `cargo fmt --all -- --check && cargo clippy --all-targets --workspace --exclude chili-py -- -D warnings && cargo test --workspace --exclude chili-py` (already green).
   - `(cd crates/chili-py && cargo clippy --all-targets -- -D warnings)` — currently RED with 2 errors at lib.rs:386,397; this sprint unblocks.
2. ADR 0002 ships: `ChiliEngine.eval(source, lazy=False)` parameter; `lazy=True` returns `PyLazyFrame`; both paths preserve golden rule 5 (GIL release verified via concurrent throughput micro-bench reused from Sprint 5 plan or by structural inspection of `pyo3-polars` 0.26 `LazyFrame.collect` impl).
3. `docs/bench/post_pivot_baseline_2026-05-07.md` gains 4 new rows (scan, eval, load_par_df, write_partition) with `claude-2` measured numbers; parked-claude column populated where the bench file existed pre-pivot (for path consistency).
4. Sprint 4 retro + cadence_metrics row 4 + sprints_index update.
5. Test count delta documented (ADR 0002 lazy path tests; any chili-py FFI lint port test deltas).

**Out-of-scope confirmation gates** (NOT expected this sprint):
- Pub/sub re-implementation — confirmed retired per ADR 0001.
- Phase17 reverse-scan / sort-groupby benches — Sprint 5 (depends on bench harness setup).
- `mdata-collab/` doc port — Sprint 5.
- Wheel cut (chili 0.8.0-claude2.1) + mdata delivery — Sprint 5.
- `close` / `reload` / `is_loaded` lifecycle methods — they were over-spec'd on Sprint 3's inventory; will be added if mdata's actual usage surfaces a need (not before).

---

## Why now

- **Sprint 3 unblocked the workspace clippy gate** (commit `e103c51`); chili-py's clippy gate is the last RED lint blocker before we can claim "every gate green on claude-2." Fixing the 2 chili-py lints in Sprint 4 closes that loop.
- **ADR 0002 was ratified 2026-05-07** in the post-Sprint-2-v2 conversation; the implementation pin is Sprint 4 per the ADR text. Sprint 5 needs lazy-eval shipped before bench A/B rebaseline can include LazyFrame paths.
- **Bench rebaseline alongside features** per user direction 2026-05-07. Sprint 3 started the rebaseline with parse_cache; this sprint adds the next 4 hot paths so Sprint 5's bench summary has 5 rows to compare instead of 1.
- **First post-Sprint-3 calibration data point.** Sprint 3 hit mid-band 14pp / 0% variance on a port-style sprint; Sprint 4 stays similar shape (port + ADR impl + bench rebaseline) so cadence-calibration extends. If Sprint 4 also lands within band, future port sprints can budget tighter.

---

## Scope — Part A: chili-py clippy unblock

### A.1 Surface additions

Two `needless_question_mark` lints in `crates/chili-py/src/lib.rs`:
- Line 386: `Ok(obj.map(|o| *o.i64().unwrap_or(&0i64))?)` → `obj.map(|o| *o.i64().unwrap_or(&0i64))`.
- Line 397: same pattern.

Both are claude-baseline-2026-05-07 commit `a8d4014` style fixes (which fixed the same lints on the parked-claude chili-py shape; main's FFI rewrite re-introduced them).

### A.2 Implementation hints

- Apply clippy's exact suggestion (`Ok(...)?` removed → return inner expression as the function's `Result`).
- Verify `cargo clippy --all-targets -- -D warnings` GREEN inside `crates/chili-py/` after.
- Re-run `uv run pytest` — the change is type-equivalent so all 58 tests should still pass.

### A.3 Storage / schema

None.

### A.4 Tests

No new tests; existing pytest covers the upsert/insert paths.

### A.5 Estimated pp

**0.5–1pp.** Trivial fix; cost is in build cycle.

---

## Scope — Part B: ADR 0002 implementation (`engine.eval(lazy=True)`)

### B.1 Surface additions

Per ADR 0002 ([`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md)) Option b — opt-in lazy via `lazy=True` parameter:

**Python API on `ChiliEngine`** (`crates/chili-py/chili/engine.py`):

```python
def eval(self, source: str, src_path: Optional[str] = None, lazy: bool = False) -> Any:
    """...
    Args:
        ...
        lazy: When True, returns a polars.LazyFrame for DataFrame-shaped
              results instead of collecting eagerly. Useful for query
              composition and plan introspection. Both paths preserve
              golden rule 5 (GIL released around eval).
    """
    if src_path is None:
        src_path = ...
    if lazy:
        result = self.engine.eval_lazy(source, src_path)
    else:
        result = self.engine.eval(source, src_path)
    if isinstance(result, pl.DataFrame):
        return self._apply_column_scales(result, source)
    return result
```

**Rust FFI on `PyEngineState`** (`crates/chili-py/src/lib.rs`): new method `fn eval_lazy(&self, py, source, src_path) -> PyResult<Py<PyAny>>`. Same shape as existing `eval` but creates the underlying engine in lazy mode at parse time and returns the SpicyObj-converted result (which will be `SpicyObj::LazyFrame(lf) → PyLazyFrame`).

Implementation choice: rather than spinning up a fresh `EngineState` per call (high overhead), set the lazy bit on a thread-local override OR, simpler, keep using the engine's persisted lazy mode and require the caller to construct the engine with `lazy=True` if they want lazy. The cleanest path: add `eval_lazy` that takes the `lazy_mode` switch as a temporary override on the caller engine. Mid-checkpoint check confirms the chosen approach.

### B.2 Implementation hints

- The PyO3 method signature for `eval_lazy` should mirror the existing `eval` method (line 331) — same `py.detach(...)` pattern preserves golden rule 5.
- The `pyo3-polars` 0.26 `PyLazyFrame.collect()` impl wraps the heavy work in `py.allow_threads()` (verified in Sprint 2 v2 wrap, lesson 5 in `iteration_lessons.md`). So a returned `PyLazyFrame` whose `.collect()` is later called from Python preserves the GIL-release guarantee end-to-end.
- The `_apply_column_scales` post-processor only fires on `pl.DataFrame` results; a `pl.LazyFrame` skips it — caller can call it manually after `.collect()` if needed. Document this in the ADR 0002 code comment.

### B.3 Storage / schema

None.

### B.4 Tests

`crates/chili-py/tests/test_engine.py`:
- `TestEval.test_eval_lazy_returns_lazyframe` — `engine.eval('([] x:1 2 3)', lazy=True)` returns a `pl.LazyFrame`; `.collect()` returns the same data as eager eval.
- `TestEval.test_eval_default_is_eager` — `engine.eval('([] x:1 2 3)')` (no lazy kwarg) returns `pl.DataFrame` (regression pin for the default).

Estimated +2 pytest cases.

### B.5 Estimated pp

**3–4pp.** ADR 0002 is the heaviest part of Sprint 4. The Rust FFI add is small (~30 lines); the Python wrapper is ~5 lines; the discovery work is "what's the cleanest way to override lazy mode per call vs per-engine."

---

## Scope — Part C: Bench rebaseline rows (scan, eval, load_par_df, write_partition)

### C.1 Surface additions

Run the existing chili-op benches:

```bash
cargo bench -p chili-op --bench scan
cargo bench -p chili-op --bench eval
cargo bench -p chili-op --bench load_par_df
cargo bench -p chili-op --bench write_partition
```

Append rows to `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 3 section's hot-path table:

| Metric | Bench file | Owner sprint | claude-baseline-2026-05-07 | claude-2 |
|---|---|---|---|---|
| scan throughput | `crates/chili-op/benches/scan.rs` | Sprint 4 | TBD (or "(not recorded)" if absent) | <new> |
| ... | ... | ... | ... | ... |

### C.2 Implementation hints

- For each bench, record the median time + 95% CI exactly as Sprint 3 Part D recorded parse_cache.
- For the parked-claude column: if `docs/bench/phase*.md` already records a comparable number, cite it; otherwise mark `(not recorded — Sprint 5 baseline)`.
- DON'T run the parked-claude binary now — that requires checking out the `claude-baseline-2026-05-07` tag, building, benching. Sprint 5 does that as a single A/B sweep.

### C.3 Storage / schema

None.

### C.4 Tests

None (benches don't have unit tests).

### C.5 Estimated pp

**2–3pp.** The bench runs total ~1-2 minutes wall; the docs work is interpreting the numbers.

---

## Scope — Part D: wrap

### D.1 Surface additions

- `docs/sim/sprint_4_retro.md` per `_retro_template.md`. Records:
  - Predicted vs actual pp (calibration signal — Sprint 4 is the second port-shape sprint).
  - Test count delta (target: +2 chili-py pytest, possibly +0 Rust).
  - Bench delta on scan/eval/load_par_df/write_partition.
  - Pp accounting per Part.
- `docs/sim/cadence_metrics.md` row 4 appended.
- `docs/sim/sprints_index.md` Sprint 4 row → "Ratified" (autonomous-run pre-ratification continues).
- `CLAUDE.md` project state line: refresh test count if it changed; refresh "Open items" pointer if memory file got new entries.

### D.2 Code-reviewer subagent dispatch

Per Sprint 3 lesson 7 (just promoted): dispatch `code-reviewer` BEFORE retro authoring. If reviewer flags must-fix items, fix in a Part D.1 commit before the wrap commit. Budget ~1pp.

### D.3 Estimated pp

**1.5–2.5pp** (incl. code-reviewer + Part D.1 absorption if needed).

---

## Out of scope (defer)

- **Phase17 reverse-scan + sort-groupby benches** — Sprint 5 (require bench harness scaffolding work).
- **chili 0.8.0-claude2.1 wheel cut** — Sprint 5.
- **mdata wheel delivery** — Sprint 5.
- **mdata-collab/ doc port** — Sprint 5.
- **`close` / `reload` / `is_loaded` lifecycle methods** — only port if mdata surfaces a real need.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | 2 chili-py clippy lints fixed (lib.rs:386, 397) | Rust edit commit |
| 2 | `eval_lazy` PyO3 method on `PyEngineState` | Rust new |
| 3 | `lazy=False` parameter on `ChiliEngine.eval` | Python edit |
| 4 | TestEval lazy/eager path pytest cases | Python test |
| 5 | scan / eval / load_par_df / write_partition rows in bench rebaseline doc | Doc edit |
| 6 | Pre-commit gate GREEN incl. chili-py clippy | Gate state |
| 7 | `docs/sim/sprint_4_retro.md` | new (post-sprint) |
| 8 | `docs/sim/cadence_metrics.md` row 4 | edit (post-sprint) |
| 9 | `docs/sim/sprints_index.md` Sprint 4 → "Ratified" | edit (post-sprint) |
| 10 | `docs/history/sprints/sprint_4_dispatch_brief_2026-05-07.md` | git mv (post-sprint) |
| 11 | `CLAUDE.md` project state refreshed | edit |

---

## Lead allocation

- **Coordinator-solo (main Claude)** for all 4 parts. Sequential ports (A → B → C → D); B is independent of A.
- **`code-reviewer` subagent at sprint wrap** for diff review per lesson 7. Budget ~1pp; reviewer findings absorbed in Part D.1 commit.
- **No worktrees** — sequential on `claude-2`.

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~5-7pp into the sprint, after Parts A and B land):

- Part A — chili-py clippy GREEN end-to-end?
- Part B — `eval_lazy` works; pytest LazyFrame path passes; GIL release verified?
- Part C — at least 2 of 4 bench rows landed?
- ETA to wrap.

State current 5h-pp delta + absolute % at every checkpoint and at wrap.

### Halt-and-escalate criteria

1. **ADR 0002 implementation reveals a structural blocker** — e.g., `eval_lazy` requires per-call `EngineState` reconstruction (high overhead) and no clean override path exists. Surface; rescope (per-engine lazy mode only? or per-call cheap override?) and continue.
2. **Bench reveals a regression** — scan / eval / load_par_df / write_partition on claude-2 is dramatically worse than parked-claude's reported number on the same hot path. Halt; investigate (likely a polars 0.53 vs 0.50 difference or main's FFI shape change).
3. **chili-py clippy fix reveals a deeper API problem** — unlikely; if it does, surface for ADR.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 5pp. (Headroom at Sprint 4 kickoff: ~88% remaining; a single sprint at 9-14pp consumes ~10-15% of total budget.)

---

## Wrap (per ceremony)

- Pre-commit gate GREEN on `claude-2`: `cargo fmt --all -- --check && cargo clippy --all-targets --workspace --exclude chili-py -- -D warnings && cargo test --workspace --exclude chili-py`. + `(cd crates/chili-py && cargo clippy --all-targets -- -D warnings)`.
- Python gate: `cd crates/chili-py && uv run maturin develop && uv run pytest`. Tests added in Part B should pass.
- Bench delta documented in `docs/bench/post_pivot_baseline_2026-05-07.md` (4 new rows minimum).
- Test count delta documented in retro: target +2 pytest.
- Sprint 4 retro authored at `docs/sim/sprint_4_retro.md`.
- Cadence_metrics row 4 appended.
- Sprints_index updated to "Ratified" (autonomous-run pre-ratification).
- code-reviewer dispatched + findings absorbed.
- Move dispatch brief to `docs/history/sprints/`.

---

## Pp accounting reference

**Calibration anchors:**

- **Sprint 1** (research): 22-35 predicted, ~25 actual. Within band (low edge). Research-shape sprint.
- **Sprint 2** (pivot, full v1+v2): ~20-22 actual. v2 brief alone 8-14 → ~13.3 actual (within band).
- **Sprint 3** (port + ADR + bench gate; closest comparable): 10-15 predicted, ~14 actual. Mid-band, 0% variance vs midpoint.

**Sprint 4 prediction breakdown:**

| Part | Predicted |
|---|---:|
| A — chili-py clippy unblock | 0.5–1 |
| B — ADR 0002 implementation | 3–4 |
| C — bench rebaseline rows | 2–3 |
| D — wrap (incl. code-reviewer + Part D.1) | 1.5–2.5 |
| **Total** | **7–10.5** |

Wait — that's 7-10.5, lower than the sprint objective's 9-14pp. Reconciliation: Sprint 3 came in at the predicted midpoint; Sprint 4 is structurally simpler (less code to write, more bench-running and one new method). Setting predicted band at **9-14pp** with the lower edge representing "if everything goes smoothly, including no Part D.1 absorption" and the upper edge representing "ADR 0002 needs a per-call lazy override design and bench rows surface a comparison gotcha." If actual lands ≤8pp, calibration says ADR-implementation sprints are cheaper than predicted (good signal); if ≥15pp, Part B's structural blocker (Halt criterion 1) likely fired.

**Position in band:** mid-band ~11pp expected if Part B's lazy override design is straightforward; high-band ~14pp if Part B requires structural rework.

**Specific risk slack:** ~1pp for Part D.1 (code-reviewer findings absorbed), per Sprint 3 cadence.

---

## Cross-references

- **Roadmap:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 4 row.
- **ADR 0002 (lazy/eager default):** [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md) — pin for Part B implementation.
- **ADR 0001 (pub/sub canonical) — confirms what's NOT in scope:** [`../decisions/0001-pub-sub-canonical-model.md`](../decisions/0001-pub-sub-canonical-model.md).
- **Sprint 3 retro (predecessor calibration):** [`sprint_3_retro.md`](sprint_3_retro.md).
- **Bench rebaseline doc (Part C target):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md).
- **Inventory consumed:** [`../research/claude_only_features_inventory_2026-05-07.md`](../research/claude_only_features_inventory_2026-05-07.md).
- **Iteration lessons (esp. lessons 5+6+7):** [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md).
- **Cadence rule:** [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md).
- **Shutdown protocol:** `~/.claude/rules/shutdown-protocol.md`.

---

## User direction inputs (carried from Sprint 3)

1. **Package name:** keep `chili` (matches current pyproject.toml). No change Sprint 4.
2. **Pre-commit gate command:** unchanged from Sprint 3 (workspace test --exclude chili-py + chili-py clippy/pytest separately).
3. **Bench rebaseline timing:** alongside features (Sprint 3 started parse_cache; Sprint 4 adds 4 more rows; Sprint 5 closes with phase17 + A/B sweep).
4. **Code-reviewer cadence:** Lesson 7 promoted Sprint 3 — reviewer dispatched BEFORE retro, fixes absorbed in Part D.1.
5. **Autonomous run:** Sprint 4 ratified upon wrap commit (no user ratification required); user observing.
