# Sprint 5 dispatch brief — bench A/B sweep + polars pin + wheel cut + mdata handoff

**Kickoff:** Immediately on Sprint 4 ratification (autonomous run, user pre-ratification).
**Owner:** coordinator-solo (main Claude); `code-reviewer` subagent at sprint wrap (Part E.1 cadence).
**Type:** verification + delivery (full bench A/B sweep against parked-claude tag-built binary; polars Python version pin; chili 0.8.0-claude2.1 wheel cut; mdata delivery handoff materials).
**Predicted pp:** 10–15 (per roadmap 6-10 + lesson 8 bench-compile overhead 3-5pp absorbed in Sprint 5 budget).
**Plan reference:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 5 row.

---

## Sprint objective

Land **the bench rebaseline measurement deferred from Sprint 4** by running
all 4 hot-path benches on both `claude-baseline-2026-05-07` (parked-claude
tag-built binary) and `claude-2` (post-Sprint-4 tip) and recording the A/B
delta in `post_pivot_baseline_2026-05-07.md`. Land the **polars Python
version pin** to resolve the DSL_SCHEMA_HASH skew that xfailed 4 lazy-return
tests in Sprint 4. Cut **chili 0.8.0-claude2.1 wheel** with mdata delivery
handoff materials. Sprint 5 is the last sprint before the 5-sprint
housekeeping sweep (Sprint 6).

**Binary success criterion:**

1. `docs/bench/post_pivot_baseline_2026-05-07.md` populated with claude-2
   AND claude-baseline-2026-05-07 numbers for: parse_cache (Sprint 3 already
   did claude-2; this sprint adds parked-claude column), scan, eval,
   load_par_df, write_partition. Delta column shows + / − vs parked-claude.
2. The 4 `pytest.mark.xfail(strict=False)` lazy tests from Sprint 4 either
   XPASS (polars pin resolved the DSL skew → marker auto-promotes) OR are
   removed in favor of explicit assertions (if the pin path proves the lazy
   surface works on the new pin).
3. chili-py wheel built via `maturin build --release` and the resulting
   `chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl` (or the post-rename
   per-naming-watch ratification) lives in `dist/`.
4. mdata handoff materials in `docs/sync/mdata_chili_2026-05-07_delivery.md`:
   the wheel artifact path, the breakage-report status (signed off?), the
   ABI/feature delta vs the wheel mdata is currently running on, the API
   migration cheatsheet for write_partitioned_df arg-order fix from Sprint 3.
5. Pre-commit gate GREEN end-to-end on claude-2; chili-py pytest 100% green
   (no xfail mask remaining; all DSL-skew xfails resolved or removed).
6. Sprint 5 retro + cadence_metrics row 5 + sprints_index update.

**Out-of-scope confirmation gates** (NOT expected this sprint):
- Phase17 reverse-scan + sort-groupby benches — Sprint 7 (deep housekeeping
  Sprint 6 first; bench-suite-v0 Sprint 7 per roadmap).
- STAC-M3-shape benchmark suite — Sprint 7.
- New ADRs — none anticipated; reactive only.
- Any Class 4 (deliberately-retired) feature reconsideration — held by
  ADR 0001 unless mdata flags a hard blocker in delivery handoff.

---

## Why now

- **mdata is operationally running on `claude-baseline-2026-05-07`-tag
  wheel.** Sprint 5 is the cutover point: A/B comparison shows whether
  claude-2 wheel is safe to swap; mdata refactor for the breakage report
  starts after delivery.
- **Sprint 4 left the polars DSL skew unresolved** (4 xfailed tests). Sprint
  5 closes that loop by pinning Python polars to the version that matches
  Rust polars 0.53.0's DSL hash. Without the pin, lazy=True is broken for
  mdata's runtime usage and any consumer that wants LazyFrame chaining.
- **5-sprint housekeeping is next** (Sprint 6 per `.claude/rules/sprint-cadence.md`).
  Sprint 5's wrap is the last sprint before deep cleanup. The post-Sprint-5
  state is the "shipped Sprints 1-5 wave" snapshot Sprint 6 demotes to
  history.

---

## Scope — Part A: polars Python version pin

### A.1 Surface additions

- Pin Python `polars==X.Y.Z` in `crates/chili-py/pyproject.toml` (currently
  unpinned; uv installed 1.39.3).
- The "right" version is whichever Python polars matches Rust polars 0.53.0's
  DSL_SCHEMA_HASH. Per pyo3-polars 0.26's release notes, that's likely
  polars Python ~1.0–1.5 range; the actual pin must be empirically
  verified by running the 4 xfailed tests and checking which version makes
  them XPASS.
- Update `uv.lock` to pin the exact version.

### A.2 Implementation hints

- Try `polars==1.5.0` first; if DSL skew persists, walk down to
  `polars==1.0.0`. Each test cycle: `uv add polars==<v>` → `uv run pytest
  tests/test_engine.py::TestEvalLazy -v` → check XPASS vs XFAIL.
- When XPASS, remove the `pytest.mark.xfail` decorators on those 4 tests
  (or convert to explicit assertions).
- If NO version in the polars 1.x range resolves the skew, check
  pyo3-polars 0.26's compatibility matrix in its README or compatibility
  test fixture.

### A.3 Storage / schema

None.

### A.4 Tests

`crates/chili-py/tests/test_engine.py::TestEvalLazy` — the 4 xfailed tests
should XPASS or be converted to explicit pass.

### A.5 Estimated pp

**1.5–2.5pp** (depending on how many version cycles before XPASS).

---

## Scope — Part B: bench A/B sweep — claude-2 vs parked-claude

### B.1 Surface additions

Per `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 5 row promises
(set up Sprint 4 Part C):

**Build #1 — claude-2 tip** (already on disk; release artifacts mostly
warm but full polars-prod compile will happen on first `cargo bench`).

**Build #2 — claude-baseline-2026-05-07 tag**:
1. `git worktree add /tmp/claude-baseline-bench claude-baseline-2026-05-07`
   (or use a separate clone; worktree is faster).
2. In the worktree, run all 4 benches: `cargo bench -p chili-op --bench
   {scan, eval, load_par_df, write_partition}` + `cargo bench -p chili-core
   --bench parse_cache`.
3. Record numbers; copy back to claude-2 docs.

**A/B comparison rows in `post_pivot_baseline_2026-05-07.md`** for:
- parse_cache hit (already 371.43 ns claude-2; need parked-claude column)
- scan headlines (3 variants)
- eval headlines (5 variants)
- load_par_df headlines (3 variants)
- write_partition (1 variant)

Total: ~13 number pairs. Doc should report median + 95% CI for each, and
`Δ%` column.

### B.2 Implementation hints

- Per Sprint 4 lesson 8: budget 3-5pp for the parked-claude binary release
  compile + 2-3pp for the bench runtimes (×2 binaries).
- Use `git worktree add` (NOT separate clone) — shared `target/` cache
  bug-prone, but separate target dirs; in practice safer to use a
  fully-separate clone for bench reproducibility. See `docs/dev_setup.md`.
- The bench runs are wall-clock heavy but token-light — most of the
  Sprint-5 cost is COMPILE wall, not work tokens.
- Halt-criterion-3 if parked-claude bench reveals a regression > 20% on
  any path (would warrant a structural look at Sprint 3's clippy hand-port
  or Sprint 4's mimalloc allocator declaration).

### B.3 Storage / schema

None.

### B.4 Tests

The benches themselves are the validation; no new pytest.

### B.5 Estimated pp

**5–7pp** (parked-claude release compile dominates; 4-5 bench files × 2
binaries; numerical write-up is ~1pp).

---

## Scope — Part C: chili 0.8.0-claude2.1 wheel cut + mdata delivery handoff

### C.1 Surface additions

**Wheel cut:**
- Update `crates/chili-py/Cargo.toml` and `pyproject.toml` version: 0.8.0
  → 0.8.0-claude2.1 (or 0.8.1 if the SemVer convention disallows the
  pre-release marker for binary distributions; verify with `maturin build
  --help`).
- Run `maturin build --release` from `crates/chili-py/`.
- Verify the wheel installs cleanly in a fresh venv: `python -m venv
  /tmp/test-venv && /tmp/test-venv/bin/pip install <wheel-path>` →
  `python -c "import chili; e = chili.ChiliEngine(); print(e.eval('1+2'))"`.

**mdata delivery handoff** (`docs/sync/mdata_chili_2026-05-07_delivery.md`):
- Wheel artifact path (relative to chili repo root or absolute).
- ABI/feature delta vs the wheel mdata currently runs.
- Breakage-report status (which mdata-side refactors need to land before
  pip install).
- API migration cheatsheet (`write_partitioned_df` arg-order fix from
  Sprint 3 Part C; `engine.eval(lazy=True)` from Sprint 4 Part B; etc.).
- Test command to verify the wheel works on mdata's HDB schema.

### C.2 Implementation hints

- Wheel naming follows maturin's automatic logic; the cdylib is named per
  `[lib] name = "chili"` and the package per `[project] name = "chili-sauce"`
  (current pyproject value). User direction 2026-05-07 said "keep `chili`
  install name" — verify this hasn't drifted.
- The Sprint 3 `_apply_column_scales` regex fix is mdata-relevant only if
  mdata calls multiple-table queries against registered scales. Check
  with the mdata team.

### C.3 Storage / schema

None.

### C.4 Tests

Manual: pip install in a fresh venv + run a smoke test. No automated test
in chili.

### C.5 Estimated pp

**2–3pp** (mostly mdata-coordinated; main work is preparing the handoff doc).

---

## Scope — Part D: wrap

### D.1 Surface additions

- `docs/sim/sprint_5_retro.md` per `_retro_template.md`.
- `docs/sim/cadence_metrics.md` row 5 appended.
- `docs/sim/sprints_index.md` Sprint 5 row → "Ratified" (autonomous-run
  pre-ratification continues).
- `CLAUDE.md` project state line: refresh test count (post-pin: pytest
  100% pass with no xfails); refresh "Open items" pointer.

### D.2 Code-reviewer subagent dispatch

Per Sprint 3 lesson 7: dispatch `code-reviewer` BEFORE retro authoring.
If reviewer flags must-fix items, fix in Part D.1 commit. Budget ~1pp.

### D.3 Estimated pp

**1.5–2.5pp** (wrap + reviewer + Part D.1 absorption).

---

## Out of scope (defer)

- **Phase17 reverse-scan + sort-groupby benches** — Sprint 7 (per roadmap).
- **STAC-M3-shape benchmark suite** — Sprint 7.
- **`close` / `reload` / `is_loaded` lifecycle methods** — only port if
  mdata surfaces a need at delivery handoff.
- **Pub/sub re-implementation** — confirmed retired per ADR 0001.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-py/pyproject.toml` polars Python version pin | edit |
| 2 | 4 xfailed lazy tests resolved (XPASS or remarked) | test edit |
| 3 | Bench A/B sweep ~13 number pairs in post_pivot_baseline doc | doc edit |
| 4 | chili 0.8.0-claude2.1 wheel built + smoke-tested | binary artifact |
| 5 | `docs/sync/mdata_chili_2026-05-07_delivery.md` handoff doc | new |
| 6 | Pre-commit gate GREEN end-to-end on claude-2 | gate state |
| 7 | `docs/sim/sprint_5_retro.md` | new (post-sprint) |
| 8 | `docs/sim/cadence_metrics.md` row 5 | edit (post-sprint) |
| 9 | `docs/sim/sprints_index.md` Sprint 5 → "Ratified" | edit (post-sprint) |
| 10 | `docs/history/sprints/sprint_5_dispatch_brief_2026-05-07.md` | git mv (post-sprint) |
| 11 | `CLAUDE.md` project state refreshed | edit |

---

## Lead allocation

- **Coordinator-solo (main Claude)** for all 4 parts. Sequential work.
- **`code-reviewer` subagent at sprint wrap** for diff review per lesson 7.
  Budget ~1pp.
- **No worktrees for chili-2 work itself**; one worktree (`/tmp/claude-baseline-bench`)
  for parked-claude bench compile/run if Part B prefers that over a fresh
  clone.

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~5-7pp into the sprint, after Parts A and
B have started):

- Part A — polars version pin landed; 4 xfailed tests resolved?
- Part B — parked-claude binary built and benched? claude-2 numbers in?
- ETA to Part C wheel cut.

State current 5h-pp delta + absolute % at every checkpoint and at wrap.

### Halt-and-escalate criteria

1. **No polars Python version resolves the DSL skew** — escalate; may
   require pyo3-polars version bump or a compatibility shim. ADR 0003
   territory.
2. **Bench A/B reveals > 20% regression on any hot path** — surface;
   investigate (Sprint 3's clippy hand-port? Sprint 4's mimalloc?
   Polars 0.53 vs prior?). Sprint 5 may need to extend or re-baseline.
3. **mdata delivery surfaces a hard blocker** — e.g., a Class 4
   feature mdata can't refactor away from. Reopen ADR 0001 for that
   specific feature, NOT the whole pub/sub canonical decision.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 5pp.
   (Headroom at Sprint 5 kickoff: ~86% remaining; Sprint 5's
   10-15pp consumes ~10-20% of total budget.)

---

## Wrap (per ceremony)

- Pre-commit gate GREEN on `claude-2`: `cargo fmt --all -- --check &&
  cargo clippy --all-targets --workspace --exclude chili-py -- -D warnings
  && cargo test --workspace --exclude chili-py`. + `(cd crates/chili-py
  && cargo clippy --all-targets -- -D warnings && uv run pytest)`.
- Bench A/B documented in `docs/bench/post_pivot_baseline_2026-05-07.md`.
- Wheel built and smoke-tested.
- mdata delivery doc authored.
- Sprint 5 retro authored at `docs/sim/sprint_5_retro.md`.
- Cadence_metrics row 5 appended.
- Sprints_index updated to "Ratified".
- code-reviewer dispatched + findings absorbed in Part D.1 if any.
- Brief moved to `docs/history/sprints/`.

---

## Pp accounting reference

**Calibration anchors (4 sprints of data):**

- **Sprint 1** (research, 22-35 pred): ~25 actual. Within band low edge.
- **Sprint 2** (pivot, full v1+v2): ~20-22 actual.
- **Sprint 3** (port + ADR + bench gate, 10-15 pred): ~14 actual. Mid.
- **Sprint 4** (chili-py clippy + ADR 0002 + bench DOWNGRADED, 9-14 pred):
  ~9 actual. Low edge — Part C downgrade saved ~1.5pp.

**Sprint 5 prediction breakdown:**

| Part | Predicted |
|---|---:|
| A — polars version pin + xfail resolution | 1.5–2.5 |
| B — bench A/B sweep (compile dominant per lesson 8) | 5–7 |
| C — wheel cut + mdata delivery handoff | 2–3 |
| D — wrap (incl. code-reviewer + Part D.1) | 1.5–2.5 |
| **Total** | **10–15** |

**Position in band:** mid-band ~12pp expected if Part B's parked-claude
binary release compile is mostly cache-warm; high-band ~15pp if compile
caches were invalidated since Sprint 4 (Cargo.toml workspace edit, polars
version pin in Sprint 5 Part A). High edge approached if mdata delivery
handoff surfaces ADR-worthy questions.

**Specific risk slack:**
- ~1pp for Part D.1 (reviewer findings absorbed).
- ~1pp for Halt criterion 1 (polars version search > 2 cycles).
- ~2pp for Halt criterion 2 (bench A/B regression investigation).

If actual ≤9pp, calibration says delivery sprints are cheaper than
predicted (Part C handoff was simpler than estimated). If actual ≥17pp,
one of the halt criteria fired and rescoping happened mid-sprint.

---

## Cross-references

- **Roadmap:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 5 row.
- **ADR 0002 (lazy/eager) — implemented Sprint 4, finalized this sprint:** [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md).
- **ADR 0001 (pub/sub canonical) — confirms what's NOT in scope:** [`../decisions/0001-pub-sub-canonical-model.md`](../decisions/0001-pub-sub-canonical-model.md).
- **Sprint 4 retro (predecessor calibration):** [`sprint_4_retro.md`](sprint_4_retro.md).
- **Bench rebaseline doc:** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) (Sprint 5 populates remaining rows).
- **mdata breakage report (Sprint 3 internal):** [`../sync/mdata_breakage_report_2026-05-07.md`](../sync/mdata_breakage_report_2026-05-07.md).
- **Inventory consumed:** [`../research/claude_only_features_inventory_2026-05-07.md`](../research/claude_only_features_inventory_2026-05-07.md).
- **Iteration lessons (esp. lessons 6, 7, 8, 9):** [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md).
- **Cadence rule:** [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md).
- **Shutdown protocol:** `~/.claude/rules/shutdown-protocol.md`.

---

## User direction inputs (carried from Sprint 4)

1. **Package name:** keep `chili` install name (matches current pyproject).
   Confirm at wheel-cut time; no rename action this sprint unless user
   surfaces.
2. **Pre-commit gate command:** unchanged.
3. **Bench A/B timing:** this sprint is the consolidated A/B sweep.
4. **Code-reviewer cadence:** Lesson 7 — reviewer dispatched BEFORE retro;
   findings absorbed in Part D.1.
5. **Autonomous run:** Sprint 5 ratified upon wrap commit (no user
   ratification required); user observing.
