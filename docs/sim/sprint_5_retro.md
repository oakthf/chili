# Sprint 5 retro — bench A/B sweep + polars pin + wheel cut + mdata handoff

**Wrap:** 2026-05-07
**Predicted:** 10–15 pp
**Actual:** ~10 pp (low-mid edge; Part B downgraded to deferred)
**Variance:** −20% vs midpoint (12.5)
**Owner:** coordinator-solo (main Claude) + code-reviewer subagent at wrap (Part D.1 cadence per Sprint 3 lesson 7)
**Plan reference:** [`../history/sprints/sprint_5_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_5_dispatch_brief_2026-05-07.md)

---

## Scope shipped

**Part A — polars Python pin + ADR 0003 (commit `da1dbc5`)**
- Tested polars Python 1.20.0, 1.30.0, 1.39.0, 1.39.3 — all fail to
  match Rust polars 0.53.0's DSL_SCHEMA_HASH.
- Discovery: chili's `hinmeru/polars-core-patch.git#v0.53.0` fork
  embeds a different DSL hash than any stock Python polars wheel.
  Structurally unfixable on the Python side.
- ADR 0003 authored: defer-resolution; pin to 1.39.3 (latest tested-stable
  for eager path reproducibility); keep lazy-return tests xfailed.
- pyproject `dependencies = ["polars==1.39.3"]` + `[dependency-groups]
  dev = [pytest, pluggy]` added. uv sync rebuilt the chili-py wheel
  (release profile, 5+ min).
- xfail markers updated to point to ADR 0003.

**Part B — bench A/B sweep DOWNGRADED**
- ORIGINAL plan: build parked-claude tag binary + claude-2 binary; run all
  4 chili-op benches + parse_cache on both; populate post_pivot_baseline
  doc with ~13 number pairs.
- ACTUAL: deferred to Sprint 7. Drivers:
  - Part A's unexpected wheel rebuild (uv sync triggered by pyproject
    `dependencies` field addition) consumed ~5-7 min release-profile
    compile time.
  - Bench A/B would require ANOTHER 10-15 min release-profile compile for
    the parked-claude binary build, plus claude-2 bench runs.
  - Cumulatively: 25-35 min wall just on compile, well into Part B's
    5-7pp lesson-8 budget.
  - Plus: Sprint 6 is the deep housekeeping sweep per cadence rule;
    Sprint 7 absorbs bench A/B more cleanly (post-housekeeping clean
    state; mdata feedback from this delivery in hand).

**Part C — chili 0.8.0-claude2.1 wheel cut + mdata delivery handoff (commit `60a126b`)**
- `maturin build --release` from `crates/chili-py/` → wheel at
  `crates/chili-py/target/wheels/chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl`
  (33 MB; 5m38s wall).
- mdata handoff doc authored:
  `docs/sync/mdata_chili_2026-05-07_delivery.md` — TL;DR, wheel
  provenance, ABI/feature delta table, migration cheatsheet, breakage
  report, smoke test, asks.

**Part D.1 — code-reviewer fixes (commit `acd478e`)**
- Critical: pub/sub finality overstated → corrected to
  "deliberately-retired pending mdata feedback."
- Warning: `engine.get_tick_count()` no-arg default was claimed in
  handoff doc but not implemented in Python wrapper. Fixed Python wrapper
  to honor `index=0` default + `inc=1` default; added regression test.
- Warning: ADR 0003 misleading "pin prepares option (a)" reworded.
- Suggestion: ADR 0003 lazy-failure scope precision.
- Suggestion: handoff §2 `publish(table, df)` predecessor column
  expanded.

**Part D — wrap (this commit)**
- `docs/sim/sprint_5_retro.md` — this file.
- `docs/sim/cadence_metrics.md` row 5 appended.
- `docs/sim/sprints_index.md` Sprint 5 → Ratified.
- `docs/sim/sprint_5_dispatch_brief_2026-05-07.md` git mv'd to
  `docs/history/sprints/`.
- `CLAUDE.md` project state refreshed (test count → 167 Rust + 61 pytest
  passing + 4 xfailed; ADR 0003 reference added; mdata wheel delivery noted).

**Tests:** +1 chili-py pytest (TestTick.test_get_tick_count_no_arg_defaults_to_index_zero
regression). Rust workspace unchanged at 166. chili-py pytest 60 → 61
passing (+ 4 xfailed = 65 collected).

**Bench delta:** none measured this sprint (Part B deferred); no regression
risk introduced (no hot-path code changed).

---

## Lessons (durable)

### 1. ADR a structural blocker the moment cost-of-resolution exceeds cost-of-deferral

**Rule.** When a Sprint discovers a structural blocker (not just a bug —
a constraint baked into the dependency graph or external API surface) and
the fix path costs >5pp while the workaround / deferral path costs <1pp,
draft an ADR documenting the blocker + the explicit defer decision +
all viable resolution paths ranked by cost. The ADR is the discovery's
durable home; without it the structural finding rots in retro notes and
gets re-discovered in a future sprint at full cost. Specifically: don't
just retro "we tried X and it didn't work" — that doesn't bind future
sprints. An ADR with Status: Accepted (defer-resolution) does.

**Why.** Sprint 5 Part A: tested 4 Python polars versions (1.20, 1.30,
1.39.0, 1.39.3). All failed to match Rust polars 0.53.0's
`DSL_SCHEMA_HASH`. Investigation revealed the root cause is chili's
`hinmeru/polars-core-patch.git#v0.53.0` fork — its polars-plan source
hashes differently from any stock Python polars wheel. NO version pin
on the Python side resolves this; the only fixes are (a) pyo3-polars
upstream releases a DSL-decoupled transfer, (b) chili replaces the
patch fork with stock polars, (c) chili custom-builds Python polars to
match. (a) is 0.5pp when it ships; (b) is 5-15pp; (c) is 5-10pp + ongoing
publish maintenance. Without ADR 0003, a future sprint would re-test
versions, re-investigate, and re-conclude — sunk discovery cost. With
the ADR, the structural finding is documented, the resolution paths
are ranked, and any future sprint can immediately decide which path to
pursue (or wait).

**Apply where.** Any Sprint that surfaces an "X is not version-fixable
because Y" finding, or "this works around Z by Y but the real fix is
W which costs >5pp." Especially load-bearing for: pyo3-polars version
transitions (lesson 5 + ADR 0003 territory); polars-core-patch fork
maintenance; FFI ABI changes. Generalizes to nxcar / mdata cross-project
dependencies where one project's pin choice locks the other into a
specific version range.

**Cost saved.** ~3-5pp per future sprint that would otherwise re-investigate
the same structural blocker. Plus risk reduction on a future engineer
attempting a fix that ranks lower in the ADR's cost-ranked options without
realizing the higher-ranked option exists. Recurs every sprint that touches
the affected surface.

### 2. uv sync triggered by pyproject `dependencies` change rebuilds maturin wheel at release profile

**Rule.** When editing `pyproject.toml` `[project] dependencies = [...]`
or `[dependency-groups] dev = [...]` (or any field that changes the
project state hash), `uv sync` will rebuild any maturin-managed editable
install at the release profile. On chili-py with the polars 0.53 dep
tree, that's a 5+ min wall cost on the first `uv sync` after each pyproject
change. Subsequent `uv run pytest` invocations reuse the cached release
artifacts. Plan accordingly: pyproject edits in a sprint should be
batched and committed BEFORE running tests, and the test gate budget
should include that one-time rebuild cost (~5pp on chili).

**Why.** Sprint 5 Part A added `polars==1.39.3` to pyproject for the
mdata wheel pin. uv sync triggered a release-profile rebuild that took
~5-7 min wall and burned ~3pp before pytest could even run. The
predicted Part A cost (1.5-2.5pp) was based on "edit pyproject + run
existing pytest" — actual was 4-5pp because of the rebuild. Generalizes:
any project where `pyproject.toml` is the source of truth for both Python
deps AND Rust build state (via maturin) has this coupling. Also applies
to bumping pyo3 / pyo3-polars versions, which trigger similar cascades.

**Apply where.** Any sprint that edits chili-py/pyproject.toml. Generalizes
to any maturin-managed editable Python project. Inverse case (pyproject
edits to a pure-Python project, no maturin) doesn't apply. Specifically:
Sprint 6 deep housekeeping should not edit pyproject without budgeting
the rebuild cost; future ADR 0003 resolution sprints (option a:
pyo3-polars upgrade) MUST budget ~5pp for this rebuild on top of the
upstream version bump.

**Cost saved.** ~3-5pp per sprint that edits pyproject + runs pytest in
the same wrap commit. Recurs on every dependency-bump sprint, ADR
resolution sprint touching Python deps, and any wheel-rev sprint.

---

## Pp accounting

| Part | Predicted | Actual |
|---|---:|---:|
| A — polars version pin + xfail resolution | 1.5–2.5 | ~4 (rebuild trigger + ADR 0003 authoring) |
| B — bench A/B sweep (DOWNGRADED → deferred Sprint 7) | 5–7 | ~0.5 (only "decide to defer" cost) |
| C — wheel cut + mdata delivery handoff | 2–3 | ~3 (handoff doc was ~250 lines) |
| D — wrap (incl. code-reviewer + Part D.1) | 1.5–2.5 | ~2.5 |
| **Total** | **10–15** | **~10** |

Below midpoint (~−20% vs predicted 12.5pp midpoint). Drivers of variance:

- **Part A ran HIGH** (~+1.5pp vs predicted high edge) due to the uv sync
  rebuild trigger (lesson 2 promotion). The polars version search was
  also more thorough than predicted (4 versions tested instead of the
  brief's "try 1.5.0 then 1.0.0").
- **Part B downgrade saved ~5-7pp.** The deferred work moves to Sprint 7
  which absorbs it cleanly post-housekeeping. The downgrade was the
  RIGHT call given lesson 8 (bench-compile cost) was already in effect.
- **Part C ran mid-band** (~3pp). Handoff doc was the bulk of the work;
  wheel cut itself was a 5m38s wait + verification.
- **Part D ran mid-band** (~2.5pp). Reviewer dispatch + Part D.1 fix
  absorption per lesson 7.

Position in band: low-mid (~10pp). Sprint 5 was the second sprint in a
row to come in low-band — Sprint 4 also at low edge with a Part C
downgrade. Two sprints with downgrades in a row suggests the brief
estimation is over-pricing measurement / bench work (which uses bench-
compile cost lesson 8 as the actual driver). Future sprint briefs
should price bench-related work using the lesson 8 framing explicitly.

---

## What surprised

- **DSL_SCHEMA_HASH skew is structural, not version-fixable.** Going
  into Sprint 5, the assumption was "find the matching Python polars
  version and pin." Actual: chili's polars-core-patch fork makes any
  stock Python polars incompatible. ADR 0003 territory. Lesson 1
  promotion captures this.

- **uv sync rebuild trigger on pyproject change.** Adding `dependencies =
  [...]` to pyproject was a 1-line edit. The downstream effect was a
  5+ min release-profile rebuild because uv considers pyproject changes
  as project-state changes that invalidate the editable install cache.
  Lesson 2 promotion.

- **Maturin's wheel metadata correctly propagates pyproject `dependencies`.**
  Reviewer S3 confirmed the wheel METADATA contains
  `Requires-Dist: polars==1.39.3` so `pip install <whl>` pulls the right
  polars. No silent override risk.

- **Code-reviewer caught the no-arg `get_tick_count()` doc/code mismatch.**
  Without the reviewer dispatch (lesson 7), the handoff doc would have
  shipped to mdata claiming a default that doesn't exist in the Python
  wrapper. mdata would have hit `TypeError: missing 1 required positional
  argument: 'index'` on first migration attempt, generating an
  embarrassing "did chili just break this?" round-trip. Caught in sprint
  for ~0.7pp.

- **Sprint 5 closed Sprint 1's ADR 0001 + ADR 0002 implementation arc**
  (claude-2 baseline + features port + lazy/eager + delivery). All three
  ADRs accepted; ADR 0003 added on top documenting the lazy-path
  structural blocker. The cumulative ADR coverage for the post-pivot
  shape is comprehensive.

- **5 sprints since pivot ratification = housekeeping next.** Per
  `.claude/rules/sprint-cadence.md`, the every-5-sprints sweep triggers
  at Sprint 6. The cumulative state to demote to history: claude-2
  baseline, clippy-unblock work, port-wave-1 features, port-wave-2 +
  ADR 0002, polars pin + wheel + mdata handoff. Major.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_5_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_5_dispatch_brief_2026-05-07.md) (moved post-ratification)
- **Cadence metrics row:** [`cadence_metrics.md`](cadence_metrics.md) row 5
- **Sprint 4 retro (predecessor calibration):** [`sprint_4_retro.md`](sprint_4_retro.md)
- **ADR 0001 (pub/sub canonical):** [`../decisions/0001-pub-sub-canonical-model.md`](../decisions/0001-pub-sub-canonical-model.md)
- **ADR 0002 (lazy/eager default):** [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md)
- **ADR 0003 (PyLazyFrame DSL incompat) — drafted this sprint:** [`../decisions/0003-pylazyframe-dsl-incompat.md`](../decisions/0003-pylazyframe-dsl-incompat.md)
- **mdata delivery handoff (drafted this sprint):** [`../sync/mdata_chili_2026-05-07_delivery.md`](../sync/mdata_chili_2026-05-07_delivery.md)
- **Bench rebaseline doc (Sprint 7 will add A/B rows):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md)
- **Inventory consumed:** [`../research/claude_only_features_inventory_2026-05-07.md`](../research/claude_only_features_inventory_2026-05-07.md)
- **Promoted lessons:** [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md) (lesson 10 ADR-structural-blocker; lesson 11 uv sync wheel-rebuild trigger)
- **Cadence rule (Sprint 6 housekeeping next):** [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md)
