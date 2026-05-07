# Sprint 2 retro — claude-2 pivot (v1 halt + v2 ratify-and-execute)

**Wrap:** 2026-05-07
**Predicted (v1 brief):** 5–9 pp (cherry-pick mdata foundation — superseded after Part A halt)
**Predicted (v2 brief):** 8–14 pp (claude-2 pivot)
**Actual:** ~20–22 pp (combining v1 halt + v2 prep + v2 Parts A-E)
**Variance vs v2 midpoint (11 pp):** +73% to +91% (well over the upper band)
**Owner:** coordinator-solo (main Claude); Explore subagent for Part B
**Plan reference v1 (superseded):** `../history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md`
**Plan reference v2:** `sprint_2_dispatch_brief_2026-05-07.md`

---

## Sprint structure (chronological)

This sprint is two arcs in one sprint number:

1. **Sprint 2 v1 (planned cherry-pick):** halted at Part A on 2026-05-07. Cherry-pick of `b20177c` produced 12 conflict regions in `chili-py/src/lib.rs` (multiple > 30 lines, largest 101); halt-criterion #1 from the v1 brief triggered. ~1.5 pp burnt.
2. **Sprint 2 v2 (pivot to claude-2):** authored new brief, ratified, executed. ~18-20 pp.

The retro covers both arcs together because they share the same sprint number and the v1 halt drove the v2 plan.

---

## Scope shipped

### Sprint 2 v1 (halted, no shipped scope)

- Cherry-pick of `b20177c` attempted; aborted on conflict surface (commit chain restored to clean state).
- v1 brief moved to history with SUPERSEDED banner.
- Iteration lesson 4 promoted: "Cherry-pick conflict accumulation — invert the merge direction" (`docs/standards/iteration_lessons.md`, commit `f5889f1`).

### Sprint 2 v2 (pivot deliverables)

**Pre-pivot artifacts (commits on `claude` branch):**
- New brief `docs/sim/sprint_2_dispatch_brief_2026-05-07.md` (`f5889f1`).
- New roadmap `docs/sim/roadmap_2026-05-07.md` replacing the cherry-pick + Sprint-8-merge arc with a port arc; original Sprint 8 deleted (accomplished structurally) (`f5889f1`).
- ADR 0001 placeholder `docs/decisions/0001-pub-sub-canonical-model.md` — Option (a): adopt main's tick/sub canonical; retire claude's; A/B via parallel binaries; no in-tree shim. Drafted in `f5889f1`, ratified in `2d9f6fb` Part C.
- Lifecycle: Sprint 1 brief moved to `docs/history/sprints/`; Sprint 2 v1 brief moved with SUPERSEDED banner.
- Path fixes for moved roadmap (CLAUDE.md docs map, competitive_position, sprint_1_retro, lesson 2 reference).
- Brief Part A scope expansion + ratification (`dea966e`).

**Part A — branch creation + planning-doc copy + bench backup + env fix (commits on `claude-2`):**
- `4fbe5eb feat(claude-2): initialize from main tip + claude planning baseline` (57 files, 9587 insertions). Forked from `main` tip `f8b6360`. Tags landed: `claude-baseline-2026-05-07` + `main-pivot-2026-05-07`. Imports from claude: `docs/`, `.claude/rules/`, `CLAUDE.md` (with branch policy + gate + project state rewritten for post-pivot).
- `fc92ce0 chore(clippy): fix 11 pre-existing lints in chili-parser/tests/utils.rs` — clean cherry-pick from claude's `71e2c41`.
- `dc189e7 chore(clippy): use clamp() instead of max().min()` — clean cherry-pick from claude's `e829bd4`.
- `0a7dfa0 chore(clippy): port chili-op test lints from claude (round 3, partial)` — partial cherry-pick of claude's `a8d4014`; chili-py portion deferred to Sprint 4 (FFI-rewrite divergence).
- `2d9f6fb fix(claude-2): apply CLAUDE.md branch policy + gate edits + document Part A gate state` — fixed unstaged CLAUDE.md edits + documented gate state in `pre_pivot_state.md`.

**Part B — reverse-direction features inventory (commit on claude-2):**
- `77dac8c docs(research): Sprint 2 v2 Part B — claude-only features inventory (reverse direction)` (794 lines). Class 1 (already-on-main): 6 features. Class 2 (shape-divergent, residual): 1 feature (package name). Class 3 (claude-only-needs-port): 8 features + bench/docs ports. Class 4 (deliberately-retired per ADR 0001): 2 features.

**Part C — ADR 0001 ratification:**
- ADR 0001 Status: Draft → Accepted (this sprint's wrap commit).

**Part D — mdata breakage report (held internal):**
- `docs/sync/mdata_breakage_report_2026-05-07.md` — full breakage scope, gain summary, refactor sequence, tentative timeline. Held until Sprint 3 + user delivery.

**Part E — wrap (this commit):**
- This retro file.
- `docs/sim/cadence_metrics.md` row 2.
- `docs/sim/sprints_index.md` updated.
- Memory updates (project_chili_branch_model.md).

**Tests:** 0 (no Rust tests added; no Python tests added). Per Sprint 2 v2 scope, ports happen Sprint 3+.

**Bench delta:** none. claude-2 inherits main's benches; rebaseline is Sprint 5.

---

## Lessons (durable)

### 1. Cherry-pick conflict accumulation — invert the merge direction

Already promoted to `docs/standards/iteration_lessons.md` lesson 4 in commit `f5889f1` during the pre-Part-A pivot work. Sourced from the v1 halt finding. Cost saved: ~10–30 pp avoided cherry-pick conflict thrashing across original Sprints 2-4 + permanent elimination of cherry-pick conflict cost from the entire 12-sprint roadmap.

### 2. (Candidate, not promoted yet) Subagent context drift on superseded planning docs

**Rule.** When dispatching a subagent for research/audit on a topic where a recent ADR or planning doc supersedes older content, instruct the subagent EXPLICITLY to read the canonical authority first (e.g., "READ `docs/decisions/0001-pub-sub-canonical-model.md` BEFORE other docs and align all framing to its decision"). Subagents pattern-match on older planning docs by default and miss superseding ADRs.

**Why.** Sprint 2 v2 Part B, 2026-05-07. The Explore subagent's draft inventory consistently framed the pub/sub recommendation as "ADR 0001 recommends Option (c) hybrid with measured retirement" — the **pre-pivot** Sprint 1 inventory recommendation, NOT ADR 0001's actual Option (a) decision. Likely root cause: agent referenced `competitive_position_2026-05-06.md` and `main_vs_claude_inventory_2026-05-06.md` §2.6 (both pre-date ADR 0001's authoring on 2026-05-07) and didn't read the ADR file directly. Main-thread correction pass touched ~12 sections; cost ~1 pp.

**Apply where.** Any Explore / general-purpose subagent dispatch where a recent ADR or planning doc supersedes the static research baseline. Especially load-bearing for: per-feature port decisions in Sprints 3-4 (where ADR 0001 Option a determines pub/sub retirement vs port); future ADR ratifications that revise older inventory recommendations.

**Cost saved.** Estimated ~1 pp per occurrence of the correction pass; could be larger if the error makes it into a wrong implementation rather than getting caught at doc review.

**Promotion status.** **NOT YET PROMOTED.** Single-incident finding; needs 1+ more occurrence to validate the pattern, OR a clearly attributable larger cost incident. If Sprint 3-4 subagent dispatches show the same drift, promote.

---

## Pp accounting

| Item | Predicted | Actual | Notes |
|---|---|---|---|
| **Sprint 2 v1 (halted)** | 5–9 | ~1.5 | Cherry-pick attempted + aborted; halt-criterion #1 triggered |
| **v2 prep + ratification** | (not in v2 brief) | ~5.5 | Pivot direction debate, brief authoring, roadmap rewrite, ADR 0001 draft, lifecycle hygiene + lesson 4 promotion |
| Part A — branch + doc copy + env fix + bench backup | 2–3 | ~5 | Cherry-pick exploration on clippy fixes burnt extra pp; CLAUDE.md unstaged-edit fix-up cost |
| Part B — reverse inventory | 3–5 | ~4 | Subagent ~3pp + main-thread correction pass ~1pp |
| Part C — ADR 0001 ratification | 1–2 | ~0.3 | Trivial — header edit only since ADR text was authored in v2 prep |
| Part D — mdata breakage report | 1–2 | ~1.5 | Comprehensive doc; in-band |
| Part E — retro + cadence_metrics + sprints_index + memory | 2–3 | ~2.5 | (this commit) |
| **v2 sub-total** | **8–14** | **~13.3** | within v2 band; v2 alone (post-prep) is well-calibrated |
| **Sprint 2 total (v1 + v2 prep + v2 Parts A-E)** | (no formal predict; 13–23 implied) | **~20–22** | Cumulative |

**Commentary.**

The **v2 brief alone calibrated well** (~13.3 actual vs 8-14 predicted, within band). The variance is in the **out-of-v2-brief work**: v1 halt (1.5pp), pre-v2-brief pivot ratification + lifecycle hygiene + lesson promotion (5.5pp). Together that's ~7pp of "between-brief" work that the v2 brief didn't predict (because the v2 brief existed only after the v1 halt + pivot direction was set).

**Calibration data point for future briefs:** when a sprint includes a halt + pivot to a re-plan, budget the meta-work (re-plan + re-brief + iteration lesson promotion + lifecycle hygiene) as a separate ~5-7 pp item that a v2 brief alone doesn't capture. Future "pivot sprints" should expect the actual to be predicted + 5-7pp.

The Part A overage (5pp vs 2-3pp predicted) is the **cherry-pick exploration tax** — I tried `git cherry-pick 9aa358d` (chili-core 19 lints) and got conflict on engine_state.rs (same FFI-rewrite divergence that triggered the original pivot), then aborted. That exploration burnt ~1-1.5pp without shipping deliverables. Worth recording: cherry-pick exploration on a divergent surface is iteration-lesson-4 territory; should default to "manual port in Sprint 3" and not even try cherry-pick.

---

## What surprised

- **Bare main has its own pre-existing fmt + clippy lints** that claude had fixed but claude-2 inherits unfixed (claude's `9aa358d`, `71e2c41`, `e829bd4`, `a8d4014` were targeted at claude code, not main code; main code has the same lints uncovered). Some cherry-pick clean (utils.rs, clamp), others don't (chili-core 19 lints — engine_state.rs divergence). Sprint 3's first deliverable is hand-porting `9aa358d`.
- **Cargo workspace feature unification** with pyo3 `extension-module` is the load-bearing reason `cargo test --workspace --exclude chili-py` is the gate (not just `cargo test`). This was discovered during pre-Part-A env diagnostic; the brief Part A scope was expanded to productionize it.
- **`git checkout claude -- CLAUDE.md`** brings the file but Edit calls AFTER the checkout don't re-stage the file automatically when an unrelated `git add` runs. Cost: 1 fix-up commit (`2d9f6fb`). Lesson candidate but probably specific enough not to surface as a durable rule.
- **Subagent referenced stale planning docs** (Lesson candidate 2 above). Borderline-promote.
- **claude branch `tick_count` was scalar `i64`, claude-2 (main) is `Vec<i64>` indexed.** This is a SHAPE-DIVERGENT surface that ADR 0001 partially resolves (claude-2 inherits main's shape; mdata adapts). Confirmed during Part B inventory.

---

## Cross-references

- Plan v1 (superseded): `../history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md`
- Plan v2: `sprint_2_dispatch_brief_2026-05-07.md`
- ADR 0001: `../decisions/0001-pub-sub-canonical-model.md`
- Reverse-direction inventory: `../research/claude_only_features_inventory_2026-05-07.md`
- mdata breakage report (held internal): `../sync/mdata_breakage_report_2026-05-07.md`
- New roadmap: `roadmap_2026-05-07.md`
- Cadence metrics row: `cadence_metrics.md`
- Iteration lessons: `../standards/iteration_lessons.md` (lesson 4 promoted from this sprint's v1 halt; lesson 5 candidate from Part B subagent drift)
- Pre-pivot bench baseline: `../history/bench_claude_baseline_2026-05-07/pre_pivot_state.md`
