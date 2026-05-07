# Sprint 6 retro — deep housekeeping sweep (5-sprint cadence)

**Wrap:** 2026-05-07
**Predicted:** 3–5 pp (per `.claude/rules/sprint-cadence.md` housekeeping
guidance + roadmap Sprint 6 row)
**Actual:** ~3 pp
**Variance:** −25% vs midpoint (4.0)
**Owner:** coordinator-solo (main Claude); no subagent dispatch (housekeeping
is self-review per cadence rule).
**Plan reference:** No dispatch brief authored — Sprint 6 is the every-5-sprints
sweep mandated by `.claude/rules/sprint-cadence.md`. Housekeeping sprints
under 5pp don't require a separate brief; the cadence rule itself defines
scope.

---

## Scope shipped

**Doc tree triage and demotion to history (commit `<this commit>`):**

- 10 pre-pivot bench files moved: `docs/bench/{phase{1..7,9},baseline,summary}.md`
  → `docs/history/bench/`. These describe the parked-claude branch's
  per-phase optimization work (2026-04-08 through 2026-04-12) and are
  not "live" documents post-pivot. The post-pivot rebaseline lives in
  `docs/bench/post_pivot_baseline_2026-05-07.md`.
- 1 pre-pivot inventory moved: `docs/research/main_vs_claude_inventory_2026-05-06.md`
  → `docs/history/research/`. Sprint 1's forward inventory; superseded
  by Sprint 2 v2's reverse inventory (`claude_only_features_inventory_2026-05-07.md`)
  used in Sprints 3-5.
- 1 superseded breakage report moved: `docs/sync/mdata_breakage_report_2026-05-07.md`
  → `docs/history/sync/`. Sprint 2 v2 internal-hold doc; absorbed into
  Sprint 5 `docs/sync/mdata_chili_2026-05-07_delivery.md`.
- 1 dated release notes moved: `docs/releases/v0.7.5_claude.md`
  → `docs/history/releases/`. Frozen release notes from a parked-claude
  era; retained as project provenance per docs-lifecycle rule.

Empty `docs/releases/` directory removed.

**Index updates:**

- `CLAUDE.md` docs map updated to reflect demotions:
  - bench: removed phase{1..7,9} + baseline + summary references; added
    "moved to docs/history/bench/" footnote.
  - research: noted main_vs_claude_inventory_2026-05-06 demotion.
  - sync: rolled `mdata_breakage_report` demotion + added
    `mdata_chili_2026-05-07_delivery.md` directly under `docs/sync/`
    bullet.

**Cadence metrics — `## Patterns observed` populated (5+ rows in):**

- Pattern 1: within-band variance ranges identified (post-pivot port
  sprints calibrate at low-mid band consistently when scope-downgrades
  absorb structural blockers).
- Pattern 2: every mid-sprint pivot in the 5-sprint window has been a
  scope-downgrade under structural cost discovery, NOT scope-creep.
- Pattern 3: code-reviewer subagent consistently finds 2-3 must-fix items
  per port sprint; budget 1pp for in-sprint absorption.
- Pattern 4: test count delta calibration (+5-10 implementation sprint,
  +0-2 delivery sprint, +0 housekeeping sprint).
- Pattern 5: bench compile cost is the dominant under-prediction surface;
  any bench/pyproject sprint must budget release-profile rebuild cost.

**Tests:** none added (housekeeping is doc-only; no code touched).
Rust workspace unchanged at 166 tests; chili-py pytest unchanged at
61 passing + 4 xfailed.

**Bench delta:** none (no hot-path code changed).

---

## Lessons (durable)

No new durable lessons promoted — Sprint 6 was within-budget, scope-clean,
and didn't surface new findings. The 5-sprint patterns observed are now
captured in cadence_metrics.md "Patterns observed" section and will inform
Sprint 7+ planning directly.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---:|---:|
| Doc tree audit | 0.5–1 | ~0.5 |
| `git mv` 13 files to docs/history/ | 0.5–1 | ~0.5 |
| CLAUDE.md docs map refresh | 0.3–0.5 | ~0.4 |
| cadence_metrics "Patterns observed" populate | 1–2 | ~1.5 |
| Retro + sprints_index update | 0.5–1 | ~0.5 |
| **Total** | **3–5** | **~3** |

Below midpoint (~−25% vs predicted 4.0pp midpoint). Drivers:

- **Doc audit was lighter than predicted.** With Sprints 3-5 having
  applied lesson 6 (re-verify inventory) at each sprint kickoff, the
  in-flight doc tree was already mostly clean. Only 13 files needed
  demotion.
- **CLAUDE.md was already small** (129 lines, well under 200-line budget),
  so no compaction needed.
- **MEMORY.md was already small** (11 lines), so no compaction needed.
- **No subagent dispatch** for housekeeping per cadence rule (no Part D.1
  absorption to budget).

Position in band: low-mid. Demonstrates that consistent in-sprint
docs-lifecycle hygiene (lesson 6, lesson 8 + retros' "don't let stale
content drift") keeps the every-5-sprints sweep cheap. If sweep regularly
runs at 3pp instead of 5pp, the cadence rule is doing its job.

---

## What surprised

- **CLAUDE.md still under 200-line budget.** Sprints 3-5 added project
  state line refreshes + iteration_lessons references; the file grew from
  118 to 129 lines (+11 lines for 3 sprints) — sustainable trajectory.
  At this rate, ≤ 200 line budget holds for ~25-30 more sprints before
  compaction needed.

- **MEMORY.md is genuinely tiny** (11 lines, 3 entries). Either the
  memory system isn't being used much OR the project's structural
  context is mostly already in CLAUDE.md + iteration_lessons.md
  (favored interpretation: the rules + ADRs + retros are the durable
  structural context; memory is for ephemeral project facts that don't
  fit elsewhere).

- **No new lessons this sprint.** Sprint 6 is the first sprint in the
  cadence to wrap without a durable lesson promotion. Pattern: housekeeping
  sprints harvest patterns into the metrics doc rather than promoting
  new rules. The rules accumulate during implementation sprints.

- **Sprint 6 absorbed in ~30 minutes wall.** Demonstrating that the
  every-5-sprints sweep is genuinely lightweight on this project. Future
  sprints can budget housekeeping at 3pp confidently.

---

## Cross-references

- **Plan:** No brief — cadence rule (`.claude/rules/sprint-cadence.md`)
  defines housekeeping scope.
- **Cadence metrics row:** [`cadence_metrics.md`](cadence_metrics.md) row 6 (will be appended in this commit).
- **Sprint 5 retro (predecessor):** [`sprint_5_retro.md`](sprint_5_retro.md)
- **docs-lifecycle rule (load-bearing for this sprint):** `~/.claude/rules/docs-lifecycle.md`.
- **claude-md-housekeeping rule:** `~/.claude/rules/claude-md-housekeeping.md`.
- **Cadence rule:** [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md).
- **Patterns observed (populated this sprint):** [`cadence_metrics.md#patterns-observed`](cadence_metrics.md).

---

## /compact recommendation

Per `.claude/rules/sprint-cadence.md` Periodic deep housekeeping protocol:
**recommend `/compact` after this sweep.** The Sprint 3-5 implementation
session has accumulated significant transcript chatter (file triage,
intermediate moves, bench-compile waiting, polars version cycling) that
isn't relevant to Sprint 7+. Compacting frees the context window for the
next sprint.

User action: invoke `/compact` in the next session turn.
