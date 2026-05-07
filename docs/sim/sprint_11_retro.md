# Sprint 11 retro — deep housekeeping #2 (every-5-sprint sweep)

**Wrap:** 2026-05-08
**Predicted:** 3–5 pp (per cadence rule housekeeping default)
**Actual:** ~1.5 pp
**Variance:** −63% vs midpoint (4.0)
**Owner:** coordinator-solo (main Claude); no subagent dispatch (housekeeping = self-review per cadence rule).
**Plan reference:** No formal dispatch brief — Sprint 11 is the second every-5-sprints sweep (after Sprint 6); cadence rule (`.claude/rules/sprint-cadence.md`) defines scope.

---

## Scope shipped

**Doc tree triage and demotion**

- 2 proposals moved to `docs/history/proposals/`:
  - `python_bindings_comparison_and_wishlist.md` (2026-04-26 author Treehouse / mdata) — wishlist items mostly absorbed across Sprints 3-7 (GIL release, exception hierarchy, column scale, overwrite_partition, query_plan, mimalloc, log built-ins all shipped).
  - `load_tree_namespaced_hdb.md` (2026-04-30 author Treehouse / mdata) — recursive `load_par_df` shipped via main FFI rewrite (chili-2 inherits); the proposal's core gap is closed.
- Empty `docs/proposals/` directory removed.

**No retros demoted** — sprint retros (1-10) stay live indefinitely as project provenance per docs-lifecycle rule.

**No code changes** — pure docs sprint.

**Index updates**

- `CLAUDE.md` project state line refresh:
  - Sprints 3-11 ratified (was 3-8 at Sprint 7 wrap).
  - Date pin: 2026-05-08.
  - Next: Sprint 12 = perf-pass-3 + Iceberg + Sprint 9 P2 carry-over.
  - User-driven backlog explicitly noted: P0 (GitHub-host fork), P6 (KDB-X CE), addr2line/dSYM setup.
  - ADR 0004 (pepper-vs-k9) added to ADR list.
- `docs/sim/sprints_index.md` Sprint 11 row appended (Ratified, no brief, retro link).
- `docs/sim/cadence_metrics.md` row 11 appended; `## Patterns observed` section gains "10-row deltas" subsection with 5 new patterns (6-10).

**Five new patterns observed** (cadence_metrics):

6. Autonomous-run macOS perf-pass + research-synthesis sprints have a structural pp ceiling (~2-5pp actual vs 5-12pp predicted).
7. Mid-sprint pivot count remains stable across all 10 sprints (5 total, all scope-downgrades under structural cost).
8. Lesson promotion rate: 17 lessons across 10 sprints (~1.5/sprint; implementation 2-3, housekeeping/research 0-1).
9. Test count delta calibration: cumulative +65 chili-py pytest over 7 sprints touching chili-py.
10. **The "user-driven step" backlog has accumulated** (P0 GitHub-host, P6 KDB-X CE, addr2line/dSYM/Xcode). Compound effect: until P0, fresh chili clones break at `cargo build`.

**Tests:** 166 Rust + 65 chili-py pytest (unchanged; pure docs sprint).

---

## Lessons (durable)

No new durable lessons promoted. Sprint 11 is housekeeping; lessons accumulate during implementation/perf sprints, not sweeps. Pattern 6-10 added to cadence_metrics's "Patterns observed" section but those are observation-deltas, not lesson-promotions.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---:|---:|
| Doc tree audit | 0.5–1 | ~0.3 |
| `git mv` 2 proposals | 0.3–0.5 | ~0.2 |
| CLAUDE.md docs map / state refresh | 0.3–0.5 | ~0.3 |
| cadence_metrics 10-row deltas | 1–2 | ~0.5 |
| Retro + sprints_index update | 0.5–1 | ~0.2 |
| **Total** | **3–5** | **~1.5** |

Below low-band (~−63% vs predicted 4.0pp midpoint). Drivers:

- **Doc tree was already cleaner** than expected — proposals were the only stale-but-still-live items. retros are intentional permanent records.
- **CLAUDE.md still under 200-line budget** (currently ~135 lines after Sprint 11 refresh; was 129 at Sprint 6 — +6 lines for 5 sprints means we have ~65 sprints of headroom before next compaction).
- **MEMORY.md still 11 lines** (no new memory entries since Sprint 6).
- **No subagent dispatch** for housekeeping per cadence rule.

Pattern 6 (autonomous-run sprint pp ceiling) plays out again: housekeeping sprint at 1.5pp, well below the 4pp default.

---

## What surprised

- **The project's documentation hygiene is genuinely good.** Two housekeeping sweeps (Sprints 6 and 11) total ~4.5pp combined out of 50pp+ of cumulative sprint work — ~10% overhead on docs maintenance. Compare to typical project where docs rot accumulates and housekeeping sweeps run 5-10pp each.

- **No retros qualified for demotion** — even Sprints 1 and 2's retros stay live as project provenance. This is the docs-lifecycle rule's "live retros are project history" interpretation playing out as expected.

- **The user-driven step backlog (P0 GitHub-host, P6 KDB-X CE, addr2line/dSYM) is now visible enough** to be a Sprint 12 prerequisite check. Pattern 10 captures this; Sprint 12's brief should explicitly check P0 before any chili-source-tree work.

---

## /compact recommendation

Per `.claude/rules/sprint-cadence.md` Periodic deep housekeeping protocol:
**recommend `/compact` after this sweep.** Sprint 7-10 implementation + retro + bench-A/B + ADR drafting accumulated significant transcript chatter (samply rebuild waits, hex-address profile analysis, ADR drafting back-and-forth) that isn't relevant to Sprint 12+. Compacting frees the context window for Sprint 12's perf-pass-3 + Iceberg work.

User action: invoke `/compact` in the next session turn.

---

## Cross-references

- **Sprint 6 retro (predecessor housekeeping):** [`sprint_6_retro.md`](sprint_6_retro.md)
- **Sprint 10 retro (predecessor sprint):** [`sprint_10_retro.md`](sprint_10_retro.md)
- **Cadence metrics (10-row deltas added this sprint):** [`cadence_metrics.md`](cadence_metrics.md)
- **docs-lifecycle rule:** `~/.claude/rules/docs-lifecycle.md`
- **claude-md-housekeeping rule:** `~/.claude/rules/claude-md-housekeeping.md`
- **Cadence rule:** [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md)

---

## Sprint 12 hand-off

Sprint 12 per roadmap = "perf-pass-3 + Iceberg eval". Predicted 6-12pp. Scope:

- **Carry-over P2 (Sprint 9):** load_multitable_5x200p mitigation. Requires symbolization infra (`cargo install addr2line` or `dsymutil` + `atos` workflow). Lesson 17 follow-up.
- **Carry-over P5 (optional):** parked-claude `.pep` re-bench for true Δ% on 3 apples-to-oranges queries. ~2-3pp.
- **Iceberg eval:** evaluate growing Iceberg compatibility on chili-2's HDB layout per `docs/research/kdb_alternatives.md` §3.2. Research synthesis + ADR territory if a real gap surfaces.
- **(Optional) chili-py concurrent eval bench** — verify golden rule 5's 6.10× concurrent throughput hasn't regressed on py-1.39.3 polars.

**Sprint 12 prerequisite check:** verify P0 (GitHub-host fork) status before starting any chili-source-tree work; if not done, document the constraint in Sprint 12 brief and proceed only on docs/research items.
