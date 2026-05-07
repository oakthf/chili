# Sprints Index — chili

Status table for chili sprints, updated at each wrap. Forward-only: this index begins
with Sprint 1 under the new cadence (per `.claude/rules/sprint-cadence.md`, seeded
2026-05-06). Past chili work — the upstream `e9092ce..b0f20e5` merge (2026-04-26), the
bench phase sweep, the `docs/history/mdata-collab/` collaboration — is recorded in its
existing locations and is not retro-fitted into this table.

---

## Active / completed sprints

| Sprint | Theme | Status | Brief | Retro | Lessons promoted |
|---|---|---|---|---|---|
| 1 | Strategic research + main↔claude inventory | Ratified 2026-05-07 | [`../history/sprints/sprint_1_dispatch_brief_2026-05-06.md`](../history/sprints/sprint_1_dispatch_brief_2026-05-06.md) | [`sprint_1_retro.md`](sprint_1_retro.md) | 2 (API divergence + post-error disk-check) |
| 2 (v1) | mdata-foundation cherry-picks (TCP listener + serde9 fix + stats) | **Superseded 2026-05-07** — halted at Part A on the b20177c cherry-pick (FFI-rewrite divergence in chili-py/src/lib.rs); pivoted to claude-2 plan | [`../history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md`](../history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md) | — | 1 (cherry-pick conflict accumulation — invert the merge direction) |
| 2 (v2) | claude-2 baseline + features inventory (PIVOT) | Wrapped 2026-05-07; awaiting ratification | [`sprint_2_dispatch_brief_2026-05-07.md`](sprint_2_dispatch_brief_2026-05-07.md) | [`sprint_2_retro.md`](sprint_2_retro.md) | 1 promoted (cherry-pick conflict accumulation, in `f5889f1` mid-sprint) + 1 candidate (subagent context drift on superseded docs; not promoted yet) |

---

## Conventions

- **Status values:** `Planned` → `In progress` → `Wrapped (awaiting ratification)` → `Ratified` → `Superseded` (rare).
- **Brief column:** path to dispatch brief (live during the sprint, moves to `docs/history/sprints/` post-ratification).
- **Retro column:** path to `docs/sim/sprint_N_retro.md` (stays live indefinitely as project provenance).
- **Lessons promoted column:** count of entries promoted to `docs/standards/iteration_lessons.md` from this sprint's retro (often 0).

See `.claude/rules/sprint-cadence.md` for the full sprint protocol.
