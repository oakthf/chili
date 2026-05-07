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
| 2 (v2) | claude-2 baseline + features inventory (PIVOT) | Ratified 2026-05-07 | [`../history/sprints/sprint_2_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_2_dispatch_brief_2026-05-07.md) | [`sprint_2_retro.md`](sprint_2_retro.md) | 2 promoted (lesson 4 cherry-pick conflict accumulation in `f5889f1`; lesson 5 verify-framework-GIL-release in post-wrap commit) + 1 candidate (subagent context drift on superseded docs; not promoted yet) |
| 3 | additive feature port wave 1 (clippy unblock + 7 features + parse_cache bench gate) | Ratified 2026-05-07 (autonomous run; user pre-ratification) | [`../history/sprints/sprint_3_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_3_dispatch_brief_2026-05-07.md) | [`sprint_3_retro.md`](sprint_3_retro.md) | 2 (lesson 6 inventory-drift verification; lesson 7 reviewer-before-retro cadence) |
| 4 | additive feature port wave 2 — chili-py clippy unblock + ADR 0002 (`engine.eval(lazy=True)`) + bench harness validation (Part C downgraded; measurement deferred to Sprint 5) | Ratified 2026-05-07 (autonomous run; user pre-ratification) | [`../history/sprints/sprint_4_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_4_dispatch_brief_2026-05-07.md) | [`sprint_4_retro.md`](sprint_4_retro.md) | 2 (lesson 8 bench-compile cost in pp predict; lesson 9 xfail-strict-false convention) |
| 5 | polars pin + ADR 0003 PyLazyFrame DSL incompat + chili 0.8.0-claude2.1 wheel cut + mdata delivery handoff (Part B bench A/B sweep deferred to Sprint 7) | Ratified 2026-05-07 (autonomous run; user pre-ratification) | [`../history/sprints/sprint_5_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_5_dispatch_brief_2026-05-07.md) | [`sprint_5_retro.md`](sprint_5_retro.md) | 2 (lesson 10 ADR-structural-blocker; lesson 11 uv sync wheel-rebuild trigger) |
| 6 | deep housekeeping sweep (every-5-sprint cadence) | Ratified 2026-05-07 (autonomous run; cadence-rule-driven, no brief required) | — (cadence rule defines scope) | [`sprint_6_retro.md`](sprint_6_retro.md) | 0 (housekeeping doesn't promote new rules; harvests patterns into cadence_metrics) |
| 7 | ADR 0003 resolution + chili 0.8.1 wheel cut + bench A/B sweep (3 regressions surfaced as Sprint 8 backlog) | Ratified 2026-05-08 (autonomous run; user pre-ratification) | — (no formal brief; scope evolved interactively) | [`sprint_7_retro.md`](sprint_7_retro.md) | 3 (lesson 12 empirical-bisection-beats-version-guess; lesson 13 worktree-based A/B benchmark methodology; lesson 14 wheel-only install protocol for downstream consumers) |
| 8 | perf-pass-1 — P1 (parse_cache) resolved by re-measure as thermal noise; P3+P4 eval bench parser fix + A/B fill; P2 load_multitable profile deferred to Sprint 9 | Ratified 2026-05-08 (autonomous run; user pre-ratification) | [`../history/sprints/sprint_8_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_8_dispatch_brief_2026-05-08.md) | [`sprint_8_retro.md`](sprint_8_retro.md) | 2 (lesson 15 ±10% target re-measure; lesson 16 macOS bench profiling needs symbol-retention override) |
| 9 | perf-pass-2 — P7 + P2 symbolized profile captured (93% polars worker time on offset 0x450c; symbolic resolution infrastructure-blocked); P5 / P6 / P2-mitigation deferred to Sprint 12 | Ratified 2026-05-08 (autonomous run; user pre-ratification) | [`../history/sprints/sprint_9_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_9_dispatch_brief_2026-05-08.md) | [`sprint_9_retro.md`](sprint_9_retro.md) | 1 (lesson 17 macOS samply autonomous-run produces unsymbolicated profiles) |
| 10 | Pepper conformance to k9 design — ADR 0004 ratifies the existing shakti_analysis §4.3 conclusion (pepper retains Polars-aligned primitive set; does NOT track k9 minimization) | Ratified 2026-05-08 (autonomous run; user pre-ratification; ADR-only sprint) | — (no formal brief; roadmap pointer + research synthesis) | [`sprint_10_retro.md`](sprint_10_retro.md) | 0 (research synthesis ratification; lessons already captured in shakti_analysis.md Sprint 1) |

---

## Conventions

- **Status values:** `Planned` → `In progress` → `Wrapped (awaiting ratification)` → `Ratified` → `Superseded` (rare).
- **Brief column:** path to dispatch brief (live during the sprint, moves to `docs/history/sprints/` post-ratification).
- **Retro column:** path to `docs/sim/sprint_N_retro.md` (stays live indefinitely as project provenance).
- **Lessons promoted column:** count of entries promoted to `docs/standards/iteration_lessons.md` from this sprint's retro (often 0).

See `.claude/rules/sprint-cadence.md` for the full sprint protocol.
