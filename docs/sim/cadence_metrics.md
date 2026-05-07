# Cadence Metrics — sprint pp + variance tracking

Per-sprint metrics record for estimation calibration over time. Filled in at each sprint
wrap; **never overwritten**. The table is the historical record — past rows stay in
their original shape even after the canonical shape evolves. Shape adapted from mdata's
`docs/sim/cadence_metrics.md`.

Forward-only: this table begins with Sprint 1 under the new cadence (per
`.claude/rules/sprint-cadence.md`, seeded 2026-05-06). Past chili work — the upstream
`e9092ce..b0f20e5` merge (2026-04-26), the bench phase sweep — is recorded in its
existing locations and is not retro-fitted into this table.

---

## Sprint metrics table

| Sprint | Theme | Pred pp | Actual pp | Variance % | Mid-sprint pivots | User-touch | Gate defects | Test count delta | Wrap timestamp |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | Strategic research + main↔claude inventory | 22–35 | ~25 | −11% (low edge) | 0 | 0 (kickoff/ratification only; no mid-sprint user msg) | 0 (research; no Rust touched) | 0 | 2026-05-07 ~00:10 SGT |

---

## Field definitions

- **Sprint:** Numeric or named identifier (e.g., `1`, `2`, `Housekeeping`).
- **Theme:** Short label for the sprint's scope (e.g., "parse-cache micro-opt",
  "load_tree implementation", "py FFI hardening").
- **Pred pp:** Predicted token-cost range from the locked dispatch brief
  (`docs/sim/sprint_N_dispatch_brief_<date>.md`). Pp = 5h-window percentage points
  per `~/.claude/rules/work-metrics.md`.
- **Actual pp:** 5h-window delta from sprint kickoff to wrap (token-meter integration
  per `~/.claude/rules/work-metrics.md` if available; else best-effort estimate).
- **Variance %:** (actual midpoint − predicted midpoint) / predicted midpoint × 100.
  Negative = under-spent.
- **Mid-sprint pivots:** Count of times the coordinator changed sprint scope or
  direction mid-sprint without user input.
- **User-touch:** Count of user messages exchanged during the sprint (kickoff +
  ratification messages excluded; in-sprint communication only).
- **Gate defects:** Count of pre-commit-gate failures during the sprint
  (`cargo fmt` / `cargo clippy` / `cargo test` / `uv run pytest` issues that needed a
  fix before commit).
- **Test count delta:** Net change in `cargo test` + `pytest` count from sprint start
  to wrap.
- **Wrap timestamp:** Local time the final commit landed.

---

## How to update

At each sprint wrap:

1. Add a row to the table with all 10 fields.
2. Capture sprint-specific lessons in the per-sprint retro
   (`docs/sim/sprint_N_retro.md`).
3. Promote durable, cost-quantified lessons to `docs/standards/iteration_lessons.md`
   per the 4-field shape — see `.claude/rules/sprint-cadence.md`.
4. Add a "Patterns observed" section below once enough rows accumulate to spot
   calibration drift (typically after sprint ~5).

---

## Patterns observed

_(populate once 5+ rows are present)_
