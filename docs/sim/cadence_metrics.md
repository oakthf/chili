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
| 2 | claude-2 pivot (v1 halt + v2 ratify-and-execute) | 8–14 (v2 brief; v1+v2 implied 13–23) | ~20–22 | +73% to +91% vs v2 midpoint; ~+27% within implied total band | 1 (v1 halt → pivot to v2) | ~6 (Part A scope ratification, env-fix go-ahead, mid-Part-B answer for next steps, Part B continue ratification, Part C-D-E continue ratification, post-Part-A check-in) | 1 (bare main fmt diff in chili-parser/tests/chili/test_error.rs; fixed via cargo fmt --all in 4fbe5eb) + clippy still RED on claude-2 deferred to Sprint 3 | 0 | 2026-05-07 (Part E commit) |
| 3 | additive feature port wave 1 (clippy unblock + 7 features + parse_cache bench gate) | 10–15 (mid 12.5) | ~14 | 0% vs midpoint | 1 (Part E.1 unplanned: code-reviewer findings absorbed in-sprint) | 0 (autonomous run; user pre-ratified entire sprint chain) | 2 (Part B build-fail until `log = "0.4"` added to chili-op deps; Part C maturin doc-comment placement) | +14 (6 Rust integration parse_cache_test + 8 chili-py pytest) | 2026-05-07 (Part E.1 commit `b269ec0`) |
| 4 | additive feature port wave 2 — chili-py clippy unblock + ADR 0002 (`engine.eval(lazy=True)`) + bench harness validation (downgraded) | 9–14 (mid 11.5) | ~9 | −22% vs midpoint | 1 (Part C downgraded mid-flight from "measure 4 benches" to "validate compile only" after bench compile cost overran 2-3pp budget) | 0 (autonomous run; user observation only) | 0 (gates green throughout Parts A/B; Part C never had a gate to fail) | +6 chili-py pytest (4 xfailed for polars Python/Rust DSL skew, 2 passing for default + lazy=False) | 2026-05-07 (Part D commit) |

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
