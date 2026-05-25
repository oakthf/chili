# Sprint Cadence — dispatch brief, retro, cadence-metrics

Applies to **every sprint** in chili. Adapted from mdata/nxcar's pattern, tuned for
chili's Rust-first surface. Forward-only: binds new sprints; past phase work
(`docs/bench/phase{1..7,9}.md`, the 2026-04-26 upstream merge) stays in its existing
shape and is not retro-fitted.

## The four artifacts of a sprint

| When | Artifact | Path |
|---|---|---|
| Kickoff | Dispatch brief | `docs/sim/sprint_N_dispatch_brief_<date>.md` |
| Mid-sprint | (informal status if asked) | — |
| Wrap | Retro | `docs/sim/sprint_N_retro.md` |
| Wrap | Cadence-metrics row appended | `docs/sim/cadence_metrics.md` |
| Post-ratification | Dispatch brief moved to history | `docs/history/sprints/sprint_N_dispatch_brief_<date>.md` |

Templates: `docs/sim/_dispatch_brief_template.md`, `docs/sim/_retro_template.md`
(underscore-prefixed to disambiguate from real sprint files).

## When a dispatch brief is required

- **Required:** any sprint ≥ 3pp predicted.
- **Optional:** ≤ 1pp single-issue hotfixes (track in commit message; retro still recommended).
- **Required regardless:** any sprint touching on-disk storage schema, the chili-py FFI
  surface, or the parse-cache hot path (Golden Rules 4 / 5 / 6 in CLAUDE.md).

## Brief / retro contents

Use the templates. Briefs name: objective + binary success criterion, why-now, scope
per sub-priority with surface additions, out-of-scope, deliverables table, lead
allocation (subagents/worktrees), mid-checkpoint plan with 4 halt-and-escalate
triggers, wrap ceremony, pp accounting reference.

Retros name: wrap timestamp + Predicted/Actual/Variance/Owner, scope shipped with
commit SHAs, durable lessons (4-field — Rule · Why · Apply where · Cost saved;
promote to `docs/standards/iteration_lessons.md`), pp accounting per sub-priority,
what surprised, cross-references.

## Cadence-metrics update

Append one row at every wrap with the 10 fields:

`Sprint | Theme | Pred pp | Actual pp | Variance % | Mid-sprint pivots | User-touch | Gate defects | Test count delta | Wrap timestamp`

Never overwrite past rows — the table is the historical record for estimation calibration.

## Commit conventions

- **Implementation:** `feat(<scope>): <short>` / `fix(<scope>): <short>`.
- **Multi-chunk within one sprint:** split by logical surface (`feat(chili-core): ...`, `feat(chili-py): ...`).
- **Retro:** `docs(sim): sprint N retro` — single commit per sprint.
- **Cadence-metrics update:** can land in the retro commit.
- **Branch:** `claude-2` only — see CLAUDE.md "Branch policy".

## Pre-commit gate (per sprint)

See CLAUDE.md "Pre-commit gate" for the full command + rationale. In short: cargo
fmt+clippy+test (workspace excluding chili-py), then `cd crates/chili-py && uv run
maturin develop && uv run pytest` for Python-bindings work, then the staged-file
audit from `~/.claude/rules/git-commit-hygiene.md` — never `git add -A`.

**Release wheel cuts** (sprint deliverable to mdata) — see CLAUDE.md Golden Rule 7
for the `-o ../../dist` invariant + prior-wheel deletion mandate.

## Periodic deep housekeeping (every 5 sprints) + post-sweep `/compact`

Per `~/.claude/rules/claude-md-housekeeping.md`:

- **Every 5 sprints since the last sweep, schedule a deep housekeeping** in the same
  wrap session. Count rows in `docs/sim/cadence_metrics.md` past the prior sweep
  wrap-commit; on the 5th, the next wrap MUST trigger the sweep before kicking off
  the next sprint.
- **At the end of every housekeeping sweep**, recommend the user run `/compact` in
  the same session before continuing other work — sweeps generate large transcripts
  (triage chatter, intermediate moves) that aren't relevant to the next task;
  compacting frees the context window so the next sprint starts clean.

Both triggers are **mandatory**, not optional.
