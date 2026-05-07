# Sprint Cadence — dispatch brief, retro, cadence-metrics

Applies to **every sprint** in chili. Adapted from mdata's pattern (which derives from
nxcar) and tuned for chili's Rust-first surface. Forward-only: this rule binds new
sprints; past phase work (`docs/bench/phase{1..7,9}.md`, the 2026-04-26 upstream merge,
etc.) stays in its existing shape and is not retro-fitted.

## The four artifacts of a sprint

Every sprint produces these in order:

| When | Artifact | Path |
|---|---|---|
| Kickoff | Dispatch brief | `docs/sim/sprint_N_dispatch_brief_<date>.md` |
| Mid-sprint | (no artifact; informal status if requested) | — |
| Wrap | Retro | `docs/sim/sprint_N_retro.md` |
| Wrap | Cadence-metrics row appended | `docs/sim/cadence_metrics.md` |
| Post-ratification | Dispatch brief moved to history | `docs/history/sprints/sprint_N_dispatch_brief_<date>.md` |

Templates live at `docs/sim/_dispatch_brief_template.md` and `docs/sim/_retro_template.md`
(underscore-prefixed to disambiguate from real sprint files).

## When a dispatch brief is required

- **Required:** any sprint ≥ 3pp predicted.
- **Optional:** ≤ 1pp single-issue hotfixes (track in commit message; retro still recommended).
- **Required regardless:** any sprint that touches the on-disk storage schema, the FFI
  surface in `chili-py`, or the parse-cache hot path (the load-bearing surfaces called
  out as Golden Rules in `CLAUDE.md`).

## Dispatch brief contents

Use the template at `docs/sim/_dispatch_brief_template.md`. Required sections:

- Sprint objective + binary success criterion.
- Why now (gate conditions cleared; downstream unblockers).
- Scope per sub-priority with surface additions.
- Out of scope (deferred items).
- Deliverables table.
- Lead allocation (subagent spawns; worktree usage if parallel).
- Mid-checkpoint plan with halt-and-escalate criteria (4 triggers — see template).
- Wrap ceremony.
- Pp accounting reference (compare to `docs/sim/cadence_metrics.md`).

## Retro contents

Use the template at `docs/sim/_retro_template.md`. Required sections:

- Wrap timestamp + Predicted / Actual / Variance / Owner.
- Scope shipped with originating commit SHAs.
- Lessons (durable) — 4-field entries (Rule · Why · Apply where · Cost saved); durable
  lessons promote to `docs/standards/iteration_lessons.md`.
- Pp accounting per sub-priority.
- What surprised (small surprises not warranting durable rule).
- Cross-references.

## Cadence-metrics update

At every sprint wrap, append one row to `docs/sim/cadence_metrics.md` with the 10 fields:

`Sprint | Theme | Pred pp | Actual pp | Variance % | Mid-sprint pivots | User-touch | Gate defects | Test count delta | Wrap timestamp`

Don't overwrite past rows. The table is the historical record for estimation calibration.

## Commit conventions

- **Implementation commits** during a sprint: `feat(<scope>): <short>` or `fix(<scope>): <short>`.
- **Multi-chunk commits** within one sprint: split by logical surface (e.g.,
  `feat(chili-core): ...`, `feat(chili-py): ...`).
- **Retro commit:** `docs(sim): sprint N retro` — single commit per sprint.
- **Cadence-metrics update:** can land in the retro commit (not a separate one).
- **Branch:** `claude` only — see CLAUDE.md "Branch policy". Never commit to `main`.

## Pre-commit gate (per sprint)

The chili pre-commit gate (matches `Taskfile.yml` and CLAUDE.md):

```sh
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test
```

Python-bindings work additionally runs from `crates/chili-py/`:

```sh
uv run maturin develop && uv run pytest
```

Then the staged-file audit from `~/.claude/rules/git-commit-hygiene.md` — never `git add -A`.

## Periodic deep housekeeping (every 5 sprints) + post-sweep `/compact`

Per `~/.claude/rules/claude-md-housekeeping.md`:

- **Every 5 sprints since the last sweep, schedule a deep housekeeping** in the same
  wrap session. Count rows in `docs/sim/cadence_metrics.md` past the prior sweep
  wrap-commit; on the 5th, the next wrap MUST trigger the sweep before kicking off the
  next sprint.
- **At the end of every housekeeping sweep**, recommend the user run `/compact` in the
  same session before continuing other work. Sweeps generate large transcripts (file
  triage chatter, intermediate moves) that aren't relevant to the next task; compacting
  frees the context window so the next sprint starts clean.

Both triggers are **mandatory**, not optional.

## Why this rule exists

Without consistent cadence artifacts, estimation drifts (no historical baseline) and
lessons are lost (each sprint relearns). mdata and nxcar both shipped this convention
after the same lesson; chili adopts it as the new baseline starting from this commit.
Past chili work (the upstream-merge milestone, the bench phase sweep) is not
retro-fitted into this scheme.
