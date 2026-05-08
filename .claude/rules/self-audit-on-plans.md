# Self-Audit on Plans

Applies to **chili**, **all agents that produce plans / proposals / sequencing docs**.

## What triggers the rule

This rule binds whenever you're about to deliver to the user any of the following:

- A `docs/proposals/<topic>.md` (any new file under `docs/proposals/`)
- A sprint dispatch brief (`docs/sim/sprint_N_dispatch_brief_<date>.md`)
- A roadmap update (any file under `docs/sim/roadmap*.md`)
- An ADR draft that touches more than one file's surface
- Any user-facing plan / proposal / sequencing doc that:
  - (a) names ≥ 3 work items / steps, OR
  - (b) cites costs in pp / wall-time, OR
  - (c) makes claims about current code state, OR
  - (d) sequences items as A → B with implied dependency

## The rule

Before delivering the plan to the user, **dispatch parallel review agents** by default. Don't wait for the user to ask.

Recommended audit shape (3 agents in parallel via a single message with multiple `Agent` tool calls):

| Agent | Subagent type | Job |
|---|---|---|
| Codebase scan | `Explore` | Find perf/correctness opportunities the plan missed by greppping current source |
| Adversarial review | `code-reviewer` | Verify the plan's technical claims against current code; flag mis-stated types, paths, costs, gain bands |
| Sequencing audit | `planner` | Check dependency ordering, missing prerequisites, missing categories, sprint sizing realism |

Each agent gets a self-contained prompt that names:
- The doc to audit (full path)
- Hard constraints (e.g., chili: no `#[global_allocator]`)
- Items already in the doc (so they don't re-derive)
- A specific output format (≤ 600 words, severity-tagged findings)

## What to do with audit results

1. If the audit surfaces material corrections (wrong sequencing, missing prereqs, mis-scoped items), **append an audit appendix to the doc** rather than rewriting from scratch. This preserves the original draft as audit trail and makes the corrections explicit. Use a `## Appendix — Independent audit (<date>)` heading with sub-sections for `Material corrections`, `Additional opportunities surfaced`, `Cross-cutting gates`, `Revised sequencing`, `Sprint sizing`.
2. If the audit surfaces minor refinements only (typos, clearer phrasing), fold them into the original draft.
3. **Always commit the audited version** — don't deliver an unaudited plan.

## When NOT to audit

Skip the parallel audit when:

- The plan is < 3 work items AND < 5pp total cost AND no current-state claims (e.g., "I'll fix this typo in foo.md" — not a plan).
- The user explicitly says "skip the audit, just give me the plan."
- You're updating an already-audited plan with a small refinement (e.g., adding one bullet to an existing tier).

## Cost

Three parallel agents at ~5pp each = ~15pp on the audit. That sounds heavy, but the alternative (delivering a plan with material errors that the user catches and bounces back) typically costs more — both in rework and in user-trust. For chili specifically: the 2026-05-08 perf-proposal incident's audit caught 6 missed perf opportunities + a wrong sequencing + an over-scoped item; the audit cost was justified.

## Why this rule exists

User observed (2026-05-08 perf-proposal incident): "I just asked you to do another round check on the plan and 6 missed perf opportunities. Why this happened — let's drill down to the root cause." Root cause: self-audit is structurally weak — the same context that drafted the plan can't reliably catch its own gaps. The fix is structural (force a different perspective), not "try harder."

This rule pairs with the global `~/.claude/rules/verify-before-claim.md` — that rule binds individual claims; this rule binds the artifact those claims live in.
