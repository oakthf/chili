# Decisions Needed

Working dashboard for cross-cutting decisions awaiting user input. Smaller-scope
tactical decisions that don't warrant a full ADR (`docs/decisions/<NNNN>-<slug>.md`)
live here until resolved or escalated. Adapted from mdata's `docs/sync/decisions-needed.md`
shape.

The two binners (per onboarding §10.4):

- **Reversible decisions** — write an ADR. Status starts `Draft`, flips to `Accepted`
  after user reviews. Don't gate other work on a Draft ADR unless you must.
- **Irreversible decisions** (data-loss risk, cross-project contract, on-disk format
  change) — drop a row here and **halt for explicit user direction**. No agent
  self-ratifies an irreversible decision.

Both files live forever. Old ADRs get linked from architecture docs, not deleted.
This file is a working dashboard — closed rows get a 1-line outcome and stay (audit
trail).

---

## Format

Each decision is a short markdown section:

- **D-NNN — Title**
- **Status:** Open / Resolved / Superseded
- **Raised:** date
- **Context:** brief description
- **Options weighed:** the candidate paths
- **Resolution (when made):** what was picked + by whom
- **Cross-references:** docs/code/discussions related

Once Resolved, leave in this file as a record. Next housekeeping sweep can move the
file to `docs/history/sync/decisions-needed_<date>.md` if it grows unwieldy.

---

<!-- First open decision goes here. -->
