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

## D-001 — W3 gate-clear signal (mdata) — RESOLVED

- **Status:** Resolved (2026-05-24).
- **Raised:** 2026-05-24.
- **Context:** The 0.8.8 delivery doc `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md` §W3 deferred W3 (Python-callable bridge) with explicit re-evaluation gate: "(a) mdata's v1-36 attach-socket cutover specifically blocks on it AND poll-on-variable proves insufficient, OR (b) chili-team has dedicated bandwidth." This row exists because the gate-clear signal arrived via in-session verbal conversation, not via `.cross_comms/inbox/`, and the written record needs to exist before Sprint 23 commits.
- **Options weighed:**
  - (i) wait for written notification via vantage-bus — slow; mdata is already blocked
  - (ii) proceed on the verbal signal + record it here (chosen)
- **Resolution:** Sprint 23 opens. User stated in this planning conversation (2026-05-24): "mdata side wants to await W3 before their migration they have other stuff to handle at the moment." Recorded here as the operative gate-clear. ADR-0007 pre-drafted at Sprint-23 gate #0; pre-impl notification + API contract sent to mdata via `.cross_comms/outbox` `design_question` for any objection before Part A starts.
- **Cross-references:** ADR-0007 §Context; Sprint 23 brief §Why-now §1 + §Appendix MC-2; 0.8.8 delivery doc §W3.

<!-- Next open decision goes here. -->
