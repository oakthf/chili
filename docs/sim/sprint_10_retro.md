# Sprint 10 retro — Pepper conformance to k9 design (ADR sprint)

**Wrap:** 2026-05-08
**Predicted:** 5–10 pp (per roadmap)
**Actual:** ~1.5 pp
**Variance:** −80% vs midpoint (7.5)
**Owner:** coordinator-solo (main Claude); no code-reviewer dispatch (no code changes; ADR-only sprint).
**Plan reference:** Roadmap [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 10 row + `shakti_analysis.md` §4.3 research synthesis.

No formal dispatch brief — Sprint 10 was a research-synthesis sprint following the roadmap pointer; the question (`should pepper track k9?`) was already answered in `shakti_analysis.md` §4.3, so the sprint's job was to *ratify* that research conclusion as a durable ADR.

---

## Scope shipped

**ADR 0004 — Pepper does NOT track k9 design simplification (commit `7d21760`)**

Status: Accepted. Pepper retains its current Polars-aligned primitive set; the k9 axiom of "minimize" doesn't apply because pepper's substrate is Polars (richer surface area than k9's Whitney runtime).

Three structural points the ADR captures:

1. **Where pepper aligns with Whitney's philosophy** (terseness, right-to-left, hardware-aggressive) — KEPT.
2. **Where pepper deliberately diverges** (minimal primitive set, macro-driven, no backward-compatibility) — KEPT DIVERGENT with explicit rationale.
3. **Revisit triggers** documented (Shakti/KDB-X adoption shifts; Polars contraction; explicit user request for pepper-mini).

The ADR also calls out what's NOT precluded:
- chili-op can absorb k9-inspired perf kernels (Sprint 12 perf-pass-3 territory).
- chili's REPL can borrow Whitney's one-screen aesthetic for *display*.

**Tests:** 166 Rust + 65 chili-py pytest (unchanged; ADR-only sprint).

**Bench delta:** none (no code touched).

---

## Lessons (durable)

No new durable lessons promoted this sprint. Sprint 10 was a synthesis-of-existing-research ADR — the lessons are the research itself (in `shakti_analysis.md`) which was authored Sprint 1 and is already durable.

The only meta-observation worth noting (not lesson-promoted): **research-synthesis-shaped sprints come in well below predicted band when the research is already done.** Sprint 10 predicted 5-10pp because the roadmap framed it as an ADR-PLUS-decision sprint; actual was 1.5pp because the decision was already made and the ADR is documentation. Future research-synthesis sprints whose source research already exists can be predicted at 1-3pp.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---:|---:|
| Read source research (`shakti_analysis.md` §4.3) | 0.5 | 0.5 |
| Author ADR 0004 | 1.0–2.0 | ~1 |
| Wrap (retro + cadence + sprints_index) | 1.5–2.0 | 0.5 (no code-reviewer; no brief move because no brief was written) |
| **(Originally predicted because brief assumed user-decision territory + scope-expanding revisit)** | (3-6 of buffer) | 0 |
| **Total** | **5–10** | **~1.5** |

Way below low-band (~−80%). Driver: the source research already concluded "no k9 conformance"; the ADR is documentation. The roadmap's "5-10pp" prediction assumed the sprint would re-litigate or scope-expand. Neither happened.

Position in band: well below low. Pattern continues: autonomous-run perf-pass / research-synthesis sprints (Sprints 8, 9, 10) consistently come in ~1.5-4pp vs predicted 5-12pp. Chili's autonomous-run-friendly sprint shape is "small and decisive" rather than "long and exploratory."

---

## What surprised

- **Sprint 10's question was already answered in Sprint 1's research.** `shakti_analysis.md` §4.3 (authored 2026-05-06 in Sprint 1) concluded pepper's divergence from k9 is justified. The roadmap (authored 2026-05-07 in Sprint 2 v2) listed Sprint 10 as ADR territory, suggesting the research conclusion needed ratification, not re-derivation. ADR 0004 closes that loop.
- **The ADR is essentially "we ratify what we already do."** Status: Accepted, but it's a "no change of direction" ADR. Future contributors get a documented "no, we don't do that" reference for any future "let's simplify pepper to match k9" proposal.
- **Sprint 10 ran 1.5pp.** Sprint 8 ran 4pp. Sprint 9 ran 2pp. The 7-day window utilization on autonomous-run perf+research sprints is structurally small.

---

## Cross-references

- **ADR 0004 (the deliverable):** [`../decisions/0004-pepper-vs-k9-design.md`](../decisions/0004-pepper-vs-k9-design.md)
- **Source research:** [`../research/shakti_analysis.md`](../research/shakti_analysis.md) §4.3
- **Sprint 9 retro (predecessor):** [`sprint_9_retro.md`](sprint_9_retro.md)
- **Cadence metrics row 10:** [`cadence_metrics.md`](cadence_metrics.md)
- **Roadmap Sprint 10 row:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md)

---

## Sprint 11 hand-off

Sprint 11 = mandatory deep housekeeping per cadence rule (every-5-sprint sweep, second occurrence after Sprint 6). Predicted 3-5pp. Standard scope:

- Doc tree audit; demote shipped Sprints 7-10 work to history.
- Update CLAUDE.md docs map; refresh state line.
- Populate cadence_metrics "Patterns observed" section deltas if 5+ rows since Sprint 6's pass.
- Recommend `/compact` at end.

Sprint 12 = perf-pass-3 + Iceberg eval + carry-over P2 (load_multitable mitigation pending symbolization infra) + carry-over P5 (parked-claude .pep re-bench, optional).
