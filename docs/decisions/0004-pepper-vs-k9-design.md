# ADR-0004 — Pepper does NOT track k9 design simplification (substrate-divergence is intentional)

**Date:** 2026-05-08 (drafted Sprint 10).
**Status:** Accepted (this ADR ratifies the existing research conclusion in `shakti_analysis.md` §4.3 as a durable design decision; it does NOT propose a change of direction).
**Cutover:** None — pepper's current shape is the decision; the ADR's purpose is to lock in the rationale so future "should we simplify pepper to match k9?" questions have a documented "no, and here's why" reference.
**Supersedes:** None.
**Related:** `shakti_analysis.md` §4.3 (Whitney's design philosophy and its mapping to pepper); `q_kdb_landscape.md` (q-syntax substrate); `competitive_position_2026-05-06.md`.

---

## Context

Whitney's stated design philosophy across 30+ years (k → k7 → Shakti's k9):
terse, composable, minimal primitives; one-screen interpreters; no operator
precedence; right-to-left evaluation; macro-driven C; aggressive hardware
targeting; no patience for backwards compatibility. Each generation of k
has tended to **shrink** the primitive set, not grow it.

chili's pepper syntax inherits q's terseness, right-to-left, no-precedence
shape — the q-syntax community-acquisition layer for kdb+/q-aware users.
But pepper's *semantics* are Polars's: a much richer (and more numerous)
primitive set than k9 has. Pepper exposes Polars's group-by, lazy-eval,
window functions, schema metadata, and ~50+ aggregation primitives that
k9 deliberately does not have.

The research synthesis question raised by `shakti_analysis.md` §4.3 was:
**Should pepper track k9's design simplification (smaller primitive set,
match Whitney's "minimal" axiom) — or retain its larger Polars-aligned
primitive set?**

---

## Decision

**Pepper retains its current Polars-aligned primitive set. It does NOT
track k9's design simplification.**

`shakti_analysis.md` §4.3 already concluded this:

> The principal pepper divergences from k9 are justified, not accidental.
> Pepper is "q-syntax targeting Polars-semantics," which is structurally
> different from k9 ("k-syntax targeting Whitney-runtime"). The q-syntax
> overlap with k9 is a community-acquisition feature; the Polars semantics
> is the substrate-strength feature; the divergence is in the middle
> layer.

This ADR ratifies that research conclusion as a durable design decision.

### Where pepper aligns with Whitney's philosophy (kept)

| Axiom | Pepper status |
|---|---|
| Terse, q-like syntax | **Aligns.** Pepper uses q-style `select`, `update`, `where`, `by`, `from` |
| Right-to-left, no precedence | **Aligns.** Inherited from q |
| Hardware-aggressive | **Aligns.** Polars dispatches via SIMD-aware kernels; chili can add AVX-512 fast paths in chili-op |

### Where pepper deliberately diverges (kept divergent)

| Axiom | Pepper divergence | Why divergent is right |
|---|---|---|
| Minimal primitive set | Pepper exposes Polars's full primitive surface (~50+ aggregations, window functions, lazy-frame ops, schema introspection) — not minimal | Chili's target user is a Polars-aware Python user who wants q-syntax-on-top of Polars, NOT a k9 purist. The substrate strength is Polars; pepper's job is to make Polars accessible, not to hide Polars's surface area |
| Macro-driven implementation | chili is Rust + Polars; macros are Rust-style (e.g., `create_exception!`), not Whitney-style C macros | Whitney's macros target his particular runtime constraints (one-screen interpreter, single-file source). Rust's tooling already provides the equivalent leverage via traits + generics + macros |
| No backward-compatibility patience | chili maintains compatibility | mdata + future downstream consumers depend on chili's pepper shape staying stable. Whitney's "no compatibility" stance fits a single-author commercial context; chili's open-source community context is different |

---

## Consequences

### Binds future work

- **No "Pepper conformance to k9" sprint.** Sprint 10's roadmap entry was a research synthesis sprint, not an implementation sprint. This ADR closes the question.
- **Pepper continues to grow with Polars.** When polars adds new primitives (e.g., new window functions, new dtype-specific aggregations), pepper exposes them via natural q-style syntax. The k9 axiom of "minimize" doesn't apply.
- **The terse-syntax property is a hard constraint.** New pepper primitives that violate q-style terseness are rejected; the q-syntax overlap is the user-acquisition feature.

### Where this ADR could be revisited

This decision is durable but not permanent. Concrete revisit triggers:

1. **Shakti / KDB-X community adoption shifts** to the point that mdata's downstream users WANT k9-like minimalism more than Polars semantics. Currently the opposite is true (mdata wants Polars).
2. **Polars's primitive set undergoes a major contraction** — unlikely; the trajectory has been growth.
3. **A future chili user explicitly requests "pepper-but-minimal"** and provides a use case that justifies a separate `pepper-mini` syntax mode. Even then, this would be a NEW syntax (additional `[lib]` target?), not a replacement of pepper.

### What it DOESN'T preclude

- **chili-op can absorb k9-inspired performance kernels** (edge-decomposition, tightly-coded inner loops) where Polars's general-purpose kernels are slower. This is a chili-op implementation choice, NOT a pepper syntax choice. Sprint 12 perf-pass-3 is the natural home for any such effort.
- **chili's REPL ergonomics can borrow Whitney's "one-screen" aesthetic** (e.g., compact stats output, dense column formatting) — this is rendering / display, not language design.

---

## Cost / value

**Cost of this ADR:** ~1pp (sprint 10) — purely research synthesis +
documentation. No code changes. No mdata refactor. No wheel re-cut.

**Value of this ADR:**

- **Locks in the strategic direction** — future sprints don't re-litigate "should pepper be more like k9?"
- **Provides a referenceable rationale** when a contributor proposes a "minimize pepper primitives" change. The ADR's "where this could be revisited" section gives them concrete triggers to argue for, OR confirms the change is out-of-scope.
- **Captures the substrate-divergence framing** — pepper's strength is its Polars substrate, NOT its k-syntax overlap. This is a non-obvious structural insight worth preserving.

**Cost of NOT having this ADR:** ~3-5pp per future sprint that re-derives the
same conclusion under sprint pressure (e.g., a contributor proposing
"let's match k9 better" without context, prompting a multi-sprint debate).
Sprint 10 spends 1pp now to save N×3-5pp over the project lifetime.

---

## Cross-references

- `docs/research/shakti_analysis.md` §4.3 — the research synthesis this ADR ratifies.
- `docs/research/q_kdb_landscape.md` — pepper's q-syntax substrate.
- `docs/research/competitive_position_2026-05-06.md` — chili's strategic positioning vs kdb+/Shakti/KDB-X.
- ADR 0001 (pub/sub canonical model) — companion ADR that locked in main's tick/sub framework.
- Sprint 10 retro: `../sim/sprint_10_retro.md` (lands at Sprint 10 wrap).
