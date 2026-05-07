# ADR-0002 — Python `engine.eval` lazy/eager default

**Date:** 2026-05-07 (drafted Sprint 2 v2 wrap; ratified per user direction same day; full
scoping + implementation lands Sprint 4)
**Status:** Accepted (option b — opt-in lazy via `lazy=False` default + `lazy=True`
parameter; user ratified 2026-05-07 in the post-Sprint-2-v2-wrap conversation)
**Cutover commits:** Sprint 4 (chili-py FFI surface adds the `lazy` parameter to
`engine.eval`; tests; docs).

---

## Context

`pyo3_polars::PyLazyFrame` provides Python ↔ Rust FFI for `polars.LazyFrame`. It's the
lazy counterpart of the eager `PyDataFrame` (which the chili-py FFI uses everywhere).
`claude-2` inherits full `PyLazyFrame` support natively from main:

- `crates/chili-py/Cargo.toml`: `pyo3-polars = { version = "0.26.0", features = ["lazy"] }`.
- `crates/chili-py/src/lib.rs:40`: `use pyo3_polars::{PyDataFrame, PyLazyFrame, PySeries};`
- `crates/chili-py/src/lib.rs:261`: `SpicyObj::LazyFrame(lf) => Ok(PyLazyFrame(lf).into_pyobject(py)?...)` — direct return path Python-side.

claude (parked-historical) **deliberately did NOT expose lazy returns to Python.** Its
`eval` impl eagerly materializes:
```rust
SpicyObj::LazyFrame(lf) => SpicyObj::DataFrame(lf.collect()?)
```
This was paired with `py.allow_threads()` wrapping the entire eager closure to
guarantee golden rule 5 (GIL released around `Engine::eval`; 6.10× concurrent
throughput).

The trade-off looked like: eager preserves golden rule 5; lazy enables cross-boundary
query optimization (predicate / projection pushdown) but supposedly forfeits GIL
release. **Sprint 2 v2 wrap conversation surfaced that this was wrong.** pyo3-polars
0.26's `LazyFrame.collect` impl wraps the heavy work in `py.allow_threads()` itself,
so GIL release is preserved on the lazy path too. Iteration lesson 5 (`docs/standards/
iteration_lessons.md` "Verify framework-level GIL-release behavior before scoping FFI
design around GIL") was promoted from this finding.

Three options surveyed:

- **(a) Stay eager-only.** Claude's behavior. Simplest. Loses cross-boundary
  optimization. Closest to current mdata behavior — zero refactor.
- **(b) Opt-in lazy via `lazy=False` default + `lazy=True` parameter.** Default
  preserves eager semantics for backward compat; advanced users opt-in for lazy
  chains. Both code paths preserve GIL release (lesson 5).
- **(c) Lazy-default.** Match upstream's API direction. Breaking change for mdata —
  every `engine.eval(...)` call needs `.collect()` appended.

---

## Decision

**Adopt option (b): opt-in lazy. Default `engine.eval(query, lazy=False)`; opt-in
`lazy=True` returns a `polars.LazyFrame`.**

Specifically:

1. Add `lazy: bool = False` parameter to `engine.eval` (chili-py PyO3 binding).
2. When `lazy=False` (default), behavior matches claude's: eagerly materialize the
   `LazyFrame` to a `DataFrame` inside the `py.allow_threads()` block; return
   `PyDataFrame`. Preserves golden rule 5; zero mdata refactor.
3. When `lazy=True`, return `PyLazyFrame` directly; let the Python caller chain
   ops + `.collect()` at their leisure. pyo3-polars' `LazyFrame.collect()` releases
   the GIL during heavy work (per lesson 5), so golden rule 5's concurrent-throughput
   property is preserved on the lazy path too — subject to A/B verification in
   Sprint 5.
4. Tests: existing eager-default tests unchanged. Add at least one lazy-path test
   that verifies (a) the returned object is a `polars.LazyFrame`; (b) `.collect()`
   produces the same result as the eager path; (c) a chained lazy operation
   (`.eval(q, lazy=True).filter(...).collect()`) executes without error.

---

## Consequences

### Binds future work

- `engine.eval`'s Python signature gains a `lazy` keyword parameter. mdata's existing
  callers (`engine.eval(query)`) continue to work unchanged.
- Sprint 4 implementation work: add the parameter, branch on it, return the right
  type. Estimated ~1-2pp port complexity (small).
- A/B comparison of concurrent throughput on the lazy path goes into Sprint 5's bench
  rebaseline doc — the 6.10× win on the eager path needs verification on the lazy
  path. If lazy regresses, document the gap; default stays eager so no live regression.
- ADR 0001 (pub/sub canonical model) is unaffected; pub/sub retirement direction
  is independent of this decision.

### Excludes

- **No silent default switch to lazy.** Any future move toward "lazy default"
  requires a new ADR (ADR 0003+) explicitly weighing the mdata-refactor cost.
- **No `lazy=True` for `write_partition` / `overwrite_partition`** — those are
  side-effecting writes, not analytics. Lazy makes no sense.

### Risks

1. **A/B might show lazy throughput regression** under specific workloads (e.g.,
   if pyo3-polars' `.collect()` doesn't release GIL aggressively enough on small
   queries with high call frequency). Mitigation: lazy is opt-in; eager default
   stays.
2. **Caller confusion about when results materialize.** Documentation in
   `crates/chili-py/README.md` (or equivalent) MUST clearly state the lazy=True
   return type + when `.collect()` is required.

---

## Alternatives considered

- **Option (a) — stay eager-only — rejected.** Loses cross-boundary optimization
  capability for advanced callers. Cheaper but less capable.
- **Option (b) — opt-in lazy — chosen (this ADR).** Best of both worlds: zero mdata
  refactor; advanced users opt-in; both paths preserve GIL release.
- **Option (c) — lazy-default — rejected.** Breaking change for mdata + every other
  caller. Would force `.collect()` everywhere. Too aggressive given GIL-release is
  preserved on opt-in path anyway.

---

## Cross-references

- **Iteration lesson driving this ADR:** `../standards/iteration_lessons.md` lesson 5
  ("Verify framework-level GIL-release behavior before scoping FFI design around
  GIL").
- **Inventory §7.2 (resolved by this ADR):** `../research/claude_only_features_inventory_2026-05-07.md` §7.2.
- **Sprint 4 brief (when authored):** Sprint 4 `dispatch_brief` will reference this
  ADR as the contract for the `lazy` parameter port.
- **Golden rule 5 (preserved by this ADR):** `CLAUDE.md` "Golden rules" §5 — GIL
  released around `Engine::eval`. Both paths (eager-default / lazy-opt-in) preserve
  this; A/B verifies.
- **Companion ADR (canonical pub/sub):** `0001-pub-sub-canonical-model.md`.
