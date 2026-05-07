# ADR-0003 — PyLazyFrame DSL hash incompatibility (Python ↔ Rust polars boundary)

**Date:** 2026-05-07 (drafted Sprint 5 Part A; ADR opened on structural blocker discovery)
**Status:** Accepted (defer-resolution; pin stock Python polars; xfail lazy-return tests; revisit when pyo3-polars publishes a release that decouples DSL hash from polars-core source identity)
**Cutover:** Sprint 5 (Part A pin lands the resolution; lazy-return path remains xfailed end-to-end on the FFI boundary)
**Supersedes:** None.
**Related:** ADR 0002 (lazy/eager default — Option b ratified Sprint 4; the Rust side ships, the Python ↔ Rust transfer is broken by this ADR).

---

## Context

Sprint 4 Part B implemented ADR 0002 Option (b): `engine.eval(query,
lazy=True)` on the Python side returns a `polars.LazyFrame`. The Rust
implementation in `crates/chili-py/src/lib.rs` correctly produces a
`pyo3_polars::PyLazyFrame` and hands it across the FFI boundary.

The FFI boundary fails. Python polars (any version 1.20–1.39 tested)
raises `polars.exceptions.ComputeError: deserialization failed`:

```
given DSL_SCHEMA_HASH: 17d5de6d3a8db58816b302a24c286a9f2babda6858d19968dfdcc1ab5ed834c1
is not compatible with this Polars version which uses DSL_SCHEMA_HASH:
124a68a58d58334d40ed37d4c05b69a962606057b5ca51b0fe84836ff26aff0d
```

(Polars older than ~1.30 fails earlier with `DSL_VERSION mismatch (24.0 vs
4.1)`; newer versions hit the schema-hash compare.)

The mismatch is **structural**, not version-fixable on the Python side.
The chili workspace uses a custom Rust polars-core fork:

```toml
# Cargo.toml (workspace)
[patch.crates-io]
polars-core = { git = "https://github.com/hinmeru/polars-core-patch.git", tag = "v0.53.0" }
```

This fork (commit `6c64273d`) is used by chili to enable polars-core
behaviors that aren't yet in upstream stock polars at the v0.53.0 tag
(or to backport upstream fixes). The fork's `polars-plan` source therefore
hashes to a different `DSL_SCHEMA_HASH` than stock polars 0.53.0's
`polars-plan` would. Stock Python polars (built against stock Rust polars
matching that polars-plan source) embeds a DSL hash that doesn't match
the chili-built Rust binary's DSL hash.

This means **PyLazyFrame round-trip is not possible** between
`chili-py`'s cdylib and any standard `pip install polars` Python wheel.
PyDataFrame round-trip (Arrow IPC under the hood) is unaffected — Arrow
IPC has a stable cross-version ABI that the DSL serialization layer
doesn't.

---

## Decision

**Pin Python polars to `1.39.3` in `crates/chili-py/pyproject.toml` (the
latest tested-stable). Document the DSL incompatibility. Keep the four
lazy-return pytest tests marked `pytest.mark.xfail(strict=False)` until
the resolution lands. Defer resolution to a future sprint when one of
the following is achievable:**

(a) **pyo3-polars upstream publishes a release that decouples DSL hash
from polars-core source identity** — i.e., transfers LazyFrame via Arrow
IPC or a stable serialized form, not DSL deserialization. This would be
the cleanest fix; tracks pyo3-polars' issue tracker.

(b) **chili stops using the polars-core-patch fork** — the fork's
non-trivial behaviors (which the chili author chose to backport) would
need to be either upstreamed to stock polars or replaced with chili-side
implementations. Likely the heavier path; the patch exists for good
reasons.

(c) **Custom-build Python polars matching chili's Rust polars build** —
ship a chili-specific Python polars wheel built from the
hinmeru/polars-core-patch'd polars source. Adds a non-trivial publish/
install pipeline to chili for marginal lazy=True benefit (eager path
already works).

Sprint 5 Part A applies (a)-track preparatory work: pin Python polars to
1.39.3 (so when pyo3-polars publishes the fix, an upgrade path is
clear); document via this ADR; keep xfail markers ready to auto-XPASS
when the fix lands.

---

## Consequences

### Binds future work

- `engine.eval(lazy=True)` returns a `polars.LazyFrame` object that
  cannot be `.collect()`'d, `.filter()`'d, or otherwise interacted with
  on the Python side until DSL incompatibility resolves. Effectively
  `lazy=True` is documented but **not usable** for end-to-end Python
  workflows until ADR 0003 resolution lands.
- mdata's chili-py consumers stay on `lazy=False` (the default; eager
  collection inside `py.detach`). This matches mdata's existing usage —
  no breaking change for them.
- Sprint 5 wheel cut ships with `lazy=True` documented + xfail-marked.
  When ADR 0003 resolution lands (option a/b/c), a follow-up wheel cut
  removes the xfail.

### Operational behavior

- Eager (default) path: works end-to-end, GIL released, golden rule 5
  preserved. No user-visible change.
- Lazy path: returns a Python `polars.LazyFrame` instance; calls on it
  fail with `ComputeError`. Users should treat `lazy=True` as a
  Sprint-5-era stub until ADR 0003 resolves.

### Roll-forward recovery

When pyo3-polars publishes a release that fixes DSL transfer (option a):
1. Bump `pyo3-polars = "0.27"` (or higher) in `crates/chili-py/Cargo.toml`.
2. Re-run `crates/chili-py/tests/test_engine.py::TestEvalLazy` — xfail
   markers should XPASS automatically (per `strict=False`).
3. Remove the four `pytest.mark.xfail(...)` decorators in favor of plain
   asserts.
4. Update this ADR's Status to "Superseded by Sprint <X> upgrade."
5. Update `docs/sync/mdata_chili_2026-05-07_delivery.md` (if still live)
   to note the lazy-path is now usable.

### Eager-only semantics for production callers

For mdata + any chili-py consumer where lazy=True isn't critical:
- `engine.eval(query)` stays the recommended call.
- `engine.eval(query, lazy=True)` is a stub that should not be wired
  into production paths until ADR 0003 resolves.

---

## Cost / value

**Cost of pinning + xfailing (option chosen):** ~0pp this sprint (Sprint 5
Part A's polars-pin scope absorbs it). No mdata refactor; no breaking
changes; no lost features (eager path covers all current production use).

**Cost of resolution (when it lands):**
- Option (a): trivial pyo3-polars version bump (~0.5pp).
- Option (b): non-trivial chili-side rework (~5-15pp).
- Option (c): infrastructure work (~5-10pp + ongoing publish maintenance).

**Value of lazy=True path:**
- Cross-boundary predicate / projection pushdown for chained Python ops.
- Plan introspection (already covered by `query_plan` separately).
- Composability with other polars LazyFrame consumers.

For mdata's current usage pattern (eager DataFrame results), the lazy=True
path adds nothing. For future consumers wanting LazyFrame chaining, ADR
0003 resolution is a hard prerequisite.

---

## Cross-references

- ADR 0002 (lazy/eager default; this ADR documents the Python-side blocker
  for ADR 0002's Option b): [`0002-eval-lazy-eager-default.md`](0002-eval-lazy-eager-default.md).
- Sprint 4 retro (where this was first surfaced as XFAIL):
  [`../sim/sprint_4_retro.md`](../sim/sprint_4_retro.md).
- Sprint 5 brief Part A (where the pin landed):
  [`../history/sprints/sprint_5_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_5_dispatch_brief_2026-05-07.md).
- pyo3-polars upstream: <https://github.com/pola-rs/pyo3-polars>.
- hinmeru/polars-core-patch fork (the DSL-divergence source):
  <https://github.com/hinmeru/polars-core-patch.git#v0.53.0>.
- Iteration lesson 9 (xfail strict=False convention) — applies here:
  [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md).
