# ADR-0003 — PyLazyFrame DSL hash incompatibility (Python ↔ Rust polars boundary)

**Date:** 2026-05-07 (drafted Sprint 5 Part A; ADR opened on structural blocker discovery; **amended 2026-05-08 Sprint 7 Part A** with corrected root cause + resolution via option 3b).
**Status:** **RESOLVED 2026-05-08** Sprint 7 Part A — chili-side polars fork at `pola-rs/polars` `py-1.39.3` tag with q-style fmt patch on top; pyo3-polars sourced from the same monorepo. All 4 previously-xfailed lazy tests now XPASS; xfail markers removed.
**Cutover:** Sprint 7 Part A.
**Supersedes:** None.
**Related:** ADR 0002 (lazy/eager default — Option b ratified Sprint 4; Sprint 7 made the lazy path actually usable end-to-end).

---

## 2026-05-08 amendment — corrected root-cause analysis (Sprint 7 Part A discovery)

The original ADR 0003 (Sprint 5) **misdiagnosed the root cause**. Investigation
in Sprint 7 Part A surfaced three corrections:

### Correction 1: the hinmeru `polars-core-patch` fork was a red herring

ADR 0003 originally claimed the DSL skew was caused by chili's
`[patch.crates-io.polars-core] = hinmeru/polars-core-patch.git#v0.53.0`.
**That was wrong.** Hard evidence:

```
$ diff -r /tmp/polars-core-patch-fork/src \
         ~/.cargo/registry/.../polars-core-0.53.0/src
Only difference: src/fmt.rs (~30 lines, q-style Datetime/Duration display)
$ cmp /tmp/polars-core-patch-fork/Cargo.toml \
      ~/.cargo/registry/.../polars-core-0.53.0/Cargo.toml
IDENTICAL
```

The fork's only delta vs crates.io polars-core 0.53.0 is `fmt.rs` (purely
Display formatting). `DSL_SCHEMA_HASH` lives in **`polars-plan`**, NOT
`polars-core` — a different crate entirely. The fork couldn't possibly
have been the cause of the DSL skew.

### Correction 2: actual root cause is `polars-plan` source-version skew between Rust and Python sides

- Rust side: chili's `pyo3-polars 0.26.0` (crates.io) → transitive dep
  `polars-plan 0.53.0` (crates.io, source fixed at the rs-0.53.0 tag,
  published 2026-02-08).
- Python side: `polars==1.39.3` (PyPI, uploaded 2026-03-20) bundles a
  `polars-plan` built from the polars monorepo at the `py-1.39.3` commit
  — **6 weeks newer than the rs-0.53.0 tag**.

Diff confirmed: 10+ files in `polars-plan/src/dsl/` differ between the two
versions (`builder_dsl.rs`, `datatype_expr.rs`, `dt.rs`, `expr/mod.rs`,
`file_scan/mod.rs`, etc.). The DSL_SCHEMA_HASH is computed at compile
time from a hash file in the polars-plan source tree; different sources
produce different hashes. The two sides therefore can't deserialize each
other's lazy-plan blobs.

### Correction 3: pyo3-polars upstream is archived

`github.com/pola-rs/pyo3-polars` HEAD is the `archive` commit dated
2025-07-28. The standalone repo is no longer maintained. pyo3-polars
functionality has been **vendored into the main polars monorepo** at
`pola-rs/polars/tree/main/pyo3-polars`. The README of the archived repo
states this explicitly. ADR 0003's original "wait for pyo3-polars 0.27"
path is therefore dead — there will never be a pyo3-polars 0.27 from the
old repo.

---

## Resolution (Sprint 7 Part A, commit `<this commit>`)

**Option 3b ratified and executed:**

1. Cloned `pola-rs/polars` at the `py-1.39.3` tag to `/tmp/polars-py-1.39.3`.
2. Applied a single 30-line q-style fmt patch to `crates/polars-core/src/fmt.rs`
   (port of the hinmeru fork's only meaningful delta). Committed as
   `8d56f02` in the local clone.
3. Replaced chili's workspace `[patch.crates-io.polars-core]` (single
   crate, hinmeru fork) with `[patch.crates-io]` block covering all 21
   `polars-*` crates pointing at `/tmp/polars-py-1.39.3/crates/<name>`.
4. Replicated the same patch block in `crates/chili-py/Cargo.toml`
   (chili-py is excluded from workspace; needs its own patch block).
5. Added `pyo3-polars = { path = "/tmp/polars-py-1.39.3/pyo3-polars/pyo3-polars" }`
   to chili-py's patch block — the in-tree pyo3-polars 0.26.0 in the
   polars monorepo is API-consistent with py-1.39.3 (the standalone
   crates.io pyo3-polars 0.26.0 still requests `compute_boolean` polars-arrow
   feature that was removed/folded post-0.53.0).
6. Bumped `chrono` to `0.4.44` in Cargo.lock to satisfy py-1.39.3's
   `^0.4.42` requirement.
7. Patched `crates/chili-op/src/df.rs` LazyFrame::pivot call to add the
   new `PivotColumnNaming::Auto` parameter (API drift between rs-0.53.0
   and py-1.39.3).

### Verification

```
cargo build --workspace --exclude chili-py: GREEN
cargo test --workspace --exclude chili-py: 166 / 0 failed
uv run maturin develop: GREEN (2m15s wall)
uv run pytest: 65 passed, 0 xfailed (was 60 passed + 4 xfailed pre-Sprint-7)
```

The 4 previously-xfailed `TestEvalLazy` tests now pass:
- `test_eval_lazy_true_returns_lazyframe` ✓
- `test_eval_lazy_true_collect_round_trips_data` ✓
- `test_eval_lazy_true_chains_filter` ✓ (predicate pushdown across FFI works)
- `test_eval_lazy_default_on_lazy_engine_still_lazy` ✓

Xfail markers removed; tests are now plain assertions.

---

## Outstanding migration work (NOT blocking lazy=True usage; tracked separately)

The path-based `[patch.crates-io]` requires `/tmp/polars-py-1.39.3` to
exist on the build machine. Acceptable for the autonomous-run experiment
that confirmed the resolution. **For production / CI / multi-developer
use**, the local clone needs a stable home:

1. **Host the patched fork on GitHub.** User-driven step: push
   `/tmp/polars-py-1.39.3` (with the q-style fmt commit on top of
   py-1.39.3) to a chili-author or chili-org repo. Then change all
   `path = "/tmp/..."` lines to `git = "..."` + `tag = "..."` /
   `rev = "..."` in both Cargo.toml patch blocks.
2. **Document the polars-version-bump procedure.** Each time chili wants
   to track a newer Python polars wheel: re-clone polars at the new
   `py-X.Y.Z` tag, re-apply the q-style fmt patch (~30 lines, low drift
   risk), update the chili Cargo.toml git rev. Estimated cost per bump:
   0.5pp.
3. **Long-term: migrate from pyo3-polars to polars-python.** The official
   PyO3 binding is now `polars-python` inside the polars monorepo.
   chili-py uses pyo3-polars's `PyDataFrame`/`PyLazyFrame`/`PySeries`
   types; these have polars-python equivalents but the API differs.
   Estimated migration cost: ~5-10pp; not blocking; best landed when
   chili next bumps the Python polars target.

---

## Original ADR text (preserved for provenance)

The text below is the **original (pre-amendment) ADR 0003** as drafted
in Sprint 5 Part A. The "Decision" section's option-(b) cost estimate
of 5-15pp is now known to be inflated; the actual resolution (option 3b
above) cost ~5pp in Sprint 7 Part A. The "Resolution paths" listing is
also outdated: option (a) "wait for pyo3-polars 0.27" is dead because
the upstream repo was archived.

---

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

Sprint 5 Part A pins Python polars to 1.39.3 — the latest tested-stable
on macOS arm64 with a clean `uv pip install`. The pin's value is **eager
path test-result reproducibility**, not lazy-path preparedness; when
pyo3-polars publishes a fix (option a), the pin is irrelevant to the
upgrade path and may be bumped or removed at that time. Document the
structural blocker via this ADR; keep xfail markers ready to auto-XPASS
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
- Lazy path: returns a Python `polars.LazyFrame` instance (the
  `isinstance(out, pl.LazyFrame)` check passes; cheap LazyFrame metadata
  ops may also work). The failure point is on the FIRST call that
  attempts to deserialize the underlying DSL — typically `.collect()`,
  `.explain()`, or `.show_graph()` — which raises
  `polars.exceptions.ComputeError: deserialization failed`. Users should
  treat `lazy=True` as a Sprint-5-era stub for any code that wants to
  actually evaluate the LazyFrame, until ADR 0003 resolves.

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
