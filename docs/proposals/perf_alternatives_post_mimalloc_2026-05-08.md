# Perf-improvement alternatives — post mimalloc removal

**Author:** post-Sprint-12 hotfix investigation
**Date:** 2026-05-08
**Status:** Proposal — no implementation. Each item documents gain estimate, cost, risk, bench gate, and prerequisites. User picks which (if any) to fund as a Sprint 13+ candidate.
**Prompted by:** the 0.8.2 hotfix (commit `359d1f6`) removed `#[global_allocator] MiMalloc` from chili-py to fix the pyarrow co-load segfault. This doc enumerates alternative perf improvements that DON'T require process-level allocator override.

---

## Tier 1 — known-instrumented, bench-gate ready (Sprint 13 carry-over)

These were already surfaced during Sprint 12 P2 partial symbolization
and documented in `docs/bench/post_pivot_baseline_2026-05-07.md`. They
have specific bench targets and pre-existing profile evidence. No new
investigation needed before scoping.

### P1.1 — Batch schema reads in `load_par_df` parallel build

**Idea.** Sprint 12 P2 partial symbolization showed **17.7% main-thread
cost on `alloc::boxed::Box<T>::new`** during `load_multitable_5x200p`
bench. Two inline sites; both are heap allocations during polars
LazyFrame schema setup. Currently each table reads its schema
independently. Batching into a single multi-table schema fetch
removes ~5x of the per-table polars setup cost.

**Expected gain.** Recover at least half of the +22.8%
`load_multitable_5x200p` regression observed Sprint 7 Part B
(claude-2 vs claude-baseline). Concretely: ~10pp on that bench.

**Cost.** ~3-5pp. Touches `crates/chili-op/src/io.rs::load_par_df`
parallel-build path.

**Risk.** Low — additive optimization; existing tests should keep
passing. Need to preserve table-discovery order semantics.

**Bench gate.** `cargo bench -p chili-op --bench load_par_df`
`load_multitable_5x200p` and `load_multitable_5x100p` shapes.

**Prereqs.** Symbolized profile (have it, Sprint 9 captured it; chili-side
resolution done Sprint 12; polars-internal kernels still unresolved at
offsets `0x450c` and `0x4834`).

### P1.2 — Pre-allocate Box arenas

**Idea.** Same Sprint 12 finding. The 17.7% cost is many small `Box`
allocations during the parallel-build phase. A bumpalo-style arena
allocator scoped to one `load_par_df` call avoids the malloc churn
without affecting any other process state.

**Expected gain.** Stacked with P1.1, recover most of the regression.
Standalone: ~5-7pp on multi-table bench.

**Cost.** ~5-8pp. More invasive than P1.1; the arena threading needs
to flow through the parallel-build closure.

**Risk.** Medium — touches a concurrency-active code path. Easy to
get wrong (e.g., arena outliving its scope, freed-while-referenced
errors, especially in rayon worker threads).

**Bench gate.** Same as P1.1. Plus: re-run parse_cache golden gate
(~397 ns) — must stay under 400 ns.

**Prereqs.** None beyond P1.1's profile evidence.

### P1.3 — Coalesce qualified-name string interning

**Idea.** Sprint 12 P2 finding — qualified-name strings (`"foo.bar"`,
`"foo.baz"`) are constructed inside the polars setup hot path during
`load_par_df`. Pre-computing these once per table BEFORE the polars
LazyFrame setup and passing them in pre-built reduces allocation
pressure at the hottest point.

**Expected gain.** ~2-4pp on `load_multitable_5x200p`. Smaller in
isolation than P1.1/P1.2, but stacks cleanly with both.

**Cost.** ~1-2pp. Mechanical refactor.

**Risk.** Low. Pure data-flow change.

**Bench gate.** Same as P1.1.

---

## Tier 2 — allocator-level alternatives (no process-wide override)

Each proposal here gives some of the mimalloc benefit while staying
clear of the pyarrow co-load failure mode. Cause-and-effect was
empirically confirmed (`docs/sync/mdata_chili_2026-05-08_pyarrow_response.md`)
to be the `#[global_allocator]` registration; alternatives that don't
register globally are interop-safe.

### P2.1 — bumpalo arena for `engine.eval` transient AST + result

**Idea.** Each `engine.eval` call builds a transient AST, evaluator
context, and result `SpicyObj` graph that is freed when the call
returns. A bumpalo arena scoped to the eval call replaces dozens of
small `Box::new` / `Vec::new` allocations with bump-pointer
allocation, freed all-at-once at scope exit.

**Expected gain.** **Unknown without measurement.** Reasonable
expectation: 5-15% on eval-heavy workloads (many small queries),
0-2% on data-heavy workloads (memory time is dominated by
DataFrame buffers, not AST nodes).

**Cost.** ~6-10pp. Touches the parser, evaluator, and SpicyObj graph
construction. Significant API surface for lifetime threading
(`<'arena>` lifetime parameter on every type that holds an AST
reference).

**Risk.** Medium-high. Lifetime correctness is subtle in evaluators;
chili's recursive evaluator + parse cache (which currently uses
`Arc<Ast>` for hit-path sharing) needs careful redesign to keep
the parse cache's owned lifetime model.

**Bench gate.** Existing eval benches (`cargo bench -p chili-op
--bench eval`) plus parse_cache hit (~397 ns golden gate must hold).

**Prereqs.** Architecture decision — bumpalo's per-call arena
conflicts with parse_cache's `Arc<Ast>` shared-ownership model.
ADR territory if pursued.

### P2.2 — `mi_alloc` named allocator on specific hot paths

**Idea.** Use the `mimalloc` Rust crate as a NAMED allocator —
i.e., `mi_alloc::MiMalloc::new()` constructed explicitly and used
on specific `Vec` / `Box` / `HashMap` instantiations via the
unstable `allocator_api`. NOT registered as `#[global_allocator]`,
so process state is untouched.

**Expected gain.** Only the Rust hot paths chosen would see
mimalloc behavior. Plausibly a fraction of the asserted-but-unmeasured
"5-10%" Sprint 3 estimate, since the hottest allocation paths are
typically inside polars-rust (already its own world) rather than
in chili-py's own code.

**Cost.** ~4-6pp + nightly Rust toolchain (allocator_api is
unstable as of 2026-05). Cost goes up if the API stabilizes
elsewhere — uncertain timeline.

**Risk.** Medium. Nightly-only is a real ergonomics cost (build
breakage on toolchain bumps). Limited to specific call sites.

**Bench gate.** Microbench each hot path before/after. Need a
specific suspect path with measured allocator-bound cost first
(currently no such evidence exists for chili-py).

**Prereqs.** Profile evidence that allocator overhead is a real
bottleneck on a specific path. The Sprint 3 mimalloc claim was
never bench-quantified; without that evidence base, this proposal
is speculative.

### P2.3 — Static-link mimalloc WITHOUT `#[global_allocator]`

**Idea.** Link mimalloc statically into the cdylib, but DON'T
register it as the Rust global allocator. The Rust internal calls
to mimalloc functions (where present) would still go through
mimalloc; everything else uses libsystem malloc.

**Reality check.** This doesn't work cleanly. mimalloc as a Rust
crate dependency without `#[global_allocator]` is essentially
inert — Rust's allocation goes through `alloc::alloc::Global`
which dispatches to whatever IS registered as `#[global_allocator]`
(or libsystem if nothing is). Static-linking mimalloc binary
without global registration just adds dead code.

**Verdict.** Not viable. Documented here so future investigation
doesn't re-derive.

---

## Tier 3 — code-path optimizations (allocator-orthogonal)

These are perf wins available regardless of allocator choice.

### P3.1 — Lazy plan caching across eval calls

**Idea.** Sprint 12's symbolization showed polars-plan setup is hot
on multi-table loads. If the same query shape is repeated across
calls (mdata's query patterns are likely repetitive — same OHLCV
pattern with different date / symbol filters), caching the
LazyFrame plan skeleton and parameterizing the filters would amortize
the plan setup.

**Expected gain.** **High variance on workload.** For mdata's
repetitive REST endpoint pattern: plausibly 30-50% reduction in
per-eval cost. For one-shot ad-hoc queries: zero. Unknown for the
mix.

**Cost.** ~8-12pp. Requires query-shape canonicalization, plan
templating, parameter-substitution layer.

**Risk.** Medium. Subtle correctness bugs possible (cached plan
returns wrong results when filter shape "looks the same" but
semantics differ).

**Bench gate.** New bench: identical-shape query repeated N times,
measure per-call cost. Plus existing eval/scan benches for no-regression.

**Prereqs.** ADR — query caching is a semantic surface change.
Catch-all "may return stale plans" failure mode needs documented
behavior.

### P3.2 — GIL release on `write_partitioned_df` and `load_partitioned_df`

**Idea.** Golden rule 5 currently mandates GIL release around
`Engine::eval`. The same release pattern for `write_partitioned_df`
and `load_partitioned_df` would enable concurrent multi-table
operations from threaded Python callers. mdata's REST server is
multi-worker; could compose these.

**Expected gain.** ~50-200% concurrent throughput on workloads
that interleave eval + I/O calls. Single-threaded callers see no
change.

**Cost.** ~3-5pp. Need to verify all I/O code paths are Send + thread-safe
through the FFI boundary.

**Risk.** Low-medium. polars I/O is already thread-safe internally.
Need careful audit of any chili-side mutable state held across the
GIL release.

**Bench gate.** Existing `tests/bench_concurrent.py` Python harness
for chili-py concurrent throughput; add write/load shapes.

**Prereqs.** Golden rule 5 audit of write/load paths.

### P3.3 — pyo3 fast-path int conversion

**Idea.** pyo3's default `Python int → Rust i64` conversion goes
through a generic `IntoPyObject` round-trip. Custom fast paths via
`PyAny::downcast_unchecked::<PyInt>()` + `PyInt::extract_i64()`
can be 2-3× faster on the conversion itself.

**Expected gain.** Tiny. Maybe 0.5-2% on FFI-heavy small-result
queries. Negligible on data-heavy. Conversion is rarely the
bottleneck.

**Cost.** ~1-2pp.

**Risk.** Low. pyo3's API is stable; downcast_unchecked is well-
documented.

**Bench gate.** Pyo3 conversion microbench. Not a high-priority bench.

**Prereqs.** None.

### P3.4 — Categorical mapping precompute / cache

**Idea.** For partitions that share the same Categorical column
metadata (mdata's case: `symbol` is Categorical across all OHLCV
partitions), the Categorical → physical-int mapping could be
computed once per HDB load and cached. Currently each eval that
returns Categorical data rebuilds it.

**Expected gain.** Workload-dependent. For mdata's symbol-heavy
queries: ~5-10% on eval. For workloads without Categoricals:
zero.

**Cost.** ~4-6pp. Need cache invalidation strategy.

**Risk.** Medium. Cache staleness on schema change is a correctness
concern.

**Bench gate.** Categorical-heavy eval bench (would need to be
written; doesn't exist today).

**Prereqs.** Sprint 12 partial profile re-examined for Categorical
costs. Cache-invalidation strategy ADR.

---

## Tier 4 — build-time / toolchain wins

### P4.1 — Profile-Guided Optimization (PGO)

**Idea.** Build chili release wheel with PGO. Run a representative
workload (e.g., `tests/bench_concurrent.py` plus the criterion
benches) to generate profile data, then rebuild with
`-Cprofile-use`. Compiler can speculatively inline hot paths.

**Expected gain.** Industry experience: 5-15% on real-world Rust
codebases. Highly variable.

**Cost.** ~3-5pp to set up the PGO build pipeline. Wheel build
time roughly doubles (need to build twice: instrumented + final).

**Risk.** Low. PGO is mature in modern Rust toolchains.

**Bench gate.** Full criterion suite + Python concurrent throughput
+ parse_cache hit (must stay ≤ 400 ns).

**Prereqs.** Define a "representative workload" for PGO training.
mdata may want to provide their query mix as the reference.

### P4.2 — symbolized PGO infrastructure (Sprint 9 P7 follow-on)

**Idea.** Sprint 9 P7 added `[profile.bench]` symbol-retention
override. Sprint 12 P2 only partially resolved the symbolized
profile because the polars source tree's own `[profile.release]`
strips symbols separately. Adding the same override to
`/tmp/polars-py-1.39.3/Cargo.toml` would unblock full
symbolization, which then enables targeted optimization of the
remaining 64% of polars-internal hot kernels (offsets `0x450c`
and `0x4834`).

**Expected gain.** Indirect — unlocks Tier 1 / Tier 3 proposals
to be more precisely targeted. Not a perf win in itself.

**Cost.** ~2pp infra + ~30 min cold rebuild + 15 GB additional
disk (per the Sprint 12 retro).

**Risk.** Low.

**Bench gate.** N/A (infra change).

**Prereqs.** Disk space for full polars debug rebuild (~15 GB).

---

## Tier 5 — discounted / not recommended

### P5.1 — jemalloc as `#[global_allocator]`

Same process-level override mechanism as mimalloc. Would have the
same pyarrow co-load failure mode. **Not viable.**

### P5.2 — wholesale Rust → C++ port for hot paths

Out of scope; chili's whole identity is Rust-on-polars. Mention
only to be explicit about what's NOT being considered.

---

## Recommended sequencing

If perf becomes a Sprint 13 priority, the most attractive ordering
is:

1. **P4.2** (symbolized PGO infra) — unlocks better targeting for
   everything below. ~2pp infra cost, no immediate perf claim, but
   prerequisite for confident bench-gating.
2. **P1.1** + **P1.3** (batch schema reads + qualified-name interning)
   — ~5-8pp combined; recovers ~half of the +22.8% multitable
   regression. Lowest risk, clearest evidence.
3. **P3.2** (GIL release on write/load) — ~3-5pp; concurrent
   throughput win for mdata's multi-worker REST server.
4. **P4.1** (PGO build) — ~3-5pp; broad 5-15% wins across the board.
5. (Beyond this, ROI gets speculative — P1.2, P2.1, P3.1, P3.4
   should each be re-scoped after P1.1/P1.3 complete and the
   profile is re-run.)

Cumulative budget for steps 1-4: ~13-20pp. Roughly 1-2 sprints.
Expected aggregate gain: 10-30% perf improvement across the eval +
load + write paths combined, *bench-gated* (unlike the unmeasured
mimalloc claim).

---

## What this proposal explicitly avoids

- **Anything that registers a process-wide global allocator.** The
  mimalloc lesson is that any `#[global_allocator]` on a Python C
  extension breaks co-load with other native C extensions. jemalloc,
  tcmalloc, snmalloc — same failure mode. Don't pursue.
- **Speculative perf claims without bench evidence.** The Sprint 3
  mimalloc port shipped on an estimated "5-10% allocation overhead
  reduction" with **no bench gate** (per inventory §4.9 verdict:
  "Bench gate: None — optional optimization"). Every proposal here
  has an explicit bench gate. If a candidate can't be bench-gated,
  it doesn't ship.
- **"Felt faster" — no quantification.** Aligned with sequencing
  principle 3 of the closed roadmap (`docs/history/sim/roadmap_2026-05-07.md`):
  "Bench-driven optimization. No 'felt faster' sprints."

---

## Cross-references

- `docs/sync/mdata_chili_2026-05-08_pyarrow_response.md` — the
  hotfix that motivated this proposal.
- `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 12 P2
  partial-symbolization findings; source for Tier 1.
- `docs/research/claude_only_features_inventory_2026-05-07.md` §4.9
  — original mimalloc port verdict ("LOW-MEDIUM priority", "Bench
  gate: None").
- `docs/sim/sprint_3_retro.md` — Sprint 3 port wave (where
  mimalloc was ported without measurement).
- `docs/standards/iteration_lessons.md` — durable rules.

---

## Appendix — Independent audit (2026-05-08)

Three parallel audits (codebase scan, adversarial review, sequencing
audit) flagged material issues with the original §1–§5 above. The
following sections are corrections, additions, and a revised
sequencing. The original §1–§5 is preserved unchanged for audit
trail.

### A.1 Material corrections to original proposals

- **P3.2 must be split.** `write_partitioned_df` is **already
  GIL-released** today: `chili-py/src/lib.rs:527` (`fn_call`) calls
  `py.detach()` and `write_partitioned_df` flows through `fn_call`.
  Only `load_par_df` (`lib.rs:532-535`) holds the GIL. Re-scope:
  - **P3.2a (write):** no work — close out.
  - **P3.2b (load_par_df):** ~5-8pp standalone, requires audit of
    chili-side `Arc<RwLock<...>>` state held across load
    (`engine_state.rs::par_df`).
- **P1.1 gain claim is unsupported.** The +22.8%
  `load_multitable_5x200p` regression is dominated by polars-internal
  worker kernels at `0x450c` (93.1% of worker time, unresolved).
  The 17.7% main-thread `Box::new` cost that P1.1 / P1.2 / P1.3
  target is **not the dominant driver** of that regression.
  Revised claim: "may reduce per-table setup overhead;
  multi-table regression impact pending re-profile after the
  change." See `docs/bench/post_pivot_baseline_2026-05-07.md`
  §"Sprint 12 P2 partial symbolization" for the actual breakdown.
- **P2.1 type-name correction.** The parse cache holds
  `Arc<Vec<AstNode>>` (`crates/chili-core/src/engine_state.rs:104`),
  not `Arc<Ast>`. Conflict with bumpalo arena is real regardless;
  doc had the wrong identifier.
- **P2.1 risk → High** (was Medium-high). Adding `'arena` lifetime
  to AST types likely requires distinct cached-AST vs arena-AST
  types, or breaks parse_cache hit-path sharing. Closer to a
  rewrite of the evaluator's ownership model than a refactor.
- **P2.2 ABI hazard added.** Mismatched alloc/dealloc across the
  chili/polars FFI is UB. Per-call mimalloc allocator instances
  must own their dealloc paths; the proposal must specify a
  free-with-same-allocator protocol before any pursuit.
- **P2.3 verdict softened.** `libmimalloc-sys` exposes raw
  `extern "C"` symbols (`mi_malloc`, `mi_free`, `mi_zalloc`,
  `mi_realloc_aligned`); Rust code can call them directly via
  `unsafe { libmimalloc_sys::mi_malloc(...) }` without registering
  mimalloc as `#[global_allocator]`. The original "essentially
  inert" verdict is too strong — direct mi_malloc calls for hot
  paths are an option (a lower-level version of P2.2), though
  whether mimalloc's library init still collides with pyarrow's
  load path is empirically untested.
- **P3.1 gain claim revised.** "30-50% reduction in per-eval cost"
  applies to the **plan-setup fraction** of eval time, not total.
  For data-heavy mdata OHLCV queries, plan setup is a small share
  of total eval (most time is in polars execution). Revised:
  "30-50% reduction in plan-setup overhead per repeated query
  shape; overall eval impact depends on plan-setup's share of
  total cost."

### A.2 Additional perf opportunities surfaced

These were not in the original §1–§5 and merit their own entries.

- **A.2.1 — `load_par_df` GIL release** (high priority; same item
  as P3.2b above). Cost ~5-8pp; gain potentially 30-100% on
  CPU-bound multi-table loads called from threaded Python.
- **A.2.2 — `upsert` / `insert` write-lock contention.** Both hold
  the engine `vars` write-lock during DataFrame extend/groupby/collect
  (`crates/chili-core/src/engine_state.rs:277-382`). Releasing the
  lock around the heavy DataFrame ops: ~3-5pp cost; gain 15-40%
  on upsert-heavy workloads (mdata's RDB ingest path).
- **A.2.3 — Polars feature-flag audit (NEW Tier).** `simd` and
  `streaming` may not be explicitly enabled for polars in
  chili's workspace `Cargo.toml`. ~3-10% potential on numeric
  Series ops if SIMD was off. Cost: compile-time only, no code
  change. Bench gate: math-heavy eval benches.
- **A.2.4 — On-disk parquet codec tuning (NEW Tier).** Page size,
  row group size, dictionary encoding affect both write throughput
  AND scan selectivity. The wide-range scan bench is currently
  ~370 ms; codec tuning can compound. Cost ~1-2pp. Bench gate:
  existing scan benches.
- **A.2.5 — Result / DataFrame cache (NEW Tier).** Distinct from
  P3.1 plan cache: cache the **materialized** DataFrame for
  identical `(query, partition-state)` pairs. For mdata's REST
  endpoint pattern (same OHLCV query, frequently repeated within
  a TTL): higher-leverage than plan caching alone (skips both
  plan setup AND polars execution). ADR territory.
- **A.2.6 — `build_qualified_name` allocations.** `engine_state.rs:1561-1568`
  allocates `Vec` + joins on every table entry during load. Use
  `format!` or `SmallVec<[&str; 4]>` for typical nesting depth.
  ~1pp; stacks with P1.3.
- **A.2.7 — CSV/JSON write DataFrame clone.** `crates/chili-op/src/io.rs:250,264`
  clones the entire DataFrame before writer; polars writers accept
  `&mut`. ~1-2pp; gain 5-15% on write_csv/write_json paths.
- **A.2.8 — `dir_has_partition_files` redundant traversals.**
  `engine_state.rs:1574-1583` re-opens the dir listing on every
  qualified-name build call. Batch dir read in Phase 1a.
  ~1-2pp; stacks with Tier 1.

### A.3 Cross-cutting gates the original missed

- **Cumulative bench target now stated.** The combined recommended
  sequence must clear:
  - `load_multitable_5x200p ≤ 1.65 ms` (recover at least half of
    the +22.8% regression observed Sprint 7 Part B)
  - `parse_cache hit ≤ 400 ns` (golden rule 6 holds)
  - `tests/bench_concurrent.py` ≥ baseline throughput (no
    regression on concurrent eval)
- **Per-step rollback criteria.** If a step bench-gates < 5pp gain
  on its target bench, revert before proceeding to the next step.
  Don't ship complexity with no measured benefit.
- **P3.2b (load GIL release) requires new concurrency tests**, not
  just benches: two threads loading the same HDB path; load racing
  a `clear_par_df`. Functional correctness, not just performance.
- **P3.4 (Categorical cache) is ADR territory** — same as P3.1, the
  original proposal missed this flag. Cache invalidation on schema
  change is a semantic surface change.

### A.4 REVISED sequencing

The original sequence (P4.2 → P1.1+P1.3 → P3.2 → P4.1) is wrong.

- **P4.2 does NOT unlock P1.1/P1.3.** The Sprint 12 partial profile
  ALREADY attributes the 17.7% Box::new cost on the chili side.
  P4.2 only unlocks the polars-internal kernels (`0x450c`,
  `0x4834`), which P1.1/P1.3 don't target. Sequencing P4.2 first
  adds 2pp of infra cost + reboot-survival risk before the
  highest-confidence optimization.
- **P4.1 and P4.2 both BLOCKED on user-driven P0** (GitHub-host
  the polars fork). The polars source tree at
  `/tmp/polars-py-1.39.3` does not survive reboot; edits to its
  `[profile.release]` are ghost. Until P0 lands, the polars-side
  `[profile.bench]` override (P4.2) and the PGO-trained wheel
  (P4.1) cannot reproduce in a fresh build environment.

**Revised sequence:**

1. **Sprint A** (no blockers): P1.1 + P1.3 + A.2.6 + A.2.7 + A.2.8
   + Polars feature-flag audit (A.2.3). Cheap, all chili-side,
   evidence in hand. Estimated ~9-13pp; implementation-sprint shape.
2. **Sprint B**: P3.2b (`load_par_df` GIL release) + A.2.2
   (upsert/insert lock release) + A.2.4 (parquet codec tuning) +
   P3.4 (Categorical cache) with ADR. Estimated ~10-14pp.
3. **(User P0 lands)** — GitHub-host the polars fork. Required for
   reproducible builds and for the polars-side Cargo.toml edits.
4. **Sprint C** (post-P0): P4.1 PGO + P4.2 symbolized polars infra.
   Then re-profile and decide whether the remaining polars-internal
   kernels (`0x450c`, `0x4834`) justify P1.2 (Box arena) or
   A.2.5 (result cache).

### A.5 Sprint sizing — REVISED

Original: "1-2 sprints, ~13-20pp." Per the closed roadmap's
cadence_metrics: implementation sprints (3, 4, 5, 7) calibrate at
9-14pp; perf/infra sprints (8, 9, 12) calibrate at 2-4pp due to
infrastructure friction (bench compile, macOS tooling, `/tmp/`
volatility).

**Revised total:** 2-3 sprints minimum.
- Sprint A: 9-13pp implementation.
- Sprint B: 10-14pp implementation (with one ADR).
- Sprint C: 5-8pp infra (post-P0).

The "1-2 sprints" estimate assumed `/tmp/` volatility was tractable;
P4.2's polars `Cargo.toml` edit recreates the Sprint 9 P2
infrastructure-friction pattern that consistently slipped.

### A.6 What did NOT change

The Tier 5 (discounted) section stands. No change to the
"never `#[global_allocator]`" hard constraint. The bench-gate-or-
revert principle stands. The proposal doc is still the
authoritative scoping reference; the user's go/no-go on individual
items remains the gating decision.
