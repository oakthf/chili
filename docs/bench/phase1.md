# Phase 1 — Build-system wins (F, G, N)

**Shipped:** 2026-04-11
**Baseline compared against:** `pre-all` (captured same day, pre-any-optimization)

## Changes

### F — `[profile.release]` tuning (`Cargo.toml:42-47`)

```diff
 [profile.release]
 strip = true
-opt-level = "z"
-lto = true
-codegen-units = 6
+opt-level = 3        # speed-optimized (was "z" — size)
+lto = "fat"          # cross-crate inlining across polars boundary
+codegen-units = 1    # one compilation unit → maximum inlining scope
```

**Why**: polars compute kernels rely heavily on LLVM auto-vectorization, which
`opt-level = "z"` actively disables. Fat LTO enables cross-crate inlining between
chili-core ↔ polars, which is where most of the perf budget lives.

**Cost**: release build time roughly **tripled** (~5min → ~11min on a cold
compile). Dev builds untouched.

### G — mimalloc global allocator

- `crates/chili-bin/Cargo.toml` +1 dep: `mimalloc = { version = "0.1", default-features = false }`
- `crates/chili-py/Cargo.toml` +1 dep: same
- `crates/chili-bin/src/main.rs`:
  ```rust
  use mimalloc::MiMalloc;
  #[global_allocator]
  static GLOBAL: MiMalloc = MiMalloc;
  ```
- `crates/chili-py/src/lib.rs`: same block

**Why**: polars' allocation pattern (many small DataFrame buffers + Arrow chunks)
is a poor fit for the macOS system allocator. mimalloc is zero-configuration and
just-works.

### N — `#[inline]` on SpicyObj conversion hot-path (`crates/chili-core/src/obj.rs`)

Added `#[inline]` to:
- `is_fn` (line 381)
- `size` (line 385)
- `str` (line 396)
- `to_bool` (line 406)
- `to_i64` (line 413)
- `to_par_num` (line 448)

**Why**: under fat LTO these are largely redundant (LTO inlines across crates
automatically), but they express intent clearly and don't hurt. Cross-crate
inlining without fat LTO would rely on `#[inline]` annotations.

## Benchmark diff — `pre-all` → `post-phase1`

All benches captured in isolation (not running concurrently with others —
parallel bench runs introduce 20-40% CPU-contention noise on the load bench
specifically).

| Bench | pre-all | post-phase1 | Change | Notes |
|---|---:|---:|---:|---|
| `scan/query_eq_single_date` | 2.8111 ms | 2.2246 ms | **−20.9%** | |
| `scan/query_narrow_range_5d` | 11.048 ms | 9.8044 ms | **−11.3%** | |
| `scan/query_wide_range_500d` | 988.63 ms | 918.86 ms | **−7.1%** | IO-bound, smaller relative gain |
| `eval/query_groupby_agg` | 7.6604 ms | 4.5460 ms | **−40.7%** | ✓ polars group-by hot path |
| `eval/query_select_star` | 1.5202 ms | 0.6156 ms | **−59.5%** | ✓ eval loop |
| `write/wpar_1k_rows_fresh_hdb` | 10.455 ms | 9.0309 ms | **−13.6%** | |
| `parse/parse_repeat_same_query` | 374.35 µs | 142.90 µs | **−61.8%** | ✓ chumsky parser |
| `parse/parse_unique_query_per_iter` | 265.81 µs | 91.597 µs | **−65.5%** | ✓ parser cold path |
| `load/load_cold_2000p` | 6.2708 ms | 5.0470 ms | **−19.5%** | |
| `load/load_warm_2000p` | 6.2451 ms | 5.0201 ms | **−19.6%** | |
| `load/load_multitable_5x200p` | 2.8712 ms | 2.5434 ms | **−11.4%** | |

**Unanimous improvement across all 11 benches.** Every single bench measured is
faster by 7-66%. No regressions.

### Biggest wins

1. **Parser: −62 to −66%** — polars `chumsky` parsing loop is extremely
   LTO-friendly. Parse cost was already small in absolute terms but the relative
   speedup suggests the parser was leaving a lot on the table with `opt-level = "z"`.
2. **eval/query_select_star: −59.5%** — the "minimal query" path is a tight
   AST-walking loop, and LTO + mimalloc combined demolish it.
3. **eval/query_groupby_agg: −40.7%** — polars' group-by + aggregation path is
   vectorization-heavy; fat LTO enables inlining into polars' hot loops.

### Smallest wins

1. **scan/query_wide_range_500d: −7.1%** — IO-bound (250k rows materialized
   across 500 parquet files). Disk throughput dominates; CPU gains are
   amortized.
2. **load/load_multitable_5x200p: −11.4%** — limited by parquet metadata reads,
   which are already at the filesystem floor.

## Regression testing

All 159 Rust tests + 16 Python tests pass in release mode:

```
chili-core lib: 102 passed
chili-core fmt_test: 1 passed
chili-op partition_filter_test: 12 passed
chili-op chili_test: 1 passed
chili-op eval_test: 18 passed
chili-op hdb_test: 1 passed
chili-op arithmetic_test: 5 passed
chili-op replay_test: 2 passed
chili-parser lib: 1 passed
chili-bin tests/lib.rs: 16 passed
chili-py pytest tests/test_partition_filter.py: 16 passed
```

## Python concurrent benchmark

Intentionally **not re-run** for Phase 1 because it's only meaningful once
Phase 6 (GIL release) lands — the GIL is still held by `Engine::eval` so
concurrency is still serialized. Keep `pre-all` numbers as the reference:
0.92× speedup (worse than serial) on 8 threads × 200 queries.

## Parallel bench interference warning

The initial `load_par_df` post-phase1 run happened while `parse_cache` was
running in parallel, producing wildly inaccurate numbers:

- `load_cold_2000p`: 6.88 ms (parallel) vs 5.05 ms (isolated) — 36% phantom
  regression
- `load_warm_2000p`: 8.65 ms (parallel) vs 5.02 ms (isolated) — 72% phantom
  regression
- `load_multitable_5x200p`: 4.85 ms (parallel) vs 2.54 ms (isolated) — 91%
  phantom regression

**Lesson**: never run chili benches concurrently. The load bench especially is
sensitive to CPU contention because it does filesystem + parquet metadata
reads. The isolation pattern is the standard for all future phases.

## What's next

Phase 2 — load path (J → B → C). Specifically targets `load_par_df`:
- **J** (schema sentinel cache): eliminate per-miss parquet open
- **B** (short-lock load): release par_df lock before directory traversal
- **C** (rayon per-table scan): parallelize across tables in load_par_df

Expected incremental gain: 20-50% more on load benches, plus improved
concurrency behavior during reload (not measurable in single-threaded benches
but real for downstream users).
