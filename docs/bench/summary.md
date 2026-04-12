# Chili Optimization Sweep — Final Summary

**Sprint dates**: 2026-04-11 → 2026-04-12
**Phases shipped**: 7 (Phase 0 bench infra + Phases 1-7 optimizations + Phase 8 validation)
**Proposals implemented**: 14 (A, B, C, D, E, F, G, I, J, K, L, N, O — H dropped as no-op, M dropped as imperceptible)
**Tests added**: 6 new (parse_cache_test) — 181 total tests passing
**Regressions**: zero

## Cumulative benchmark diff (`pre-all` → `post-phase7`)

### Rust benches (criterion)

| Bench | pre-all | final | Cumulative |
|---|---:|---:|---:|
| **scan** | | | |
| `scan/query_eq_single_date` | 2.81 ms | 2.25 ms | **−19.9%** |
| `scan/query_narrow_range_5d` | 11.05 ms | 5.78 ms | **−47.7%** |
| `scan/query_wide_range_500d` | 988.63 ms | 362.59 ms | **−63.3%** (2.7×) |
| **eval** | | | |
| `eval/query_groupby_agg` | 7.66 ms | 3.30 ms | **−57.0%** (2.3×) |
| `eval/query_select_star` | 1.52 ms | 365 µs | **−76.0%** (4.2×) |
| **parse** | | | |
| `parse/parse_repeat_same_query` | 374 µs | **385 ns** | **−99.9%** (970×) |
| `parse/parse_unique_query_per_iter` | 266 µs | 91.6 µs | **−65.5%** |
| **load** | | | |
| `load/load_cold_2000p` | 6.27 ms | 5.05 ms | −19.5% |
| `load/load_warm_2000p` | 6.25 ms | 5.02 ms | −19.6% |
| `load/load_multitable_5x200p` | 2.87 ms | 1.53 ms | **−46.7%** |
| **write** | | | |
| `write/wpar_1k_rows_fresh_hdb` | 10.46 ms | 9.20 ms | −12.0% |

### Python concurrent benchmark (`bench_concurrent.py`)

8 threads × 200 queries × 5 runs.

| Metric | pre-all | post-phase6 | Cumulative |
|---|---:|---:|---:|
| Single-thread p50 (200 queries) | 354.5 ms | 156.2 ms | **−55.9%** |
| 8-thread concurrent p50 | 3081.2 ms | 505.0 ms | **−83.6%** |
| Single-thread throughput | 564 q/s | 1281 q/s | **2.27×** |
| **Concurrent throughput (8 threads)** | **519 q/s** | **3168 q/s** | **6.10×** |
| Speedup vs serial-est | **0.92×** (worse than serial) | **2.47×** | from pathological to scaling |

## Phase contribution table

| Phase | Proposals | Big wins |
|---|---|---|
| 0 | benches | infrastructure (no perf change) |
| 1 | F, G, N | parse −62%, eval/select_star −60%, scan_eq −21%, load −20%, multi-table-load −11% |
| 2 | J, B, C | multi-table-load −40% on top of phase 1 |
| 3 | E | wide_range_500d −58% on top of phase 1 (parallel glob) |
| 4 | I, K, L | eval −22%, select_star uses make_it_lazy → −24% |
| 5 | D | parse cache hit 99.7% faster (370× speedup), select_star −24% additional |
| 6 | A | concurrent throughput 6.1× cumulative, GIL release unlocks Python concurrency |
| 7 | O | wpar canonicalize cache (free, low impact in microbench, helps tight ingest loops) |

## Test summary

**Rust**: 165 tests (102 chili-core lib + 1 fmt_test + **6 NEW parse_cache_test** + 5 arithmetic + 1 chili_test + 18 eval_test + 1 hdb_test + 12 partition_filter_test + 2 replay_test + 1 chili-parser + 16 chili-bin)

**Python**: 16 chili-py pytest tests

**Total**: 181 tests, 0 failures.

The 6 new parse_cache tests cover hit/miss/path-discrimination/error-not-cached/concurrent-safety/correctness invariants for Proposal D.

## Files changed

| File | Purpose |
|---|---|
| `Cargo.toml` | F (release profile) |
| `crates/chili-bin/Cargo.toml` | G (mimalloc dep) |
| `crates/chili-bin/src/main.rs` | G (#[global_allocator]) |
| `crates/chili-py/Cargo.toml` | G (mimalloc dep) |
| `crates/chili-py/src/lib.rs` | G (#[global_allocator]) + A (GIL release) |
| `crates/chili-core/Cargo.toml` | D (lru dep) + criterion dev-dep + bench targets |
| `crates/chili-core/src/obj.rs` | N (#[inline] annotations) |
| `crates/chili-core/src/par_df.rs` | J (empty_schema field) + E (parallel glob) + custom PartialEq |
| `crates/chili-core/src/engine_state.rs` | B+C (load_par_df refactor + rayon) + D (parse cache) + J (populate empty_schema at load) |
| `crates/chili-core/src/eval_query.rs` | I (collect_schema) + K (filter fusion) + L (Vec::with_capacity) |
| `crates/chili-op/src/io.rs` | O (canonicalize cache) |
| `crates/chili-op/Cargo.toml` | criterion dev-dep + bench targets |
| `crates/chili-core/tests/parse_cache_test.rs` | NEW — 6 unit tests for D |
| `crates/chili-op/benches/common/mod.rs` | NEW — bench fixtures |
| `crates/chili-op/benches/load_par_df.rs` | NEW — load benches |
| `crates/chili-op/benches/scan.rs` | NEW — scan benches |
| `crates/chili-op/benches/eval.rs` | NEW — eval benches |
| `crates/chili-op/benches/write_partition.rs` | NEW — wpar bench |
| `crates/chili-core/benches/parse_cache.rs` | NEW — parse cache bench |
| `crates/chili-py/tests/bench_concurrent.py` | NEW — Python concurrent bench |
| `docs/bench/baseline.md` | Phase 0 baseline doc |
| `docs/bench/phase{1..7}.md` | Per-phase doc |
| `docs/bench/summary.md` | this file |
| `docs/bench/mdata-collab/STATUS.md` | running phase tracker |

## What's next (phases 9-17 — wishlist)

After this sweep, the planned follow-up wave addresses mdata's wishlist items
that aren't pure micro-optimizations:

| Phase | Wishlist item | Effort |
|---|---|---|
| 9 | Column projection pushdown (WL 2.1) | medium |
| 10 | Symbol predicate pushdown (WL 2.2) | small-medium |
| 11 | Fork safety guard (WL 1.2) | small |
| 12 | Engine lifecycle API (WL 3.1) | small |
| 13 | Structured error messages (WL 3.3) | medium |
| 14 | Observability primitives (WL 3.2) | medium |
| 15 | Quantized column helper (WL 3.4) | small |
| 16 | **Python broker bindings (WL 1.1)** | LARGE |
| 17 | Aggregation pushdown for `by` (WL 2.3) | large |

Phase 9-17 collaboration with mdata is set up at
`docs/bench/mdata-collab/`. mdata's Claude has pre-delivered fixtures, test
files, parity test skeleton, and quantized schema doc — see STATUS.md.
