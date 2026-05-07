# Phase 3 — Parallel glob in scan (E)

**Shipped:** 2026-04-11
**Baseline compared against:** `post-phase1` (load-path changes from Phase 2 don't touch the scan hit path, so Phase 2's diff on scan is nil).

## Change

### E — Parallel `glob::glob` expansion in `scan_partitions` and `scan_partition_by_range`

- `crates/chili-core/src/par_df.rs`:
  - Imported `rayon::prelude::*`
  - Rewrote `scan_partitions` to `par_iter().map(...).collect::<SpicyResult<Vec<Vec<PathBuf>>>>()?` then flatten, preserving input order
  - Rewrote `scan_partition_by_range` the same way

**Key invariant**: rayon's ordered `collect` preserves input sequence, so the final `Vec<PathBuf>` has paths in the same order as the pre-parallel implementation (ascending partition index). `polars::LazyFrame::scan_parquet_files` appears to respect input order for determinism, so this is important.

## Benchmark diff — `post-phase1` → `post-phase3`

Isolated run (no parallel bench interference).

| Bench | post-phase1 | post-phase3 | Δ phase1→3 | Cumulative vs pre-all |
|---|---:|---:|---:|---:|
| `scan/query_eq_single_date` | 2.2246 ms | 2.2135 ms | −0.5% (noise) | **−21.3%** |
| `scan/query_narrow_range_5d` | 9.8044 ms | **6.0948 ms** | **−37.8%** | **−44.9%** |
| `scan/query_wide_range_500d` | 918.86 ms | **381.55 ms** | **−58.5%** | **−61.4%** |

### Interpretation

- `query_eq_single_date` is a single-partition `scan_partition` call — does not
  touch either glob-expanding function. Unchanged as expected.
- `query_narrow_range_5d` exercises `scan_partition_by_range` with 5 partitions.
  5 parallel globs save ~3.7ms of sync filesystem lookups on top of the fat-LTO
  baseline.
- `query_wide_range_500d` is the headline bench. **2.4× speedup** comes from
  500 parallel globs + polars getting the paths list faster, which lets its
  rayon workers start reading earlier.

The wide-range number is larger than pure glob-cost math would predict (~25ms
of sync glob at 50µs each). The extra speedup appears to come from polars'
rayon pool being able to overlap file reads with whatever remaining glob work
is in flight when `scan_parquet_files` is called. Investigating further would
require a polars flame graph but the practical gain is real and stable
(confidence interval 381.03–382.13 ms on 50 samples).

## Regression tests

159 Rust tests pass in release mode:

- chili-core lib: 102
- chili-core fmt_test: 1
- chili-op: 39 (5 arithmetic + 1 chili_test + 18 eval_test + 1 hdb_test + 12 partition_filter_test + 2 replay_test)
- chili-parser: 1
- chili-bin tests/lib.rs: 16

Specifically the 12 `partition_filter_test` integration tests pass unchanged —
these exercise `scan_partitions` + `scan_partition_by_range` with the exact
same pepper query shapes mdata uses in production.

## Ordered-collect verification

Critical correctness question: does `par_iter().map(...).collect::<Vec<_>>()` preserve input order in rayon? Answer: **yes** — rayon's collect for `IndexedParallelIterator` (which `ParallelIterator::par_iter` on a slice is) guarantees insertion order matches input order. This is documented in rayon's docs and is relied on throughout the polars codebase.

If a future rayon version changed this behavior, the `partition_filter_test.rs::range_*` suite would immediately fail because the ordered file list feeds into `scan_parquet_files` which produces a logical concatenation — rows from 2024-01-02 must appear before rows from 2024-01-03 for the test assertions to hold.

## Side effect on `scan_partitions` (in operator)

Proposal E also applies to `scan_partitions`, which handles pepper's `in`
operator (`where date in 2024.01.02 2024.01.04 2024.01.08`). No dedicated bench
exists for this path but the code is now consistent with
`scan_partition_by_range` and should see a proportional speedup on
multi-partition `in` queries.

## What's next

Phase 4 — eval-path micro-optimizations (I, K, L):

- **I**: replace `lf.clone().filter(lit(false)).collect()` with `lf.clone().collect_schema()` in `make_it_lazy` — saves 1-3 ms per query
- **K**: fuse sequential `.filter()` calls into a single `.and()` chain in `eval_fn_query`
- **L**: `Vec::with_capacity(n)` pre-allocation for where/op/by expression vectors (simpler than SmallVec, no new dep)

Expected marginal gain (~1-5% on `eval/query_select_star` and `eval/query_groupby_agg`), but good to ship for cleanliness + consistency.
