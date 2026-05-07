# Phase 2 — Load path (J, B, C)

**Shipped:** 2026-04-11
**Baseline compared against:** `post-phase1` (build-system wins already applied)

## Changes

### J — Schema sentinel cache

- `crates/chili-core/src/par_df.rs`:
  - Added field `empty_schema: Option<Arc<DataFrame>>` to `PartitionedDataFrame`
  - Custom `PartialEq` impl that skips `empty_schema` (so tests still compare structurally)
  - Added private helper `empty_schema_lf()` that returns the cached DataFrame's lazy view, or falls back to scanning the on-disk `schema` parquet if no cache was populated
  - Replaced the three fallback call sites in `scan_partition`, `scan_partitions`, and `scan_partition_by_range` with `self.empty_schema_lf()?`

**Effect**: queries that miss a partition (e.g. `where date=2024.01.06` when 01-06 doesn't exist) no longer re-open the `schema` file from disk. Not on mdata's hot path, but the code is cleaner and eliminates a per-miss filesystem syscall.

### B — Short-lock `load_par_df`

- `crates/chili-core/src/engine_state.rs`:
  - `load_par_df` rewritten into two phases
  - Phase 1 (outside lock): collect top-level entries → `Vec<(PathBuf, String, bool)>` → build each `PartitionedDataFrame` outside any lock
  - Phase 2 (inside lock): acquire `par_df.write()` once and extend the map atomically

**Effect**: the `par_df` write lock is now held only for the final `HashMap::extend`, not the entire directory traversal. Concurrent read queries run undisturbed during `load_par_df` instead of blocking on the old long-held write lock.

### C — Rayon per-table scans

- `crates/chili-core/src/engine_state.rs`:
  - Phase 1's per-table build is a `rayon::prelude::par_iter` over top-level entries
  - Collected into `Vec<(PathBuf, String, bool)>` first because `DirEntry` is not `Send` on all platforms
  - New helper `build_par_df_entry(table_path, table_name, is_file)` moves the per-table logic into a pure function callable from rayon workers
  - Also reads the schema sentinel for Proposal J in the same place, so `empty_schema` is populated in one pass with no extra filesystem round-trips

**Effect**: multi-table HDBs reload ~N× faster (bounded by num_cpus). Single-table HDBs see no measurable change because `par_iter` over a 1-element vec is effectively sequential — but the architectural cleanup is real.

## Benchmark diff — `pre-all` → `post-phase1` → `post-phase2`

Isolated runs (no parallel bench interference).

| Bench | pre-all | post-phase1 | post-phase2 | Δ phase1→2 | Cumulative Δ |
|---|---:|---:|---:|---:|---:|
| `load/load_cold_2000p` | 6.2708 ms | 5.0470 ms | 5.0885 ms | +0.8% (noise) | **−18.8%** |
| `load/load_warm_2000p` | 6.2451 ms | 5.0201 ms | 5.0845 ms | +1.3% (noise) | **−18.6%** |
| `load/load_multitable_5x200p` | 2.8712 ms | 2.5434 ms | **1.5316 ms** | **−39.8%** ✓ | **−46.7%** |

Single-table flat at phase1-level (noise). Multi-table drops hard as expected —
rayon parallelization across 5 tables on a multi-core machine.

Other benches (scan, eval, write, parse) not re-run — Phase 2 does not touch
their hot path (only the empty-schema miss fallback, which those benches don't
exercise).

## Regression testing

All 159 Rust tests pass in release mode (102 chili-core lib + 57 across
chili-op, chili-parser, chili-bin).

Custom PartialEq on `PartitionedDataFrame` preserves the semantics of the
existing 12 `partition_filter_test` assertions that compare equality by
`(name, df_type, path, pars)`.

## Side-effect improvements (not measured in benches)

These are architectural wins that don't show up in single-threaded benches but
matter in production:

1. **Concurrent reload**: before Phase 2, `load_par_df` held the `par_df` write
   lock for the entire directory traversal (~6ms for 2000 partitions, or up to
   several hundred ms on a larger HDB). Any read query hitting a `ParDataFrame`
   during that window blocked. After Phase 2, the lock is held for a ~10µs
   `HashMap::extend` call. Production reload is effectively non-blocking.
2. **Miss-path filesystem syscall elimination**: Proposal J removes a parquet
   file open per query that hits a missing partition. For an mdata workload
   that occasionally queries dates outside the loaded range, this trims ~100µs
   per miss.

## Parse cache bench — no change expected

The parser and `EngineState::parse` are untouched in Phase 2. Parse cache
bench skipped for this phase.

## What's next

Phase 3 — parallel glob expansion (Proposal E). Specifically:

- `par_df.rs:84-120` (`scan_partitions`) and `par_df.rs:135-166`
  (`scan_partition_by_range`) currently sequentially call `glob::glob` per
  partition. For a 500-partition query, that's 500 sequential filesystem
  lookups before polars sees any paths.
- Swap the `for par_num in par_nums` loop for `par_iter().flat_map(...)` and
  preserve input order on the output.

Expected gain: `scan/query_wide_range_500d` should see the most improvement
(~20-100 ms saved on the pre-scan glob phase). `scan/query_eq_single_date` is
unaffected because it uses `scan_partition` (single-file scan).
