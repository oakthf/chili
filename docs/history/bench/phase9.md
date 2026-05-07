# Phase 9 — Column projection pushdown (WL 2.1) — VERIFIED, NO CHANGE NEEDED

**Date:** 2026-04-12
**Status:** Polars 0.53 already does this correctly. Phase 9 is a no-op.

## Summary

mdata's wishlist Priority 2.1 asked chili to implement column projection
pushdown, citing a 7× slowdown vs DuckDB on Q2/Q3/Q7. Empirical testing
shows that polars 0.53's lazy optimizer already pushes projection through
both `scan_partition` (single-file) and `scan_partition_by_range` (multi-file).
**No chili-side change is needed for column projection pushdown.**

## How we verified

Built a 10-column wide HDB (mirroring mdata's ohlcv shape) — 20 partitions ×
100 symbols × 100 rows × 11 columns. Timed projected vs unprojected queries
through `chili.Engine.eval`:

| Query path | All 11 cols | 1 col (close) | Saving |
|---|---:|---:|---:|
| Single partition (`scan_partition`) | 0.808 ms | 0.279 ms | **−65%** |
| Multi partition × 20 (`scan_partition_by_range`) | **10.13 ms** | **1.88 ms** | **−81%** |
| Multi partition × 20 + symbol filter | 2.31 ms | 1.78 ms | −23% |

Polars optimizer is correctly translating `lf.select([col("close")])` into
a parquet read that fetches only the `close` column. Both single-file and
multi-file scan paths benefit. The optimizer also correctly preserves columns
referenced by `where` predicates (`symbol` is read even if only `close` is
projected because the filter needs it).

The implementation is in:
- `polars-plan-0.53.0/src/plans/optimizer/projection_pushdown/mod.rs:507-513`
- `polars-io-0.53.0/src/parquet/read/reader.rs:53-60`

The patched polars-core fork (`hinmeru/polars-core-patch`) does not modify
the projection pushdown optimizer — confirmed by inspecting the fork.

## What this means for mdata's reported 7× slowdown

mdata's Q2 query is `select from ohlcv_1d where date in 2024_dates, symbol=SPY`
— **no column projection at all**. The 7× DuckDB gap on Q2/Q3/Q7 is NOT a
column-projection issue. It must come from one of:

1. **Symbol predicate pushdown** (WL 2.2 / chili Phase 10) — DuckDB skips
   row groups that don't contain SPY using parquet column statistics.
2. **Parquet decompression strategy** — DuckDB's parquet reader may be
   significantly faster than polars' for the specific shape of mdata's
   files.
3. **Row group layout** — mdata's ohlcv_1d files may have all rows in a
   single big row group, defeating any per-row-group pruning either engine
   could do.

Phase 10 will profile and verify whether predicate pushdown for symbol
filters is reaching the parquet reader.

## Bench artifact retained

The wide-fixture projection benches added in Phase 9 (`projection/select_*`
in `crates/chili-op/benches/eval.rs`) are kept as **regression guards** —
they pin the current projection-pushdown behavior so any future polars
version bump or chili refactor that breaks pushdown is caught immediately.

## What changed (code)

Only test infrastructure:
- `crates/chili-op/benches/common/mod.rs`: added `make_wide_row()` and
  `build_wide_hdb()` helpers (10-column ohlcv shape).
- `crates/chili-op/benches/eval.rs`: added 4 new benches under the
  `projection/` group.

No changes to chili source code.

## Decision: skip ahead to Phase 10

Phase 10 (symbol predicate pushdown verification) is the real bottleneck
target for mdata's Q2/Q3/Q7. Proceeding directly without further Phase 9
work.
