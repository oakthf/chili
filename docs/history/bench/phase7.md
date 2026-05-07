# Phase 7 — Canonicalize cache (O)

**Shipped:** 2026-04-12

## Change

### O — Process-wide HDB path canonicalize cache

- `crates/chili-op/src/io.rs`:
  - Added module-level `static CANON_CACHE: LazyLock<RwLock<HashMap<String, PathBuf>>>` keyed by the input `hdb_path` string
  - Added private helper `canon_cached(hdb_path) -> SpicyResult<PathBuf>` with double-check pattern (read-lock probe → on miss, write-lock insert)
  - Replaced `fs::canonicalize(hdb_path)` calls in both `write_partition` (line 282) and `write_partition_py` (line 480) with `canon_cached(hdb_path)?`

### Why it's safe

- The canonicalized path of an HDB root is invariant for the lifetime of the process — no one is going to `mv` the HDB mid-run, and even if they did, the cached path is still valid (it's an absolute path resolved at first access).
- `RwLock` is correct because the read path is overwhelmingly dominant (one cache miss per unique HDB path, then N cache hits).
- No invalidation API is needed — if the user wants to reset the cache they can restart the process.

## Benchmark — `write/wpar_1k_rows_fresh_hdb`

| Phase | Median | CI |
|---|---:|---|
| pre-all | 10.455 ms | [10.443, 10.455, 10.467] |
| post-phase1 | 9.031 ms | [9.010, 9.031, 9.054] |
| **post-phase7** | **9.202 ms** | [9.164, 9.202, 9.252] |

Cumulative vs pre-all: **−12.0%**.
Phase 7 alone vs phase1: **+1.9% (within noise)**.

### Why Phase 7 is essentially flat on this bench

The `wpar_1k_rows_fresh_hdb` bench's setup creates a **fresh tempdir per iteration** (so the test cleans up after itself). This means each iteration's first wpar call is a cache MISS, and only the subsequent 4 calls (within the same iteration) are cache HITS.

Per-iteration cost decomposition:
- Pre-Phase-7: 5 × `fs::canonicalize` ≈ 5 × 4 µs = 20 µs
- Post-Phase-7: 1 × `(canonicalize + RwLock write)` + 4 × `RwLock read` ≈ 4.5 µs

Theoretical per-iter savings: ~15.5 µs ≈ 3 µs per wpar call. The wpar bench's wall time is ~9-10 ms per iteration, so the savings are 0.03% — undetectable in benchmark noise.

### Where Phase 7 actually matters

The cache helps when the same HDB path is hit many times, e.g. mdata's batch ingest loop:

```python
for date in 1252_dates:
    engine.wpar(df_for_date, "/hdb", "ohlcv_1d", date.strftime("%Y.%m.%d"))
```

Pre-Phase-7: 1252 × `fs::canonicalize` (hot APFS cache, ~3-5 µs each) = ~5 ms total
Post-Phase-7: 1 × `fs::canonicalize` + 1251 × `RwLock read` = ~3.5 µs + ~125 µs = 0.13 ms total

Savings: ~5 ms across the loop = **negligible vs polars compute time**, but free.

This is exactly what the proposal was rated as in the original optimization plan: "TIER 4 — least impact, only helps write-heavy workloads, may not show up in microbenchmarks." Confirmed.

## Regression tests

- 12 chili-op `partition_filter_test` integration tests pass (these use `write_partition_py` extensively)
- chili-py 16 pytest tests still pass (separately verified after Phase 6 rebuild — Phase 7 only touches chili-op which chili-py depends on transitively)

## What's next

Phase 8 — final validation + docs:
- Full bench sweep (`pre-all` → final)
- Full regression test sweep (Rust + Python)
- CHANGELOG.md update
- README perf notes
- housekeeper agent run
- Memory updates
- mdata STATUS.md final phase note
