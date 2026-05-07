# Chili Optimization Baseline — `pre-all`

Captured **2026-04-11** before starting the 14-proposal optimization sweep (Phase 0).
Profile: current `Cargo.toml` release profile = `opt-level = "z"`, `lto = true`, `codegen-units = 6`.
Machine: macOS Darwin 25.4.0, Apple Silicon.

These are the numbers every post-phase diff is compared against.

## Fixture

Two distinct fixture sets, built in reverse date order (to exercise the R1-fix
load-path) and cleaned up between runs.

- **2000-partition HDB** (`scan`, `load_par_df`): 2000 dates × 5 symbols × 100 rows = ~1M rows total
- **100-partition HDB** (`eval`): 100 dates × 50 symbols × 500 rows = ~2.5M rows total
- **Multi-table HDB** (`load_multitable_5x200p`): 5 tables × 200 partitions × 3 symbols × 1 row
- **Parse-cache bench**: no fixture — measures `EngineState::parse` directly

## Rust benchmarks (criterion, 50 samples per scenario)

| Bench | Baseline (`pre-all`) | Notes |
|---|---:|---|
| `scan/query_eq_single_date` | **2.8111 ms** | Single-partition exact match via `scan_partition` |
| `scan/query_narrow_range_5d` | **11.048 ms** | 5-partition inclusive range via `scan_partition_by_range` |
| `scan/query_wide_range_500d` | **988.63 ms** | 500-partition wide range, materializes ~250k rows |
| `eval/query_groupby_agg` | **7.6604 ms** | `select mean price, sum volume by symbol where date range` |
| `eval/query_select_star` | **1.5202 ms** | `select from t where date=X`, small result |
| `write/wpar_1k_rows_fresh_hdb` | **10.455 ms** | Write 5 partitions × 1000 rows via `write_partition_py` |
| `parse/parse_repeat_same_query` | **374.35 µs** | `parse("select from t where ...")` repeated |
| `parse/parse_unique_query_per_iter` | **265.81 µs** | Unique query string per iteration (cold-parse) |
| `load/load_cold_2000p` | **6.2708 ms** | `load_par_df` on 2000-partition HDB |
| `load/load_warm_2000p` | **6.2451 ms** | Same as cold; criterion iterates with warm page cache |
| `load/load_multitable_5x200p` | **2.8712 ms** | 5 tables × 200 partitions |

## Python concurrent benchmark (chili-py, 8 threads × 200 queries × 5 runs)

| Metric | Baseline (`pre-all`) | Notes |
|---|---:|---|
| Single-thread p50 (200 queries) | **354.5 ms** | Bound on per-query cost |
| 8-thread concurrent p50 | **3081.2 ms** | Wall-clock of 8×200 concurrent queries |
| Throughput single-thread | **564 q/s** | Baseline throughput per core |
| Throughput 8-thread concurrent | **519 q/s** | **SLOWER than single-thread** |
| Speedup vs serial-estimate | **0.92×** | Ideal = 8.0× |

**Interpretation**: The GIL is serializing every chili query. 8 threads fighting for the GIL is slightly worse than 1 thread running sequentially, because of thread-scheduling overhead with no parallelism benefit. This is the exact condition Proposal A (GIL release) is designed to fix.

## Running the benchmarks

```bash
# Full baseline capture (one-time, before any change)
cargo bench -p chili-op --bench load_par_df -- --save-baseline pre-all
cargo bench -p chili-op --bench scan -- --save-baseline pre-all
cargo bench -p chili-op --bench eval -- --save-baseline pre-all
cargo bench -p chili-op --bench write_partition -- --save-baseline pre-all
cargo bench -p chili-core --bench parse_cache -- --save-baseline pre-all
cd crates/chili-py && uv run python tests/bench_concurrent.py --save pre-all

# After a phase, compare against baseline:
cargo bench -p chili-op --bench scan -- --baseline pre-all --save-baseline post-phaseN
# Criterion prints a diff table. HTML report in target/criterion/report/index.html
```

## Saved artifacts

- Rust baseline: `target/criterion/*/pre-all/` (criterion-native JSON)
- Python baseline: `target/chili-bench/concurrent_clients_pre-all.json`
- Each post-phase run should save under `post-phase1`, `post-phase2`, etc.
