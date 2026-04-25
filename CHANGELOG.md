# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.7.5] — 2026-04-26

### Changed (chili-py FFI — major perf rework, breaking)

- **Removed Arrow IPC bytes round-trip from `eval()`.** `Engine.eval()` now returns `polars.DataFrame` directly via `pyo3_polars::PyDataFrame` (zero-copy wrapper). Previously the result crossed the FFI as IPC bytes and was decoded Python-side via `pa.ipc.open_stream(BytesIO(bytes)).read_all()` + `pl.from_arrow`. Encode + decode + buffer copies were ~5 ms each on a 3 MB result; small evals now measure ~9 µs/call end-to-end.
- **Removed Arrow IPC bytes round-trip from `wpar()` and `overwrite_partition()`.** Both now accept a `polars.DataFrame` directly (via `PyDataFrame`). The Python wrapper no longer calls `df.write_ipc_stream(buf)` before invoking the FFI.
- **Added `Engine.tick_upd(table, df)`** with Rust-side IPC serialization (GIL released). Replaces the Python-side `df.write_ipc_stream() + publish()` path that the wrapper previously used. The wire format and subscriber callback signature are unchanged — subscribers still receive IPC bytes.
- **Adopted `spicy_to_py` / `spicy_from_py_bound` conversion helpers** ported from upstream `purple-chili/chili` commit `bf9fa14`. Lays groundwork for future `set_var` / `fn_call` surface without the FFI bytes detour.
- **Bumped `pyo3` 0.22 → 0.27 and added `pyo3-polars 0.26`.** Required for the direct-DataFrame path. Migration: `Python::with_gil` → `Python::attach`, `py.allow_threads` → `py.detach`, `PyTzInfo::utc(py)` now returns `Result<Borrowed, _>`, exception types via `py.get_type::<T>()` instead of `T::type_object_bound(py)`.
- **Minimum Python version raised to 3.10** (from 3.7) — required by `pyo3 0.27`'s `abi3-py310` feature.
- **Dropped `pyarrow` dependency.** No longer needed with the direct-PyDataFrame path. `polars >= 0.20` remains the only runtime dep for `chili-pie`.

### Added (chili-py)

- **`Engine.tick_upd(table, df)`** — DataFrame → Rust-side IPC serialize → publish. Faster than the Python-side `df.write_ipc_stream()` path; serialization runs with the GIL released.
- **9 regression tests** (`tests/test_direct_dataframe.py`) pinning the new contract: eval returns `pl.DataFrame`, wpar/overwrite_partition reject bytes input, structured exceptions still importable post-pyo3-bump.

### Compatibility note for downstream consumers (mdata, nxcar)

- `engine.eval(query)` now returns `pl.DataFrame` directly. Code that decoded IPC bytes (`pa.ipc.open_stream(...)` etc.) must drop that step.
- `engine.wpar(df, ...)` / `engine.overwrite_partition(df, ...)` accept the DataFrame as the first argument. Code that pre-serialized to IPC bytes must remove the `df.write_ipc_stream(buf)` step.
- Python ≥ 3.10 required.

### Cherry-picked from upstream

- Workspace `chrono` dep (`b0f20e5`-style consolidation): `chrono = "0.4"` promoted to `workspace.dependencies`; per-crate refs use `chrono = { workspace = true }`.
- `.gitignore` additions: `.mypy_cache/`, `.venv/`.

### Not adopted from upstream (intentional)

- `chili-pie → chili-sauce` package rename (`a0a42f6`): mdata/nxcar import `chili-pie`. Will revisit when upstream actually publishes `chili-sauce` to PyPI.
- Upstream's low-level `PyEngineState` class + `fn_call` dispatcher: local `Engine` class is a higher-level API mdata depends on (broker pubsub, fork-safety, structured exceptions, query_plan, set_column_scale, overwrite_partition, GIL release).
- manylinux 2_28 GH workflows (`0bfc8c5`): we don't ship via that pipeline.

## [Pre-0.7.5]

### Added

- **Headless mode** (`--headless` flag) to run Chili as a stable daemon without TTY. Automatically triggered when `--port > 0` and stdin is not a TTY (subprocess/pipe/systemd). IPC server threads remain active while main thread is parked.
- **Python bindings** (`crates/chili-py`) via PyO3 + maturin. New `Engine` class exposes `load()`, `eval()`, and `wpar()` for direct Python access to the Chili runtime. (Original IPC-bytes design; replaced by direct PyDataFrame in 0.7.5.)

### Fixed

- **Partition date filter returned wrong rows** (R1). `load_par_df` populated `PartitionedDataFrame.pars` directly from `fs::read_dir`, whose iteration order is filesystem-dependent and not sorted on macOS APFS or many Linux filesystems. Downstream `scan_partition` / `scan_partition_by_range` / `scan_partitions` all use `slice::binary_search`, which has undefined behavior on unsorted input. Symptoms: `where date=X` returned 0 rows for most X, `where date>=X, date<=Y` silently dropped middle dates, lower/upper-bound-only queries returned off-by-one partition sets. Fixed by sorting `par_vec` at load time with an inline invariant comment (`crates/chili-core/src/engine_state.rs`).
- **Silently dropped non-partition filter when placed before a partition predicate.** `eval_query` unconditionally set `skip_part_clause = true` whenever `where_exp[0]` was any `BinaryExp`, even if no partition predicate was extracted. Queries like `where symbol='AAPL', date=X` silently skipped the symbol filter and then errored with `requires 'ByDate' condition` because `partitions` was empty. Rewrote partition-predicate extraction to scan every where-clause for `date`/`year` predicates, consume only the matching indices, and leave non-partition clauses untouched (`crates/chili-core/src/eval_query.rs`).
- **Range predicates were not tightened into a single partition range.** `where date>=X, date<=Y` previously used only the first clause for partition pruning (`[X, i32::MAX]`) and relied on a post-scan row filter over the synthesized `date` column to drop the rest. Now both bounds combine into a tight range `[X, Y]`, avoiding unnecessary partition reads.

### Added (tests)

- `crates/chili-core/src/par_df.rs`: 15 unit tests covering `scan_partition_by_range` boundary conditions (empty range, inverted range, nonexistent bounds, unbounded lower/upper, missing inner dates, unsorted-pars regression guard).
- `crates/chili-op/tests/partition_filter_test.rs`: 12 integration tests that write a real temp HDB in non-sorted creation order, load it, and exercise every shape of partition predicate through the full query path.
- `crates/chili-py/tests/test_partition_filter.py`: 16 end-to-end pytests covering the R1 repro matrix through the Python bindings (equality per-date, narrow/wide/half-open ranges, strict bounds, `within`, `in`, mixed symbol+date clauses).
- `crates/chili-core/tests/parse_cache_test.rs`: 6 new unit tests for the parse cache (hit/miss/path-discrimination/error-not-cached/concurrent-safety/correctness).

### Performance — optimization sweep (Phases 1-7, 2026-04-12)

A 14-proposal optimization sweep landed end-to-end on 2026-04-11/12. Cumulative wins (criterion benchmarks, isolated runs):

| Workload | pre-all | post-sweep | Cumulative |
|---|---:|---:|---:|
| Partition equality query (single date) | 2.81 ms | 2.25 ms | **−19.9%** |
| Narrow range query (5 partitions) | 11.05 ms | 5.78 ms | **−47.7%** |
| Wide range query (500 partitions) | 988.63 ms | 362.59 ms | **−63.3%** (2.7×) |
| Group-by aggregation | 7.66 ms | 3.30 ms | **−57.0%** (2.3×) |
| `select * from t where date=X` | 1.52 ms | 365 µs | **−76.0%** (4.2×) |
| Parse repeated query (cache hit) | 374 µs | **385 ns** | **−99.9%** (970×) |
| Multi-table HDB load (5×200) | 2.87 ms | 1.53 ms | **−46.7%** |
| Single-table HDB load (2000 partitions) | 6.27 ms | 5.05 ms | −19.5% |
| Partition write (`wpar`) | 10.46 ms | 9.20 ms | −12.0% |

**Python concurrent throughput (8 threads × 200 queries)**:

| Metric | pre-all | post-sweep |
|---|---:|---:|
| Single-thread throughput | 564 q/s | 1281 q/s |
| 8-thread concurrent throughput | **519 q/s** | **3168 q/s** |
| Speedup vs serial (ideal = 8×) | **0.92×** (worse than serial) | **2.47×** |

Concurrent Python throughput is **6.10× higher** end-to-end. Going from 0.92× speedup (8 threads slower than 1) to 2.47× speedup is the difference between "Python concurrency is broken on chili" and "Python concurrency works".

Component changes:
- **Build profile** (`Cargo.toml`): `opt-level = "z"` → `3`, `codegen-units = 6` → `1`, `lto = true` → `"fat"`. Parser, eval, scan, load all see 7-66% speedup.
- **Allocator**: `mimalloc` set as `#[global_allocator]` in `chili-bin/src/main.rs` and `chili-py/src/lib.rs`.
- **Inline annotations**: `#[inline]` on hot SpicyObj conversion methods (`is_fn`, `size`, `str`, `to_bool`, `to_i64`, `to_par_num`).
- **Schema sentinel cache** (`crates/chili-core/src/par_df.rs`): `PartitionedDataFrame::empty_schema: Option<Arc<DataFrame>>` populated at load time. Removes per-miss parquet open.
- **Two-phase `load_par_df`** (`crates/chili-core/src/engine_state.rs`): build all `PartitionedDataFrame` entries outside the lock; commit via single `HashMap::extend`. Concurrent readers no longer block during reload.
- **rayon parallel per-table scans** in `load_par_df`: 5-table HDB load **−39.8%**.
- **rayon parallel `glob::glob`** in `scan_partitions` and `scan_partition_by_range` (`crates/chili-core/src/par_df.rs`): wide-range query **−58.5%** in addition to all prior phases.
- **`collect_schema()` shortcut** in `make_it_lazy` (`crates/chili-core/src/eval_query.rs`): replaces `lf.filter(lit(false)).collect().get_column_names()` round-trip with metadata-only schema lookup.
- **Filter fusion** in `eval_fn_query`: sequential `.filter(c1).filter(c2)...` → `.filter(c1.and(c2)...)`.
- **Vec pre-allocation** for where/op/by expression vectors with `Vec::with_capacity(n)`.
- **LRU parse cache** (`crates/chili-core/src/engine_state.rs`): `Mutex<LruCache<(String, String), Arc<Vec<AstNode>>>>`, capacity 256. **Cache hit is 970× faster** than re-parsing.
- **GIL release in chili-py** (`crates/chili-py/src/lib.rs`): `Engine::eval`, `Engine::wpar`, `Engine::load` wrap their bodies in `py.allow_threads`. Cumulative concurrent throughput **6.10×**.
- **Canonicalize cache** in `crates/chili-op/src/io.rs`: process-wide `RwLock<HashMap<String, PathBuf>>` keyed on input HDB path. Eliminates `fs::canonicalize` syscall in tight wpar loops.

See `docs/bench/summary.md` for the full breakdown and `docs/bench/phase{1..7}.md` for per-phase notes.

### Python API — mdata wishlist phases (9-15, 2026-04-11/12)

Extensions to `chili-py` (Python bindings) shipping after the core optimization sweep:

- **Phase 9 — Polars projection pushdown verified (no code change)**: polars 0.53 already pushes column projection through both `scan_partition` and `scan_partition_by_range`. An 11-column OHLCV partition with a 1-column select shows 81% I/O reduction. No chili changes required.

- **Phase 10 — Symbol predicate pushdown** (`wpar` `sort_columns` parameter): `engine.wpar(df, hdb, table, date, sort_columns=["symbol"])` sorts the partition by symbol before writing and sets `row_group_size=16384` (16 row groups per ~1 M-row partition). Polars uses parquet row-group min/max statistics to skip row groups on `where symbol=X`, reducing I/O from full-partition scans. Pruning ratio improved from 0.75× to 0.21× in benchmarks.

- **Phase 11 — Fork detection guard**: `Engine` records its construction PID. Any method call (`load`, `eval`, `wpar`) from a forked child process raises `RuntimeError` immediately with a clear message: "Use `multiprocessing.get_context('spawn')` instead of 'fork', or create a new Engine in each child process."

- **Phase 12 — Engine lifecycle API**: New methods on `Engine`:
  - `close()` — release Rust state immediately (deterministic cleanup; subsequent calls raise `AttributeError`)
  - `unload()` — drop all loaded partitions but keep the engine alive
  - `reload()` — re-scan the last-loaded HDB directory for new partitions
  - `is_loaded()` — return True if at least one partitioned table is loaded
  - `table_count()` — return the number of loaded partitioned tables

- **Phase 13 — Structured Python exception hierarchy**: 7 typed exception classes exported from the `chili` module, all extending `RuntimeError` for backwards compatibility:
  - `ChiliError` — base class for all chili errors
  - `PepperParseError` — syntax/parse errors
  - `PepperEvalError` — evaluation errors
  - `PartitionError` — missing `date` predicate or partition not found
  - `TypeMismatchError` — argument type or count mismatch
  - `NameError` — undefined variable or function
  - `SerializationError` — Arrow IPC serialization/deserialization failure

- **Phase 14 — Observability primitives**: `engine.stats()` returns a dict with `partitions_loaded`, `parse_cache_len`, and `hdb_path`. `engine.parse_cache_len()` exposed directly. `engine.query_plan(query)` stubbed — raises `RuntimeError` with a descriptive message; full implementation deferred pending a lazy-mode eval path in `EngineState`.

- **Phase 15 — Quantized column dequantization helper**: `engine.set_column_scale(table, column, factor)` registers a scale factor. On any subsequent `engine.eval()` call, result columns of type `Int64` matching a registered `(table, column)` pair are automatically cast to `Float64` and divided by `factor`. Float64 columns are left untouched (graceful no-op on un-quantized HDBs). `engine.clear_column_scales()` removes all registered factors.

- **Phase 16 — Python broker bindings (WL 1.1)**: In-process pub/sub for `Engine`. New methods:
  - `engine.publish(topic, ipc_bytes) -> int` — deliver Arrow IPC bytes to all subscribers of a topic; returns per-topic monotonic sequence number
  - `engine.subscribe(topics, callback)` — register a callback `(topic, seq, ipc_bytes)` invoked from a background Rust thread (GIL released during channel ops)
  - `engine.tick_upd(table, df) -> int` — convenience: serialize a polars DataFrame and publish it
  - `engine.broker_eod(eod_message)` — broadcast `("__eod__", 0, payload)` to all subscriber callbacks regardless of topic
  - Implementation: bounded `mpsc::sync_channel` (capacity 1024) per subscriber per topic; `AtomicBool` shutdown flag prevents queue drain on deallocation; backpressure logged via `log::warn` on full channel
  - Also fixed a pre-existing `_column_scales` class-variable bug (Phase 15) that caused scale factors to be shared across all Engine instances

- **Post-Phase-16 additions**:
  - `engine.overwrite_partition(df, hdb_path, table, date, sort_columns=None)` — deletes all existing shard files for the given date and writes a single fresh `_0000` file. Unlike `wpar()` (which appends `_0001`, `_0002` etc.), enables safe in-place HDB rewrites (dtype migrations, re-sorting). Schema validation enforced.
  - `engine.broker_eod()` now fully implemented — broadcasts the EOD sentinel to all subscriber channels.
  - `engine.query_plan(query)` — creates a temporary lazy-mode engine, loads the HDB, evaluates the query to a `LazyFrame`, and returns `describe_plan()` without collecting. Equivalent to `EXPLAIN` in SQL. Requires an HDB to be loaded.

- **Phase 17 — Aggregation pushdown (WL 2.3)**: Closed without code changes. Profiling on a real 252-partition HDB showed `select last close by symbol from ohlcv_1d` running at **181 ms** — within the original <200 ms target. Phases 10 (sorted writes) + 15 (dequantize in Python) brought Q11 from 1,850 ms → 181 ms (10.2×). Remaining gap vs polars native (181 ms vs 63 ms) is chili eval machinery overhead, not aggregation strategy. Profiling results: `docs/bench/mdata-collab/benchmarks/phase17_profile_results.json`.

### Tests added (Phases 16-17)

- `crates/chili-py/tests/test_broker.py`: 11 pytest tests for broker bindings (round-trip, multi-topic isolation, fan-out 2/4/8 subscribers, sequence monotonic, sequence per-topic independence, stress 2000 frames × 4 subs, `tick_upd`, no-subscriber publish, EOD delivery)
- `crates/chili-py/tests/test_extras.py`: 8 pytest tests for `overwrite_partition` (replaces data, removes multiple shards, creates fresh on nonexistent), `broker_eod` (reaches all subscribers, does not interfere with normal publish), `query_plan` (returns string, does not execute, requires HDB)
- **Total**: 35 Python pytest tests, 165 Rust tests, 200 tests overall

## [0.7.4] - 2026-03-21

### Added

- Cache source code from REPL and IPC connections for printing error messages

## [0.7.2] - 2026-02-24

### Removed

- Removed the `%` operator
- Vim syntax highlighting support, chiz will support it soon

### Fixed

- Fixed the handling of Windows paths
- Fixed the symbol token for Windows paths
- Fixed `upsert` and `insert` to support DataFrame as the first argument

## [0.7.1] - 2026-02-22

### Added

- Supported Windows

## [0.7.0] - 2026-02-21

### Added

- Refactored to use chumsky as the parser
- Unified the binaries into one chili binary
- Added `-P` or `--pepper` flag to enable REPL using pepper syntax
- Supported macOS

## [0.6.4] - 2026-02-08

### Added

- Improved the handling of MixedList in the deserialization process to return an empty list when appropriate.
- Modified ListExp, SelectExp, ByExp, Table, Matrix, and Dict to support optional trailing commas.
- Enhanced BracketExp and ColNames to maintain consistency with the new syntax rules.

## [0.6.3] - 2026-01-25

### Added

- Lazy evaluation mode for the runtime
- New built-in function `collect` to collect a lazy DataFrame
- `os.pid` to retrieve the process ID
- `os.version` to retrieve the operating system version
- `os.syntax` to retrieve the syntax type (chili or pepper)

## [0.6.2] - 2025-12-14

### Added

- New built-in function `insert` to insert data from DataFrame or list and keep the last record for each group
- New built-in function `.os.mem` to retrieve memory statistics
- Memory limit command line option for the runtime

### Changed

- Enhanced upsert function to enforce DataFrame type for the first argument
- Updated parser to support new syntax for column definitions

## [0.6.1] - 2025-12-10

### Added

- Support for new syntax highlighting and validation in Chili language
- Short-circuit evaluation for logical operators (`||`, `&&`, `??`) in AST and evaluation logic
- Support for `if` statements with optional `else` blocks for `chili` language
- New binary operators and control keywords in grammar definitions

### Changed

- Enhanced startup banner with vintage feature support and updated graphics
- Updated job function name formatting in EngineState evaluation
- Updated parser to support new control flow structures
- Adjusted tests to reflect changes in parsing and evaluation structure

### Fixed

- Corrected comparison operator in 'lt' function

## [0.6.0] - 2025-12-07

### Added

- Initial release of Chili
- Support for two language syntaxes:
  - chili: a modern programming language similar to JavaScript
  - pepper: a vintage programming language similar to q
- Integration with Polars for data manipulation
- Support for Arrow/Parquet data storage
- Vim syntax highlighting support
