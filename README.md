![chili](https://github.com/purple-chili/chili/blob/main/icon.png?raw=true)

![PyPI - Version](https://img.shields.io/pypi/v/chili-pie?style=for-the-badge&color=%2321759b) ![Visual Studio Marketplace Version](https://img.shields.io/visual-studio-marketplace/v/jshinonome.vscode-chili?style=for-the-badge&label=vscode%20ext&color=%23007ACC) ![PyPI - Version](https://img.shields.io/pypi/v/chiz?style=for-the-badge&label=chiz&color=%23FFA600)

# Chili: for data analysis and engineering.

## Language syntaxes

[Chili](https://purple-chili.github.io/) uses [Polars](https://www.pola.rs/) for data manipulation and [Arrow](https://arrow.apache.org/)/[Parquet](https://parquet.apache.org/) for data storage. It provides two language syntaxes:

- chili: a modern programming language similar to `javascript`.
- pepper: a vintage programming language similar to `q`.

## Performance

After the 2026-04-12 optimization sweep (Phases 1-7, 14 individual proposals — see `docs/bench/summary.md` and `CHANGELOG.md`):

| Workload | Before sweep | After sweep | Speedup |
|---|---:|---:|---:|
| Single-date equality query | 2.81 ms | 2.25 ms | 1.25× |
| Narrow range (5 partitions) | 11.05 ms | 5.78 ms | 1.91× |
| Wide range (500 partitions) | 988 ms | 363 ms | **2.72×** |
| Group-by aggregation | 7.66 ms | 3.30 ms | **2.32×** |
| Small `select * where date=X` | 1.52 ms | 365 µs | **4.16×** |
| Multi-table HDB load | 2.87 ms | 1.53 ms | **1.88×** |
| Repeat-query parse (cache hit) | 374 µs | **385 ns** | **970×** |
| Python concurrent throughput (8 threads) | 519 q/s | **3168 q/s** | **6.10×** |

The Python concurrent number is the headline: chili-py released the GIL around `Engine::eval`, so an 8-thread Python client can now drive the engine in parallel instead of serializing through the GIL. Pre-sweep, 8 threads were *slower* than 1 thread (0.92× speedup); post-sweep, 8 threads scale to 2.47× single-thread.

## Python API (chili-py)

The `chili-py` crate exposes the Chili runtime to Python via PyO3 + maturin. See `crates/chili-py/README.md` for full documentation. Key surface area:

- **Query**: `engine.eval(query)` — returns a `polars.DataFrame`
- **Write**: `engine.wpar(df, hdb, table, date, sort_columns=None)` — append a partition shard
- **Overwrite**: `engine.overwrite_partition(df, hdb, table, date, sort_columns=None)` — replace a partition in-place (delete shards, write fresh `_0000`)
- **Lifecycle**: `close()`, `unload()`, `reload()`, `is_loaded()`, `table_count()`
- **Observability**: `stats()` returns `{partitions_loaded, parse_cache_len, hdb_path}`; `query_plan(query)` returns a polars `EXPLAIN`-equivalent plan string
- **Quantized columns**: `set_column_scale(table, col, factor)` — auto-dequantize `Int64` price columns to `Float64` on `eval()` results
- **Broker / pub-sub**: `publish(topic, ipc_bytes) -> seq`, `subscribe(topics, callback)`, `tick_upd(table, df) -> seq`, `broker_eod(payload)` — in-process pub/sub backed by bounded mpsc channels; GIL released during channel operations
- **Structured exceptions**: `ChiliError`, `PartitionError`, `PepperParseError`, `PepperEvalError`, `TypeMismatchError`, `NameError`, `SerializationError` — all extend `RuntimeError`
- **Fork safety**: calling any method from a forked child raises `RuntimeError` with a clear "use spawn not fork" message

Run the benchmarks yourself:
```bash
cargo bench -p chili-op --bench scan -- --save-baseline mine
cargo bench -p chili-op --bench eval
cargo bench -p chili-op --bench load_par_df
cargo bench -p chili-op --bench write_partition
cargo bench -p chili-core --bench parse_cache
cd crates/chili-py && uv run python tests/bench_concurrent.py --save mine
```

## Document Index

### Current reference docs

| Document | Purpose |
|----------|---------|
| [`docs/releases/v0.7.5_claude.md`](docs/releases/v0.7.5_claude.md) | **0.7.5 release notes (2026-04-26)** — bytes-removal FFI rewrite, pyo3 0.27 bump, mdata/nxcar migration guide |
| [`crates/chili-py/README.md`](crates/chili-py/README.md) | Python bindings (chili-py): build, install, full API surface |
| [`CHANGELOG.md`](CHANGELOG.md) | Release notes: all shipped features, bug fixes, and optimization phases |
| [`docs/bench/baseline.md`](docs/bench/baseline.md) | Pre-sweep benchmark baseline (`pre-all`, 2026-04-11) |
| [`docs/bench/summary.md`](docs/bench/summary.md) | Optimization sweep final summary (Phases 1-7, 2026-04-12) |
| [`docs/bench/phase{1..7,9}.md`](docs/bench/) | Per-phase benchmark snapshots (shipped 2026-04-11/12) |
| [`docs/bench/mdata-collab/artifacts/quantized_schema.md`](docs/bench/mdata-collab/artifacts/quantized_schema.md) | mdata Int64 price storage schema — scale factor and per-column map |
| [`docs/bench/mdata-collab/mdata_vs_kdb_comparison.md`](docs/bench/mdata-collab/mdata_vs_kdb_comparison.md) | Feature and performance comparison: mdata+chili vs kdb+/q |
| [`docs/bench/mdata-collab/tests/README.md`](docs/bench/mdata-collab/tests/README.md) | mdata-derived test corpus for chili-py PyO3 boundary verification |

### Historical docs (frozen reference)

Under [`docs/history/`](docs/history/):

- `mdata-collab/STATUS_2026-04-12.md` — Complete chili↔mdata collaboration phase tracker (Phases 0–17, all shipped/closed 2026-04-12); preserved as project provenance
- `mdata-collab/artifacts/broker_parity_unix_status_2026-04-12.md` — Unix-backend parity test results from before Phase 16 shipped; ABC interface freeze contract (superseded by Phase 16 delivery)

## References

- [Polars](https://www.pola.rs/): an open-source library for data manipulation.
- [chumsky](https://github.com/zesterer/chumsky): a high performance parser.
- [kola](https://github.com/jshinonome/kola): source code is reused for interfacing with `q`.
- [Nushell](https://www.nushell.sh/): a new type of shell.

Thanks to the authors of these projects and all other open source projects for their great work.
