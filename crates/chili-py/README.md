# chili-py — Python bindings for Chili

High-performance kdb+/q-compatible analytical engine accessible from Python via PyO3 + maturin.

## Build

```bash
cd crates/chili-py
maturin develop
```

This compiles the Rust extension and installs it into the active Python environment as the `chili` module.

## Usage

```python
import chili

engine = chili.Engine(pepper=True)          # kdb+/q syntax
engine.load("/path/to/hdb")                 # load partitioned HDB
df = engine.eval("select from ohlcv_1d where date=2024.01.02")  # polars.DataFrame
engine.wpar(df, "/hdb", "ohlcv_1d", "2024.01.02")               # write partition
```

## Partition date predicates

Queries against a date-partitioned table (a table whose files on disk are named
`YYYY.MM.DD_NNNN`) **must** include a `date` condition in the where clause so
Chili can prune partitions up front. A bare `select from table` on a partitioned
table errors with `requires 'ByDate' condition` by design.

All of these shapes are supported and route through partition pruning:

| Pattern | Meaning |
|---|---|
| `where date=2024.01.02`               | exact single partition |
| `where date>=2024.01.02, date<=2024.01.04` | tight inclusive range (both bounds prune) |
| `where date>=2024.01.02`              | half-open lower bound |
| `where date<=2024.01.04`              | half-open upper bound |
| `where date>2024.01.02, date<2024.01.05` | strict bounds (exclusive) |
| `where date within 2024.01.02 2024.01.04` | k/q inclusive range |
| `where date in 2024.01.02 2024.01.04 2024.01.08` | explicit partition list |

Non-partition predicates can appear **before or after** the partition predicate
in the same where clause and are applied as row-level filters after partition
pruning. `where symbol=\`AAPL, date=2024.01.03` and `where date=2024.01.03, symbol=\`AAPL`
are both valid and produce the same result.

Writing out-of-order: partitions can be written to an HDB in any order via
`engine.wpar`. The subsequent `engine.load` deterministically sorts the
in-memory partition index so narrow-range queries pick the right files
regardless of on-disk filesystem iteration order.

## Symbol predicate pushdown (`sort_columns`)

By default chili writes a single parquet row group per partition. Symbol
predicates (`where symbol='AAPL'`) are applied as row-level filters after the
full partition is read into memory.

To enable parquet row-group pruning by symbol, sort the partition at write time:

```python
engine.wpar(df, "/hdb", "ohlcv_1d", "2024.01.02", sort_columns=["symbol"])
```

This sorts the DataFrame by `symbol` before writing and forces
`row_group_size=16384` (16 row groups per ~1 M-row partition). On a subsequent
`eval`, polars uses the parquet min/max statistics to skip row groups entirely
for `where symbol=X`, reducing I/O by roughly 4-5× on typical OHLCV tables.

Rebuild the HDB once with `sort_columns=["symbol"]` and all subsequent queries
benefit automatically — no query-string changes needed.

## Engine lifecycle API

```python
engine = chili.Engine()
engine.load("/hdb")

engine.is_loaded()       # True if at least one partitioned table is loaded
engine.table_count()     # number of loaded partitioned tables

engine.unload()          # drop all loaded partitions; engine stays alive
engine.reload()          # re-scan the last-loaded HDB for new partitions

engine.close()           # release Rust state immediately (deterministic cleanup)
```

`close()` is preferred over relying on Python's GC to reclaim the Rust state,
especially in migration scripts and tests where deterministic cleanup matters.
After `close()`, any subsequent call raises `AttributeError`.

## Structured exception hierarchy

All chili exceptions extend `RuntimeError` for backwards compatibility.
Callers can catch specific failure modes:

```python
import chili

try:
    engine.eval("select from ohlcv_1d")          # missing date predicate
except chili.PartitionError:
    print("missing date predicate")
except chili.PepperParseError:
    print("syntax error")
except chili.PepperEvalError:
    print("runtime evaluation error")
except chili.TypeMismatchError:
    print("wrong argument type or count")
except chili.NameError:
    print("undefined variable or function")
except chili.SerializationError:
    print("Arrow IPC serialization failure")
except chili.ChiliError:
    print("some other chili error")
except RuntimeError:
    print("also caught here for backwards compat")
```

All exception classes are importable from the `chili` module:
`from chili import ChiliError, PartitionError, PepperParseError, ...`

## Observability

```python
engine.stats()
# {
#   "partitions_loaded": 3,
#   "parse_cache_len": 42,
#   "hdb_path": "/path/to/hdb"
# }

engine.parse_cache_len()   # LRU parse cache occupancy (max 256 entries)
```

`stats()` is suitable for Prometheus metric export or liveness health checks.

`engine.query_plan(query)` is stubbed and raises `RuntimeError` — full
implementation requires a lazy-mode eval path and is deferred.

## Quantized column dequantization

For HDBs where price columns are stored as `Int64` with a scale factor
(e.g. price × 1,000,000 stored as integer), register the scale at engine
construction time:

```python
PRICE_COLS = ["open", "high", "low", "close", "vwap"]
SCALE = 1_000_000

engine = chili.Engine()
for col in PRICE_COLS:
    engine.set_column_scale("ohlcv_1d", col, SCALE)
    engine.set_column_scale("ohlcv_1m", col, SCALE)

engine.load("/hdb")

# eval() automatically dequantizes matching Int64 columns to Float64
df = engine.eval("select from ohlcv_1d where date=2024.01.02")
# close column is Float64, not Int64

engine.clear_column_scales()   # remove all registered scale factors
```

The auto-dequantize applies only when the column dtype in the result is `Int64`.
Float64 columns are left untouched — this is a safe no-op on un-quantized HDBs.

## Fork safety

`chili.Engine` is not fork-safe after construction. Rust threads, `RwLock`
internals, and the mimalloc allocator pool are all in an undefined state after
`fork`. Any method call from a forked child process raises `RuntimeError`
immediately:

```
chili.Engine is not fork-safe after construction. This engine was created in
PID 1234 but is now running in PID 5678. Use multiprocessing.get_context('spawn')
instead of 'fork', or create a new Engine in each child process.
```

Use `multiprocessing.get_context("spawn")` or create a fresh `Engine` in each
worker process. The GIL is released during `load`, `eval`, and `wpar`, so
threading (not multiprocessing) scales well for concurrent query workloads.

## Arrow IPC Bridge

DataFrames cross the Rust/Python boundary as Arrow IPC bytes. This is necessary because Chili uses `hinmeru/polars-core-patch`, which is ABI-incompatible with the published `py-polars` (PyPI). The Arrow IPC bridge is version-independent and adds ~1-5ms overhead for typical workloads.

## Dependencies

- **pyarrow** >= 14.0 — required for IPC serialization
- **polars** >= 0.20 — required for DataFrames
- **pyo3** 0.22 — Rust/Python bindings (handled by maturin)
- **chili-core** and **chili-op** — Rust crates in the same monorepo

## Module Configuration

- **Crate type**: `cdylib` (shared library)
- **Module name**: `chili` (importable as `import chili`)
- **Python source directory**: `python/` (PEP 517 layout)
- **Build backend**: maturin
