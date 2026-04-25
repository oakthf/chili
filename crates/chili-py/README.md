# chili-py — Python bindings for Chili

High-performance kdb+/q-compatible analytical engine accessible from Python via PyO3 + maturin.

**Package name:** `chili-pie` (PyPI). Importable module: `chili`. Local fork; the upstream `purple-chili/chili` author plans to publish his own bindings as `chili-sauce` but the two have different surface APIs.

**Requirements:** Python ≥ 3.10, Polars ≥ 0.20. No `pyarrow` dependency.

## What's new in 0.7.5 (2026-04-26)

The Rust↔Python boundary no longer uses Arrow IPC bytes:

- `engine.eval(query)` returns `polars.DataFrame` directly (zero-copy via `pyo3_polars::PyDataFrame`). No `pa.ipc.open_stream` decode.
- `engine.wpar(df, ...)` and `engine.overwrite_partition(df, ...)` accept the DataFrame directly. No `df.write_ipc_stream(buf)` pre-step.
- `engine.tick_upd(table, df)` serializes Rust-side with the GIL released; the wire format and subscriber callback contract are unchanged (subscribers still receive IPC bytes).

If you're upgrading from 0.7.4: drop any `pa.ipc.*` decoding in eval-result handling and any `df.write_ipc_stream(buf)` pre-write. Function signatures otherwise unchanged.

## Build

```bash
cd crates/chili-py
uv run maturin develop --release
```

This compiles the Rust extension and installs it into the active Python environment as the `chili` module.

## Usage

```python
import chili

engine = chili.Engine(pepper=True)          # kdb+/q syntax
engine.load("/path/to/hdb")                 # load partitioned HDB
df = engine.eval("select from ohlcv_1d where date=2024.01.02")  # polars.DataFrame
engine.wpar(df, "/hdb", "ohlcv_1d", "2024.01.02")               # append partition shard
engine.overwrite_partition(df, "/hdb", "ohlcv_1d", "2024.01.02")  # replace partition in-place
```

## Partition write vs overwrite

`wpar()` appends a new shard file (`_0001`, `_0002`, ...) to an existing
partition directory. This is the default append-safe behavior.

`overwrite_partition()` deletes all existing shard files for the given date and
writes a single fresh `_0000`. Use this for in-place HDB rewrites such as dtype
migrations, re-sorting by `sort_columns`, or correcting bad data. Schema
validation is enforced on both paths.

```python
# Rebuild the 2024-01-02 partition sorted by symbol (enables row-group pruning)
old_df = engine.eval("select from ohlcv_1d where date=2024.01.02")
engine.overwrite_partition(old_df, "/hdb", "ohlcv_1d", "2024.01.02", sort_columns=["symbol"])
engine.reload()  # re-scan HDB to pick up the new shard
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

`engine.query_plan(query)` returns a polars logical plan string (equivalent to
`EXPLAIN`) without executing the query. It creates a temporary lazy-mode engine,
loads the HDB, evaluates the query to a `LazyFrame`, and returns
`LazyFrame.describe_plan()`. Requires an HDB to be loaded.

```python
plan = engine.query_plan("select from ohlcv_1d where date=2024.01.02")
print(plan)
# FILTER [(col("date")) == (2024-01-02)]
#   SCAN PARQUET /hdb/ohlcv_1d/2024.01.02_0000
```

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

## Broker / in-process pub-sub

`Engine` provides a lightweight in-process pub/sub broker backed by bounded
`mpsc` channels (capacity 1024 per subscriber per topic). Intended for
streaming live data from a feed manager to multiple downstream consumers in
the same process.

```python
import chili, io, threading, polars as pl

engine = chili.Engine()
received = []

def on_tick(topic: str, seq: int, ipc_bytes: bytes):
    df = pl.from_arrow(pa.ipc.open_stream(io.BytesIO(ipc_bytes)).read_all())
    received.append((topic, seq, df))

engine.subscribe(["trade", "quote"], on_tick)

# From a feed thread:
seq = engine.tick_upd("trade", trade_df)  # serialize DataFrame + publish
seq = engine.publish("trade", raw_ipc_bytes)  # publish pre-serialized bytes

# End-of-day signal to all subscribers:
engine.broker_eod(b"eod_payload")  # delivers ("__eod__", 0, payload) to every callback
```

**Threading model**: each `subscribe()` call spawns one background Rust thread
per topic. Threads acquire the GIL only during callback invocation, so they
never block concurrent `eval()` calls. The `AtomicBool` shutdown flag in the
`Engine` drop implementation prevents the threads from draining the queue
after deallocation.

**Backpressure**: if a subscriber's channel is full, `publish()` drops the
message and logs a warning via `log::warn`. Callers should process batches
promptly or buffer externally.

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
