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

## Arrow IPC Bridge

DataFrames cross the Rust/Python boundary as Arrow IPC bytes. This is necessary because Chili uses `hinmeru/polars-core-patch`, which is ABI-incompatible with the published `py-polars` (PyPI). The Arrow IPC bridge is version-independent and adds ~1–5ms overhead for typical workloads.

## Dependencies

- **pyarrow** ≥ 14.0 — required for IPC serialization
- **polars** ≥ 0.20 — required for DataFrames
- **pyo3** 0.22 — Rust↔Python bindings (handled by maturin)
- **chili-core** and **chili-op** — Rust crates in the same monorepo

## Module Configuration

- **Crate type**: `cdylib` (shared library)
- **Module name**: `chili` (importable as `import chili`)
- **Python source directory**: `python/` (PEP 517 layout)
- **Build backend**: maturin
