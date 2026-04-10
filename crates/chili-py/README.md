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
df = engine.eval("select from ohlcv_1d")    # returns polars.DataFrame
engine.wpar(df, "/hdb", "ohlcv_1d", "2024.01.02")  # write partition
```

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
