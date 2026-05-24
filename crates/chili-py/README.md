# Chili Sauce 🌶️

Python bindings for [Chili](https://purple-chili.github.io/)'s `EngineState`, powered by [PyO3](https://pyo3.rs/).

## Installation

```bash
pip install chili-sauce
```

Requires Python ≥ 3.10.

## Quick Start

```python
from chili import ChiliEngine

engine = ChiliEngine()

# Evaluate expressions
engine.eval("1 + 2")  # => 3

# Variable management
engine.set_var("x", 42)
engine.get_var("x")  # => 42

# Work with Polars DataFrames
import polars as pl

df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
engine.set_var("df", df)
engine.get_var("df")
```

## IPC / Remote Queries

```python
# Open a handle to a remote Chili process
h = engine.open_handle("chili://:1800")

# Send synchronous queries
engine.sync(h, b"1+1")                # bytes  — sent as a raw string query
engine.sync(h, ["set", "a", 2])       # list   — sent as a function call (func, args…)
```

## Python-callable bridge (W3, 0.8.9+)

Register a Python callable so it becomes invokable from pepper / chili-IPC
under a chosen name. Useful for daemons that need a control verb wired to
Python-side bookkeeping (e.g. drain a buffer, finalize a partition) rather
than pure pepper.

```python
from chili import ChiliEngine

engine = ChiliEngine()

def eod_fire(date):
    # ... Python-side bookkeeping (drain buffer, write to disk, etc.) ...
    return f"acked {date}"

engine.engine.register_fn(".mdata.eod.fire", eod_fire, arity=1)

# Local invocation:
engine.engine.fn_call(".mdata.eod.fire", ["2026-05-24"])
# => "acked 2026-05-24"

# Over chili-IPC (the typical mdata shape):
client = ChiliEngine()
h = client.open_handle("chili://:1800")
client.sync(h, (".mdata.eod.fire", "2026-05-24"))
# => "acked 2026-05-24"

# Tear down when done:
engine.engine.unregister_fn(".mdata.eod.fire")
```

- Arity is **explicit** at registration; mismatched call → partial-applied Func.
- Python exceptions propagate as `ChiliError` with the traceback embedded.
- The callable may freely call back into `engine.fn_call` / `engine.set_var`
  / `engine.get_var` (re-entrancy is safe; lock-free dispatch).
- Wire serialization: external Funcs are call-form only — invoke via
  `sync(h, (name, *args))`, not `sync(h, name)` (str-form lookup).
- See `docs/decisions/0007-w3-python-callable-bridge.md` for the full
  contract.

## Features

- **Evaluate** Chili or Pepper expressions from Python
- **Variable management** — get, set, delete, and list variables
- **Polars integration** — pass DataFrames bidirectionally between Python and Chili
- **Partitioned storage** — write and load date-partitioned Parquet tables
- **IPC / TCP** — start a TCP listener for remote connections
- **Tick plant** — built-in pub/sub infrastructure for real-time data
- **Python-callable bridge** — register Python functions as pepper-invokable

## Type Mapping

| Python type         | Chili type  |
| ------------------- | ----------- |
| `int`               | `Int`       |
| `float`             | `Float`     |
| `bool`              | `Bool`      |
| `str`               | `Symbol`    |
| `bytes`             | `String`    |
| `None`              | `Null`      |
| `list`              | `MixedList` |
| `dict`              | `Dict`      |
| `datetime.date`     | `Date`      |
| `datetime.time`     | `Time`      |
| `datetime.datetime` | `Datetime`  |
| `polars.DataFrame`  | `DataFrame` |

## Development

```bash
# Build and install in development mode
maturin develop --release --manifest-path crates/chili-py/Cargo.toml

# Run tests
pytest crates/chili-py/tests/
```
