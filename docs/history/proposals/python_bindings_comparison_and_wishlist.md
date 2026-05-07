# Chili Python bindings — design comparison and wishlist

**Author:** Treehouse Finance (downstream user of chili driving a market-data warehouse called *mdata*).
**Date:** 2026-04-26
**Audience:** Hinmeru / `purple-chili` author, for collaboration on the next phase of `chili-py`.
**Context:** We've maintained a parallel chili-py implementation in our local fork (`Engine` class, package `chili-pie`) that grew to support a Python-driven analytics service. Upstream now ships `PyEngineState` (commits `bf9fa14..b0f20e5`, package `chili-sauce`). After the 2026-04-26 merge of upstream's bytes-removal FFI rewrite into our local fork, the two designs share the same conversion layer but expose different surfaces. This document captures the differences and proposes a wishlist of features `PyEngineState` could adopt to serve app-grade Python clients without forcing them to fork.

---

## Same engine, different binding shapes

Both classes wrap the same `Arc<chili_core::EngineState>`. They differ in **what the FFI surface exposes** and **how it behaves under concurrency**.

After our merge of upstream commit `bf9fa14`, **the conversion layer is identical** — both use `spicy_to_py` / `spicy_from_py_bound` to return polars DataFrames directly via `pyo3_polars::PyDataFrame`. Zero-copy. No Arrow IPC bytes round-trip. Small `eval()` measures ~9 µs/call end-to-end on M-series Apple silicon.

So the comparison below is purely about **what each class exposes on top of that shared conversion layer**.

---

## Side-by-side

| Aspect | `PyEngineState` (upstream `chili-sauce`) | `Engine` (local `chili-pie`) |
|---|---|---|
| Lib name | `engine_state` | `chili` |
| Wraps | `Arc<EngineState>` | `Arc<EngineState>` (same) |
| Default syntax | chili (modern) | pepper (q-like) |
| Design philosophy | Thin 1:1 over `EngineState` methods — REPL/tooling shape | Curated app-grade API — service shape |
| FFI conversion | `spicy_to_py` / `spicy_from_py_bound` (canonical pyo3-polars) | `spicy_to_py` / `spicy_from_py_bound` (same, post-merge) |
| GIL behavior on long ops | **Held** for parse + eval + collect | **Released** via `py.detach` on `eval`, `wpar`, `overwrite_partition`, `tick_upd`, `query_plan`, `load` |
| Concurrent throughput (8 threads) | ~0.9× single-thread (GIL-bound, worse than serial) | ~6.1× single-thread (3168 q/s vs 519 q/s on benchmarks) |

### Methods unique to `PyEngineState`

Direct passthroughs to `EngineState` internals — useful for REPL tooling, language servers, q-style scripting hosts:

- `set_var(id, value)` / `get_var(id)` / `has_var(id)` / `del_var(id)` — variable assignment from Python
- `fn_call(name, args)` — generic dispatcher; call any registered function by name
- `set_source(path, src)` / `get_source(idx)` — manual source-cache management
- `import_source_path(rel, path)` — import semantics
- `tick(inc)` / `get_tick_count()` — tick counter
- `get_displayed_vars()` / `list_vars(pattern)` — REPL introspection
- `is_lazy_mode()` / `is_repl_use_chili_syntax()` — config flags
- `clear_par_df()` — direct partition-cache clear
- `shutdown()` — engine teardown

### Methods unique to `Engine`

Application-grade features that don't exist in `PyEngineState`. Each one was added to satisfy a real downstream need:

| Method | What it does | Why it exists |
|---|---|---|
| `py.detach` wrapping on every long op | Releases the GIL during parse → eval → collect | Multi-threaded Python clients need to drive the engine in parallel; without GIL release, throughput goes *down* with thread count |
| `check_fork()` | Records `init_pid` at construction; every method raises `RuntimeError` if PID changed | mdata's `ProcessPoolExecutor('fork')` on macOS deadlocked on poisoned `RwLock` instead of erroring; clear message beats silent hang |
| Structured exception hierarchy (`ChiliError` + 6 subclasses: `PepperParseError`, `PepperEvalError`, `PartitionError`, `TypeMismatchError`, `NameError`, `SerializationError`) | Lets callers `except chili.PartitionError:` separately from parse errors | App code needs to distinguish "user typo" from "missing date predicate" from "bad data" without string-matching error messages |
| `overwrite_partition(df, hdb, table, date, sort_columns)` | Delete all `{date}_*` shards, write fresh `_0000` | In-place HDB rewrites (dtype migrations, re-sorting). `wpar` only appends — there's no way to fix a partition without it |
| `tick_upd(table, df)` | Rust-side IPC serialize + publish in one call | Faster than `df.write_ipc_stream()` Python-side; the serialize runs with GIL released |
| `publish(topic, ipc_bytes)` / `subscribe(topics, callback)` / `broker_eod(payload)` | In-process pub/sub broker — bounded mpsc channels (1024), per-topic background threads, backpressure | mdata's tick-update fan-out: REST clients subscribe to live updates, RDB writer subscribes to flush; the Rust broker drives both |
| `query_plan(query)` | Describe-plan without execution (DuckDB EXPLAIN equivalent) | Lets users see partition pruning + projection pushdown decisions; builds a temp lazy-mode engine to call `describe_plan()` |
| `stats()` | Returns `{partitions_loaded, parse_cache_len, hdb_path}` | Prometheus / health-check / observability |
| `is_loaded()` / `table_count()` / `reload()` / `unload()` / `close()` | Lifecycle methods at the right granularity for app code | Tests need deterministic teardown; long-running services need re-load after staging-area swap |
| `set_column_scale(table, col, factor)` + auto-dequant in `eval` | Register Int64-quantized columns; auto-cast to Float64 / divide on read | mdata stores prices as Int64 × 1_000_000 for parquet stat pruning; readers want Float64 transparently |
| `Drop` impl that signals `broker_shutdown` | Subscriber threads exit immediately instead of draining queue with the GIL | Tests / scripts that build many engines were spending seconds in teardown without this |

---

## Wishlist — features `PyEngineState` could adopt

Listed in priority order based on impact for any non-REPL Python client. Each item is a minimal change to the upstream class. We've shipped all of them locally; happy to contribute.

### P1 — Release the GIL on long ops

**Status:** absent in `PyEngineState`. Single biggest perf gap.

**Change:**
```rust
fn eval(&self, py: Python<'_>, source: &str) -> PyResult<Py<PyAny>> {
    let state = Arc::clone(&self.inner);
    let source = source.to_owned();
    let obj = py.detach(move || -> Result<SpicyObj, SpicyError> {
        let mut stack = Stack::new(None, 0, 0, "");
        let args = SpicyObj::String(source);
        let src_path = if state.is_repl_use_chili_syntax() { "repl.chi" } else { "repl.pep" };
        state.eval(&mut stack, &args, src_path)
    }).map_err(|e| ChiliError::new_err(e.to_string()))?;
    spicy_to_py(py, obj)
}
```

`EngineState` is already `Send + Sync` (it's used across `thread::spawn` in `chili-bin`'s IPC server), and nothing inside the closure touches Python objects, so the closure body is fully `Send`.

**Impact:** an N-thread Python client driving the same engine scales close to N× throughput, bounded by the polars rayon pool size. We measured 6.10× on 8 threads vs 0.92× without GIL release (worse than serial because of contention).

Apply the same pattern to `load_par_df` and any other multi-millisecond method.

### P2 — Fork-safety guard

**Status:** absent in `PyEngineState`. Causes silent deadlocks.

**Change:**
```rust
struct PyEngineState {
    inner: Arc<EngineState>,
    init_pid: u32,
}

impl PyEngineState {
    fn check_fork(&self) -> PyResult<()> {
        if std::process::id() != self.init_pid {
            return Err(PyRuntimeError::new_err(format!(
                "EngineState is not fork-safe. Created in PID {} but running in PID {}. \
                 Use multiprocessing.get_context('spawn') instead of 'fork', \
                 or create a new EngineState in each child process.",
                self.init_pid, std::process::id(),
            )));
        }
        Ok(())
    }
}
```

Call `check_fork()` at the top of every `#[pymethods]`. Cost: one PID compare per call.

**Impact:** Python's default `multiprocessing` start method on Linux is `fork`. Fork-after-Rust-thread-creation breaks `RwLock` / `Arc` / mimalloc thread-local pools. Without this, child processes deadlock on the first `eval` call. With this, they get a clear error pointing at the fix (`spawn` instead of `fork`, or new engine per child).

### P3 — Structured exception hierarchy

**Status:** `PyEngineState` exposes only `ChiliError`. App code needs more granularity.

**Change:**
```rust
create_exception!(chili, ChiliError, PyRuntimeError);
create_exception!(chili, PepperParseError, ChiliError);
create_exception!(chili, PepperEvalError, ChiliError);
create_exception!(chili, PartitionError, ChiliError);
create_exception!(chili, TypeMismatchError, ChiliError);
create_exception!(chili, NameError, ChiliError);
create_exception!(chili, SerializationError, ChiliError);

fn map_spicy_error(err: &SpicyError) -> PyErr {
    match err {
        SpicyError::ParserErr(_) => PepperParseError::new_err(err.to_string()),
        SpicyError::EvalErr(msg) if msg.contains("condition for this partitioned") =>
            PartitionError::new_err(err.to_string()),
        SpicyError::EvalErr(_) => PepperEvalError::new_err(err.to_string()),
        SpicyError::NameErr(_) => NameError::new_err(err.to_string()),
        SpicyError::MismatchedTypeErr(..)
        | SpicyError::MismatchedArgTypeErr(..)
        | SpicyError::MismatchedArgNumErr(..)
        | SpicyError::MismatchedArgNumFnErr(..) => TypeMismatchError::new_err(err.to_string()),
        SpicyError::NotAbleToSerializeErr(_)
        | SpicyError::DeserializationErr(_)
        | SpicyError::NotAbleToDeserializeErr(_) => SerializationError::new_err(err.to_string()),
        _ => ChiliError::new_err(err.to_string()),
    }
}
```

All subclasses extend `ChiliError`, which extends `RuntimeError` — so existing `except RuntimeError:` and `except chili.ChiliError:` catches still work.

**Impact:** lets app code do `except chili.PartitionError:` to surface a "missing date predicate" message to the user, separately from `PepperParseError` (typo) or `TypeMismatchError` (bad arg). String-matching error messages is fragile.

### P4 — `overwrite_partition` semantic

**Status:** absent in `PyEngineState`. `wpar` is append-only.

**Use case:** an HDB needs to be rewritten in place. Examples we hit:
- Float64 → Int64 quantization sweep over 1252 daily partitions.
- Re-sorting partitions by `symbol` to enable parquet row-group pruning.
- Fixing bad ingest by replacing the affected partition.

Without `overwrite_partition`, the only options are: (a) delete files manually outside chili, or (b) accept that re-running `wpar` creates `_0001`, `_0002`, ... shards alongside the bad data.

Implementation is small (~50 lines): canonicalize `hdb_path`, parse date, list `{table}/{date}_*`, `remove_file` each, then call the regular write path with no existing shards.

### P5 — `tick_upd(table, df)` for direct DataFrame publish

**Status:** absent. Users have to do `df.write_ipc_stream(buf); publish(table, buf.getvalue())` from Python — two unnecessary buffer copies and the serialize runs with the GIL held.

**Change:** add a method that takes `PyDataFrame`, calls `IpcStreamWriter::new(&mut buf).finish(&mut frame)` inside `py.detach`, then publishes. Subscriber API contract unchanged (still receives bytes). One-method addition.

### P6 — Optional broker (`publish` / `subscribe` / `broker_eod`)

**Status:** absent. We added an in-process broker because mdata needed live tick-update fan-out without TCP roundtrips.

This is a bigger feature than P1–P5 — fully understandable if upstream wants to keep the surface lean. But if the chili roadmap considers in-process pub/sub at all, our design is small (~120 LOC) and battle-tested:

- `topic → Vec<SyncSender<(topic, seq, bytes)>>` with bounded channels (cap 1024)
- One background thread per `subscribe()` call that reads the channel and invokes the Python callback under `Python::attach`
- `try_send` for backpressure (drop on full)
- `Drop` impl that sets a shutdown flag so subscriber threads don't drain queues during teardown

If you're interested, we'd happily contribute this as an opt-in `broker` feature flag.

### P7 — `query_plan(query)` — describe-plan without executing

**Status:** absent. Useful for users debugging "why is this query slow" — shows partition pruning, projection pushdown, predicate pushdown decisions in one string.

**Change:** build a temporary lazy-mode `EngineState`, load the same HDB, eval the query (which yields a `LazyFrame`), call `describe_plan()`, return the string. ~20 lines.

### P8 — `stats()` for observability

**Status:** absent. Prometheus exporters / health checks need a one-shot dict of internal metrics.

**Change:** trivial — return `{partitions_loaded: usize, parse_cache_len: usize, hdb_path: Option<String>}`. Three method calls into `EngineState`.

### P9 — Quantization helper (`set_column_scale`)

**Status:** absent. This one is mdata-specific and reasonable to leave out of upstream — but flagging in case other quant-data users hit the same pattern.

We store OHLCV prices as `Int64 × 1_000_000` on disk (better parquet stats, exact prices). On read, `set_column_scale("ohlcv_1d", "close", 1_000_000)` registers a one-time auto-dequant, so callers see Float64. Lives mostly in the Python wrapper; the Rust side could expose a hook (`register_column_scale(table, col, factor)`) that `eval` checks against.

If chili-sauce users start asking about it, the design is in our `Engine.set_column_scale` and we can share it.

---

## Naming question

Upstream renamed `chili-pie → chili-sauce` in `pyproject.toml` but hasn't published to PyPI yet. We left our local package as `chili-pie 0.7.5` because mdata and one other downstream depend on the import path. When `chili-sauce` actually ships to PyPI, we'd love to converge — happy to coordinate the rename.

Suggested path (no urgency):
1. Author publishes `chili-sauce 0.8.0` to PyPI with `PyEngineState`.
2. We add a `chili-sauce` compatibility wrapper to our package (re-exports the `PyEngineState` API on top of our existing Rust crate so users can choose which surface they want).
3. We deprecate `chili-pie` (or keep it as a thin wrapper around the upstream package once features above are upstream).

Or in the other direction:
1. Upstream merges P1–P5 into `PyEngineState`.
2. We retire our `Engine` class and migrate downstream to `PyEngineState`.
3. `chili-pie` package becomes a no-op / forwarder.

Either order works for us — the bottleneck is feature parity, not naming.

---

## What we already adopted from upstream

For completeness — the merge that shipped on 2026-04-26:

- **Bytes-removal FFI rewrite** (`bf9fa14`): both `eval()` and `wpar()` used to round-trip Arrow IPC bytes; now use `pyo3_polars::PyDataFrame` directly. ~9 µs/call small eval.
- **`spicy_to_py` / `spicy_from_py_bound`** verbatim from upstream.
- **pyo3 0.22 → 0.27** + **pyo3-polars 0.26.0**.
- **`chrono` workspace dep** + Python ≥ 3.10 floor.
- **`.gitignore`** additions (`.mypy_cache/`, `.venv/`).

Not adopted:
- chili-pie → chili-sauce rename (downstream import dependency).
- manylinux 2_28 release workflows (we don't ship via PyPI).

---

## Closing

The bytes-removal merge was the single biggest perf win we've ever made on this surface — thank you for shipping it. The wishlist above is what would let us migrate from `Engine` to `PyEngineState` and stop maintaining a parallel binding altogether.

Open to discussion on any of P1–P9, scope, sequencing, or design alternatives. Happy to send PRs against `purple-chili/chili main` for any item you'd accept.

— Treehouse Finance, on behalf of mdata
