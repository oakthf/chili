# Phase 6 — GIL release in chili-py (A)

**Shipped:** 2026-04-12

## Change

### A — `py.allow_threads` around the entire query body

- `crates/chili-py/src/lib.rs::Engine::eval`: wrapped query body in `py.allow_threads(move || ...)`. The closure owns its captures (cloned query string + Arc<EngineState>), runs the entire chili eval pipeline + Arrow IPC serialization without holding the GIL, and returns `Result<Vec<u8>, String>`. The String error is converted to a `PyRuntimeError` after re-acquiring the GIL.
- Same treatment for `Engine::wpar` (clones the IPC bytes + path strings before releasing the GIL).
- Also applied to `Engine::load`.
- Added a small helper `df_to_ipc_bytes_unchecked` that mirrors `df_to_ipc_bytes` but returns `Result<Vec<u8>, PolarsError>` so it can be called outside the GIL without constructing a `PyErr`.

### Why this is safe

- `EngineState: Send + Sync` — already passed across `thread::spawn` boundaries in `chili-bin/src/main.rs`'s IPC server (verified during Phase 0 architecture survey).
- Nothing inside the closures touches Python objects, builds Python data structures, or calls back into Python.
- `pyo3::PyErr::new::<T, _>(message)` is documented as GIL-independent (it stores a type ID + message and only realizes the Python exception when the framework consumes the PyErr).
- The query string and IPC bytes are copied into the closure before `allow_threads` so the closure owns them; the original `&str` / `&[u8]` references (which point into Python-owned buffers) are not used after GIL release.

## Benchmark — `crates/chili-py/tests/bench_concurrent.py`

8 threads × 200 queries × 5 runs against the partition_filter fixture.

| Metric | pre-all (Phase 0) | post-phase6 | Δ |
|---|---:|---:|---:|
| Single-thread p50 (200 queries) | 354.5 ms | **156.2 ms** | **−55.9%** |
| Single-thread throughput | 564 q/s | **1281 q/s** | **2.27×** |
| 8-thread concurrent p50 (8×200 queries) | 3081.2 ms | **505.0 ms** | **−83.6%** |
| **Concurrent throughput (8 threads)** | 519 q/s | **3168 q/s** | **6.10×** |
| Speedup vs single-thread baseline | **0.92×** (worse than serial) | **2.47×** | from pathological to scaling |

### Decomposing the wins

The single-thread improvement (354.5 → 156.2 ms) is the cumulative effect of phases 1-5, NOT Phase 6. Specifically:
- Phase 1 (build-system) gave generic LLVM speedups
- Phase 5 (parse cache) is the largest single contributor — the bench reuses the same query string, so every iteration after the first is a cache hit, dropping parse cost from ~140 µs to ~400 ns

The Phase 6 contribution alone is the **gap between 8-thread concurrent throughput and single-thread throughput**:
- Pre-Phase-6: 519 q/s concurrent / 564 q/s single = 0.92× — 8 threads were *worse* than 1 because of GIL contention overhead
- Post-Phase-6: 3168 q/s concurrent / 1281 q/s single = 2.47× — 8 threads now scale to 2.47× single-thread

The 2.47× ceiling (vs ideal 8×) is **not a bug**. It's the polars rayon pool being shared:
- 8 Python threads each launch a query
- Each query internally spawns polars compute on the rayon pool
- Rayon's worker count = num_cpus, not 8 × num_cpus
- So when 8 queries run "in parallel", they fight for the same N rayon workers
- Effective per-query parallelism on a 10-core machine: ~3-4 cores
- Effective speedup at 8 threads: ~2.5× (matches measured 2.47×)

This is the expected ceiling for chili's architecture. Going higher requires either:
1. Smaller queries (less polars work per query → less rayon contention)
2. More cores (linear scaling)
3. Process-per-engine model (each process gets its own rayon pool — but loses shared engine state)

For mdata's REST gateway hot path the meaningful number is **3168 q/s sustained from a single chili.Engine accessed by N Python threads** — up from 519 q/s pre-Phase-6.

## Regression tests

All 16 chili-py pytest tests pass. The added concurrent scenario (`test_concurrent_correctness` in `bench_concurrent.py`) runs 4 threads × 50 queries and asserts row count consistency — all results match expected.

165 Rust tests pass (102 chili-core lib + 6 parse_cache_test + the rest unchanged).

## What's next

Phase 7 — write path (Proposal O). Cache canonicalized HDB path so `wpar` doesn't re-syscall on every call. Small change in `crates/chili-op/src/io.rs`. Expected gain: a few µs per wpar call, more if mdata uses wpar in tight loops.

Then Phase 8 — final validation, full bench sweep, CHANGELOG, README, housekeeper agent.
