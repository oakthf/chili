# Claude-Only Features Inventory — Reverse Direction (claude → main port needs)

**Author:** Sprint 2 v2 Part B (reverse-direction comprehensive audit; Explore subagent draft + main-thread pub/sub correction pass to align with ADR 0001 Option a)
**Date compiled:** 2026-05-07
**Fork point:** `d7a748b` (chili 0.7.4, 2026-04-13)
**Current claude tip:** `dea966e` (= tag `claude-baseline-2026-05-07`, parked-historical)
**Baseline main tip:** `f8b6360` (= tag `main-pivot-2026-05-07`)
**Companion doc:** `main_vs_claude_inventory_2026-05-06.md` (forward direction — main → claude pickup needs, Sprint 1)
**Architectural context:** `docs/decisions/0001-pub-sub-canonical-model.md` (ADR 0001, Option a — adopt main's tick/sub canonical; retire claude's models; A/B via parallel binary builds; no in-tree shim)
**Methodology:** `git log d7a748b..claude --oneline` (52 commits), `git diff main..claude --stat`, file-by-file inspection of key surfaces with `git show <ref>:<file>` and `grep -n`.

---

## 1. Methodology

This audit inventories **every feature, API, infrastructure, and documentation delta** on the `claude` branch that does NOT exist on `main` (bare upstream). The goal is to classify each surface for Sprint 3-4 porting priorities.

**Scope:**
- **In scope:** Feature code, test infrastructure, benchmarks, docs/proposals, logging, allocators, GIL management, exception hierarchy, broker pub/sub, query planning, partition I/O shapes, tick counter shape, parse cache, Python module layout.
- **Out of scope:** CI/release workflows, project cadence files (`.claude/rules/`, `CLAUDE.md`), chore/fmt-only commits, pure-refactor commits that don't change observable behavior.

**Commands run:**
```bash
# Chronological view of all commits since fork
git log d7a748b..claude --oneline | wc -l        # 52 commits
git log d7a748b..claude --oneline | head -100

# Surface delta
git diff main..claude --stat | head -150

# File-level comparisons (samples)
git show claude:<file> | grep -n <surface>
git show main:<file> | grep -n <surface>
git show <commit> --stat
```

**Commit range analyzed:** 52 commits on claude since fork point d7a748b (2026-04-13) through tip `dea966e` (2026-05-07).

---

## 2. Class 1: Already-on-main (native upstream features)

These features are present on both claude and main with **equivalent or compatible shape**. No porting needed; claude's version can be superseded by main's or kept as-is with no reconciliation risk.

| Feature | Where on main | Where on claude | Shape compatibility | Status |
|---------|---|---|---|---|
| **FFI rewrite — direct PyDataFrame** | `08fe588` cherry-picked conceptually from main commits #1-#8 (`e9092ce..b0f20e5`) | `crates/chili-py/src/lib.rs:220-320` (`spicy_to_py`, `spicy_from_py_bound`, direct DataFrame marshaling via `pyo3_polars::PyDataFrame`) | Identical (same conversion helpers ported from upstream `bf9fa14`) | Main's FFI shape is already on claude; no divergence |
| **Fork detection guard** | `b20177c` (refs Phase 11) | `crates/chili-py/src/lib.rs:331-380` (`check_fork()`, records PID at construct time) | Identical; same logic | Main's shape is already on claude |
| **Workspace `chrono` dep** | `b0f20e5` | `Cargo.toml:15`, `crates/*/Cargo.toml` | Unified workspace dependency | Already on claude; same structure |
| **`parse_cache_len()` method** | `9b65a50` (Phase 5) | `crates/chili-core/src/engine_state.rs:1636-1638` & `crates/chili-py/src/lib.rs:396` | Identical API | Already on claude; can use main's if more optimized |
| **TCP listener extraction** | `b20177c` (Phase 8) | `crates/chili-core/src/engine_state.rs:1425-1500` (bundled with EngineState) | Method-level identical; callable from both Rust + Python | Already functional on claude |
| **GIL release on long ops** | Scattered in main (commits #1-#8) | `crates/chili-py/src/lib.rs` (`py.detach()`, `py.allow_threads()` equivalents on eval, wpar, load, fn_call) | Identical pattern | Main's GIL strategy matches claude's |

---

## 3. Class 2: Shape-divergent (both have feature, different implementation)

These features exist on **both** branches with **substantively different API signatures, lock models, or behavior.** One shape must be chosen for the canonical codebase; the other marked for A/B comparison.

### 3.1 Pub/Sub (three competing models; the plan-pivot finding)

| Aspect | Claude model #1 (in-process Python callback) | Claude model #2 (cross-process TCP) | Main's model (tick/sub framework) — **canonical per ADR 0001** |
|---|---|---|---|
| **API entry point** | `publish(topic: str, ipc_bytes: &[u8]) -> i64` (`crates/chili-py/src/lib.rs:594`) | `publish(table, bytes: &[Vec<u8>]) -> ()` (partial, `crates/chili-core/src/engine_state.rs:1103`) | `init_tick(schema, log_dir, date)` + `publish(table, df: DataFrame)` + bundled `tick.pep` / `sub.pep` + `.tick.upd` / `.sub.init` |
| **Subscriber contract** | `(topic: str, seq: i64, ipc_bytes: bytes) -> None` (Python callback) | `i64` handles (TCP sockets) | DataFrame + tplog durability |
| **Delivery mechanism** | Bounded `mpsc::sync_channel` (capacity 1024) per-subscriber-per-topic | TCP socket iteration | tplog replay + live subscription |
| **Rust-side locking** | `Arc<Mutex<HashMap<String, Vec<mpsc::SyncSender<(String, i64, Vec<u8>)>>>>>` | `topic_map: Arc<RwLock<HashMap<i64, Handle>>>` | tplog file + memory-mapped state |
| **GIL behavior** | Channel ops + callback invocation = GIL released for wire, held for callback | No GIL state (engine-level) | No GIL state (engine-level) — **must verify golden rule 5 (6.10× concurrent throughput) preserved when ported** |

**Commentary:**

1. **Three models cannot coexist; ADR 0001 picks Option (a) — adopt main's tick/sub canonical; retire claude's models.** The Sprint 1 forward inventory (`main_vs_claude_inventory_2026-05-06.md` §2.6) had originally proposed three options (a / b / c). The 2026-05-07 pivot from cherry-pick to invert-and-restart selected Option (a) explicitly: "I want a clean shape done here first, then I can ask mdata to adapt" (user direction 2026-05-07). ADR 0001 ratifies this as the canonical decision.

2. **A/B comparison done via parallel binary builds — NOT in-tree dual implementation.** Build pre-pivot binary from `claude-baseline-2026-05-07` tag; build post-pivot binary from `claude-2` tip; run them in parallel under matched workloads; compare metrics (msg/s throughput, p50/p99 publish→delivery latency, GIL-release behavior under N concurrent Python callers, memory/subscriber, lock contention, tplog write amplification). No `crates/chili-py/tests/bench_pub_sub_models.py` shim is needed.

3. **Re-implementation gate.** Per ADR 0001's "binds future work" clause: claude's pub/sub models go to Class 4 (deliberately-retired) **unless** Sprint 3-4 surfaces a concrete mdata blocker that justifies re-implementing them as a separate Python escape hatch on claude-2. If that happens, it requires a new ADR — not a silent decision.

4. **Where it impacts porting:** Claude's `publish(ipc_bytes)` + `subscribe(callback)` + `tick_upd(table, df)` + `broker_eod()` are **NOT ported by default.** mdata's existing callers refactor on the mdata side per the breakage report (`docs/sync/mdata_breakage_report_2026-05-07.md`, drafted in Sprint 2 v2 Part D, held until Sprint 3 mdata sign-off). Claude-2 ships only main's tick/sub surface.

**Files:**
- Claude: `crates/chili-py/src/lib.rs:330-700` (in-process Python pub/sub — to be retired)
- Claude: `crates/chili-core/src/engine_state.rs:1103-1200` (TCP listener + partial topic_map — to be retired/superseded)
- Main: `7948744` commit (tick/sub framework — already on `claude-2` since claude-2 = main tip + delta)

**Verdict:** **DELIBERATELY-RETIRED per ADR 0001 (Option a). See §5.1 for retirement detail. The "shape-divergent" framing in this section is bookkeeping only — main's shape wins.**

### 3.2 `tick_count` shape — scalar i64 vs `Vec<i64>`

| Aspect | Claude shape | Main shape | Impact |
|---|---|---|---|
| **Type** | `RwLock<i64>` (`crates/chili-core/src/engine_state.rs:88`) | `RwLock<Vec<i64>>` (main commit `01c1227`) | Scalar on claude; multi-indexed on main |
| **API** | `tick(inc: i64) -> i64` (no index param); `get_tick_count() -> i64` (no index param) | `tick(index: usize, inc: i64) -> i64`; `get_tick_count(index: usize) -> i64` | Index param required on main; optional on claude |
| **Initialization** | `tick_count: RwLock::new(0)` | `RwLock::new(vec![0i64; MAX_HANDLE_NUM])` where `MAX_HANDLE_NUM=1024` | Main pre-allocates 1024 slots |
| **Python API** | `engine.tick(inc=1) -> int` (default index=0); `engine.get_tick_count() -> int` (default index=0) | `engine.tick(index=0, inc=1) -> int`; `engine.get_tick_count(index=0) -> int` | Explicit index param on main |
| **Bounds checking** | None on claude | Main's `01c1227` adds `HandleOutOfRangeErr` for index ∉ [0, 1024) | Main is stricter |

**Commentary:**

The difference is **API shape only**; behavior is compatible when using index 0 (the default on both). Main's multi-indexing is a strict superset — it supports per-handle tick counters (one per subscribed I/O handle or logical stream). Claude's scalar design assumes a single global tick counter.

**Which shape wins?** Main's multi-indexed shape is preferable long-term (it's future-proofing for per-subscriber tick streams in a broker context). However, claude's scalar design is simpler and sufficient for current mdata usage (single global repl counter).

**A/B axis:** None — this is not a performance-sensitive surface. The choice is cleanliness of API. Per ADR 0001 (Option a — adopt main's tick/sub canonical), main's Vec-with-index shape wins by default since claude's pub/sub is being retired. claude-2 ALREADY has main's `tick_count: Vec<i64>` shape (it's on claude-2 from the main fork).

**Files:**
- Claude (now historical): `crates/chili-core/src/engine_state.rs:88, 146, 1705-1714`
- Claude Python (now historical): `crates/chili-py/src/lib.rs:452-464`
- Main / claude-2: `01c1227` commit (Vec + index param) — already in place.

**Verdict:** **ALREADY-ON-CLAUDE-2 (no port action needed); claude's scalar shape is parked-historical with the rest of the pub/sub surface per ADR 0001.**

### 3.3 Python module packaging — `chili` vs `chili-sauce` vs `chili-pie`

| Aspect | Claude | Main | Decision impact |
|---|---|---|---|
| **Wheel name** | `crates/chili-py/pyproject.toml:3` → `name = "chili"` | `name = "chili-sauce"` (main commit `a0a42f6`, 2026-04-24) | PyPI package name; mdata imports `chili` currently |
| **Module import** | `import chili` + `from chili import Engine, ChiliError, ...` | `from chili_sauce import ...` (upstream rename) | Downstream compat breakage if renamed |
| **Python subpackage layout** | `crates/chili-py/python/chili/__init__.py` (wrapper class) + compiled `chili.chili` (Rust extension) | `crates/chili-py/chili/__init__.py` (original layout pre-rename) | claude's two-level layout is cleaner |

**Commentary:**

Main's commit `a0a42f6` renamed the PyPI package from `chili-pie` → `chili-sauce`. CLAUDE.md project state explicitly noted: "we stay on chili-pie because mdata/nxcar import it." However, the actual pyproject.toml on claude says `name = "chili"` (not `chili-pie`), creating a **discrepancy between CLAUDE.md and reality.**

**Open question for the user (flagged in Section 7):** What is the intended package name? Is it `chili-pie` (CLAUDE.md claim) or `chili` (actual pyproject.toml)? If mdata is importing `chili` or `chili-pie`, verify which is correct. The rename to `chili-sauce` is **deliberately skipped** per CLAUDE.md intent; no porting needed.

**Files:**
- Claude: `crates/chili-py/pyproject.toml:3, 5`
- Claude: `crates/chili-py/python/chili/__init__.py` (cleaner two-level layout)
- Main: `a0a42f6` (rename commit; skipped)

**Verdict:** **SHAPE-DIVERGENT — skip main's rename; verify actual package name in Section 7 open questions.**

---

## 4. Class 3: Claude-only-needs-port (feature absent on main)

These features are **unique to claude** and will need to be re-implemented or adapted on main during Sprints 3-4. Ranked by port priority + estimated complexity.

### 4.1 Structured exception hierarchy (`ChiliError` → 6 subclasses)

**Feature:** Python exception classes that inherit from a `ChiliError` base (extending `RuntimeError`), with typed subclasses for parse, eval, partition, type, name, and serialization errors.

**Shipping status on claude:** Phase 13 (WL 3.3), commit `663c9ed` (2026-04-29).

**Files:**
- `crates/chili-py/src/lib.rs:26-50` (exception definitions via `create_exception!` macro)
- `crates/chili-py/src/lib.rs:52-75` (error mapping function `spicy_err_to_py`)
- `crates/chili-py/python/chili/__init__.py:17-24` (re-export)

**API shape (claude):**
```python
from chili import (
    ChiliError,         # base class
    PepperParseError,   # parse errors
    PepperEvalError,    # eval errors
    PartitionError,
    TypeMismatchError,
    NameError,
    SerializationError,
)

try:
    engine.eval("invalid query")
except PepperParseError:
    print("parse failed")
except ChiliError:
    print("other chili error")
```

**Main's state:** `main` has a simpler exception model with only `ChiliError` (base) + `ChiliParseError`, `ChiliEvalError` etc. (names differ from claude's `PepperParseError` / `PepperEvalError`). Main does NOT export a rich hierarchy to Python.

**Porting complexity:** **SMALL** (15-20 LOC in Rust, ~10 LOC in Python wrapper). The `create_exception!` macro calls are straightforward; the mapping logic in `spicy_err_to_py` is pattern-matching on `SpicyError` variants, which is already done on main at the lower level.

**Porting priority:** **HIGH** — mdata's error handling relies on catching specific exception types. This must land before any production porting of chili-py consumers.

**Port sequence:** After FFI rewrite is complete; can land independently.

**Bench gate:** None.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, HIGH priority.**

---

### 4.2 Logger built-ins (`.log.{info,warn,debug,error}`)

**Feature:** Four built-in functions registered in the global namespace: `.log.debug(msg)`, `.log.info(msg)`, `.log.warn(msg)`, `.log.error(msg)`, each logging to stderr via the `log` crate.

**Shipping status on claude:** Implemented in `crates/chili-py/src/lib.rs` (functions #97-109), mirrored from `crates/chili-bin/src/logger.rs`.

**Files:**
- `crates/chili-py/src/lib.rs:79-140` (log functions + registration in `built_in_fns` HashMap)

**API shape (claude):**
```pepper
q) .log.info "hello world"
q) .log.warn "warning message"
q) .log.debug "debug info"
q) .log.error "error condition"
```

**Main's state:** Main does NOT have these built-ins in the global function registry. Logging is possible via `log::info!` etc. inside Rust functions, but not callable from Pepper/Chili scripts.

**Porting complexity:** **SMALL** (copy the functions + registration; ~60 LOC total). The functions are simple wrappers around the `log` crate.

**Porting priority:** **MEDIUM** — useful for debugging and observability, but not load-bearing. mdata does not depend on it.

**Port sequence:** Can land independently after FFI setup is stable.

**Bench gate:** None.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, MEDIUM priority.**

---

### 4.3 Engine lifecycle API (close, unload, reload, is_loaded, table_count)

**Feature:** Five methods on the Python `Engine` class for controlling engine state across its lifetime.

**Shipping status on claude:** Phase 12 (WL 3.1), commit `0f45dac` (2026-04-28).

**Files:**
- `crates/chili-py/python/chili/__init__.py:66-91` (wrapper methods)

**API shape (claude):**
```python
engine = chili.Engine()
engine.load("path/to/hdb")
engine.unload()        # drop partitions, keep engine alive
engine.reload()        # re-scan HDB for new partitions
is_loaded = engine.is_loaded()
table_cnt = engine.table_count()
engine.close()         # release state immediately
```

**Rust-side implementation:**
- `unload()` calls `clear_par_df()` (implemented on main; clears partitioned DataFrames)
- `reload()` calls `load_par_df(last_hdb_path)` again
- `is_loaded()` and `table_count()` are simple accessor queries on `par_df` map

**Main's state:** Main does NOT expose these lifecycle methods to Python. The `EngineState` in Rust has internal functions, but the PyO3 binding does not wire them up.

**Porting complexity:** **SMALL** (~40 LOC in Python wrapper + ~20 LOC in Rust bindings). The underlying Rust functions exist on main; only the Python exposure is missing.

**Porting priority:** **MEDIUM** — improves resource management ergonomics, but not critical for basic usage (engine is dropped automatically at process end). mdata may benefit for long-running daemons.

**Port sequence:** Lands after Python bindings are stable.

**Bench gate:** None.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, MEDIUM priority.**

---

### 4.4 In-process Python broker pub/sub (publish, subscribe, tick_upd, broker_eod)

**Feature:** In-process pub/sub for Python callbacks, with bounded `mpsc::sync_channel` per-subscriber-per-topic and GIL-managed callback invocation.

**Shipping status on claude:** Phase 16 (WL 1.1), commits `2150f9f` + `bbfec3a` (2026-05-01).

**Files:**
- `crates/chili-py/src/lib.rs:330-700` (full in-process pub/sub implementation)

**API shape (claude):**
```python
def callback(topic, seq, ipc_bytes):
    # Process published frame
    df = pl.from_ipc_buffer(ipc_bytes)
    print(f"Received {topic} seq={seq}: {len(df)} rows")

engine.subscribe(["prices", "trades"], callback)
seq1 = engine.publish("prices", arrow_ipc_bytes)
seq2 = engine.tick_upd("trades", df)  # convenience method: serialize df then publish
engine.broker_eod(eod_bytes)  # broadcast EOD to all subscribers
```

**Rust-side implementation:**
- `py_subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::SyncSender<(String, i64, Vec<u8>)>>>>>` per-instance
- `topic_seq: AtomicUsize` per-topic monotonic counter
- Backpressure handling: drop message and warn on full channel
- Callback invocation: thread spawned per subscriber, GIL acquired for callback

**Main's state:** Main has the **tick/sub framework** (tick.pep + sub.pep + init_tick + publish(df) + subscribe) but NOT this in-process Python callback model. The tick/sub model is designed for durability via tplog, not Python callbacks.

**Porting complexity:** **MEDIUM** (250+ LOC total; includes thread management, GIL handling, channel setup, backpressure logic). The implementation is non-trivial but self-contained.

**Porting priority:** **NONE by default per ADR 0001 (Option a) — retired.** mdata's existing callers refactor on the mdata side per the breakage report; claude-2 ships only main's tick/sub framework. If Sprint 3-4 surfaces a concrete mdata blocker that justifies re-implementation as a separate Python escape hatch, that requires a NEW ADR (not silent re-port).

**Port sequence:** **NOT PORTED.** Re-classified to Class 4 (deliberately-retired); see §5.1.

**A/B comparison axis:** **Parallel binary builds, NOT in-tree.** Build pre-pivot binary from `claude-baseline-2026-05-07` tag (with claude's pub/sub) and post-pivot binary from `claude-2` tip (with main's tick/sub); run them in parallel under matched workloads; document deltas in Sprint 5 wrap doc. No in-tree A/B harness.

**Verdict:** **MOVED TO CLASS 4 (DELIBERATELY-RETIRED per ADR 0001 Option a). See §5.1.**

---

### 4.5 Query plan introspection (query_plan method)

**Feature:** `engine.query_plan(query, hdb_path)` returns the optimized query plan as a string (equivalent to SQL `EXPLAIN`).

**Shipping status on claude:** Phase 14 (WL 3.2, partial), commit `147f7ab` (2026-04-29).

**Files:**
- `crates/chili-py/src/lib.rs:400-427` (Python method)
- Underlying Rust: creates temporary lazy-mode engine, loads HDB, parses + evaluates to `LazyFrame`, returns `describe_plan()`.

**API shape (claude):**
```python
plan_str = engine.query_plan("select symbol, close from t where date=2024.01.03", "/path/to/hdb")
print(plan_str)
# Output:
# DF ["symbol", "close"]:
#   SCAN ...
#   FILTER date = 2024.01.03
#   SELECT ["symbol", "close"]
```

**Main's state:** Main does NOT have query_plan. Equivalent functionality would require introspection of polars' LazyFrame query plan, which is engine-level work, not exposed.

**Porting complexity:** **SMALL** (~30 LOC in Python wrapper + ~50 LOC in Rust lazy-eval path). Most of the work is already in place (lazy mode exists on both branches); only the string representation is missing.

**Porting priority:** **LOW-MEDIUM** — observability feature, useful for debugging but not required for correctness. mdata may use it for query optimization auditing.

**Port sequence:** Can land independently after lazy-mode evaluation is solid on main.

**Bench gate:** None.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, LOW-MEDIUM priority.**

---

### 4.6 Column quantization dequantization helper (set_column_scale, clear_column_scales)

**Feature:** `engine.set_column_scale(table, column, factor)` registers a scale factor. On any subsequent `eval()`, Int64 result columns matching the registered `(table, column)` pair are automatically cast to Float64 and divided by `factor`.

**Shipping status on claude:** Phase 15 (WL 3.4), commit `a8d98d6` (2026-04-29).

**Files:**
- `crates/chili-py/src/lib.rs:450-700` (integration into eval pipeline)
- `crates/chili-py/python/chili/__init__.py:48-64` (Python wrapper)

**API shape (claude):**
```python
engine.set_column_scale("ohlcv_1d", "close", 10000)
# Now engine.eval("select close from ohlcv_1d") returns close as Float64 / 10000

engine.clear_column_scales()
```

**Rust-side implementation:**
- `_column_scales: Dict[str, Dict[str, i64]]` (table → column → factor)
- Post-eval hook: iterate result columns, check for registered scales, cast + divide if match

**Main's state:** Main does NOT have this feature. It's a chili-specific optimization for HDBs that use integer quantization (e.g., close prices stored as i64 × 10000).

**Porting complexity:** **SMALL** (~80 LOC total; mostly glue code). The core logic is a simple post-eval column transform.

**Porting priority:** **MEDIUM** — useful for mdata's OHLCV HDBs (which use quantized prices), but not blocking. If skipped, mdata's Python code just needs to do the division manually.

**Port sequence:** Can land independently after Python eval binding is stable.

**Bench gate:** None — this is a developer ergonomics feature, not a perf surface.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, MEDIUM priority.**

---

### 4.7 `overwrite_partition` separate function

**Feature:** Separate `overwrite_partition(df, hdb_path, table, date, sort_columns=None)` method (distinct from `wpar()`/`write_partition()`). Deletes all existing shard files for a given date and writes a single fresh `_0000` file, enabling safe in-place HDB rewrites (dtype migrations, re-sorting).

**Shipping status on claude:** Post-Phase-16, commit `bbfec3a` (2026-05-01).

**Files:**
- `crates/chili-py/src/lib.rs:520-593` (Python method)
- `crates/chili-op/src/io.rs:494-600` (Rust native implementation `overwrite_partition_py`)

**API shape (claude):**
```python
engine.overwrite_partition(df, "/path/to/hdb", "ohlcv_1d", "2024.01.03", sort_columns=["symbol"])
# Deletes all *.parquet files for 2024.01.03, writes _0000.parquet in their place
```

**Main's state:** Main (commit `3aeee62`) added an `overwrite=Bool` parameter to the existing `write_partition()` function instead of creating a separate function. The spring 1 inventory (`main_vs_claude_inventory_2026-05-06.md` §2.3) recommended skipping main's `write_partition(overwrite=…)` merge and keeping claude's separate function API.

**Porting complexity:** **SMALL** (~100 LOC total; file-level isolated logic). The Rust implementation is orthogonal to main's write_partition.

**Porting priority:** **MEDIUM** — useful for HDB maintenance workflows (dtype migration, re-sorting), but not required for normal reads/appends. mdata may use it for HDB reorg.

**Port sequence:** Can land independently after wpar binding is stable. Do NOT merge main's `write_partition(overwrite=…)` flag; keep claude's separate function.

**Bench gate:** None.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, MEDIUM priority. Skip main's write_partition(overwrite=...) flag.**

---

### 4.8 GIL release on query eval (py.detach / py.allow_threads pattern)

**Feature:** `Engine::eval()` and related methods wrap their query body in `py.detach()` to release the Python Global Interpreter Lock, enabling concurrent Python thread execution while the Rust query engine runs.

**Shipping status on claude:** Golden rule 5 (6.10× concurrent throughput), Phase 1+ commits spanning `1c2c24c` onwards.

**Files:**
- `crates/chili-py/src/lib.rs:446-486` (eval with GIL release)
- `crates/chili-py/src/lib.rs:487-519` (wpar with GIL release)
- Underlying pattern: `let obj = py.detach(move || map_spicy_error(self.inner.eval(...))); spicy_to_py(py, obj?)`

**Main's state:** Main's `chili-py` binding (commits #1-#8) includes GIL release, but the pyo3 0.27 migration changed the API from `py.allow_threads()` → `py.detach()` with slightly different closure semantics.

**Porting complexity:** **ALREADY COMPLETE** — the pyo3 0.27 migration on both branches means main already has this pattern. No porting needed; just verify both use `py.detach()`.

**Porting priority:** **N/A** — already on main via the FFI rewrite.

**Bench gate:** None — this is a correctness/concurrency feature measured by the golden rule 5 benchmark (6.10× throughput).

**Verdict:** **ALREADY-ON-MAIN (via FFI rewrite, commits #1-#8).**

---

### 4.9 Mimalloc global allocator in chili-py

**Feature:** `mimalloc` crate set as the `#[global_allocator]` in both `chili-bin` and `chili-py`, improving allocation efficiency for polars' DataFrame buffer patterns.

**Shipping status on claude:** Phase 1 (build-system wins), commits `1c2c24c` onwards.

**Files:**
- `crates/chili-py/Cargo.toml:45` (`mimalloc = { version = "0.1", default-features = false }`)
- `crates/chili-py/src/lib.rs:344-346` (allocator registration)

**Main's state:** Main does NOT include mimalloc in `chili-py`. Main's `chili-bin` DOES include it (upstream commit `e9092ce` #1, per the FFI merge baseline), but the chili-py crate did not pick it up.

**Porting complexity:** **TRIVIAL** (2-line Cargo.toml + 3-line Rust). Just copy the allocator registration.

**Porting priority:** **LOW-MEDIUM** — performance optimization, not required for correctness. Estimated ~5-10% allocation overhead reduction on DataFrame-heavy workloads, but the chili-py process is usually Python-dominated, so the gain is smaller than in chili-bin.

**Port sequence:** Can land independently, ideally alongside other perf improvements.

**Bench gate:** None — optional optimization.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, TRIVIAL complexity, LOW-MEDIUM priority.**

---

### 4.10 Parse cache regression test suite

**Feature:** `crates/chili-core/tests/parse_cache_test.rs` — 6 unit tests covering parse cache LRU invariants (hit equivalence, source uniqueness, path discrimination, eviction, concurrent safety, correctness).

**Shipping status on claude:** Commit `1c2c24c` (added as part of optimization sweep).

**Files:**
- `crates/chili-core/tests/parse_cache_test.rs:1-130` (full test file)

**Main's state:** Main does NOT have this test file. Main has the parse cache feature (commit `9b65a50`), but not the regression tests.

**Porting complexity:** **SMALL** (~130 LOC; pure test code, no changes to implementation needed). Can copy verbatim or adapt line counts if LRU implementation differs.

**Porting priority:** **MEDIUM** — regression prevention for the parse cache (golden rule 6: 385ns invariant). Should land alongside the cache implementation.

**Port sequence:** Lands alongside parse cache when main's version is validated against the benchmark.

**Bench gate:** Parse cache hit latency <400ns (golden rule 6).

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, MEDIUM priority. Bench-gated.**

---

### 4.11 Comprehensive benchmark suite (docs/bench/phase*.md)

**Feature:** Detailed benchmark documentation for optimization phases 1-17, with criterion benchmarks in `crates/chili-op/benches/` and Python benchmarks in `crates/chili-py/tests/bench_*.py`.

**Shipping status on claude:** Commits `cc86aea` + `8522bce` + `c4eb742` (Phase 1-17 tracking + profiling artifacts).

**Files:**
- `docs/bench/phase1.md` through `docs/bench/phase17_profile.py` (15+ files, ~2000 LOC total)
- `docs/bench/mdata-collab/` (broker parity test, quantized schema analysis, phase 17 profiling)
- `crates/chili-core/benches/parse_cache.rs`
- `crates/chili-op/benches/{eval.rs, scan.rs, write_partition.rs}` (additions for Phases 1+)
- `crates/chili-py/tests/bench_concurrent.py` (8-thread concurrent throughput: 6.10× vs single-thread)

**Main's state:** Main has basic criterion benchmarks but NOT the detailed phase documentation or the Python concurrency benchmark (`bench_concurrent.py`).

**Porting complexity:** **MEDIUM** (~2000 LOC of docs + code; mostly documentation, some test fixtures). The criterion benchmark code can be copied; the docs are self-contained.

**Porting priority:** **MEDIUM-HIGH** — benchmarks are the ground-truth for golden rules (rules 4, 5, 6) and Phase 17 profiling validates the mdata compatibility (Phase 17 closed with 181 ms Q11 latency, within the <200 ms target).

**Port sequence:** Lands alongside the optimization commits they document. Can be deferred if the "forward" cherry-picks are prioritized.

**Bench gate:** Each phase documented in `phase*.md` has implicit bench gates (e.g., Phase 1 build-system wins measured with criterion; Phase 17 profiling on real HDB).

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, MEDIUM complexity, MEDIUM-HIGH priority. Bench-gated.**

---

### 4.12 Bench artifacts and mdata collaboration docs

**Feature:** Detailed profiling results, broker parity tests, and quantized schema analysis for mdata integration validation.

**Shipping status on claude:** Commits `c4eb742`, `1e3e59b`, `3d75460` (phases 16-17 pre-verification, mdata comparison).

**Files:**
- `docs/bench/mdata-collab/artifacts/broker_parity_test.py` (TCP/IPC broker comparison, 516 LOC)
- `docs/bench/mdata-collab/artifacts/quantized_schema.md` (Int64 quantization patterns for OHLCV HDBs)
- `docs/bench/mdata-collab/artifacts/phase17_profiling_kit.py` (live profiling on mdata HDB, 332 LOC)
- `docs/bench/mdata-collab/benchmarks/phase17_profile_results.json` (actual profiling output)
- `docs/bench/mdata-collab/mdata_vs_kdb_comparison.md` (chili vs kdb+ feature comparison, 200 LOC)

**Main's state:** Main does NOT have these artifacts. They are claude-specific collaboration docs between chili and mdata teams.

**Porting complexity:** **MEDIUM** (~1000 LOC of docs + test code; pure documentation and test fixtures). No code changes required; just copy the files.

**Porting priority:** **LOW-MEDIUM** — these are reference artifacts for mdata integration, not load-bearing for main. Useful for future product feature planning, but not required for Sprints 3-4.

**Port sequence:** Can land at the end of the port phase, after core features are shipped.

**Bench gate:** None — these are observational/comparison docs, not gates.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, MEDIUM complexity, LOW-MEDIUM priority.**

---

### 4.13 Docs: proposals, research, release notes

**Feature:** Documentation on proposed features, research findings, and changelog entries specific to claude's development.

**Shipping status on claude:** Commits across `94c3c9d`, `f7ea195`, `13cd30f`, `20dd567`, `180c883` (research, proposals, decisions).

**Files:**
- `docs/research/` (Part A-E research, main↔claude inventory)
- `docs/proposals/` (load_tree HDB, PyEngineState comparison)
- `docs/decisions/` (ADR 0001 and sprint cadence rules)
- `CLAUDE.md` (project state, test counts, FFI merge notes, golden rules)
- `README.md` (updated for claude-specific features)
- `CHANGELOG.md` (detailed release notes for 0.7.5)

**Main's state:** Main has basic README and old CHANGELOG; does NOT have the comprehensive research/proposal/decision docs.

**Porting complexity:** **SMALL** (docs-only; no code changes). Some files (e.g., CHANGELOG) will need to be merged with main's version; others (e.g., ADR 0001) are pure-claude.

**Porting priority:** **LOW** — documentation is not blocking for feature shipping, though it's valuable for future maintainers. Can land post-feature port as a wrap-up.

**Port sequence:** Last in the sequence, after features are shipped.

**Bench gate:** None.

**Verdict:** **CLAUDE-ONLY-NEEDS-PORT, SMALL complexity, LOW priority.**

---

## 5. Class 4: Deliberately-retired (features being abandoned per ADR 0001)

These features are **explicitly being retired** per architectural decisions (ADR 0001) and will NOT be ported. Documentation is provided for mdata breakage reporting.

### 5.1 In-process Python pub/sub (retired per ADR 0001 Option a)

**Feature:** `publish(topic, ipc_bytes)`, `subscribe(topics, callback)`, `tick_upd`, `broker_eod` on the Python `Engine` class (claude-specific implementation).

**Status:** **Retired** per ADR 0001 (Option a — adopt main's tick/sub framework as canonical; no in-tree shim). claude-2 ships only main's tick/sub surface. Claude's models live on the parked-historical `claude` branch (tagged `claude-baseline-2026-05-07`) for A/B reference.

**Mdata breakage impact:** mdata's existing `engine.publish(ipc_bytes)` + `engine.subscribe(callback)` callers refactor to upstream's tick/sub shape:
- `engine.publish(topic, ipc_bytes)` → `engine.publish(table, df: DataFrame)` (DataFrame-shaped, with tplog durability).
- `engine.subscribe(topics, callback)` → `.sub.init` Pepper-script subscribe pattern via `sub.pep`.
- Estimated mdata-side migration: **2-5pp** to refactor tp/rdb consumers.
- Documented in `docs/sync/mdata_breakage_report_2026-05-07.md` (Sprint 2 v2 Part D — held internal until Sprint 3 mdata sign-off).

**A/B comparison axis (parallel binary builds, NOT in-tree shim):**
- Build pre-pivot binary from `claude-baseline-2026-05-07` tag (with claude's pub/sub).
- Build post-pivot binary from `claude-2` tip (with main's tick/sub).
- Run both in parallel under matched workloads (msg/s throughput, p50/p99 publish→delivery latency, GIL behavior under N concurrent callers, memory/subscriber, lock contention, tplog write amplification).
- Document deltas in `docs/bench/post_pivot_pubsub_comparison_<date>.md` (Sprint 5 wrap deliverable).

**Re-implementation gate (per ADR 0001):** If A/B comparison shows claude's pub/sub measurably wins on a metric mdata cares about, OR if mdata's Sprint 3-4 refactor surfaces a concrete blocker, the user can re-open via a new ADR. claude-2 does NOT carry compatibility shims by default.

**Files affected (mdata side):**
- Any code calling `engine.publish()`, `engine.subscribe()`, `engine.tick_upd()`, `engine.broker_eod()`.

**Verdict:** **DELIBERATELY-RETIRED per ADR 0001 (Option a). Action: NONE on claude-2; mdata refactors per breakage report; A/B comparison happens via parallel binaries in Sprint 5.**

---

### 5.2 Cross-process TCP pub/sub (partial implementation)

**Feature:** Engine-level `publish(table, bytes)` and `topic_map` TCP socket subscriber tracking in `crates/chili-core/src/engine_state.rs` (~100 LOC partial implementation).

**Status:** Sketched on claude but never completed; **status on main is more complete via the tick/sub framework.**

**Retirement decision:** **Skip porting to main.** The tick/sub framework (main's commit `7948744`) is the canonical solution. Claude's TCP model is incomplete and superseded by upstream's design.

**Files affected (mdata side):** None — this was never exposed to Python; internal-only.

**Verdict:** **DELIBERATELY-RETIRED. Reason: Superseded by main's tick/sub framework.**

---

## 6. Cross-reference matrix: Sprint 1 forward inventory ↔ Sprint 2 reverse inventory

This section maps each surface from the **forward** direction (main → claude pickup needs, `main_vs_claude_inventory_2026-05-06.md` §3) to its corresponding **reverse** direction classification.

| Forward surface (main → claude) | Wishlist priority | Forward verdict | Reverse direction (claude → main) | Reverse class | Porting needed |
|---|---|---|---|---|---|
| **TCP listener extraction** | P0 | PICKUP-NOW | `start_tcp_listener()` | **Class 1: Already-on-main** | NO |
| **Recursive load_par_df + bounds** | P0 | PICKUP-NOW | Main's Vec-indexed tick; claude's scalar tick | **Class 2: Shape-divergent** | YES (adopt main's shape) |
| **tick/sub pub/sub framework** | P0 | (already on claude-2 = main tip) | claude's in-process Python pub/sub | **Class 4: Deliberately-retired per ADR 0001 (Option a)** | NO (mdata refactors; parallel-binary A/B in Sprint 5) |
| **Parse cache** | — | SKIP (already on claude) | Parse cache regression tests | **Class 3: Claude-only-needs-port** | YES (MEDIUM priority, bench-gated) |
| **Stats + MissingParCondErr** | P1 | PICKUP-PARTIAL | Query plan + column scale + lifecycle API | **Class 3: Claude-only-needs-port** | YES (SMALL complexity, MEDIUM priority) |
| **FFI rewrite** | — | SKIP (content already merged) | Direct PyDataFrame marshaling | **Class 1: Already-on-main** | NO |
| **chili-pie → chili-sauce rename** | — | SKIP (not picked up) | Package name discrepancy | **Class 2: Shape-divergent** | NO (skip rename; clarify name in open questions) |

---

## 7. Open questions for the user

The following questions require user clarification before final porting decisions can be made.

### 7.1 Python package name: `chili` vs `chili-pie` vs `chili-sauce`?

**Issue:** CLAUDE.md project state claims "we stay on chili-pie because mdata/nxcar import it." However, `crates/chili-py/pyproject.toml:3` says `name = "chili"` (not `chili-pie`).

**Questions:**
- What is the actual installed package name that mdata imports from? Is it `import chili`, `import chili_pie`, or `import chili_sauce`?
- Should claude retain the name `chili` (current), rename to `chili-pie` (per CLAUDE.md intent), or eventually migrate to `chili-sauce` (upstream's choice)?
- If chili-sauce is the eventual target, when should the migration happen, and what is the breakage window for mdata?

**Impact:** Minimal for core features, but important for ecosystem consistency and downstream import statements.

**Recommendation:** Verify with mdata team and update CLAUDE.md to match reality.

---

### 7.2 PyLazyFrame support on claude — is it complete?

**Issue:** Main's commit `98fbd7f` adds `numpy` dep and `PyLazyFrame` support. Claude's imports suggest `pyo3_polars::PyDataFrame` extensively, but `PyLazyFrame` was not surveyed in detail.

**Questions:**
- Does claude's `chili-py` currently support returning `polars.LazyFrame` from `eval(lazy=True)` without errors?
- If so, does main's `98fbd7f` add new functionality beyond what's already present?
- If not, is LazyFrame support deferred for a future phase, or should it land as part of the FFI stabilization?

**Impact:** LazyFrame is not blocking for mdata's immediate workloads (Phase 17 uses eager DataFrames), but may be useful for query optimization workflows (query_plan introspection).

**Recommendation:** Spot-check `crates/chili-py/src/lib.rs` for `PyLazyFrame` usage and clarify scope before Sprint 3 kicks off.

---

### 7.3 Confirm ADR 0001 ratification status before Sprint 3 kickoff

**Issue:** ADR 0001 (Option a — adopt main's tick/sub canonical; retire claude's models; no in-tree shim; A/B via parallel binary builds) is **drafted in `docs/decisions/0001-pub-sub-canonical-model.md` as Status: Draft.** Ratification happens at Sprint 2 v2 wrap (Part E retro). Once ratified, claude's `publish(ipc_bytes)`, `subscribe(callback)`, `tick_upd`, `broker_eod` are firmly in Class 4 (deliberately-retired).

**Questions:**
- Does the user confirm ADR 0001 (Option a) at Sprint 2 v2 wrap, or pivot back toward Option c (hybrid)?
- Confirm that mdata's tp/rdb refactor budget can accommodate the API change in Sprint 3-5 timeframe, or do they need a longer migration window?
- Should the A/B comparison happen in Sprint 5 (post-port) or earlier (concurrent with port work)?

**Impact:** ADR 0001 is the blocker for Sprint 3 brief drafting. Sprint 3 brief assumes Option (a); if the user reverses to Option (c), Sprint 3 scope needs significant rework (re-implement claude's pub/sub on claude-2 alongside main's).

**Recommendation:** Default ratify ADR 0001 (Option a) at Sprint 2 v2 wrap. mdata-side cost is documented in the breakage report; user has already signed up for that path on 2026-05-07 ("I don't need mdata code to keep working for now; I want a clean shape done here first").

---

### 7.4 Is the parse cache bench gate (golden rule 6: 385ns invariant) on the critical path for Sprints 3-4?

**Issue:** Claude's parse cache hits in ~385ns; this is a hard-won optimization that depends on careful lock choice and cache shape. Main's parse cache may differ; committing to main's shape requires validation that the hit latency is preserved.

**Questions:**
- Should the parse cache regression test suite (`crates/chili-core/tests/parse_cache_test.rs`) be ported before or after main's cache implementation is validated?
- If main's implementation is slower, should claude's version be adopted instead, or should both coexist briefly during Sprint 3 for A/B measurement?
- Is the 385ns target non-negotiable for mdata, or is there flexibility if a slightly slower but simpler implementation suffices?

**Impact:** Medium — affects performance baselines and potentially the build profile (Profile.release tuning for LTO etc.).

**Recommendation:** Benchmark main's parse cache latency early in Sprint 3. If ≥1µs, flag for user decision on which shape to adopt.

---

### 7.5 For the `tick_count` shape divergence (scalar i64 vs Vec<i64>), which indexing model should main adopt?

**Issue:** Claude's scalar tick counter is simpler; main's Vec-indexed model is future-proofing for per-subscriber tick streams in a broker context.

**Questions:**
- Is the per-subscriber tick counter feature needed for mdata's immediate roadmap, or is a scalar counter sufficient?
- If Vec is chosen but not immediately used, is there a risk of code rot (complexity without corresponding feature usage)?
- Should the shape decision be bundled with the pub/sub ADR (since per-subscriber ticks make sense primarily in a multi-subscriber broker context), or is it independent?

**Impact:** Low-medium — API shape choice, no performance implications.

**Recommendation:** Per ADR 0001 (Option a), main's Vec-with-index shape wins (claude-2 already has it from main fork). claude's scalar shape is parked-historical with the rest of the pub/sub surface; no port action.

---

### 7.6 Should benchmark artifacts (docs/bench/phase*.md + profiling kit) be ported to main now, or deferred until features are shipped?

**Issue:** The benchmark suite is comprehensive and valuable for golden rule tracking, but it's decoupled from feature porting. Porting it early provides a testbed for validating port correctness; deferring it saves porting effort if features are the priority.

**Questions:**
- Are the golden rules (4, 5, 6 in particular) non-negotiable constraints for main's acceptance, or are they claude-specific targets?
- Should each Sprint 3-4 feature land with its corresponding benchmark from claude, or should benchmarks be a separate post-port phase?
- Is the mdata profiling kit (`phase17_profiling_kit.py`) needed to validate that mdata's HDB still meets latency targets post-port?

**Impact:** Low-medium — organization/validation question, not blocking features.

**Recommendation:** Plan to port benchmarks alongside critical features (parse cache, pub/sub), defer less critical ones (phase 14-15 benchmarks).

---

## 8. Port-complexity estimates at a glance

| Feature | Class | Complexity | Priority | Bench gate | Sprint |
|---|---|---|---|---|---|
| Structured exception hierarchy | 3 | SMALL | HIGH | None | 3 |
| Logger built-ins | 3 | SMALL | MEDIUM | None | 3 |
| Engine lifecycle API | 3 | SMALL | MEDIUM | None | 3 |
| Column scale dequantization | 3 | SMALL | MEDIUM | None | 3 |
| `overwrite_partition` function | 3 | SMALL | MEDIUM | None | 3 |
| Mimalloc in chili-py | 3 | TRIVIAL | LOW-MEDIUM | None | 3 |
| Query plan introspection | 3 | SMALL | LOW-MEDIUM | None | 3 |
| Parse cache regression tests | 3 | SMALL | MEDIUM | Parse cache <400ns | 3-4 |
| In-process Python pub/sub (all 4 methods) | 3 | MEDIUM | HIGH (mdata) | Perf/compactness/efficiency vs upstream | 4+ (ADR gate) |
| Tick counter shape change | 2 | SMALL | LOW-MEDIUM | None | 4+ (pub/sub gate) |
| Benchmark suite (phase docs) | 3 | MEDIUM | MEDIUM-HIGH | Per-phase gates | 3-4 (phased) |
| Mdata collaboration artifacts | 3 | MEDIUM | LOW-MEDIUM | None | 4+ |
| Docs/proposals/research | 3 | SMALL | LOW | None | 4+ |

**Total estimated effort:**
- **Sprint 3:** ~8-10pp (exception hierarchy + loggers + lifecycle + column scale + overwrite + query_plan + mimalloc + parse_cache_test + remaining chili-core clippy port from claude's `9aa358d`). Bench-gated on parse_cache latency.
- **Sprint 4:** ~6-8pp (full benchmark port + remaining shape-divergent reconciliation + claude-py FFI lints port from claude's `a8d4014` chili-py portion). Pub/sub re-implementation NOT in scope per ADR 0001 (Option a).
- **Sprint 5:** ~6-10pp (bench rebaseline + parallel-binary A/B comparison vs `claude-baseline-2026-05-07` + chili-py wheel cut + mdata breakage report delivery + cutover).
- **Post-Sprint-5:** ~3-5pp (mdata artifacts + comprehensive docs port).

---

## 9. Summary by class

### Class 1: Already-on-main (6 items)
- **Count:** 6 features (FFI rewrite, fork guard, chrono dep, parse_cache_len, TCP listener, GIL release)
- **Status:** No porting needed; claude's versions can be replaced by or aligned with main's.
- **Action:** Verify API compatibility during merge; prioritize FFI stability.

### Class 2: Shape-divergent (1 item — pub/sub moved to Class 4 per ADR 0001)
- **Count:** 1 feature (Python package name `chili` vs `chili-pie` vs `chili-sauce`).
- **Status:** tick_count and pub/sub originally classified here; both reclassified once ADR 0001 ratified Option (a) — tick_count → Already-on-claude-2 (main's Vec shape inherits); pub/sub → Class 4 (deliberately-retired).
- **Action:** Clarify package name (open question §7.1).

### Class 3: Claude-only-needs-port (8 items, all in Sprint 3-4)
- **Count:** 8 features (exceptions, loggers, lifecycle, column scale, overwrite_partition, mimalloc, query_plan, parse_cache_tests). Plus benchmark + mdata artifacts + docs ports.
- **Sprint 3 priority:** 8 small/medium items (~8-10pp total, bench-gated on parse_cache latency).
- **Sprint 4-5:** Benchmark suite port + bench rebaseline + parallel-binary A/B (~6-10pp).
- **Action:** Prioritize Sprint 3 items; A/B comparison via parallel binaries in Sprint 5.

### Class 4: Deliberately-retired (2 items, firm per ADR 0001 Option a)
- **Count:** 2 features (in-process Python pub/sub, TCP pub/sub).
- **Status:** Retired per ADR 0001. claude-2 ships only main's tick/sub framework. Re-implementation requires a NEW ADR.
- **Action:** mdata refactors per breakage report; parallel-binary A/B comparison in Sprint 5; document deltas.

---

## 10. Recommended port sequence for Sprints 3-4

**Sprint 3 (core features, bench-gated on parse cache):**
1. Structured exception hierarchy (exceptions.rs + Python exports) — **SMALL**, HIGH priority
2. Logger built-ins (4 functions + registration) — **SMALL**, MEDIUM priority
3. Engine lifecycle API (5 methods on Python Engine) — **SMALL**, MEDIUM priority
4. Column scale dequantization (set_column_scale + clear) — **SMALL**, MEDIUM priority
5. `overwrite_partition` function (keep separate from write_partition) — **SMALL**, MEDIUM priority
6. Query plan introspection (engine.query_plan) — **SMALL**, LOW-MEDIUM priority
7. Mimalloc in chili-py (allocator registration) — **TRIVIAL**, LOW-MEDIUM priority
8. Parse cache regression tests (crates/chili-core/tests/parse_cache_test.rs) — **SMALL**, MEDIUM priority + **bench gate on hit latency <400ns**
9. Initial benchmark infrastructure (phase 1-7 docs + criterion harness) — **MEDIUM**, MEDIUM-HIGH priority (landing alongside features)

**Sprint 4 (port wave 2; clippy + benchmarks):**
1. Remaining chili-py FFI lints port from claude's `a8d4014` (chili-py portion deferred from Part A) — **SMALL**, MEDIUM priority
2. Full benchmark port (phase 8-17 docs, mdata collaboration artifacts staging) — **MEDIUM**, MEDIUM priority
3. Tick counter shape — **NO ACTION** (claude-2 already has main's Vec shape from main fork; claude's scalar is parked-historical)
4. Pub/sub re-implementation — **NO ACTION** by default per ADR 0001 (Option a). If a concrete mdata blocker surfaces during Sprint 3-4 refactor, requires a NEW ADR.

**Sprint 5 (bench rebaseline + parallel-binary A/B + wheel cut):**
1. Bench rebaseline on claude-2 — **MEDIUM**, MEDIUM-HIGH priority
2. Parallel-binary A/B comparison: build `claude-baseline-2026-05-07`-tag binary AND `claude-2`-tip binary; run them in parallel under matched workloads; document deltas in `docs/bench/post_pivot_comparison_<date>.md`. **No in-tree A/B harness.**
3. chili-py wheel cut (`chili 0.8.0-claude2.1` or post-naming-watch ratified) — **SMALL**, HIGH priority
4. mdata breakage report delivery + cutover — **SMALL** (doc + handoff), HIGH priority

**Post-Sprint-5 (wrap-up, docs):**
1. Mdata collaboration artifacts (broker parity test, quantized schema, profiling kit) — **MEDIUM**, LOW priority
2. Comprehensive docs/proposals port (CLAUDE.md integration, ADR docs, research) — **SMALL**, LOW priority
3. Optional: parking_lot lock refactor from main's `2286dec` (requires bench validation) — **MEDIUM**, LOW priority, bench-gated on parse_cache

---

## References

- **Companion forward-direction inventory:** `main_vs_claude_inventory_2026-05-06.md` (Sprint 1 research)
- **ADR 0001 (canonical):** `docs/decisions/0001-pub-sub-canonical-model.md` (Option a — adopt main's tick/sub canonical; retire claude's models; A/B via parallel binary builds; no in-tree shim).
- **Golden rules:** `CLAUDE.md` (rules 4, 5, 6 on quantization, GIL release, parse cache)
- **Benchmark baselines:** `docs/bench/summary.md`, `docs/bench/phase{1..17}.md`
- **Mdata wishlist:** `~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`

