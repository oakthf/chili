# `load_par_df` state audit — GIL-release safety

**Date:** 2026-05-09
**Sprint:** 13.5 Part D
**Author:** main Claude (coordinator-solo)
**Verdict:** **GREEN** — `load_par_df` is safe to call with the GIL released.
**Audience:** Sprint 14 P3.2b implementer; mdata callers asking "is concurrent
load_par_df safe today?"

---

## TL;DR

`crates/chili-core/src/engine_state.rs::load_par_df` (`engine_state.rs:1468`)
holds **only one shared-state lock** during execution, and only during a
bounded `HashMap::extend`. All other phases run on local stack/heap state
plus rayon parallelism. `EngineState` is `Send + Sync` (proven in production
by `start_tcp_listener` which spawns `Arc<EngineState>` to a separate thread).

Wrapping `engine.load_par_df()` in `py.detach(...)` (Sprint 14 P3.2b) is
**safe**: no panic risk, no deadlock risk, no data corruption. The only
observable side-effect of concurrent calls is non-deterministic last-write-
wins ordering on overlapping table names, which matches the behavior of any
two sequential `load_par_df` calls.

This audit also surfaces a **finding for Sprint 14 scope review** (§5.1):
the user-facing `chili.engine.ChiliEngine.load_partitioned_df()` Python
wrapper already releases the GIL via `fn_call → py.detach` (lib.rs:527).
The GIL-held path is `engine.engine.load_par_df()` (the direct FFI binding),
which is what P3.2b literally targets but is not the path a typical Python
caller uses.

---

## 1. Shared state held during `load_par_df`

`load_par_df` is a 3-phase operation. Source: `engine_state.rs:1468-1506`.

| Phase | Lines | What it touches | Lock acquired |
|---|---|---|---|
| 1a — directory traversal | 1484-1486 | local `PathBuf`, local `Vec<(PathBuf, String, bool)>`, recursion via `collect_table_entries`, `fs::read_dir` syscalls | none |
| 1b — parallel build | 1491-1496 | rayon `par_iter` over `entries`; each `build_par_df_entry` opens parquet schema sentinel files, builds `PartitionedDataFrame` independently | none (rayon thread-pool init may take its own internal lock once per process; not a hazard) |
| 2  — extend par_df  | 1501-1504 | acquires `self.par_df.write()`, calls `HashMap::insert` for each new entry, releases lock at scope exit | `par_df: RwLock<HashMap<String, PartitionedDataFrame>>` (write) |

`EngineState` declares 9 shared-state locks (`engine_state.rs:87-104`):

```rust
vars: RwLock<HashMap<String, SpicyObj>>,
par_df: RwLock<HashMap<String, PartitionedDataFrame>>,
source: RwLock<Vec<(String, String)>>,
handle: RwLock<IndexMap<i64, Handle>>,
tick_count: RwLock<Vec<i64>>,
job: RwLock<IndexMap<i64, Job>>,
topic_map: RwLock<HashMap<String, Vec<i64>>>,
arc_self: RwLock<Option<Arc<Self>>>,
parse_cache: Mutex<LruCache<...>>,
```

`load_par_df` touches **only `par_df`**. The other 8 are not contended by
this method.

---

## 2. `Send + Sync` verification

### 2.1 `EngineState` is `Send + Sync`

Proven in production by `start_tcp_listener` (`engine_state.rs:1981`,
mirrored in `crates/chili-py/src/lib.rs:558-561`):

```rust
let state = Arc::clone(&self.inner);
std::thread::spawn(move || {
    state.start_tcp_listener(port, remote, users);
});
```

For `Arc<EngineState>` to be sent across a thread boundary, `EngineState`
must be `Send + Sync`. `parking_lot::{RwLock, Mutex}<T>` implement
`Send + Sync` when `T: Send`, and every field's inner type is `Send` (no
`Rc`, no `Cell` outside locks, no raw pointers).

### 2.2 `PartitionedDataFrame` is `Send + Sync`

Source: `crates/chili-core/src/par_df.rs:30`:

```rust
pub struct PartitionedDataFrame {
    pub name: String,                       // Send + Sync
    pub df_type: DFType,                    // enum, derives Clone, Send + Sync
    pub path: String,                       // Send + Sync
    pub pars: Vec<i32>,                     // Send + Sync
    pub empty_schema: Option<Arc<DataFrame>>, // Send + Sync (Arc<polars::DataFrame> is Send + Sync)
}
```

No `unsafe impl !Send` or `!Sync` exists; auto-derived. Confirmed by the
fact that `par_df.par_iter()` (rayon) at `engine_state.rs:1492` compiles —
rayon requires `T: Send` on the iterator's item type.

### 2.3 `Bound<'_, PyAny>` is NOT held during `load_par_df`

The FFI signature `fn load_par_df(&self, hdb_path: &str)` (`lib.rs:532`)
takes only `&str` — no `Bound<'_, PyAny>` arg is captured into the
GIL-released closure. This satisfies pyo3's `Send` constraint on
`py.detach`'s closure: a `Bound<PyAny>` would not be `Send` and would
require explicit handling.

---

## 3. Concurrency hazards under GIL release

Hazard analysis: assume Sprint 14 P3.2b wraps `load_par_df` in `py.detach`,
allowing N Python threads to enter the function concurrently.

### 3.1 Two threads call `load_par_df(hdb_path)` on the same path

- **Phase 1a (no lock):** both threads enumerate the same directory tree
  independently. Each builds its own `Vec<(PathBuf, String, bool)>`. No
  mutation of shared state. Wasteful (2× the syscalls) but safe.
- **Phase 1b (no lock):** both threads run rayon `par_iter` on their own
  `entries`. Both rayon invocations share the global rayon thread-pool
  (one pool per process), but rayon's internal scheduling is correct
  under concurrent submission — workers steal from a shared queue. Wasteful
  (2× the schema-file opens) but safe.
- **Phase 2 (par_df write lock):** both compete for `par_df.write()`.
  `parking_lot::RwLock` is fair-acquire by default; one thread enters
  first, calls `HashMap::insert` for its keys, releases. The other enters
  and inserts the same keys (last-write-wins). Result: `par_df` ends up
  with the union of both entry sets. Identical keys → values overwritten.

  **Result:** no panic, no corruption. Final state is consistent with one
  caller having loaded the path twice in sequence.

### 3.2 Two threads call `load_par_df` on different paths

Same as 3.1, but Phase 2 inserts disjoint keys → both sets land. Safe and
expected.

### 3.3 Thread A calls `load_par_df`, Thread B calls `clear_par_df`

`clear_par_df` (`engine_state.rs:1458`) acquires the same `par_df.write()`
lock and calls `HashMap::clear()`. Two scenarios:

- **clear → load:** clear empties first; load's Phase 2 then inserts its
  entries into an empty map. Result: only A's entries.
- **load → clear:** load's Phase 2 inserts; clear then wipes everything.
  Result: empty `par_df`.

**Visible non-determinism but no corruption.** Same outcome as if a single
thread had called the two methods in either order.

### 3.4 Thread A calls `load_par_df`, Thread B calls `eval` (read path)

`eval`'s read path through `get_par_df` (`engine_state.rs:1447`) acquires
`self.par_df.read()`. While A holds the write lock during Phase 2, B's
read blocks (parking_lot fair); when A releases, B reads. B may see
either pre-A or post-A state depending on ordering. **Safe; no panic.**

### 3.5 Thread A calls `load_par_df`, Thread B calls `upsert` / `insert` / `set_var`

These touch only the `vars` lock, not `par_df`. Independent locks → no
contention. Safe.

### 3.6 Thread A calls `load_par_df`, Thread B calls `parse_cache_len` or any parsing

`parse_cache: Mutex<LruCache<...>>` is independent from `par_df`. No
interaction. Safe.

### 3.7 Rayon thread-pool re-entry

If the calling Python thread is itself a rayon worker (e.g., the harness
parallelizes loads inside a rayon scope), Phase 1b's `par_iter` could in
theory recurse into the same pool. rayon handles this correctly via
work-stealing — no deadlock. Confirmed by absence of deadlock complaints
from `start_tcp_listener` paths which also use rayon-owned tasks.

### 3.8 Hazard ruled out: borrow-checker forbids holding `&mut SpicyObj` across `py.detach`

`upsert_var` and `insert_var` (`engine_state.rs:277, 317`) hold `vars.write()`
through `df.extend()`. If a future P3.2-style proposal tried to release the
GIL inside those methods, the borrow checker would reject capturing
`&mut SpicyObj` (returned by `vars.get_mut(id)`) into a `Send` closure. This
is the static reason the Sprint 13.5 audit recommended **descoping A.2.2
(vars-write-lock release)** until profile evidence justifies the
clone-then-swap workaround. `load_par_df` does not have this constraint —
it builds new `PartitionedDataFrame` values from scratch, not via mutable
borrow of existing `vars` entries.

---

## 4. Verdict

**GREEN — safe to wrap `engine.load_par_df` in `py.detach`.**

Required conditions all hold:

- `EngineState: Send + Sync` (production-proven via `start_tcp_listener`).
- `PartitionedDataFrame: Send + Sync` (auto-derived).
- `load_par_df`'s only mutable shared-state access is `par_df.write()` for
  a bounded `HashMap::extend` window.
- No `Bound<PyAny>` is held during the function body.
- No re-entrancy into Python required (no `Python::with_gil` re-acquire
  inside the function).
- All concurrency hazards (3.1-3.7) resolve to "non-deterministic but
  consistent state" or "safe via existing lock semantics."

Sprint 14 P3.2b can proceed.

---

## 5. Observations for Sprint 14 scope review

### 5.1 The user-facing path already releases the GIL

`chili.engine.ChiliEngine.load_partitioned_df()` (Python wrapper, `chili-py/chili/engine.py:262`):

```python
def load_partitioned_df(self, hdb_path: str) -> None:
    self.fn_call("load", [hdb_path])
    self._hdb_path = hdb_path
```

This routes through `engine.engine.fn_call("load", [...])`, which DOES
release the GIL (`lib.rs:527`):

```rust
let obj = py.detach(move || map_spicy_error(self.inner.fn_call(func, &args)));
```

The GIL-held path is the direct FFI call `engine.engine.load_par_df(hdb)`
(`lib.rs:532-536`):

```rust
fn load_par_df(&self, hdb_path: &str) -> PyResult<()> {
    self.check_fork()?;
    map_spicy_error(self.inner.load_par_df(hdb_path))?;  // no py.detach
    Ok(())
}
```

**Implication for Sprint 14:**
- For typical Python callers (using `load_partitioned_df`): GIL is already
  released. Sprint 14 P3.2b is a no-op for them.
- For callers reaching the FFI directly (mdata's REST workers if they
  bypass the wrapper, future bench harnesses): P3.2b unblocks concurrency.
- Sprint 13.5 Part B's bench data confirms this: `concurrent_load` (fn_call
  path) scales to ~12.9K calls/s at N=4; `concurrent_load_direct` stays
  flat at ~4.85K calls/s regardless of N (perfect serial bottleneck).

Sprint 14 may want to consider whether to also remove the `fn_call`
indirection entirely, OR document the direct-FFI path as deprecated, OR
both.

### 5.2 `clear_par_df` has the same shape

`clear_par_df` (`lib.rs:539-543`) is also GIL-held; same audit applies.
Sprint 14 P3.2b should release GIL on both for symmetry.

### 5.3 Phase 1b rayon scaling cap

The fn_call path's bench shows N=4→8 throughput regression (12.9K → 8.5K
calls/s) — Phase 2 lock contention between threads dominates once Phase 1b
parallelism saturates the rayon pool. Sprint 14 P3.2b would not improve
this; only a finer-grained `par_df` lock (e.g., per-table mutex) would.
Out of Sprint 14 scope; flag for future consideration.

---

## 6. Optional micro-test (D.2)

Sprint 13.5 Part B's `concurrent_load_direct` shape at N=8 ran 24238
load_par_df calls in 5 seconds without panic, deadlock, or wrong
behavior. This serves as a 5-second stress test of the GIL-held path.
The fn_call path ran 42629 calls at N=8 in 5 seconds. Both passed.

A longer-duration stress test (30 minutes, repeated `load + clear` from
4 threads) is **not** required for the Sprint 14 readiness gate. The
static analysis above is sufficient; the fact that `start_tcp_listener`
runs continuously in production with a separate Python thread holding
`Arc<EngineState>` is the strongest existing evidence.

---

## Cross-references

- `crates/chili-core/src/engine_state.rs:87-112` — `EngineState` field
  declarations.
- `crates/chili-core/src/engine_state.rs:1468-1506` — `load_par_df` body.
- `crates/chili-core/src/par_df.rs:30-40` — `PartitionedDataFrame` shape.
- `crates/chili-py/src/lib.rs:520-543` — FFI bindings (eval, fn_call,
  load_par_df, clear_par_df).
- `crates/chili-py/chili/engine.py:262-275` — `load_partitioned_df` /
  `clear_partitioned_df` Python wrappers.
- `docs/sim/sprint_13.5_dispatch_brief_2026-05-09.md` — Sprint 13.5 brief
  that commissioned this audit.
- `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 13.5 baseline
  numbers (added in Part B wrap).
