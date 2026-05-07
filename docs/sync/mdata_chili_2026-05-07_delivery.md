# chili → mdata wheel delivery, 2026-05-07

**Wheel artifact:** `crates/chili-py/target/wheels/chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl`
**Branch:** `claude-2` tip (commit at delivery time — see Wheel provenance below)
**Predecessor wheel mdata is currently running:** built from `claude` branch
tip at tag `claude-baseline-2026-05-07` (commit `dea966e`).
**Status:** Internal (held until mdata sign-off on Section 4 below).

---

## TL;DR for mdata

1. **Eager `engine.eval(query)` is unchanged.** mdata's existing call shape
   continues to work identically. No refactor needed for the eager path.
2. **`engine.eval(query, lazy=True)` is documented but unusable** for now —
   ADR 0003 documents the polars-core-patch fork DSL incompat. Don't
   wire `lazy=True` into mdata's production paths.
3. **Pub/Sub: claude's two pub/sub models are *deliberately-retired pending
   mdata feedback*** per ADR 0001. The ADR's "binds future work" clause
   reserves reopening via a new ADR if mdata surfaces a hard blocker. Default
   path: mdata callers using `engine.publish(ipc_bytes)` and the in-process
   Python broker refactor to main's tick/sub framework (`init_tick` /
   `tick.upd` / `sub.init`). If that's not viable, raise it now (§7 Ask 1)
   and chili will draft an ADR to reconsider. See §4 breakage report.
4. **`tick_count` shape changed** from claude's scalar `i64` to main's
   `Vec<i64>` (with index argument on `engine.get_tick_count(index)`).
   mdata callers need a one-line update.
5. **`write_partitioned_df` arg-order bug fixed.** mdata callers using
   `engine.write_partitioned_df(df, hdb_path, table, date, ...)` should
   double-check; if you used `fn_call("wpar", [...])` directly, that path
   was always correct.
6. **Bench A/B comparison is incomplete** — Sprint 5's parked-claude vs
   claude-2 sweep was deferred to Sprint 7 (post-housekeeping Sprint 6).
   Until then, mdata can run the wheel in a non-production environment
   and report any latency / throughput regression you observe.

---

## 1. Wheel provenance

```
File:   chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl
Built:  2026-05-07 (Sprint 5 Part C, claude-2 branch)
Profile: release (--release)
Python: ≥ 3.10 (abi3-py310)
Platform: macOS 11.0+ arm64
Polars: pinned in pyproject to `polars==1.39.3`
```

Get the precise commit SHA + git status from the source tree at delivery
time:

```bash
cd ~/code/chili
git log -1 --format="%H %s" claude-2
git status
```

The wheel is **not signed** and **not published to PyPI**. mdata installs
directly from the file path:

```bash
pip install /path/to/chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl
```

(Or via `uv add` with a local file source; `uv pip install <path>` works.)

---

## 2. ABI / feature delta vs the currently-running mdata wheel

| Surface | parked-claude (currently running) | claude-2 (this delivery) | mdata refactor needed? |
|---|---|---|---|
| `engine.eval(query)` (eager) | Returns DataFrame | Returns DataFrame | No |
| `engine.eval(query, lazy=True)` | (not present) | Returns LazyFrame **but unusable** end-to-end (ADR 0003) | No (don't use until ADR 0003 resolves) |
| `engine.publish(ipc_bytes)` (in-process broker) | Present | **REMOVED** per ADR 0001 (deliberately-retired pending mdata feedback) | YES — see §4 |
| `engine.publish(table, df)` (tick/sub) | parked-claude shape was `publish(table, ipc_bytes)` (parses bytes back into DataFrame internally) | Present, main's canonical (accepts polars DataFrame directly) | YES — adopt new API |
| `engine.subscribe(topic, callback)` | Present (in-process Python callback) | **REMOVED** per ADR 0001 (deliberately-retired pending mdata feedback) | YES — see §4 |
| `engine.get_tick_count()` (scalar) | Present | **REPLACED** by `get_tick_count(index)` | YES — one-line update |
| `engine.tick(inc)` (scalar) | Present | **REPLACED** by `tick(index, inc)` | YES — one-line update |
| `engine.write_partitioned_df(df, hdb_path, table, date, ...)` | Worked (custom-arg-order) | Fixed (canonical-arg-order) | Verify — likely no change for typical mdata usage |
| `engine.overwrite_partition(df, hdb_path, table, date)` | Present (separate fn) | Present (thin wrapper around `write_partitioned_df(overwrite=True)`) | No |
| `engine.set_column_scale(table, column, factor)` | Present | Present | No |
| `engine.clear_column_scales()` | Present | Present | No |
| `engine.query_plan(query, hdb_path)` | Present | Present | No |
| `engine.table_count()` | Present | Present | No |
| `engine.unload()` (alias for `clear_par_df`) | Present | Use `engine.clear_par_df()` | YES — rename |
| `engine.close()` / `reload()` / `is_loaded()` | (not present on parked-claude either) | (not present) | No |
| `.log.{info,warn,debug,error}` Pepper built-ins | Present | Present | No |
| Exception hierarchy (`ChiliError` + 6 subclasses) | Present | Present | No |
| mimalloc global allocator | Present | Present | No |
| Parse cache regression tests | Present (in chili) | Present (in chili) | N/A |
| Parse cache hit latency (golden rule 6 ≤ 400 ns) | ~385 ns reported | **371.43 ns measured** | No regression |

---

## 3. Migration cheatsheet

### 3.1 `tick_count` shape — scalar → indexed

**Before (parked-claude):**
```python
n = engine.get_tick_count()
engine.tick(5)  # increment by 5
```

**After (claude-2):**
```python
n = engine.get_tick_count(index=0)  # or just engine.get_tick_count() — defaults to index=0
engine.tick(index=0, inc=5)
```

Both paths default `index=0`, so the simplest migration is no-op for
single-stream callers. Multi-stream callers explicitly pass `index`.

### 3.2 Pub/Sub — claude's in-process broker → main's tick/sub framework

**Before (parked-claude):**
```python
engine.publish("ohlcv_1m", ipc_bytes)
engine.subscribe("ohlcv_1m", callback=on_msg)
```

**After (claude-2):**
```python
engine.init_tick(schema={"ohlcv_1m": ohlcv_schema}, log_dir="/tplog", date=today)
engine.publish("ohlcv_1m", df)  # df is a polars DataFrame, NOT bytes
# Subscribers connect via TCP socket and call .sub.init in pepper:
engine.subscribe("chili://localhost:5000", topics=["ohlcv_1m"])
```

Key shifts:
- `publish` accepts a polars DataFrame, not Arrow IPC bytes.
- Subscribers are out-of-process (TCP socket), not in-process Python
  callbacks.
- `init_tick` must be called once before `publish` (sets up the schema +
  tplog file).

### 3.3 `engine.unload()` → `engine.clear_par_df()`

```python
# Before:
engine.unload()

# After:
engine.clear_par_df()
```

(`engine.table_count()` is unchanged.)

### 3.4 `engine.eval(query, lazy=True)` — DOCUMENTED BUT UNUSABLE

`lazy=True` returns a `polars.LazyFrame` object that cannot currently be
`.collect()`'d due to the polars-core-patch fork DSL incompat (ADR 0003).
Don't use it in production paths until a follow-up wheel ships the fix.

---

## 4. Breakage report — mdata refactor needed

The breakage tracking moved from `docs/sync/mdata_breakage_report_2026-05-07.md`
(internal Sprint 2 v2 working doc) to this delivery doc.

### 4.1 Pub/Sub — required refactor

mdata's callers of `engine.publish(ipc_bytes)` and `engine.subscribe(...)`
must migrate to the tick/sub API per Section 3.2. Estimated mdata work:
**1-2 days** (per the original Sprint 2 v2 breakage report estimate),
depending on how many call sites exist and whether the subscriber callbacks
need to convert from in-process to out-of-process.

### 4.2 `tick_count` indexing — trivial refactor

One-line update per call site (Section 3.1). Estimated mdata work:
**~30 min**.

### 4.3 `unload` → `clear_par_df` — trivial refactor

Rename per Section 3.3. Estimated mdata work: **~10 min**.

### 4.4 `write_partitioned_df` arg-order — verify only

Sprint 3 Part C fixed a pre-existing arg-order bug. If mdata uses
`engine.write_partitioned_df(df, hdb_path, table, date, ...)` as-documented,
it now actually works (it was silently broken on parked-claude due to wpar
expecting `[path, partition, table, df, ...]` while the wrapper passed
`[df, hdb_path, table, date, ...]`). Most likely mdata used `fn_call("wpar",
[...])` directly with the canonical arg order — in which case no change
needed. **Verify**: grep mdata for `write_partitioned_df`. If found, test
once on the new wheel.

---

## 5. Smoke test for mdata to verify the wheel

In a fresh venv:

```bash
python -m venv /tmp/chili-test-venv
/tmp/chili-test-venv/bin/pip install \
  /path/to/chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl
/tmp/chili-test-venv/bin/python -c "
from chili import ChiliEngine
e = ChiliEngine(pepper=True)
assert e.eval('1 + 2') == 3
import polars as pl
out = e.eval('([] x:1 2 3; y:4 5 6)')
assert isinstance(out, pl.DataFrame), type(out)
assert out.shape == (3, 2)
print('chili-2 wheel works on basic eager path')
"
```

If this fails, ping the chili side. The wheel is identified by its
build SHA in `~/code/chili/git log -1`.

---

## 6. What's NOT in this delivery

- Bench A/B sweep numbers — Sprint 7 (post-housekeeping Sprint 6).
- LazyFrame transfer fix (ADR 0003 resolution) — future sprint, blocked
  on pyo3-polars upstream OR chili replacing the polars-core-patch fork.
- Phase17 reverse-scan + sort-groupby benches — Sprint 7.
- STAC-M3-shape benchmark suite — Sprint 7.

---

## 7. Asks for mdata team

1. **Sign-off on the breakage scope** above. The tick/sub migration is
   the heaviest piece; chili's ADR 0001 considers it deliberately retired.
   If mdata can't refactor away from the in-process broker for some
   reason, surface NOW so we can reopen the ADR before Sprint 7 wraps the
   bench A/B comparison.
2. **Run the smoke test** in §5 against the wheel; confirm "works"
   so chili can move to bench A/B in Sprint 7.
3. **Provide an mdata-side runtime sanity check** if you have one
   (latency/throughput baseline against parked-claude wheel) so chili
   Sprint 7 has a "did we regress in the wild?" data point alongside
   the criterion microbenchmarks.

---

## 8. Cross-references

- ADR 0001 — pub/sub canonical model: [`../decisions/0001-pub-sub-canonical-model.md`](../decisions/0001-pub-sub-canonical-model.md).
- ADR 0002 — engine.eval lazy/eager default (Option b): [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md).
- ADR 0003 — PyLazyFrame DSL incompat: [`../decisions/0003-pylazyframe-dsl-incompat.md`](../decisions/0003-pylazyframe-dsl-incompat.md).
- Sprint 3 retro — write_partitioned_df arg-order fix: [`../sim/sprint_3_retro.md`](../sim/sprint_3_retro.md).
- Sprint 4 retro — ADR 0002 implementation: [`../sim/sprint_4_retro.md`](../sim/sprint_4_retro.md).
- Sprint 5 retro — wheel cut + this delivery: [`../sim/sprint_5_retro.md`](../sim/sprint_5_retro.md).
- Bench rebaseline (claude-2 parse_cache): [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md).
- Inventory consumed Sprint 3-5: [`../research/claude_only_features_inventory_2026-05-07.md`](../research/claude_only_features_inventory_2026-05-07.md).
