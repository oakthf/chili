# chili → mdata wheel delivery, 2026-05-08 (Sprint 7 Part A)

**Wheel artifact:** `dist/chili_sauce-0.8.1-cp310-abi3-macosx_11_0_arm64.whl`
**Branch:** `claude-2` tip
**Predecessor wheel:** `chili_sauce-0.8.0-cp310-abi3-macosx_11_0_arm64.whl`
(Sprint 5 delivery, 2026-05-07; superseded by this release).
**Status:** Internal — held until mdata sign-off on §4 install protocol.

---

## ⚠️ CRITICAL — install protocol for mdata

**The previous delivery (Sprint 5) was installed by mdata as an
EDITABLE link directly into chili's repo (e.g., `uv pip install -e
/Users/oakadmin/code/chili/crates/chili-py`). That is FORBIDDEN going
forward.** Editable installs caused mdata's runtime to break during
chili's compile work in Sprints 4–7 (mdata's Python imports resolved
to chili's mid-rebuild state).

**Required install path** for this delivery and all future ones:

```bash
# 1. Uninstall ANY existing chili / chili-sauce / chili-py install (especially editable):
uv pip uninstall chili-sauce chili-py chili 2>/dev/null

# 2. Install ONLY from the wheel artifact:
uv pip install /Users/oakadmin/code/chili/dist/chili_sauce-0.8.1-cp310-abi3-macosx_11_0_arm64.whl

# 3. Verify the install is wheel-based (not editable):
uv pip show chili-sauce
#   Expected: "Location: ...site-packages" (NOT a path inside chili's repo)
```

**Do NOT** use `pip install -e <path>`, `uv add file:///path/to/chili-py
--editable`, or any equivalent. Only the wheel.

---

## TL;DR for mdata

1. **Install via wheel only** (§ above). Editable installs break under
   chili's compile cycles.
2. **`engine.eval(query, lazy=True)` is now USABLE** (ADR 0003 resolved
   in Sprint 7 Part A). Returns a real `polars.LazyFrame` with
   predicate-pushdown across the FFI boundary preserved.
3. **`engine.eval(query)` (eager default) unchanged** from Sprint 5
   delivery.
4. **Pub/sub: claude's two pub/sub models are *deliberately-retired
   pending mdata feedback*** per ADR 0001 (unchanged from Sprint 5
   delivery). If mdata can't refactor, surface NOW so a new ADR can
   reopen the pub/sub decision before Sprint 8.
5. **`tick_count` shape change** from claude's scalar `i64` to main's
   `Vec<i64>` with index argument (unchanged from Sprint 5 delivery).
6. **`write_partitioned_df` arg-order fix** (Sprint 3 Part C — already
   in the 0.8.0 wheel; still applies here).
7. **Eager DataFrame transfer is unaffected** (already worked on the
   0.8.0 wheel).

---

## 1. Wheel provenance

```
File:     dist/chili_sauce-0.8.1-cp310-abi3-macosx_11_0_arm64.whl
Built:    2026-05-08 (Sprint 7 Part A, claude-2 branch)
Profile:  release (--release)
Python:   ≥ 3.10 (abi3-py310)
Platform: macOS 11.0+ arm64
Polars (Python, runtime dep): pinned to `polars==1.39.3`
Polars (Rust, compiled in): pola-rs/polars at the `py-1.39.3` tag,
                            with chili-side q-style fmt patch on top
                            (commit 8d56f02 in /tmp/polars-py-1.39.3).
                            Same source the Python polars 1.39.3 wheel
                            is built from → DSL_SCHEMA_HASH matches by
                            construction → lazy-frame transfer works.
```

Get precise commit SHA at delivery time:

```bash
cd /Users/oakadmin/code/chili
git log -1 --format="%H %s" claude-2
git status
```

The wheel is **not signed** and **not published to PyPI**. mdata installs
directly from the file path (see § above for the strict protocol).

---

## 2. ABI / feature delta vs the 0.8.0 wheel mdata is currently running

| Surface | 0.8.0 wheel (Sprint 5) | 0.8.1 wheel (this delivery) | Refactor? |
|---|---|---|---|
| `engine.eval(query)` (eager) | ✓ works | ✓ works | No |
| `engine.eval(query, lazy=True)` | Returned `pl.LazyFrame` but `.collect()` raised `ComputeError` | ✓ works end-to-end with predicate pushdown | No (becomes usable; was previously a documented stub) |
| `engine.eval(query, lazy=False)` | ✓ same as default | ✓ same as default | No |
| `engine.publish(...)` (pub/sub) | (deliberately-retired pending mdata feedback) | (same) | YES if mdata uses pub/sub — surface to chili before Sprint 8 |
| `engine.get_tick_count(index)` | ✓ vec-indexed | ✓ vec-indexed (now also accepts no-arg form, default `index=0`) | No (defaults backward-compatible) |
| `engine.tick(index, inc)` | ✓ vec-indexed | ✓ vec-indexed (now also accepts no-arg form, default `index=0, inc=1`) | No |
| `engine.write_partitioned_df(...)` | ✓ canonical arg-order | ✓ canonical arg-order | No |
| `engine.overwrite_partition(...)` | ✓ thin wrapper | ✓ thin wrapper | No |
| `engine.set_column_scale(table, col, factor)` | ✓ regex word-boundary match | ✓ same | No |
| `engine.clear_column_scales()` | ✓ | ✓ | No |
| `engine.query_plan(query, hdb_path)` | ✓ pepper-syntax-only | ✓ pepper-syntax-only | No |
| `engine.table_count()` | ✓ | ✓ | No |
| `.log.{info,warn,debug,error}` Pepper built-ins | ✓ | ✓ | No |
| Exception hierarchy (`ChiliError` + 6 subclasses) | ✓ | ✓ | No |
| mimalloc global allocator | ✓ | ✓ | No |
| Parse cache hit latency (golden rule 6 ≤ 400 ns) | ~371 ns | TBD (will re-measure in Sprint 7 Part B; expected unchanged) | No |
| Polars Python runtime dep | `polars==1.39.3` (declared) | `polars==1.39.3` (still pinned) | No (no change) |

---

## 3. The lazy-frame fix (the main reason for this delivery)

In Sprint 5's 0.8.0 wheel, `engine.eval(query, lazy=True)` returned a
`pl.LazyFrame` object that **could not be `.collect()`'d** — Python
polars's `PyLazyFrame.deserialize_binary` raised `ComputeError:
deserialization failed (DSL_SCHEMA_HASH mismatch)` because the Rust
side and Python side compiled their `polars-plan` from different commits.

In this 0.8.1 wheel, chili's Rust side now compiles `polars-plan` from
the **same source commit** (`pola-rs/polars` at `py-1.39.3` tag) that
Python polars 1.39.3 is built from. The DSL_SCHEMA_HASH matches; lazy
deserialization succeeds; the LazyFrame object is fully functional with
predicate-pushdown across the FFI boundary.

mdata code that previously had to use eager-only:

```python
# 0.8.0 era
df = engine.eval("select close from t where date>=2024.01.01")
out = df.filter(pl.col("close") > 100).collect()
```

Can now (optionally) chain on the lazy boundary:

```python
# 0.8.1 era
out = (
    engine.eval("select close from t where date>=2024.01.01", lazy=True)
    .filter(pl.col("close") > 100)
    .collect()
)
# Predicate pushed into the LazyFrame plan; engine returns smaller
# materialized result vs the eager path.
```

**This is opt-in.** mdata's existing eager-default callers continue to
work unchanged.

---

## 4. mdata install protocol (REQUIRED — read carefully)

### 4.1 Uninstall existing chili (especially if editable)

```bash
# In whatever venv mdata uses for its chili dependency:
uv pip list | grep -iE "chili|chili-sauce"
uv pip uninstall -y chili-sauce chili-py chili
```

**Verify uninstall is complete:**

```bash
python -c "import chili" 2>&1
# Expected: ModuleNotFoundError: No module named 'chili'
```

If `import chili` still works after uninstall, an editable install is
still on the path. Find and remove it:

```bash
find . -name "*.pth" -exec grep -l chili {} \;
# Remove any .pth files referencing chili source paths.
```

### 4.2 Install from the wheel artifact

```bash
uv pip install /Users/oakadmin/code/chili/dist/chili_sauce-0.8.1-cp310-abi3-macosx_11_0_arm64.whl
```

(Or copy the wheel to mdata's local artifact store and install from there.)

### 4.3 Verify install is wheel-based (NOT editable)

```bash
uv pip show chili-sauce
```

Expected output:

```
Name: chili-sauce
Version: 0.8.1
Location: /Users/oakadmin/code/mdata/.venv/lib/python3.12/site-packages
                                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
              Location must be inside mdata's site-packages.
              If Location points anywhere inside chili's repo
              (e.g., /Users/oakadmin/code/chili/crates/chili-py/...),
              this is an EDITABLE install — uninstall and redo.
```

### 4.4 Smoke test

```bash
python -c "
from chili import ChiliEngine
import polars as pl
e = ChiliEngine(pepper=True)
assert e.eval('1 + 2') == 3, 'eager scalar eval'
out = e.eval('([] x:1 2 3; y:4 5 6)')
assert isinstance(out, pl.DataFrame) and out.shape == (3, 2), 'eager dataframe'
lazy = e.eval('([] x:1 2 3 4 5)', lazy=True)
assert isinstance(lazy, pl.LazyFrame), 'lazy returns LazyFrame'
filtered = lazy.filter(pl.col('x') > 2).collect()
assert filtered['x'].to_list() == [3, 4, 5], 'lazy collect with filter'
print('chili-sauce 0.8.1 wheel works: eager + lazy paths OK')
"
```

If any assertion fails, ping the chili side. The wheel is identified by
its build SHA in `git log -1` of chili's `claude-2` branch.

---

## 5. Pub/Sub status

Unchanged from Sprint 5 delivery (`mdata_chili_2026-05-07_delivery.md`,
moved to `docs/history/sync/` Sprint 6).

ADR 0001 retains pub/sub as **deliberately-retired pending mdata
feedback**. mdata's call sites for `engine.publish(ipc_bytes)` and
`engine.subscribe(callback)` need to refactor to main's tick/sub
framework. **If that refactor is infeasible, surface to chili before
Sprint 8 kickoff** so a new ADR can reopen the pub/sub decision.

---

## 6. What's in this delivery that wasn't in 0.8.0

- Lazy=True FFI works (the headline change; ADR 0003 resolved).
- `engine.get_tick_count()` and `engine.tick()` accept no-arg defaults
  (Sprint 5 Part D.1 fix).
- ADR 0003 amended with corrected root-cause analysis + resolution
  record.
- Workspace + chili-py Cargo.toml `[patch.crates-io]` blocks pinning
  all polars-* crates + pyo3-polars to `pola-rs/polars` at `py-1.39.3`
  tag with chili's q-style fmt patch on top (replaces the obsolete
  hinmeru/polars-core-patch fork dependency).

---

## 7. What's STILL not in this delivery

- Bench A/B comparison numbers (parked-claude binary vs claude-2 binary
  on scan / eval / load_par_df / write_partition / parse_cache). Coming
  in Sprint 7 Part B.
- Phase17 reverse-scan + sort-groupby benches — Sprint 12.
- STAC-M3-shape benchmark suite — Sprint 7 Part B (or a Sprint 8 carve-out
  if Part B's A/B sweep absorbs the budget).
- KDB-X CE head-to-head — Sprint 9 if KDB-X CE is GA.

---

## 8. Asks for mdata team

1. **Confirm wheel-only install** (§4) is in place. The previous editable
   install caused outages during chili's mid-Sprint compile work.
2. **Smoke test** (§4.4) passes against this 0.8.1 wheel.
3. **Confirm or surface blocker** on the pub/sub refactor (§5). Default
   path (refactor to tick/sub) stands; reopen-via-new-ADR remains an
   option if the refactor is infeasible.
4. **Optional: try `lazy=True`** in one mdata workflow that currently
   chains predicates on engine results. Report any unexpected behavior.

---

## 9. Cross-references

- ADR 0001 — pub/sub canonical model: [`../decisions/0001-pub-sub-canonical-model.md`](../decisions/0001-pub-sub-canonical-model.md).
- ADR 0002 — engine.eval lazy/eager default (Option b): [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md).
- ADR 0003 — PyLazyFrame DSL incompat (resolved Sprint 7 Part A): [`../decisions/0003-pylazyframe-dsl-incompat.md`](../decisions/0003-pylazyframe-dsl-incompat.md).
- Sprint 5 delivery (superseded by this doc): [`../history/sync/mdata_chili_2026-05-07_delivery.md`](../history/sync/mdata_chili_2026-05-07_delivery.md) (will move there when Sprint 7 Part B wraps).
- Sprint 7 Part A retro: [`../sim/sprint_7_retro.md`](../sim/sprint_7_retro.md) (lands at Sprint 7 wrap).
- Bench A/B comparison: [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) (Sprint 7 Part B will populate scan/eval/load/write rows).
