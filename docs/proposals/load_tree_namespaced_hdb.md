# Proposal — `Engine.load_tree(...)`: Namespaced HDB Loader for Hierarchical Data Trees

**Author:** Treehouse Finance (downstream user of chili driving a market-data warehouse called *mdata*).
**Date:** 2026-04-30
**Audience:** Hinmeru / `purple-chili` author, for the next minor release of `chili` / `chili-py`.
**Context:** mdata has outgrown the single-level HDB layout (`data/hdb/{table}/{partition}`). To support multiple environments (prod / sim / bt / hist / arhv), multiple asset classes (equity / crypto / futures / options), and a clean separation between vendor-raw and reconciled-derived data, we are migrating to a **5-level tree**:

```
data/{env}/hdb/{asset_class}/{stage}/{table_name}/{YYYY.MM.DD_NNNN}
```

Today's `Engine.load(path)` is **2-level**: it walks `path/{table}/` looking for partition FILES at the table level (`load_par_df` in `crates/chili-core/src/engine_state.rs`, line ~1583, filters with `is_file()`). Subdirectories at the table level are silently dropped, so `Engine.load("data/prod/hdb/")` returns zero tables under our new layout.

We propose a new **additive, non-breaking** API method `Engine.load_tree(...)` that walks a deeper tree and exposes tables under flat namespace-prefixed names. This unblocks researchers using `pepper` syntax for ad-hoc analysis without forcing them to call `load(...)` once per leaf directory.

This proposal preserves `Engine.load(...)` exactly as it is today.

---

## The mdata layout at a glance

Locked in `~/code/mdata/docs/decisions/0005-data-tree-layout.md`. The path shape:

```
data/
├── prod/
│   └── hdb/
│       ├── equity/
│       │   ├── raw/
│       │   │   ├── massive_ohlcv_1m_ws/2024.01.12_0000
│       │   │   ├── massive_ohlcv_1m_bf/2024.01.12_0000
│       │   │   ├── massive_trade_ws/2024.01.12_0000
│       │   │   ├── ib_ohlcv_1m_bf/2024.04.24_0000
│       │   │   ├── ib_position_ws/2026.04.30_0000
│       │   │   └── ...
│       │   └── derived/
│       │       ├── ohlcv_1m/2024.01.12_0000      ← post-reconcile canonical
│       │       ├── ohlcv_5m/...
│       │       ├── ohlcv_1d_adjusted/...
│       │       ├── position/...
│       │       └── pnl/...
│       ├── crypto/
│       │   ├── raw/
│       │   │   ├── binance_trade_ws/...
│       │   │   ├── bybit_position_ws/...
│       │   │   └── coingecko_marketcap_bf/...
│       │   └── derived/
│       │       ├── ohlcv_1m/...                  ← cross-exchange merged
│       │       └── pnl/...
│       ├── futures/{raw,derived}/...
│       └── options/{raw,derived}/...
├── sim/    (same shape under sim/hdb/)
├── bt/     (one tree per backtest run-id under bt/<run-id>/hdb/)
├── hist/   (aged-out prod, same shape)
└── arhv/   (cold archive, same shape; partition files may be tar.zst)
```

Tables across asset classes can have **identical short names** — both `equity/derived/ohlcv_1m` and `crypto/derived/ohlcv_1m` are real, distinct tables. The chili load API must disambiguate.

## What today's loader does

`load_par_df(path)` in `crates/chili-core/src/engine_state.rs`:

1. Reads `path` as a directory; for each top-level entry:
   - If a **file** → registers it as an unpartitioned table (single-file).
   - If a **directory** → reads its contents looking for partition FILES matching `YYYY.MM.DD_NNNN` or `YYYY_NN`; subdirectories at this level are silently filtered out by `is_file()`.
2. Indexes each table under its bare directory name in `par_df: RwLock<HashMap<String, PartitionedDataFrame>>`.

`pepper` queries reference tables as bare identifiers — `select from massive_ohlcv_1m` — so `par_df`'s key is exactly the user-facing table name.

This works perfectly for a flat HDB. It cannot reach a 5-level tree.

## Proposal — `Engine.load_tree(root, namespace_separator="_", max_depth=4)`

A new method on `Engine` (or `PyEngineState` if upstream prefers — both wrap the same `EngineState`):

```rust
// crates/chili-core/src/engine_state.rs
impl EngineState {
    /// Load a hierarchical HDB tree, generating flat namespace-prefixed
    /// table names from path components.
    ///
    /// `root` is the tree root. The loader walks at most `max_depth`
    /// directory levels deep looking for partition files. When it finds
    /// a directory whose contents are partition files (and optionally a
    /// `schema` sentinel), that directory is registered as a table.
    /// The table name is derived by joining the path components from
    /// `root` to the table dir using `namespace_separator`.
    ///
    /// Example: with `root = "data/prod/hdb"` and the layout above,
    /// the loader registers tables as:
    ///   - "equity_raw_massive_ohlcv_1m_ws"
    ///   - "equity_raw_massive_ohlcv_1m_bf"
    ///   - "equity_derived_ohlcv_1m"
    ///   - "crypto_raw_binance_trade_ws"
    ///   - "crypto_derived_ohlcv_1m"
    /// (note that equity and crypto's derived/ohlcv_1m no longer collide.)
    ///
    /// Tables under `root` itself (depth 0, today's flat layout) are
    /// registered unprefixed, preserving compatibility with mixed trees.
    pub fn load_tree(
        &self,
        root: &str,
        namespace_separator: &str,
        max_depth: u32,
    ) -> SpicyResult<()> { ... }
}
```

### Detection rule (recursive walk)

For each directory `d` under `root` (DFS or BFS, doesn't matter):

1. List `d`'s entries.
2. **If any entry is a partition FILE** (matches `YYYY.MM.DD_NNNN` or `YYYY_NN`, or is the `schema` sentinel) → `d` is a **table directory**. Register it. Do not recurse into `d`'s subdirs (a table dir cannot contain nested tables — partition files are leaves).
3. **Else if `d` only contains subdirectories** → recurse into each subdir, depth + 1, up to `max_depth`.
4. **Else** (mixed / unrecognised) → log a warning, skip.

The `max_depth` parameter is a safety bound to prevent runaway walks if `root` is misconfigured. For mdata's 5-level tree (env / hdb / asset / stage / table / partition), pointing `load_tree` at `data/prod/hdb` requires `max_depth=3` to find tables at `equity/raw/{table}/`.

### Naming rule

The table key registered in `par_df` is the path from `root` to the table dir, joined by `namespace_separator`, **excluding** the table dir's own name? No — **including**. Example:

```
root = "data/prod/hdb"
table dir = "data/prod/hdb/equity/raw/massive_ohlcv_1m_ws"
relative path = "equity/raw/massive_ohlcv_1m_ws"
table key = "equity_raw_massive_ohlcv_1m_ws"
```

Using `_` as the default separator matches mdata's existing in-table naming convention (`massive_ohlcv_1m_ws`). The separator is configurable so users with different conventions can pick their own.

### Backward compatibility

`Engine.load(root)` continues to behave exactly as today (2-level scan). `Engine.load_tree(root, sep, depth)` is purely additive. A user who calls only `load(...)` sees no behavior change. A user who needs hierarchical access opts in by calling `load_tree(...)` instead.

The `par_df` HashMap structure does not change. The keys it stores can already be arbitrary strings (today they happen to all be flat names; with `load_tree` they'll be underscore-joined namespace strings). All downstream code that consumes `par_df` (eval, scan, the pepper grammar) keeps working — `select from equity_derived_ohlcv_1m` is parsed exactly like `select from ohlcv_1m` is today.

## Python binding

The thin pyo3 wrapper:

```python
# chili-py
class Engine:
    def load_tree(
        self,
        path: str,
        namespace_separator: str = "_",
        max_depth: int = 4,
    ) -> None:
        """Load a hierarchical HDB tree, generating flat namespace-prefixed
        table names. See Rust docstring for detection / naming rules."""
        self._inner.load_tree(path, namespace_separator, max_depth)
        self._hdb_path = path  # so reload() works against the tree root
```

Researchers can then do:

```python
import chili
engine = chili.Engine(pepper=True)
engine.load_tree("data/prod/hdb")

# In pepper / q-style:
result = engine.eval("select from equity_derived_ohlcv_1m where date = 2024.01.12")
result = engine.eval("select from crypto_raw_binance_trade_ws where symbol = `BTCUSDT")

# Cross-namespace join works because table names are unique:
result = engine.eval("equity_derived_pnl lj crypto_derived_pnl")
```

## Alternatives considered

**(a) `Engine.load_namespaced(map: dict[str, str])`** — explicit `{prefix: path}` mapping. More flexible (any prefix per leaf, not just path-derived) but verbose. Could be added on top of `load_tree` later if a use case appears.

**(b) Recursive `Engine.load(root, recursive=True)`** — a flag on the existing method. Rejected because it would change the meaning of an existing method's `path` argument depending on the flag, complicating the mental model.

**(c) Keep flat layout in chili and force users to flatten their HDB** — rejected by mdata; the schema-divergence between asset classes (equity vs crypto sessions, exchange codes, etc.) makes asset-class hierarchy a structural necessity, not a stylistic choice.

**(d) Symlink farm at the user level** — what mdata is implementing as the immediate workaround (a generated `data/prod/chili_view/` flat dir with symlinks to every real table). Works without chili changes but adds a maintenance step (rebuild on every table rename) and produces awkward paths in `pepper` error messages. Acceptable as a stopgap; not a long-term answer.

## Performance considerations

The tree walk is filesystem-bound (one `read_dir` per directory). For mdata's expected scale (~5 envs × ~4 asset classes × 2 stages × ~30 tables = ~1200 directories at full population), the walk completes in single-digit milliseconds on local SSD. The existing `load_par_df` already uses rayon for parallel partition enumeration; `load_tree` can reuse the same pattern by collecting all candidate table dirs in phase 1 and parallelising the per-table `PartitionedDataFrame` build in phase 2.

## Estimated scope

Rust side: ~80 LoC in `crates/chili-core/src/engine_state.rs` (the recursive walker + the existing `build_par_df_entry` reused unchanged). Python binding: ~10 LoC in the pyo3 wrapper. Tests: a few synthetic 3-level trees in `crates/chili-core/tests/`.

## Open questions for the author

1. Preferred name? `load_tree`, `load_namespaced`, `load_recursive`, something else?
2. Default `max_depth` — 3? 4? 5? mdata needs 3 for `data/prod/hdb/equity/raw/<table>/<partitions>` (root depth 0 → asset depth 1 → stage depth 2 → table depth 3). Erring on a larger default (5 or 6) seems safe given the cost is just a few `read_dir` calls.
3. Should the loader emit warnings on mixed dirs (some files, some subdirs) or on subdirs whose contents look table-like but lack a partition-file pattern? Verbose vs silent.
4. Any objection to overloading the `par_df` HashMap with namespace-prefixed keys? An alternative is a parallel HashMap for namespaced tables, but that complicates eval lookup. mdata's preference is to keep the existing single map.

## Why this matters to mdata

Without this enhancement, mdata's research / ad-hoc analysis story regresses: today researchers can `engine.load("data/hdb")` and immediately query in pepper. After Wave 3, they would have to call `load(...)` repeatedly per leaf and live with table-name collisions, or use the symlink-view workaround which obscures the on-disk reality.

With the enhancement, the new layout becomes a strict ergonomic improvement: more structured, easier to clean up, easier to extend, and queryable from chili in a single `load_tree(...)` call.

We're happy to prototype the implementation in our local fork and submit a patch if that's helpful.
