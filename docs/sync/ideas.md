# Ideas — backlog of unscoped items

Capture-as-you-think file for ideas not yet tied to a sprint or ADR. No particular
priority. Cull when items get scoped (move to a sprint dispatch brief) or rejected
(move to history with a rejection note).

Adapted from mdata's `docs/sync/ideas.md` shape.

---

## Format

`- [tag] **Title** — short hook describing the idea + (optionally) why it's
interesting + (optionally) cross-reference.`

One bullet per idea; max 3 lines. **Bracketed tag is mandatory** on every new entry —
untagged entries are a sweep target during housekeeping. Canonical tag set:

- `[architecture]` — design / API / module structure
- `[ops]` — tooling / CI / cron / process
- `[incident]` — bug or anomaly worth recording beyond the commit message
- `[observation]` — cross-project / external finding worth capturing
- `[validation]` — test / bench / soak / verification idea

If an entry grows beyond 3 lines, promote to a real plan, ADR, or `docs/proposals/`
proposal.

Reading is async — user reviews and replies inline with blockquotes. Never branch
implementation off an idea entry without first promoting it to a sprint brief or ADR.

---

- [architecture] **Per-table mutex on `par_df`** — replace `par_df: RwLock<HashMap<String, PartitionedDataFrame>>` with a per-table mutex (e.g., `RwLock<HashMap<String, Arc<RwLock<PartitionedDataFrame>>>>` or `dashmap::DashMap`). Sprint 14 confirmed the `par_df.write()` lock is the binding constraint at N≥4 on `concurrent_load_direct` (regression at N=4→N=8). Concurrent loads of *different* tables would no longer contend. Cost: trickier `get_par_df`/`load_par_df` semantics; needs a lock-ordering proof. Trigger: profile evidence on a real multi-table workload showing the regression matters.
- [architecture] **Read-Copy-Update on `par_df`** — write to a clone of the HashMap outside the lock, swap the inner `Arc<HashMap>` atomically (`arc_swap` crate) on commit. Eliminates `load_par_df` Phase 2 lock contention entirely. Cost: write-side memory amplification (clone the HashMap per call); reads see eventual consistency. Trigger: same as per-table mutex but where write throughput dominates.
- [architecture] **Coalesce concurrent loads on the same `hdb_path`** — when N threads call `engine.load_par_df(path)` concurrently, only one performs the work; the others block on a futures-style `OnceCell` and read the result. Eliminates the wasted Phase 1a/1b duplication noted in `docs/sync/load_par_df_state_audit.md` §3.1. Cost: small — a per-path mutex / DashMap of in-flight loads. Trigger: profile evidence that real callers (mdata REST workers) overlap calls on the same path.
- [architecture] **`load_partitioned_df_eager` → `pl.DataFrame`** — return a materialized DataFrame directly instead of `None + load-into-engine`, for the "I just want the data, not the engine state" use case. Cost: thin wrapper around scan + collect; sort_columns / row-group pruning behavior must match the load-into-engine path. Trigger: mdata-suggested 2026-05-09 (low priority — `eval(lazy=False)` works); revisit if a second consumer asks. mdata cross-ref: `~/code/mdata/docs/sync/chili_0.8.3_upgrade_assessment_2026-05-09.md` §3 suggestion 1.
- [observation] **chili-native pepper read overhead** — `clear + load + eval` adds ~30-40ms fixed cost on top of `pl.scan_parquet().collect()` for 2M-row partitions (mdata real-data bench, 2026-05-09). Probable culprit: parse cache miss on first eval + engine-state setup. Profile breakdown might surface a tighter read path. Trigger: a chili consumer wanting pepper for hot-path reads (mdata routes most queries through Polars/DuckDB; not currently a hot path). mdata cross-ref: same doc §2 finding 5.
- [validation] **CI bench fixture ≥ 100k rows for codec/IO regression catching** — chili's 1000-row codec fixture overstates ZSTD storage win by ~70% vs 2M-row real OHLCV (synthetic 3.17× vs real-data 1.88×). mdata is offering an anonymized 100k-row OHLCV fixture for chili-side CI. Cost: small — add fixture to `crates/chili-py/tests/` + parametrize existing codec tests. Trigger: cheap to add when next touching codec test surface; would catch scaling-related regressions earlier. mdata cross-ref: same doc §3 suggestion 4.
- [architecture] **Struct-shaped FFI for `ParquetWriteConfig`** — ADR-0005 §6 already commits to this when ≥3 Parquet write options needed; `data_page_size` / `bloom_filter` / `dictionary` are the obvious next knobs. mdata won't push for it Phase 1 but confirms welcome. Trigger: a 3rd Parquet option is requested, OR mdata Sprint 40 (Type-2 delta-tables benchmark) surfaces row-group-pruning needs. mdata cross-ref: same doc §3 suggestion 3.
