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
