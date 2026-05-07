# Main ↔ Claude Inventory — 22 commits since fork point d7a748b

**Author:** Sprint 1 main thread (deep Part D)
**Date compiled:** 2026-05-06
**Fork point:** `d7a748b` (chili 0.7.4)
**Counterparty doc:** `~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`
**Companion docs:** `q_kdb_landscape.md`, `kdb_alternatives.md`, `shakti_analysis.md`.

This document inventories every commit on `main` since the d7a748b fork (22 total),
classifies each, and predicts conflict surface against `claude`. It cross-references
the mdata wishlist's 5 requested cherry-picks against claude's existing surface area
to flag overlap (the user's "watch for half-done overlap" risk).

**Headline finding (the plan-pivot the Sprint 1 brief flagged as halt-criterion #2):**
the wishlist's **"cherry-pick the 5 commits cleanly"** premise is at risk. Claude
already has divergent implementations of `publish` / `subscribe` / `parse_cache` /
`overwrite_partition` under **different API shapes** than upstream's. Cherry-picks
will produce **substantive line-level conflicts**, not trivial whitespace. A merge
resolver must choose between two competing shapes for each surface. This is
recoverable — but it is a Sprint-2-plan adjustment that the wishlist did not
anticipate. Detail in §3.

---

## 1. The 22 commits — chronological inventory

Classification legend: **feat** = new feature, **refactor** = restructuring, **fix** = bugfix, **dep** = dep bump, **ci** = release/CI, **docs** = README/CHANGELOG, **rename** = upstream's `chili-pie → chili-sauce` rename which we deliberately did NOT pick up (CLAUDE.md project state).

| # | sha | date | classification | one-line subject | wishlist? |
|---|---|---|---|---|---|
| 1 | `e9092ce` | 2026-04-15 | dep+feat | Update version to 0.7.5; load_par_df parallel build; criterion benchmarks added (`load_par_df`, `scan`, `eval`, `write_partition`) | — |
| 2 | `9b65a50` | 2026-04-21 | feat | LRU parse cache in EngineState; parse_cache benchmark | — (already on claude under different lineage; see §2.1) |
| 3 | `bf9fa14` | 2026-04-24 | feat | First chili-py crate (`chili-py`); EngineState Python bindings | — (claude has divergent FFI rewrite per `08fe588`; see §2.2) |
| 4 | `a0a42f6` | 2026-04-24 | rename+ci | Rename Python pkg to `chili-sauce`; `release-py-binding.yml` workflow | — (NOT picked up; claude stays on `chili-pie` per CLAUDE.md) |
| 5 | `4734186` | 2026-04-24 | dep+feat | `chrono` workspace dep; `spicy_from_py_bound` improvements | — (chrono already on claude; conversions overlap) |
| 6 | `a5ead35` | 2026-04-24 | dep+ci | Cargo.lock churn; Taskfile streamlining | — |
| 7 | `0bfc8c5` | 2026-04-24 | ci | manylinux 2_28 in release workflow | — |
| 8 | `b0f20e5` | 2026-04-24 | dep+ci | Taskfile LD_LIBRARY_PATH; .gitignore; pyo3 features; setuptools-rust build req | — (partial pickup via `08fe588`) |
| 9 | `3aeee62` | 2026-04-26 | feat | `EngineState::stats()` method; `MissingParCondErr`; `write_partition(overwrite=…)` | **P1 wishlist** (overlaps claude's separate `overwrite_partition` fn; see §2.3) |
| 10 | `98fbd7f` | 2026-04-29 | dep+feat | numpy dep; `PyLazyFrame` support in chili-py | — (LazyFrame surface present on claude already? — needs verification §2.7) |
| 11 | `b20177c` | 2026-04-30 | refactor+feat | **Extract TCP listener into EngineState; add `start_tcp_listener` PyO3 binding** | **P0 wishlist** (clean pickup expected; §2.4) |
| 12 | `778cac0` | 2026-05-02 | rename | Rename `py-binding` Taskfile tasks → `py-sauce` | — (NOT pickable; tied to upstream rename we declined) |
| 13 | `2286dec` | 2026-05-02 | dep+refactor | `parking_lot` workspace dep; `lru` 0.17→0.18; refactor EngineState locks | — (touches load-bearing parse_cache and topic_map locks; risk §2.1) |
| 14 | `01c1227` | 2026-05-02 | refactor | tick_count → `Vec<i64>`; tick/get_tick_count take index param | **Wishlist helper** (cherry-pick order recommends incl. as `b20177c → 01c1227 → aa227b3 → 7948744 → 3aeee62`) |
| 15 | `aa227b3` | 2026-05-03 | feat | tick_count bounds guard (handle 0..1024); recursive `load_par_df`; multi-subscriber | **P0 wishlist** (recursive load_par_df is needed for mdata Wave-3 5-level HDB; see §2.5) |
| 16 | `7948744` | 2026-05-03 | feat+fix | **Big one:** serde9 nested-MixedList deserialization fix; `init_tick` / `publish` (DataFrame) / `subscribe`; bundled `tick.pep` / `sub.pep`; in-engine job scheduler + memory monitor | **P0 wishlist** (HEAVY conflict; see §2.6) |
| 17 | `2b7bc9c` | 2026-05-03 | dep+ci | reedline 0.45→0.47; manylinux=auto in CI | — |
| 18 | `d05bf1b` | 2026-05-03 | ci | Remove musllinux job from release workflow | — (later partially reverted by `f8b6360`) |
| 19 | `98586cf` | 2026-05-03 | dep+config | `.pytest_cache` to .gitignore; `chili-py` excluded from workspace; pyo3 feature pruning | — (conflicts the `chili-pie` retention) |
| 20 | `00cda45` | 2026-05-04 | docs | README features/installation/architecture | — |
| 21 | `d1faa65` | 2026-05-04 | docs | Script extension fix `.chi` | — |
| 22 | `f8b6360` | 2026-05-06 | ci | Add musllinux x86_64 build (partial revert of `d05bf1b`) | — |

Counts: feat × 7, refactor × 3, fix × 1 (bundled in 7948744), dep × 8, ci × 5, docs × 2, rename × 2 (some commits have multiple tags; classification above lists primary).

---

## 2. Conflict surface — claude already has divergent implementations of several wishlist surfaces

This section is the **load-bearing finding** that drives Sprint 2 planning. The
wishlist proposes a clean cherry-pick of 5 commits. Reality: claude has earlier,
divergent implementations of several of the same surfaces. The cherry-picks will
not apply cleanly.

### 2.1 Parse cache (claude has it, different commit lineage from `9b65a50`)

**On claude (`crates/chili-core/src/engine_state.rs:97`):** `parse_cache: Mutex<LruCache<(String, String), Arc<Vec<AstNode>>>>`. Cache hit ~385ns (CLAUDE.md golden rule 6). The implementation arrived on claude before the d7a748b fork OR was independently developed; CLAUDE.md treats it as load-bearing.

**On upstream main (`9b65a50`):** introduces an LRU parse cache. The shape and lock model are different from claude's — and `2286dec` then refactors locks to `parking_lot`.

**Conflict prediction:** if we cherry-pick `9b65a50`, the result will collide line-for-line with claude's existing `parse_cache` field. Resolution: skip `9b65a50` (we already have the feature). Forward direction is to cherry-pick the `parking_lot` lock refactor from `2286dec` *only*, after evaluating whether parking_lot is faster than std::sync on our hot path (must bench before merging — golden rule 6).

**Verdict on `9b65a50`:** **SKIP** (already on claude). Verdict on `2286dec`: **PICKUP-LATER** with bench gate.

### 2.2 chili-py FFI rewrite (already merged conceptually via `08fe588`)

**On claude:** commit `08fe588` (2026-04-26) titled "feat(chili-py): merge upstream FFI rewrite — direct PyDataFrame, no IPC bytes" merged the *content* of upstream's FFI rewrite (range `e9092ce..b0f20e5`, commits #1-#8) into the working tree, but did NOT cherry-pick the upstream commits themselves. CLAUDE.md project state pins this.

**On upstream main:** commits #1 (`e9092ce`) through #8 (`b0f20e5`), and partially #10 (`98fbd7f`).

**Conflict prediction:** the *content* is on claude. Cherry-picking these commits would partially duplicate-apply the same diffs and partially collide. The right path forward is `git merge --no-ff <upstream-tag>` rather than cherry-pick, OR explicitly skip these and treat them as already-handled.

**Verdict on commits #1-#8 (and parts of #10):** **SKIP** (content already on claude under `08fe588`).

### 2.3 `overwrite_partition` (claude has it as a separate fn; main folded it into write_partition)

**On claude (`crates/chili-py/src/lib.rs:520`):** `fn overwrite_partition(...)` — a separate Python-facing method. The implementation appears to predate the d7a748b fork (or arrived very early on claude).

**On upstream main (`3aeee62`):** added an `overwrite=Bool` flag to the existing `write_partition` function, AND added the `stats()` method, AND added `MissingParCondErr`.

**Conflict prediction:** the API shapes diverge. Cherry-picking `3aeee62` would give us BOTH a separate `overwrite_partition` (claude) AND `write_partition(overwrite=True)` (cherry-picked). Need to decide which is canonical. mdata calls `overwrite_partition` per the existing surface area — preserving claude's separate fn is the lower-risk path; the `stats()` + `MissingParCondErr` parts can be cherry-picked cleanly without taking the `write_partition(overwrite=…)` portion.

**Verdict on `3aeee62`:** **PICKUP-PARTIAL** — pickup `stats()` method + `MissingParCondErr` + error-handling improvements; **drop** the `write_partition(overwrite=…)` change to preserve claude's separate-function API.

### 2.4 `start_tcp_listener` (clean pickup — claude does NOT have this)

**On claude:** confirmed missing. `grep -r "start_tcp_listener" crates/` returns nothing.

**On upstream main (`b20177c`):** extracts the existing ~80-line TCP listener block from `chili-bin/src/main.rs` into `EngineState::start_tcp_listener()` and exposes it as a PyO3 binding.

**Conflict prediction:** **clean cherry-pick expected.** The TCP listener block in claude's `main.rs` is the same ~80-line shape (per the FFI-merge baseline from `08fe588`); refactoring it into `EngineState` is a code move, not a content rewrite. Conflicts only at the import boundary in `chili-bin/main.rs` and the new method in `engine_state.rs`. Should land in <1pp.

**Verdict on `b20177c`:** **PICKUP-NOW** (foundation for all wishlist work).

### 2.5 Recursive `load_par_df` + multi-subscriber + tick bounds (`aa227b3`)

**On claude (`crates/chili-core/src/engine_state.rs`):** the existing `load_par_df` (called via `Engine.load(path)`) is **2-level**: walks `path/{table}/` looking for partition FILES at the table level. Subdirectories are silently dropped (cf. `docs/proposals/load_tree_namespaced_hdb.md`). claude does NOT have a multi-subscriber model — `topic_map` stores `Vec<i64>` per topic but the broadcast logic is partial.

**On upstream main (`aa227b3`):** recursive `load_par_df` (dot-separated qualified names like `sub.trade`); multi-subscriber broadcast; HandleOutOfRangeErr for handle ∉ 0..1024.

**Conflict prediction:** **moderate.** The recursive `load_par_df` change directly conflicts with the existing 2-level walker — same function, different semantics. Resolution: take upstream's recursive version (it's a strict superset; flat path still works). Multi-subscriber overlay sits on top of the existing `topic_map`; conflicts at the subscribe/unsubscribe code paths. HandleOutOfRangeErr is additive.

**Verdict on `aa227b3`:** **PICKUP-NOW with conflict resolution**. Estimated 2-4pp to resolve cleanly.

### 2.6 The big one: `7948744` — heavy conflict with claude's existing pub/sub

**On claude:** TWO publish/subscribe models coexist in the working tree.

1. **In-process Python pub/sub** (`crates/chili-py/src/lib.rs:594-700`): `publish(topic, ipc_bytes)` returns a per-topic seq i64; `subscribe(topics, callback)` registers a Python callback. Uses an internal `py_subscribers: Arc<Mutex<HashMap<...>>>` and `topic_seq` counters. **This is claude-specific** — built for nxcar/mdata in-process pub/sub.
2. **Cross-process TCP pub/sub** (`crates/chili-core/src/engine_state.rs:1103`): `publish(table, bytes: &[Vec<u8>])` returns `()`. Iterates `topic_map` (i64 handles) and writes IPC bytes to each subscriber's TCP handle. **This is the engine-level model**, partially built.

**On upstream main (`7948744`):** introduces a THIRD model — `init_tick(schema, log_dir, date)` + `publish(table, df: DataFrame)` + bundled `tick.pep` / `sub.pep` providing `.tick.upd` (write-tplog-then-broker-publish) and `.sub.init` (replay-from-tplog then live subscribe). This is the **canonical kdb+ tickerplant** topology that mdata's wishlist explicitly requests.

**Conflict prediction:** **HEAVY.** Both `publish` methods on claude collide with the upstream version's signature (DataFrame instead of bytes). Both subscribe methods collide. The bundled `tick.pep` / `sub.pep` files don't exist on claude. The serde9 nested-MixedList fix (the `fix:` portion of the commit) is orthogonal and should land regardless.

**This is the plan-pivot finding.** The wishlist's premise is that `7948744` cherry-picks cleanly into claude. It will not. We have **three competing pub/sub models** that need to be reconciled into one.

**Recommended path forward (Sprint 2 candidate):**

1. **Phase 1 (Sprint 2):** Land `b20177c` (start_tcp_listener — clean) + the `serde9` fix portion of `7948744` (orthogonal — `crates/chili-core/src/serde9.rs`-only, the nested-MixedList offset bug fix is independent of the pub/sub work).
2. **Phase 2 (Sprint 3):** Land `aa227b3` (recursive load_par_df + multi-subscriber + bounds guard) — moderate conflict; resolve and bench.
3. **Phase 3 (Sprint 4 — the architectural pivot):** Decide the canonical pub/sub model. Options:
   - **Option (a) — merge upstream's tick/sub framework as the canonical model**, retire claude's two existing models, port mdata's in-process Python callback usage to the new shape. Highest reconciliation cost; cleanest end state; matches the wishlist's request.
   - **Option (b) — keep claude's two models, port the tplog-write durability contract from upstream as a chili-py feature on top.** Lowest reconciliation cost; preserves current chili-py callers; diverges from upstream forever.
   - **Option (c) — hybrid:** adopt upstream's `tick.pep` / `sub.pep` as the *canonical Pepper-script-level* model AND keep claude's Python `publish(ipc_bytes)` / `subscribe(callback)` as a low-level escape hatch. Complexity middle-ground; preserves both audiences.

**Recommend Option (c) with measured retirement** (user direction 2026-05-07):
adopt (c) as the *transitional* state — both models coexist temporarily — but **gate
the eventual retirement of claude's models on A/B data**, not on a unilateral
"hybrid forever" decision. The end state per `project_chili_vision` is full
main→claude merge (Sprint 8 in the roadmap), at which point one model has to win;
the A/B measurement window between Sprint 4 (hybrid landing) and Sprint 8 (merge)
is when we earn the right to retire claude-branch's pub/sub models with data, not
opinion.

**A/B comparison axes** (must be measured before retirement):

1. **Performance** — throughput (msg/s under N subscribers), latency (p50/p99 publish→delivery), GIL-release behaviour under concurrent Python callers (chili's golden rule 5 — 6.10× concurrent throughput depends on this).
2. **Compactness** — LOC delta, dep graph diff, binary-size impact of retiring claude's models vs upstream's tick/sub. Compactness is a stated chili goal per `project_chili_vision`.
3. **Efficiency** — memory footprint per-subscriber, lock contention under load, tplog write amplification.

**How to measure:**

- (i) **In-tree mock stress test.** Stand up a benchmark in `crates/chili-py/tests/bench_pub_sub_models.py` (or equivalent) that publishes N msg/s across both models, scaling N until p99 latency degrades. Run on commodity hardware to match chili's deployment story. Lands in Sprint 4 tail or as a small Sprint 5 deliverable.
- (ii) **Direct code audit.** Compare line counts, lock complexity, code paths for the two models. Cheap; produces a "static" verdict that may bias the dynamic measurement. Useful as a sanity check before standing up (i).
- (iii) **mdata production-audience feedback.** mdata is the *only* current production user of claude-branch's pub/sub. Once Sprint 4 ships the hybrid, ask mdata to dogfood both surfaces in their tp/rdb refactor and report back: which is more ergonomic to write against, which fails in surprising ways, which they'd vote to keep. **User-coordinated, not autonomous** — chili can't dispatch mdata work.

**Retirement gate** (when one model is allowed to be removed):

- Either upstream's tick/sub framework wins on ≥2 of the 3 axes (perf / compactness / efficiency) AND mdata reports no blocking ergonomic regressions vs claude's `publish(ipc_bytes)` — at which point claude's models retire ahead of Sprint 8.
- Or upstream's framework loses on ≥2 axes — at which point Option (c) becomes durable and we feed the data back to upstream as a "your tick/sub framework is missing X / Y" wishlist.
- Or the measurement is inconclusive — Sprint 8's full merge forces the call by removing the option to maintain two implementations.

The A/B measurement infrastructure itself is durable: it serves as the regression-prevention test for any future pub/sub work, and as the methodology template for similar "competing implementation" decisions (likely to recur — claude has its own divergent shape on `parse_cache`, `overwrite_partition`, and possibly more we haven't surveyed yet).

**Verdict on `7948744`:** **PICKUP-PARTIAL** in Sprint 2 (serde9 fix only); pub/sub portion is **PICKUP-AFTER-ADR** in Sprint 4+.

### 2.7 PyLazyFrame support (`98fbd7f`)

**On claude:** `chili-py/src/lib.rs` imports `pyo3_polars::PyDataFrame` extensively but `PyLazyFrame` not surveyed in this pass. Needs a quick spot-check before Sprint 2 verdict.

**Verdict on `98fbd7f`:** **PICKUP-LATER** — flag for Sprint 2 prep.

### 2.8 Stats method + MissingParCondErr (`3aeee62`)

Treated under §2.3 above (overwrite-partition discussion).

### 2.9 The chili-pie ↔ chili-sauce rename (`a0a42f6`, `778cac0`, `98586cf`)

**Decision pinned in CLAUDE.md project state:** "we stay on chili-pie because mdata/nxcar import it." But ground-truth check at `crates/chili-py/pyproject.toml` says `name = "chili"`. There's a discrepancy between CLAUDE.md and the actual pyproject.toml package name.

**Action item flagged for retro / next sprint:** verify the actual installed package name; reconcile CLAUDE.md with reality.

**Verdict on `a0a42f6` / `778cac0` / `98586cf`:** **SKIP** — upstream's rename to `chili-sauce` is not picked up regardless of the pie/chili clarification.

---

## 3. Cross-reference table — wishlist asks vs reality

| Wishlist priority | Wishlist sha | claude state | Sprint 2 readiness |
|---|---|---|---|
| **P0 #1 — TCP listener** | `b20177c` | NOT on claude | **CLEAN cherry-pick** — Sprint 2 ready |
| **P0 helper** | `01c1227` | tick_count refactor; tick(), get_tick_count() take index | **MODERATE conflict** with claude's tick handling — Sprint 2 with care |
| **P0 #3 — multi-sub + recursive load** | `aa227b3` | claude has 2-level load_par_df + partial topic_map | **MODERATE conflict** — Sprint 2 with merge work |
| **P0 #2 — tick/sub pub-sub** | `7948744` | claude has TWO competing pub/sub models | **HEAVY conflict** — Sprint 2 takes serde9 fix only; pub/sub deferred to Sprint 4+ ADR |
| **P1 — stats() + overwrite** | `3aeee62` | overwrite_partition exists as separate fn | **PICKUP-PARTIAL** — stats() clean, overwrite folded into write_partition NOT picked up |

**Ratification path for the user:**

The mdata wishlist asked for "rebuild the wheel with these 5 commits cherry-picked." Reality is "two of the five conflict heavily; one needs partial pickup; two are clean." The conservative read is that **mdata will NOT get the pub/sub framework they asked for from a Sprint 2 cherry-pick alone.** Sprint 2 lands the foundation (TCP listener + serde9 fix + recursive load_par_df + stats); pub/sub reconciliation lives in a Sprint 4+ ADR.

If mdata cannot wait that long, an alternative is **Sprint 2 = full main → claude merge** instead of cherry-picks. That is one of the strategic options in the project_chili_vision memory. It gets the wishlist surface in one shot but at the cost of a much messier merge resolution and a loss of the FFI-rewrite history we deliberately picked up. The trade-off should be a user decision.

---

## 4. Pickup verdicts at a glance

**Pickup-now (Sprint 2):**
- `b20177c` (TCP listener) — clean.
- Portions of `7948744` (serde9 nested-MixedList fix only) — clean.
- `aa227b3` (recursive load_par_df + multi-sub + bounds) — moderate, cherry-pick with merge resolution.
- Portions of `3aeee62` (stats + MissingParCondErr) — clean if we drop the write_partition(overwrite=…) part.

**Pickup-with-helper:**
- `01c1227` (tick_count refactor) — needed before `aa227b3` per wishlist's recommended order.

**Pickup-after-ADR (Sprint 4+):**
- Pub/sub portions of `7948744` — requires architectural decision on canonical model (Option a / b / c per §2.6).

**Pickup-later (Sprint 3+, with bench gate):**
- `2286dec` (parking_lot lock refactor) — must bench parse_cache hot path before adopting (golden rule 6).
- `98fbd7f` (PyLazyFrame support) — flag for Sprint 2 prep; verify need.

**Skip (already on claude or rejected):**
- `e9092ce` through `b0f20e5` (commits #1-#8) — content already merged via `08fe588`.
- `9b65a50` (LRU parse cache) — already on claude under different lineage.
- Renames `a0a42f6` / `778cac0` / `98586cf` — chili-pie retained per CLAUDE.md (with action item to verify pyproject.toml).
- CI / docs commits #6, #7, #18, #20, #21, #22 — non-load-bearing; pickup if they apply trivially, skip if they don't.

---

## 5. Risks and open questions

1. **CLAUDE.md vs pyproject.toml discrepancy on chili-pie name.** Action item.
2. **PyLazyFrame on claude.** Spot-check needed before Sprint 2.
3. **parking_lot vs std::sync on parse_cache hot path.** Bench-gated decision.
4. **Three pub/sub models reconciliation.** ADR territory. Sprint 4+.
5. **Should Sprint 2 be a cherry-pick OR a full main → claude merge?** User decision; both options have trade-offs.
6. **What does mdata actually need from `7948744`?** The wishlist names `init_tick / publish / subscribe / tick.pep / sub.pep / .tick.upd / .sub.init` as the proof-of-life surface. If chili-py's existing `publish(ipc_bytes)` could grow a `publish(df: DataFrame)` overload without retiring the bytes form, the conflict could shrink. Worth a 10-minute design discussion with mdata before Sprint 2.

---

## 6. Cross-references

- mdata wishlist: `~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`
- Companion research: `q_kdb_landscape.md`, `kdb_alternatives.md`, `shakti_analysis.md`
- Vision memory: `~/.claude/projects/-Users-oakadmin-code-chili/memory/project_chili_vision.md`
- Branch model memory: same dir, `project_chili_branch_model.md`
- CLAUDE.md project state pins (date pin, version, test count, FFI merge note): root `CLAUDE.md`
- Existing proposal docs that pre-date this inventory: `docs/proposals/load_tree_namespaced_hdb.md` (Engine.load_tree), `docs/proposals/python_bindings_comparison_and_wishlist.md` (PyEngineState vs Engine comparison)
