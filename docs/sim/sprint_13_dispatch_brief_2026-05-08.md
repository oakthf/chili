# Sprint 13 dispatch brief — `load_par_df` hot path optimization (P1.1, P1.3, A.2.6, A.2.8, A.2.3)

**Kickoff:** 2026-05-08 — user-ratified post end-of-roadmap perf-proposal audit (commit `72d5e24`) + sequencing prioritization session.
**Owner:** coordinator-solo (main Claude); code-reviewer subagent at wrap (per lesson 7).
**Type:** implementation (perf-pass on the load path; bench-gated).
**Predicted pp:** 9–13.
**Plan reference:** `docs/proposals/perf_alternatives_post_mimalloc_2026-05-08.md` §A.4 ("Revised sequencing — Sprint A") + Sprint 12 P2 partial symbolization findings in `docs/bench/post_pivot_baseline_2026-05-07.md`.
**ADR references:** none new. Existing constraints from ADR 0003 (polars py-1.39.3 fork) + golden rule 6 (parse_cache hit ≤ 400 ns) bind the scope.

---

## Sprint objective

Recover at least half of the +22.8% `load_multitable_5x200p` regression observed Sprint 7 Part B (claude-2 vs `claude-baseline-2026-05-07` tag) by attacking the chili-side allocation patterns the Sprint 12 P2 profile attributed to 17.7% main-thread `Box::new` cost. **Binary success criterion:** `load_multitable_5x200p ≤ 1.65 ms` after all changes stack, with `parse_cache hit ≤ 400 ns` (golden rule 6) holding throughout AND `tests/bench_concurrent.py` showing no concurrent-throughput regression.

---

## Why now

- Sprint 12 closed the original 12-sprint roadmap; Sprint 13 is the first roadmap-scoped successor and is the highest-confidence chili-side perf opportunity in the post-mimalloc-fix backlog.
- Evidence is already in hand — Sprint 9 captured the symbolized chili-side profile, Sprint 12 attributed costs. No new profiling infrastructure needed before starting.
- All 5 items in scope have **zero blockers** — no user-driven P0, no `/tmp/` volatility, no upstream version dependencies.
- Sprint A in the audited sequence (`docs/proposals/perf_alternatives_post_mimalloc_2026-05-08.md` §A.4) is explicitly the no-blocker starter; Sprint B (concurrent throughput) and Sprint C (PGO) are gated on Sprint A's bench result + user P0 respectively.

---

## Scope — Part A: parallel-build hot path (P1.1 + P1.3 + A.2.6 + A.2.8)

### A.1 Surface additions

No new public API. All four changes are internal optimizations of `crates/chili-op/src/io.rs::load_par_df` and adjacent helpers in `crates/chili-core/src/engine_state.rs`.

- **P1.1 (batch schema reads).** Currently each table reads its polars LazyFrame schema independently in `build_par_df_entry`. Batch the schema fetch across all tables in a single multi-table read pass before the per-table parallel build kicks off; pass the pre-built schemas in.
- **P1.3 (qualified-name interning).** Pre-compute qualified-name strings outside the polars LazyFrame setup hot path and pass them in as `&str` references rather than constructing inside the parallel-build closure.
- **A.2.6 (`build_qualified_name` allocations).** `crates/chili-core/src/engine_state.rs:1561-1568` allocates `Vec<&str>` then `.join(".")` on every call. For typical nesting depth (≤ 4 segments), `format!` with explicit segment count or `SmallVec<[&str; 4]>` avoids the heap allocation entirely.
- **A.2.8 (`dir_has_partition_files` redundant traversals).** `engine_state.rs:1574-1583` opens and reads the directory listing on every call. Called per qualified-name build → multi-table loads multiply the syscall count. Batch the dir-read result in Phase 1a (the dir-discovery phase) and pass in as a `&HashMap<PathBuf, bool>` lookup.

### A.2 Implementation hints

- The Sprint 12 partial profile (in `docs/bench/post_pivot_baseline_2026-05-07.md`) attributes 17.7% main-thread cost to `alloc::boxed::Box<T>::new` across two inline sites in the polars LazyFrame setup path. P1.1 + P1.3 + A.2.6 + A.2.8 together target the chili-side allocation pressure; the polars-internal `0x450c` kernel (93.1% of worker time) is NOT addressed by this sprint and would require Sprint C's symbolized polars rebuild to attack.
- Mirror the parse_cache hot-path conventions in `crates/chili-core/src/parse_cache.rs` — the load path is similarly hot, similar cache discipline applies. **Do not introduce locks on the hot path.**
- `SmallVec` requires adding the dep; `format!` doesn't. Default to `format!` for A.2.6 unless a microbench shows allocation pressure remains.
- Phase 1a in `load_par_df` is where the dir walk happens; Phase 1b is the parallel build. A.2.8 batches at Phase 1a → Phase 1b passes pre-computed lookup. P1.3 batches at Phase 1a → Phase 1b passes pre-built strings. Same architectural shape; coordinate the API change to take both at once.

### A.3 Storage / schema

No on-disk format changes. The Int64-quantized price-column convention (golden rule 4) is unaffected. mdata storage layer coordination not needed.

### A.4 Tests

- Existing `crates/chili-op/tests/` integration tests must continue passing (no behavioral change expected).
- Existing `crates/chili-py/tests/` pytest must continue passing (65/65 baseline).
- Add Rust integration test for A.2.6 verifying `build_qualified_name` output is identical pre/post-change across a few representative inputs (empty prefix, 1-segment, 4-segment, deeper). Pure correctness preservation.
- A.2.8 batched form must produce same `dir_has_partition_files` boolean as the per-call form for every directory in the test corpus.

---

## Scope — Part B: Polars feature-flag audit (A.2.3)

### B.1 Surface additions

- Audit `Cargo.toml` workspace polars feature flags + `crates/chili-op/Cargo.toml` chili-op-side polars features.
- Verify `simd` is enabled (might be in `default-features = true`; check the polars `default = [...]` list at `/tmp/polars-py-1.39.3/crates/polars/Cargo.toml`).
- Verify `streaming` enablement state.
- If either is OFF and not deliberately disabled: enable, rebuild, re-bench.

### B.2 Implementation hints

- Compile-time only. No code change.
- If a feature change forces a release-profile rebuild, lesson 8 (predict bench-related sprint cost in full release-profile compile time) applies — budget ~5+ min wall for the chili-py wheel rebuild alone.
- `lesson 11` (uv sync triggered by pyproject change) does NOT apply since this is workspace `Cargo.toml` only, not `crates/chili-py/pyproject.toml`.

### B.3 Tests

- No new tests. The Part C bench A/B sweep validates whether the feature flag change moved the numbers.

---

## Scope — Part C: Bench A/B sweep + reviewer + retro

### C.1 Bench A/B

- Run `cargo bench -p chili-op --bench load_par_df` (focus: `load_multitable_5x200p` + `load_multitable_5x100p` + `load_singletable`) and `cargo bench -p chili-core --bench parse_cache` BEFORE Part A starts (capture baseline at HEAD `f1ccc8f`).
- Apply Part A + Part B changes incrementally; re-run benches after each cluster (P1.1+P1.3 first; then A.2.6+A.2.8; then A.2.3 feature flags).
- Document per-step delta in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 13 section.
- If `parse_cache hit > 400 ns` at any step: HALT, re-measure (lesson 15: ±10% target re-measure). If still over 400ns after 3 runs, revert the offending change and continue with remaining items.

### C.2 Code-reviewer dispatch

Per lesson 7, dispatch `code-reviewer` subagent at the end of Part A + B before authoring retro. Findings absorbed in-sprint as a focused Part C.1 commit (or noted as candidate lessons if not material).

### C.3 Retro + cadence row + sprints_index

- `docs/sim/sprint_13_retro.md` — predicted/actual/variance, what surprised, lessons (durable + candidate), pp accounting per sub-priority.
- `docs/sim/cadence_metrics.md` — append row 13.
- `docs/sim/sprints_index.md` — flip Sprint 13 row to Ratified after user signs off retro.

---

## Out of scope (defer)

| Item | Reason |
|---|---|
| **P1.2 (pre-allocate Box arenas)** | Sprint 13 first; if regression isn't recovered, P1.2 enters Sprint 14 backlog. Re-evaluate after Sprint 13's bench result. |
| **A.2.7 (CSV/JSON write DataFrame clone)** | Different code path (write, not load). Could fold opportunistically if budget allows in Part A; otherwise standalone Sprint 14 candidate. |
| **P3.2b (load_par_df GIL release)** | Sprint 14 (concurrent throughput sprint) per audited sequence. Distinct concern (concurrency), distinct bench shape (`tests/bench_concurrent.py`). |
| **P3.4 (Categorical mapping cache)** | Sprint 14 with ADR. Semantic surface change; out of scope here. |
| **Polars-internal kernel optimization** (`0x450c` 93.1% of worker time, `0x4834` 26.7% of main) | Sprint C in audited sequence; blocked on user P0 (GitHub-host the polars fork) for the symbolized rebuild. |
| **Result/DataFrame caching, lazy plan caching** | ADR territory; deferred indefinitely until profile evidence justifies. |

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-op/src/io.rs` — `load_par_df` parallel-build refactor (P1.1 + P1.3 + A.2.8 lookup integration) | edit |
| 2 | `crates/chili-core/src/engine_state.rs` — `build_qualified_name` (A.2.6) + `dir_has_partition_files` batched form (A.2.8) | edit |
| 3 | `Cargo.toml` (workspace) and/or `crates/chili-op/Cargo.toml` — polars feature-flag tuning (A.2.3, contingent on audit result) | edit (or no-op) |
| 4 | `crates/chili-op/tests/` — `build_qualified_name` correctness preservation test | new |
| 5 | `crates/chili-op/tests/` — `dir_has_partition_files` batched-form preservation test | new |
| 6 | `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 13 section with per-step bench A/B numbers | edit |
| 7 | `docs/sim/sprint_13_retro.md` | new (post-sprint) |
| 8 | `docs/sim/cadence_metrics.md` — row 13 | edit (post-sprint) |
| 9 | `docs/sim/sprints_index.md` — Sprint 13 row | edit |

---

## Lead allocation

**Coordinator-solo** for Parts A, B, and C.1 (bench A/B). Implementation is mechanical refactor with bench validation; no parallel subagent dispatch needed for scope this concentrated.

**`code-reviewer` subagent** at end of Part A + B before retro (Part C.2). Standard ~2-3pp dispatch for findings absorption per lesson 7.

No worktree (not running parallel sprints; Sprint 14 doesn't kick off until Sprint 13 retro is ratified).

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~5–6pp), post a status:

- Did P1.1 + P1.3 land cleanly? Did Part A's intermediate bench show movement on `load_multitable_5x200p`?
- Is parse_cache hit still ≤ 400 ns? (Golden rule 6 must hold throughout.)
- Did A.2.6 + A.2.8 stack cleanly with P1.1 + P1.3, or did one revert another?
- ETA to wrap.

Halt-and-escalate criteria:

1. **Scope-blowing bug** — if any change breaks an existing test (Rust or pytest) and root-cause exceeds 2pp to fix, halt.
2. **Plan-pivot finding** — if profile evidence shifts (e.g., re-running the bench shows the +22.8% regression isn't there anymore — possibly already self-resolved by post-Sprint-7 polars-fork re-pin), halt and surface to user.
3. **User-decision needed** — if A.2.3 audit reveals a polars feature flag change that conflicts with mdata's expected polars feature set (cross-project surface), halt for ratification.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 8pp, halt per `~/.claude-team/rules/work-metrics.md`.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- Python-bindings wrap: `cd crates/chili-py && uv run maturin develop && uv run pytest`.
- Bench delta documented in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 13 section: per-step deltas + cumulative recovery vs Sprint 7 Part B baseline.
- Test-count delta documented (expect +1–2 Rust tests; chili-py pytest unchanged).
- Author retro at `docs/sim/sprint_13_retro.md`.
- Append row 13 to `docs/sim/cadence_metrics.md`.
- Move dispatch brief to `docs/history/sprints/` post-ratification.
- HALT until user ratifies retro.

---

## Pp accounting reference

Closest historical comparable sprints from `docs/sim/cadence_metrics.md`:

- **Sprint 3** (additive feature port wave 1, 7 features + parse_cache bench gate) — predicted 10–15, actual ~14. Comparable shape: multi-item implementation sprint with bench gate + reviewer dispatch. Sprint 13 is smaller (5 items, all in one code path) but introduces bench A/B overhead.
- **Sprint 8** (perf-pass-1) — predicted 6–12, actual ~4. Comparable but came in WAY under (P1 was thermal noise; lesson 15). Sprint 13 has clearer scope (5 concrete items vs Sprint 8's "investigate") so won't follow that under-shoot.
- **Sprint 12** (perf-pass-3 + Iceberg eval) — predicted 6–12, actual ~3. Research-shaped; not comparable to Sprint 13's implementation shape.

Sprint 13 expected at the **mid-band** (10–11pp), capped above by:
- Polars feature-flag audit triggering an unexpected release-profile rebuild (lesson 8: ~5+ min wall, ~3pp tokens).
- A.2.8 batched-form refactor turning out to require deeper architectural changes than the audit anticipated (audit estimate was ~1–2pp; if it's actually ~3–4pp, Sprint 13 lands at the high end).

Capped below by: P1.1 + P1.3 turning out to already be dominated by polars-internal kernels (in which case the chili-side 17.7% Box::new attribution recovers less than expected, and Sprint 13 ships smaller wins).

---

## Cross-references

- Audited proposal: `docs/proposals/perf_alternatives_post_mimalloc_2026-05-08.md` §A.4 (recommended Sprint A scope).
- Sprint 12 P2 partial symbolization: `docs/bench/post_pivot_baseline_2026-05-07.md` (Box::new attribution evidence).
- Sprint 7 Part B bench A/B: same doc (the +22.8% regression the recovery target is calibrated against).
- mdata delivery doc: `docs/sync/mdata_chili_2026-05-08_delivery.md` (notes 0.8.2 wheel; Sprint 13 may produce a 0.8.3 wheel if perf-recovered, or no wheel if no user-visible change ships).
- Iteration lessons referenced: lesson 7 (reviewer-before-retro), lesson 8 (bench-related sprint compile cost), lesson 15 (re-measure ±10% target), lesson 17 (macOS samply autonomous-run profiling).
- Cross-project: none. mdata is on 0.8.2 (no segfault; standing on the post-mimalloc-fix wheel). Sprint 13 doesn't change the FFI surface, only internal performance.
