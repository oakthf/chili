# Sprint 3 dispatch brief — additive feature port wave 1 + clippy unblock

**Kickoff:** TBD — gated on user-coordinated mdata sign-off on
`docs/sync/mdata_breakage_report_2026-05-07.md` AND user ratification of this brief.
**Owner:** coordinator-solo (main Claude); `code-reviewer` subagent at sprint wrap for diff review.
**Type:** implementation (first Rust ports onto claude-2; first sprint to actually move feature code, vs Sprint 2 v2's pivot+inventory paperwork).
**Predicted pp:** 10–15.
**Plan reference:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 3 row.
**Inventory reference:** [`../research/claude_only_features_inventory_2026-05-07.md`](../research/claude_only_features_inventory_2026-05-07.md) Class 3 features.
**Pivot context:** Sprint 2 v2 (claude-2 from main tip) ratified 2026-05-07; this sprint is the first port wave onto the new base.

---

## Sprint objective

Land the **8 small/trivial Class 3 features** that don't require architectural decisions, plus
**hand-port `9aa358d`'s 19 chili-core clippy lints** (Sprint 2 v2 Part A deferred this; cherry-pick failed on `engine_state.rs` divergence so manual port is the path). Begin **bench rebaseline** by running parse_cache + scan + eval + load_par_df + write_partition benches as each port lands. Pre-commit gate must be GREEN at sprint wrap (`cargo fmt --check + clippy --all-targets -- -D warnings + test --workspace --exclude chili-py`).

**Binary success criterion:**

1. Pre-commit gate GREEN end-to-end on `claude-2` after Part A (clippy unblock).
2. The 8 SMALL/TRIVIAL Class 3 features land on `claude-2`:
   - structured exception hierarchy (`ChiliError` + 6 subclasses);
   - 4 logger built-ins (`.log.{info,warn,debug,error}`);
   - engine lifecycle API (close, unload, reload, is_loaded, table_count);
   - column scale dequantization (`set_column_scale`, `clear_column_scales` — golden rule 4 preservation);
   - `overwrite_partition` separate function (preserve mdata's existing API);
   - `query_plan` introspection method;
   - mimalloc global allocator in chili-py cdylib;
   - parse_cache regression test suite — **with bench gate on golden rule 6 (≤400ns hit; user direction 2026-05-07: non-negotiable)**.
3. Bench rebaseline started: `docs/bench/post_pivot_baseline_2026-05-XX.md` records parse_cache + at-least-one-other-hot-path number on claude-2 vs `claude-baseline-2026-05-07` tag.
4. Sprint 3 retro + cadence_metrics row 3 + sprints_index update.
5. Test count delta documented (claude's claude-only tests landed where applicable).

**Out-of-scope confirmation gates** (NOT expected this sprint):
- Pub/sub re-implementation (deliberately retired per ADR 0001 Option a; mdata refactors).
- Tick_count Vec port (already on claude-2 from main).
- chili-py FFI lints port from claude's `a8d4014` chili-py portion (Sprint 4 territory).
- Wheel cut (Sprint 5).

---

## Why now

- **mdata is operationally on `claude-baseline-2026-05-07`-built wheel** until Sprint 5 ships claude-2 wheel. Sprint 3 ports the additive features that close the functional gap on claude-2.
- **Pre-commit gate is currently RED** on claude-2 due to inherited bare-main clippy lints (~19 chili-core) — Sprint 2 v2 Part A documented but didn't fix per scope discipline. Sprint 3 Part A unblocks; every sprint commit thereafter has a green gate.
- **First implementation sprint on the new base.** Calibrates implementation-pp on claude-2 (Sprint 2 v2 was research/paperwork-heavy; this is real Rust). Next port-arc sprints (4, 5) calibrate against this.
- **Bench rebaseline alongside features** per user direction 2026-05-07 ("alongside features"). Each port commit that touches a hot path runs the matching bench; the cumulative comparison vs `claude-baseline-2026-05-07` tag-built binary lands in Sprint 5 wrap doc.

---

## Scope — Part A: clippy unblock (hand-port `9aa358d`)

### A.1 Surface additions

Hand-port the 19 lints from claude's commit `9aa358d` to claude-2's bare-main code. **Cherry-pick is NOT viable** (Sprint 2 v2 Part A confirmed the conflict on `engine_state.rs` due to FFI-rewrite divergence); manual port is the path.

Lints to suppress / fix on claude-2 (verified RED per Sprint 2 v2 Part A pre_pivot_state.md):

- `needless_borrow` (multiple sites; pattern: `&value` where `value` is already borrowed)
- `too_many_arguments` (>7 args; pattern: add `#[allow(clippy::too_many_arguments)]` on the function)
- `clone_on_copy` on `Language` (which is `Copy`; pattern: drop `.clone()`)
- `unnecessary_cast` (usize → usize) (pattern: drop the cast)
- `field_reassign_with_default` (pattern: use struct literal init)
- `declare_interior_mutable_const` + `borrow_interior_mutable_const` (pattern: switch `const` to `static LazyLock`)
- `iterating on map's values` (pattern: replace `.iter()` with `.values()`)

### A.2 Implementation hints

- Use `cargo clippy --all-targets -- -D warnings 2>&1 | head -100` to enumerate lints.
- For each lint, check claude's `9aa358d` diff: `git show claude-baseline-2026-05-07 9aa358d -- <file>` shows how claude fixed it. Apply the same fix (or `#[allow]` annotation) on claude-2's file.
- Some lints in claude's `9aa358d` are on claude-only code that doesn't exist on claude-2 (e.g., parse_cache shape claude has but main has different). Skip those.
- After each batch of fixes, re-run clippy to see progress. Iterate until green.
- Iteration lesson 1 (`docs/standards/iteration_lessons.md`): if grinding cascades > 3 rounds, stop and escalate.

### A.3 Storage / schema

None.

### A.4 Tests

After Part A: `cargo test --workspace --exclude chili-py` should be GREEN (the prerequisite for all subsequent parts).

### A.5 Estimated pp

**2–3pp.** Mostly mechanical lint fixes; one round of cargo build + clippy iteration.

---

## Scope — Part B: 4 SMALL/TRIVIAL additive features

Land the additive Class 3 features from `claude_only_features_inventory_2026-05-07.md` §4 that have **no architectural decisions blocking** and **no bench-gate dependency**:

### B.1 Structured exception hierarchy (Phase 13 / WL 3.3)

**Source:** claude commit `663c9ed` (2026-04-29) — in chili-py/src/lib.rs:26-50 (exception definitions) + chili-py/src/lib.rs:52-75 (mapping function `spicy_err_to_py`) + chili-py/python/chili/__init__.py:17-24 (re-export).

**Port:**
- Add `create_exception!` macro calls for `ChiliError` (extends `RuntimeError`) + 6 subclasses (`PepperParseError`, `PepperEvalError`, `PartitionError`, `TypeMismatchError`, `NameError`, `SerializationError`).
- Add `spicy_err_to_py` mapping fn (pattern-matches on `SpicyError` variants, returns the most specific Python exception class).
- Add re-exports in `crates/chili-py/python/chili/__init__.py` (or equivalent on claude-2's layout).

**Tests:** Add Python pytest cases that verify each exception class is raised on its trigger (parse error → PepperParseError, etc.). Estimated +6 pytest cases.

**Estimated:** ~1pp.

### B.2 Logger built-ins (`.log.{info,warn,debug,error}`)

**Source:** claude `crates/chili-py/src/lib.rs:109-171` (`log_debug_fn`, `log_info_fn`, `log_warn_fn`, `log_error_fn` + `LOG_FN` static registry).

**Port:**
- Add `log_str` helper.
- Add 4 logging functions wrapping `log::debug!`, `log::info!`, `log::warn!`, `log::error!`.
- Add `LOG_FN: LazyLock<HashMap<...>>` registry registering them as Pepper built-ins (`.log.debug`, `.log.info`, etc.).
- Engine init wires up `LOG_FN` registry.

**Tests:** Add Rust unit test that asserts `.log.info("hello")` triggers a log macro at info level. Estimated +1 Rust test.

**Estimated:** ~0.5–1pp.

### B.3 Engine lifecycle API

**Source:** Per inventory §4.3 — claude has `close`, `unload`, `reload`, `is_loaded`, `table_count` methods on the Python `Engine` class.

**Port:**
- Add 5 PyO3 methods on the `Engine` struct in chili-py.
- Each method delegates to the underlying `EngineState` (close → drop the lock or signal shutdown; unload → reset `par_df_count` etc.).
- Re-export in Python wrapper class.

**Tests:** Add pytest cases per method. Estimated +5 pytest cases.

**Estimated:** ~0.5–1pp.

### B.4 mimalloc global allocator (TRIVIAL)

**Source:** claude `crates/chili-py/src/lib.rs:1-7` — `use mimalloc::MiMalloc; #[global_allocator] static GLOBAL: MiMalloc = MiMalloc;`.

**Port:**
- Add `mimalloc` crate to `crates/chili-py/Cargo.toml` dependencies.
- Add the `#[global_allocator]` declaration at the top of `crates/chili-py/src/lib.rs`.

**Tests:** None directly testable; spot-check with a manual `cargo build -p chili-py --release` and inspect binary symbols.

**Estimated:** ~0.3pp.

### Part B total: ~2.3–3.3pp.

---

## Scope — Part C: column scale + overwrite_partition + query_plan

### C.1 Column scale dequantization (golden rule 4)

**Source:** Per inventory §4.6 — claude has `set_column_scale(table, column, scale)` + `clear_column_scales` on the Python Engine. Stores per-column dequantization factors; applies them when columns are read off-disk via `engine.eval` / `load_par_df`.

**Port:**
- Find the `EngineState` field that stores column scales on claude (likely `column_scales: HashMap<(String, String), f64>` or similar).
- Add the field to claude-2's `EngineState`.
- Add `set_column_scale` + `clear_column_scales` methods (Rust + PyO3 binding).
- Wire dequantization into the read path (where Int64 columns come off Parquet, multiply by scale to produce Float64).
- **Critical:** golden rule 4 ("Storage schema is Int64-quantized for price columns") MUST be preserved end-to-end. mdata's HDB partitions are quantized; claude-2 must dequantize on read transparently.

**Tests:** Port claude's existing tests for column scaling; add at least one regression test that writes Int64-quantized data + reads via `engine.eval` + asserts Float64 dequantization happened.

**Estimated:** ~1–1.5pp.

### C.2 `overwrite_partition` separate function

**Source:** claude `crates/chili-py/src/lib.rs:520` — `fn overwrite_partition(...)` as a separate Python-facing method.

**Port:**
- Add `overwrite_partition` PyO3 binding on claude-2's chili-py.
- Implementation delegates to `chili_op::write_partition_py` (or equivalent) with the overwrite flag set.
- **Do NOT** fold into `write_partition(overwrite=True)` per inventory §2.3 (preserves mdata's existing API surface).

**Tests:** Pytest case verifying overwrite semantics (write a partition, overwrite it, verify new content).

**Estimated:** ~0.5–1pp.

### C.3 `query_plan` introspection

**Source:** claude `crates/chili-py/src/lib.rs:400-427` — Python method that returns the optimized lazy query plan as a string (equivalent to SQL EXPLAIN).

**Port:**
- Add `query_plan(query, hdb_path)` PyO3 binding.
- Internally: create a temporary lazy-mode engine, load the HDB, parse + evaluate to `LazyFrame`, return `.describe_plan()`.
- **Note (for the user's PyLazyFrame question):** this method DEPENDS on `LazyFrame` support in chili-py's FFI surface. If claude-2's chili-py can return a `LazyFrame` (via `pyo3_polars::PyLazyFrame`), this port is straightforward. If not, this port may surface that gap.

**Tests:** Pytest case verifying `engine.query_plan("select last close by symbol from ohlcv_1d")` returns a non-empty string containing expected plan keywords.

**Estimated:** ~0.5–1pp.

### Part C total: ~2–3.5pp.

---

## Scope — Part D: parse_cache regression tests + bench gate (golden rule 6)

### D.1 Surface additions

**Source:** claude has parse_cache regression tests (per inventory §4.10). Bench: `crates/chili-core/benches/parse_cache.rs` with criterion harness measuring hit latency.

**Port:**
- Port the regression test suite to `crates/chili-core/tests/parse_cache_test.rs` (or `crates/chili-core/src/engine_state.rs` test module).
- Verify `chili-core/benches/parse_cache.rs` exists on claude-2 (it does — main has it from `9b65a50`); if claude has additional bench cases, port them.
- Run `cargo bench -p chili-core --bench parse_cache`.
- **Verify hit latency ≤ 400ns (golden rule 6 invariant; user direction 2026-05-07 NON-NEGOTIABLE).** claude's reported number is ~385ns.

### D.2 Halt-and-escalate criterion (load-bearing)

If main's parse_cache hit latency on claude-2 is **> 400ns**, that is golden rule 6 violation. Two paths:

**Path 1 (preferred): port claude's parse_cache shape onto claude-2.** Claude's implementation differs in lock model + cache shape; if it's faster, port it (replacing main's). Estimated +1–2pp Part D scope.

**Path 2 (escalate): user decision on whether to relax the 385ns target.** The user said non-negotiable, so this path requires an explicit "we're going to relax this" — surface and halt.

### D.3 Bench rebaseline document

**Start:** `docs/bench/post_pivot_baseline_2026-05-XX.md` (XX is the wrap date).

Initial entry: parse_cache hit latency on claude-2 (post-port) vs claude-baseline-2026-05-07 (from `docs/history/bench_claude_baseline_2026-05-07/`) tag. Format:

| Metric | claude-baseline-2026-05-07 | claude-2 (post-Sprint-3) | Delta |
|---|---|---|---|
| parse_cache hit (ns) | ~385 | TBD | TBD |

Sprint 4 + 5 add scan, eval, load_par_df, write_partition rows.

### D.4 Estimated pp

**2–3pp.** Bench run is fast (~30s); the work is interpreting the number + the contingency for Path 1 if it fails.

---

## Scope — Part E: wrap

### E.1 Surface additions

- `docs/sim/sprint_3_retro.md` per `_retro_template.md`. Records:
  - Predicted vs actual pp.
  - Test count delta (target: +6 exceptions + 1 logger + 5 lifecycle + 1 column scale + 1 overwrite + 1 query_plan + parse_cache regression set ≈ +15-20 Python pytest + a few Rust tests).
  - Bench delta on parse_cache (and any other hot path that was touched).
  - Pp accounting per Part.
- `docs/sim/cadence_metrics.md` row 3 appended.
- `docs/sim/sprints_index.md` Sprint 3 row → "Wrapped (awaiting ratification)".
- CLAUDE.md project state line: refresh test count + version line if version changed.

### E.2 Estimated pp

**1–2pp.**

---

## Out of scope (defer)

- **Bench artifacts beyond parse_cache** (scan, eval, load_par_df, write_partition rows in `post_pivot_baseline_<date>.md`) — Sprints 4+ as those features are touched.
- **Pub/sub re-implementation** — deliberately retired per ADR 0001; mdata refactors per breakage report.
- **chili-py FFI lints port from claude's `a8d4014` chili-py portion** — Sprint 4 (FFI surface needs fuller port pass).
- **PyLazyFrame full survey** — flagged in Sprint 2 v2 inventory §7.2 as open question; if Part C surfaces a blocker, escalate; otherwise Sprint 4.
- **mdata wheel cut + delivery** — Sprint 5.
- **Larger benchmark suite (phase17 etc.)** — Sprint 4.
- **mdata collaboration artifacts** (`docs/bench/mdata-collab/...`) — Sprint 4-5.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `9aa358d` 19 chili-core lints hand-ported (gate green) | Rust port commit |
| 2 | Structured exception hierarchy ported | Rust + Python port |
| 3 | 4 logger built-ins ported | Rust port |
| 4 | Engine lifecycle API ported | Rust + Python port |
| 5 | mimalloc allocator declared | Cargo.toml + lib.rs edit |
| 6 | Column scale dequantization ported (golden rule 4 preserved) | Rust + Python port |
| 7 | `overwrite_partition` separate fn ported | Rust + Python port |
| 8 | `query_plan` introspection ported | Rust + Python port |
| 9 | Parse cache regression tests ported | Rust test port |
| 10 | parse_cache bench ≤ 400ns verified (golden rule 6) | Bench gate |
| 11 | `docs/bench/post_pivot_baseline_2026-05-XX.md` (initial entry) | new doc |
| 12 | Pre-commit gate GREEN on claude-2 | Gate state |
| 13 | `docs/sim/sprint_3_retro.md` | new (post-sprint) |
| 14 | `docs/sim/cadence_metrics.md` row 3 | edit (post-sprint) |
| 15 | `docs/sim/sprints_index.md` Sprint 3 → "Wrapped (awaiting ratification)" | edit (post-sprint) |
| 16 | `CLAUDE.md` project state refreshed (test count + version if relevant) | edit |

---

## Lead allocation

- **Coordinator-solo (main Claude)** for all 5 parts. The work is sequential (each port builds on the gate being green from Part A; bench gate at Part D depends on parse_cache code being in shape).
- **`code-reviewer` subagent at sprint wrap** for an independent diff review of the Sprint 3 commits before pre-commit gate. Single dispatch; expected to flag any obvious regression or missed invariant. Budget ~1pp.
- **No worktrees** — sequential ports on `claude-2` directly. Hard rollback per iteration lesson 1 if a port goes wrong mid-sequence.
- **SHUTDOWN_SIGNAL discipline** — same as Sprint 2 v2: check before each major part. Watchdog daemon writes signal at 5h ≥ 90%; baseline at Sprint 3 kickoff TBD.
- **mdata sign-off** is a Sprint 3 KICKOFF prerequisite — the breakage report (`docs/sync/mdata_breakage_report_2026-05-07.md`) is delivered to mdata BEFORE Sprint 3 Part A starts. mdata's response (any blocker / scope objection) returns to chili before Part A executes. User-coordinated.

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~5–7pp into the sprint, after Parts A and B land):

- Part A — clippy unblocked, gate green?
- Part B — 4 SMALL/TRIVIAL features all in place?
- Test count delta running ahead-of / behind-schedule?
- ETA to wrap.

State current 5h-pp delta + absolute % at every checkpoint and at wrap.

### Halt-and-escalate criteria

1. **Scope-blowing port complexity** — If any Part B/C feature port reveals MEDIUM/LARGE complexity that the inventory mis-estimated as SMALL, halt and rescope. Most likely candidate: column scale dequantization if the read-path integration is more invasive than expected.
2. **Plan-pivot finding (Part D parse_cache)** — main's parse_cache hits > 400ns on claude-2; golden rule 6 violation. Halt; user decides whether to (a) port claude's shape inline (+1-2pp) or (b) relax target.
3. **User-decision needed** — Surfaces an ADR-worthy question. Most likely on `overwrite_partition` if mdata's actual usage suggests folding into `write_partition(overwrite=True)` would be cleaner than preserving the separate fn.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 6pp.

---

## Wrap (per ceremony)

- Pre-commit gate GREEN on claude-2: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`. **Should run cleanly** after Part A's clippy unblock.
- Python gate: `cd crates/chili-py && uv run maturin develop && uv run pytest`. Tests added in Parts B/C should pass.
- Bench delta documented in `docs/bench/post_pivot_baseline_2026-05-XX.md` (parse_cache row at minimum).
- Test count delta documented in retro: target ~+15-20 Python pytest + a few Rust tests.
- Sprint 3 retro authored at `docs/sim/sprint_3_retro.md` per template.
- Cadence_metrics row 3 appended.
- Sprints_index updated to "Wrapped (awaiting ratification)".
- Promote any high-cost lesson to `docs/standards/iteration_lessons.md`. Likely candidate: lesson 5 (subagent context drift on superseded planning docs) — promote if Sprint 3 dispatches surface it again.
- HALT until user ratifies.

---

## Pp accounting reference

**Calibration anchors:**

- **Sprint 1** (research): 22-35 predicted, ~25 actual. Within band.
- **Sprint 2 v2 brief alone** (pivot + paperwork, post-prep): 8-14 predicted, ~13.3 actual. **Within band.** Suggests v2-style briefs calibrate well when the meta-work is excluded.
- **Sprint 2 cumulative** (incl. v1 halt + pivot prep): ~20-22 actual. Pivot sprints with cherry-pick exploration overhead need +5-7pp slack vs the brief alone.

**Sprint 3 prediction breakdown:**

| Part | Predicted |
|---|---:|
| A — clippy unblock (hand-port `9aa358d`) | 2–3 |
| B — 4 SMALL/TRIVIAL features | 2.3–3.3 |
| C — column scale + overwrite + query_plan | 2–3.5 |
| D — parse_cache tests + bench gate | 2–3 |
| E — wrap | 1–2 |
| `code-reviewer` subagent dispatch | ~1 |
| **Total** | **10.3–15.8** |

**Position in band:** mid-band ~13pp expected; if actual comes in ≤8pp, calibration says implementation sprints on claude-2 are cheaper than predicted (good signal); if actual ≥18pp, port complexity was higher than inventory estimated (calibration data point for Sprints 4-5).

**Specific risk slack:** ~1pp for Path 1 contingency (parse_cache bench gate fails → port claude's shape inline). If hit, total expands to 11–17pp.

---

## Cross-references

- **Roadmap:** `roadmap_2026-05-07.md` Sprint 3 row.
- **Inventory (port plan):** `../research/claude_only_features_inventory_2026-05-07.md` — Class 3 §4.1-4.10.
- **ADR 0001 (canonical pub/sub model):** `../decisions/0001-pub-sub-canonical-model.md` — confirms what's NOT in scope (pub/sub).
- **mdata breakage report (held until Sprint 3 kickoff):** `../sync/mdata_breakage_report_2026-05-07.md`.
- **Pre-pivot bench baseline:** `../history/bench_claude_baseline_2026-05-07/pre_pivot_state.md`.
- **Cadence rule:** `../../.claude/rules/sprint-cadence.md`.
- **Iteration lessons:** `../standards/iteration_lessons.md` — lessons 1 (hard rollback), 4 (cherry-pick conflict accumulation; informs Part A approach: hand-port not cherry-pick).
- **Shutdown protocol:** `~/.claude/rules/shutdown-protocol.md`.
- **Project memories:** `project_chili_vision`, `project_chili_branch_model` (post-pivot), `project_chili_naming_watch` (package name held at `chili` per user direction 2026-05-07).

---

## User direction inputs (2026-05-07)

The following user answers shape this brief:

1. **Package name:** keep `chili` (matches current pyproject.toml). No rename action this sprint; continue holding per `project_chili_naming_watch.md` memory.
2. **PyLazyFrame scope:** flagged as open question; will be elaborated to user separately. Sprint 3 Part C `query_plan` port may surface it; if so, escalate.
3. **ADR 0001 ratification:** confirmed (Status: Accepted in Sprint 2 v2 wrap commit `3283af8`).
4. **Parse cache 385ns invariant:** **NON-NEGOTIABLE.** Bench gate at Part D enforces ≤400ns; if main's implementation can't hit, Path 1 (port claude's shape) executes inline.
5. **Tick_count shape:** resolved by ADR 0001; claude-2 inherits main's Vec shape.
6. **Benchmark porting timing:** alongside features. Part D starts the rebaseline doc; Sprints 4-5 add rows for hot paths touched by those sprints' ports.
