# Sprint 3 retro — additive feature port wave 1 + clippy unblock

**Wrap:** 2026-05-07
**Predicted:** 10–15 pp (with up to 16.8 pp incl. Path 1 contingency)
**Actual:** ~14 pp (mid-band; no contingency triggered)
**Variance:** 0% vs midpoint (12.5)
**Owner:** coordinator-solo (main Claude) + code-reviewer subagent at wrap
**Plan reference:** [`../history/sprints/sprint_3_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_3_dispatch_brief_2026-05-07.md)

---

## Scope shipped

**Part A — clippy unblock (commit `e103c51`)**
- 21 chili-core / chili-op / chili-parser-tests clippy errors hand-ported.
- Pre-commit gate gate-state RED → GREEN on `claude-2`.
- Files touched: `engine_state.rs`, `eval_query.rs`, `parser.rs`, `serde9.rs`,
  `side_effect_fn.rs`, `stack.rs`, `utils.rs` (chili-core); `io.rs` (chili-op);
  `tests/utils.rs` (chili-parser fmt drift autofix).

**Part B — 4 SMALL/TRIVIAL features → 3 actually-needed (commit `35a16c4`)**
- B.1 exception hierarchy: **already on main** (chili-py/src/lib.rs:42-70 inherited
  from FFI rewrite). Skipped — inventory drift; ~0.3pp saved.
- B.2 logger built-ins: moved `chili-bin/src/logger.rs` → `chili-op/src/logger.rs`
  (DRY: shared between chili-bin and chili-py); registered via
  `chili_op::LOG_FN` in chili-py.
- B.3 `table_count` lifecycle method: ported (parked-claude only had `unload`
  + `table_count`; the inventory's claimed 5 methods over-spec'd).
- B.4 mimalloc global allocator: declared in chili-py + chili-bin.

**Part C — column scales + overwrite_partition + query_plan (commit `0a65873`)**
- C.1 column scale dequantization: pure-Python wrapper on `ChiliEngine` (golden rule 4 read-side helper; on-disk schema unchanged).
- C.2 `overwrite_partition`: thin wrapper delegating to `write_partitioned_df`.
  Drive-by fix: claude-2 inherited a broken `wpar` arg-order bug from main
  (passed `[df, path, table, date, ...]` instead of `[path, partition, table, df, ...]`),
  plus needed Categorical Series for `sort_columns` and date-string parsing.
- C.3 `query_plan` introspection: new PyO3 method on PyEngineState; spins up
  fresh lazy-pepper EngineState, returns `LazyFrame::describe_plan()`. GIL
  released via `py.detach`.

**Part D — parse_cache regression suite + bench gate (commit `b80ceaf`)**
- 6 regression tests ported verbatim from parked-claude tag.
- Bench measured: parse_cache hit **371.43 ns median** (golden rule 6 ≤400ns
  PASS without contingency); cold path 95.37 µs.
- New doc `docs/bench/post_pivot_baseline_2026-05-07.md` started; Sprints 4-5
  append rows.

**Part E.1 — code-review fixes (commit `b269ec0`)**
- C1 substring-fragile match → word-boundary regex `\b(?:from|join)\s+<table>\b`.
- W3 single-table break removed; multi-table joins now dequantize correctly.
- W2 `query_plan` docstring narrowed to pepper-syntax only.
- S3 hygiene: `.gitignore` covers `.claude/state/`, `.claude/settings.local.json`,
  `crates/chili-py/{Cargo,uv}.lock`.

**Tests:** +14 (6 Rust integration via `parse_cache_test.rs` + 8 Python pytest
via `test_engine.py`). Rust workspace 160 → 166. chili-py pytest 43 → 58.

**Bench delta (golden rule 6):** parse_cache hit 371.43 ns vs parked-claude
reported ~385 ns; outperforms on the same hardware. Bench rebaseline doc
captures the comparison anchor.

---

## Lessons (durable)

### 1. Verify inventory claims against current code before scope-locking the brief

**Rule.** When porting features from one branch onto a freshly-rebased branch, re-grep
the destination codebase for each claimed-missing surface BEFORE accepting the
inventory's classification. Inventories drift the moment the destination branch
moves; rebases especially absorb upstream-shipped features that look "claude-only"
relative to a stale fork point.

**Why.** Sprint 3 started from `claude_only_features_inventory_2026-05-07.md` Class 3
listing 4 SMALL/TRIVIAL features (B.1-B.4). Two checks before Part B kickoff revealed
B.1 (exception hierarchy with `ChiliError` + 6 subclasses + `spicy_error_to_pyerr`)
was already on `main` and inherited verbatim by claude-2's tip — main's FFI rewrite
had absorbed it. Similarly inventory said B.3 needed 5 lifecycle methods;
parked-claude only ever had 2 (`unload`, `table_count`), and one of those
(`unload`-equivalent) was already on claude-2 as `clear_par_df`. Net: the inventory
over-spec'd 4 methods that didn't need porting at all. Catching this in 5min of
grep saved ~1pp of doomed implementation work.

**Apply where.** Every sprint that consumes an inventory or audit doc more than
~7 days old, especially across rebases or main-side merges. The audit was
authored 2026-05-07 morning; by Sprint 3 (same afternoon) it had already drifted.
Rebases compress feature-drift latency to hours.

**Cost saved.** ~1pp implementation + ~1pp test-writing for features that didn't
need porting. Plus the value of NOT writing duplicate exception classes that would
have collided with main's existing definitions at link time.

### 2. Run a code-reviewer subagent BEFORE the retro commit, not after

**Rule.** Schedule the `code-reviewer` subagent dispatch as the LAST work item of
sprint Part E (immediately before retro authoring), not after. Its findings often
surface fixable correctness issues (substring-match fragility, single-table loop
breaks, etc.) that should land as a focused E.1 commit on the same sprint —
otherwise they leak into the next sprint as out-of-scope debt.

**Why.** Sprint 3's code-reviewer flagged 3 must-fix items in the column-scale port
(C1 substring-fragile match risking golden rule 4 false-rescaling; W3 silent
single-table loop limit; W2 query_plan docstring over-promise). All three were
sub-1pp fixes. Landing them in the same sprint as commit `b269ec0` (Part E.1)
preserves the sprint's audit trail: "Sprint 3 shipped this and code-reviewer
agreed it was correct after these specific fixes." Pushing them to Sprint 4 would
have either bled them into unrelated FFI work or caused a "we already retro'd"
hesitation.

**Apply where.** Every sprint that ports non-trivial logic across branches. Build
the cadence: implement → reviewer → fix → retro, in that order, in one continuous
session.

**Cost saved.** ~1pp deferred-fix overhead + the latent bug risk of golden rule 4
violations that mdata could have hit on a `from trades` vs `from all_trades`
collision in production. Reviewer dispatch at wrap caught what self-review missed.

---

## Pp accounting

| Part | Predicted | Actual |
|---|---:|---:|
| A — clippy unblock (hand-port 9aa358d) | 2–3 | ~2.5 |
| B — 4 SMALL/TRIVIAL features (3 needed) | 2.3–3.3 | ~2.7 |
| C — column scale + overwrite + query_plan | 2–3.5 | ~4.0 |
| D — parse_cache tests + bench gate | 2–3 | ~2.0 |
| E — wrap + retro + cadence metrics + index | 1–2 | ~1.5 |
| code-reviewer subagent dispatch | ~1 | ~0.7 |
| E.1 — review-fix follow-up commit | (not predicted) | ~0.6 |
| **Total** | **10.3–15.8** | **~14.0** |

Mid-band; ~+12% vs midpoint. Drivers of variance:

- **Part C ran ~0.5pp over** because of three drive-by surprises that were
  decided-fix-now rather than defer: the `wpar` arg-order bug, the
  `sort_columns → Categorical Series` validator gap, and the
  `date-string → datetime.date` conversion. Each fix was small individually
  (~0.15-0.2pp); the pattern is "porting a feature surfaces an upstream
  bug along the way". Sprint 4 should budget for similar drive-bys.
- **Part B ran under** because B.1 exception hierarchy was already on main
  (saved ~1pp) and B.3 was 1 method instead of 5 (saved ~0.5pp).
- **Code-reviewer + E.1 ran combined ~1.3pp** vs predicted ~1pp for review only.
  E.1 was unplanned scope absorbed in-sprint per lesson 6 above.

---

## What surprised

- **Inventory drift after just one rebase.** Sprint 1's main↔claude inventory
  was authored against fork point `d7a748b` (2026-04-13). Sprint 2 v2's reverse
  inventory was authored against the post-pivot tips (2026-05-07 morning).
  Sprint 3 started later that same day. By that point the morning's inventory
  had already drifted on B.1 (exception hierarchy) — `main` had absorbed
  parked-claude's shape via the FFI rewrite. Lesson 6 promotion captures this.

- **`wpar` arg-order on claude-2 was broken pre-port.** No pytest exercised
  `write_partitioned_df` end-to-end before this sprint, so the bug was latent.
  Exposing it via `overwrite_partition` (a thin wrapper) was the forcing
  function. Mdata likely hit this if it ever called `write_partitioned_df`,
  unless it used `engine.fn_call("wpar", ...)` directly with the canonical
  arg order.

- **parse_cache outperforms parked-claude on the same hardware.** 371.43 ns
  hit median on claude-2 vs ~385 ns reported on parked-claude. Likely a
  combination of main's `parse_cache` shape (LRU + smaller key allocations)
  and the `LazyLock` migration done in Part A removing a layer of indirection.
  Either way: contingency Path 1 was unnecessary, and golden rule 6 holds with
  comfortable margin.

- **chili-py wheel name is `chili-sauce`.** Maturin output reports
  `chili_sauce-0.8.0-cp310-...` — the upstream rename is in flight and the
  `pyproject.toml` already changed. Naming watch memory has it; held at
  `chili` install name per user direction 2026-05-07.

- **Code-reviewer caught real bugs at zero cost.** 49k tokens for the dispatch.
  Reviewer's C1 finding (substring-fragility in `_apply_column_scales`) would
  have surfaced as a production miss-rescale on a multi-table query against
  mdata's `trades` / `all_trades` schema. Reviewer's W3 (single-table break)
  is the same kind of latent bug. Both fixed in E.1.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_3_dispatch_brief_2026-05-07.md`](../history/sprints/sprint_3_dispatch_brief_2026-05-07.md) (moved post-ratification)
- **Cadence metrics row:** [`cadence_metrics.md`](cadence_metrics.md) row 3
- **Sprint 2 v2 retro (predecessor):** [`sprint_2_retro.md`](sprint_2_retro.md)
- **Bench rebaseline doc started this sprint:** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md)
- **Inventory consumed:** [`../research/claude_only_features_inventory_2026-05-07.md`](../research/claude_only_features_inventory_2026-05-07.md)
- **Promoted lessons:** [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md) (lesson 6 + lesson 7 candidates)
- **ADR 0001 (pub/sub canonical) — out of scope confirmation:** [`../decisions/0001-pub-sub-canonical-model.md`](../decisions/0001-pub-sub-canonical-model.md)
- **ADR 0002 (lazy/eager) — Sprint 4 work pinned:** [`../decisions/0002-eval-lazy-eager-default.md`](../decisions/0002-eval-lazy-eager-default.md)
