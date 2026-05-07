# mdata ← chili breakage report — 2026-05-07 pivot

> **HOLD: do not deliver to mdata until user ratifies + Sprint 3 starts.** mdata stays
> on the current chili wheel built from `claude-baseline-2026-05-07` tag (= claude
> branch tip pre-pivot) until Sprint 5 ships the new `claude-2` wheel. This report is
> drafted in Sprint 2 v2 Part D and is the source-of-truth for the mdata-side
> migration plan once delivered.

---

## TL;DR for mdata

1. **Chili pivoted from `claude` branch to `claude-2` branch on 2026-05-07.** `claude-2`
   is forked from upstream `main` tip (`f8b6360`); claude is parked-historical
   (tagged `claude-baseline-2026-05-07`).
2. **You stay on the existing chili wheel until further notice.** The new wheel
   (`chili 0.8.0-claude2.1` or post-naming-watch ratified) ships in Sprint 5
   (estimated 2-4 weeks from 2026-05-07 — exact date TBD per port-pace actuals).
3. **The new wheel will be NOT BACKWARD COMPATIBLE on three surfaces** (pub/sub,
   tick counter, possibly `overwrite_partition`). Migration is mdata-side
   refactor work.
4. **You will gain features** mdata's wishlist asked for (recursive `load_par_df`,
   tick/sub framework with tplog durability, multi-subscriber broadcast,
   `engine.stats()`).
5. **A/B benchmark comparison** (claude-baseline vs claude-2 binaries) happens in
   Sprint 5 wrap. mdata's production telemetry (if mdata can supply matched
   workload signatures) helps validate the retirement decision.

---

## Why we pivoted

The original Sprint 2 plan (cherry-pick `b20177c` + serde9 fix from `7948744` +
partial `3aeee62`) hit halt-criterion #1 in Part A on 2026-05-07. The first
cherry-pick (`b20177c`) produced 12 conflict regions in `crates/chili-py/src/lib.rs`
(multiple > 30 lines, largest 101) because all three planned wishlist commits
were authored against upstream's pre-FFI-rewrite chili-py shape; claude carries the
post-FFI-rewrite shape from `08fe588` (2026-04-26). All three would have hit the
same divergence cost — recurring tax, not a one-off.

Pivot direction: park `claude`, restart `claude-2` from main tip, port claude-only
features onto the new base. Strategic upside: collapses the originally-Sprint-8
"main → claude full merge" milestone into the present + eliminates recurring
cherry-pick conflict cost across the entire 12-sprint roadmap.

See `docs/standards/iteration_lessons.md` lesson 4 ("Cherry-pick conflict
accumulation — invert the merge direction") for the durable rule.

---

## What you gain on `claude-2`

These are the wishlist features mdata asked for in
`~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`. All are already on
main and inherit to claude-2 from the fork.

| Feature | Source upstream commit | Status on claude-2 |
|---|---|---|
| TCP listener extracted into `EngineState::start_tcp_listener` | `b20177c` | Already in place |
| `engine.stats()` runtime statistics | `3aeee62` | Already in place |
| `MissingParCondErr` partition error | `3aeee62` | Already in place |
| serde9 nested-MixedList deserialization fix | `7948744` (orthogonal portion) | Already in place |
| Recursive `load_par_df` (5-level HDB) | `aa227b3` | Already in place |
| Multi-subscriber broadcast (multi-handle topic_map) | `aa227b3` | Already in place |
| `HandleOutOfRangeErr` (handle ∉ [0, 1024)) | `aa227b3` | Already in place |
| `tick_count: Vec<i64>` shape with index param | `01c1227` | Already in place |
| **tick/sub framework** (`init_tick` + `publish(table, df: DataFrame)` + `tick.pep` + `sub.pep` + `.tick.upd` + `.sub.init`) | `7948744` | **Already in place — canonical pub/sub model per ADR 0001** |
| Bundled Pepper scripts for tickerplant topology | `7948744` | Already in place |

mdata's tp/rdb/archiver topology becomes natively supported once mdata refactors
to the new API surfaces.

---

## What breaks on the mdata side

The new wheel is **not backward compatible** on the following surfaces. mdata's
existing chili-importing code needs refactoring.

### Breakage 1: pub/sub API — `publish(ipc_bytes)` → `publish(table, df)`

**What changed:** Per ADR 0001 (Option a — adopt main's tick/sub canonical), claude's
in-process Python pub/sub model is **retired**. claude-2 ships only main's
tick/sub framework.

| Before (claude wheel) | After (claude-2 wheel) |
|---|---|
| `engine.publish(topic: str, ipc_bytes: bytes) -> int` (returns per-topic seq) | `engine.publish(table: str, df: polars.DataFrame) -> None` (writes to tplog + broker publishes) |
| `engine.subscribe(topics: list[str], callback: Callable) -> None` | `.sub.init` Pepper-script subscribe pattern via `sub.pep` (replay tplog + live subscribe) |
| `engine.tick_upd(table, df)` (claude-only) | `.tick.upd` Pepper API via `tick.pep` |
| `engine.broker_eod()` (claude-only) | (no direct equivalent — eod handled by tplog rotation) |

**mdata refactor scope:**
- All callers of `engine.publish(topic, ipc_bytes)` migrate to
  `engine.publish(table, df)` with DataFrame-shaped payloads.
- All callers of `engine.subscribe(topics, callback)` migrate to `.sub.init`
  Pepper-script pattern. Callbacks become Pepper-script functions, not Python.
- `engine.tick_upd` callers migrate to `.tick.upd` (similar shape; minor
  ergonomic differences).
- `engine.broker_eod` callers — refactor depends on what they did. Likely
  replaced by tplog rotation logic.

**Estimated mdata-side effort: 2-5pp** (per ADR 0001 §Risks). Depends on how
deeply pub/sub is woven into mdata's tp/rdb refactor.

**Why no in-tree shim:** ADR 0001 explicitly rejects in-tree dual implementation
("clean shape first" per user direction 2026-05-07). A/B comparison happens via
parallel binary builds (claude-baseline vs claude-2 binaries running side-by-side
under matched workloads), not in-tree.

### Breakage 2: `tick_count` API — scalar vs Vec-with-index

**What changed:** claude's scalar `tick(inc) -> i64` and `get_tick_count() -> i64`
become Vec-indexed `tick(index: usize, inc: i64) -> i64` and
`get_tick_count(index: usize) -> i64` (default index=0 supported).

| Before (claude wheel) | After (claude-2 wheel) |
|---|---|
| `engine.tick(inc=1) -> int` | `engine.tick(index=0, inc=1) -> int` |
| `engine.get_tick_count() -> int` | `engine.get_tick_count(index=0) -> int` |

**mdata refactor scope:** Trivial. If mdata uses `tick(inc)` calls without index,
they continue to work with default `index=0`. New multi-handle workflows can
take advantage of the index.

**Estimated mdata-side effort: <1pp.**

### Breakage 3 (likely, pending Sprint 3-4 verification): `overwrite_partition` separate fn

**What may change:** claude has `engine.overwrite_partition(...)` as a separate
function. main has `engine.write_partition(..., overwrite=True)` flag instead. ADR
direction (per Sprint 1 inventory §2.3): preserve claude's separate fn. **But: if
Sprint 3 port discovers that main's flag-based shape is cleaner OR that claude's
fn implementation diverges in load-bearing ways, the call may flip.**

**mdata refactor scope:** TBD. If preserved (default), zero refactor. If flipped to
main's shape, all `engine.overwrite_partition(table, df, ...)` calls migrate to
`engine.write_partition(table, df, overwrite=True, ...)`.

**Estimated mdata-side effort: 0pp (default) or <1pp (if flipped).** Sprint 3
brief confirms.

### Breakage 4 (potential): chili-py wheel name

**What may change:** Per `project_chili_naming_watch` memory, chili-py is in the
middle of a rename in upstream's mid-flight: `chili-pie` (claude branch) → `chili`
(actual claude pyproject.toml) → `chili-sauce` (upstream's chosen new name in flight)
→ `chili-source` or `chili-pie` (upstream author's stated future direction).

**Default for Sprint 5 wheel cut:** keep `chili` (matches current pyproject.toml
on both claude and claude-2). User holds the rename pending upstream's official
release notes.

**mdata refactor scope:** Zero if the name stays `chili`. If it changes,
`import chili` → `import <new_name>` everywhere in mdata.

**Estimated mdata-side effort: 0pp (default) or 0.5pp (if rename happens).**

---

## What stays the same

- `engine.eval(query)` — no signature change.
- `engine.write_partition(...)` — no signature change unless Breakage 3 flips.
- `engine.load(path)` — recursive directory walking is additive; flat layouts continue to work.
- All other surface area: parse_cache, query_plan, set_column_scale, etc.
- Int64-quantized storage (golden rule 4) — preserved on claude-2 once Sprint 3
  ports the dequantization helpers from claude.
- GIL released around `eval` (golden rule 5) — preserved on claude-2 once Sprint
  3 verifies the GIL-release pattern carries through.
- Parse cache hot-path latency (golden rule 6, ~385ns) — bench-gated in Sprint 3
  port; if main's lock model can't hit this, claude's is ported instead.

---

## Recommended migration sequence

**Phase 0 — now through Sprint 5 wrap:** mdata stays on the current chili wheel
(`claude-baseline-2026-05-07`-tag-built). No mdata-side action required.

**Phase 1 — Sprint 5 wrap (~2-4 weeks from 2026-05-07):** chili-2 wheel ships.
mdata receives:
- The new wheel.
- This breakage report (delivered then; held internal until then).
- A/B benchmark comparison results (`docs/bench/post_pivot_comparison_<date>.md`).
- A specific Sprint 6 / mdata-coordination cutover window.

**Phase 2 — mdata refactor branch:** mdata creates a refactor branch in their
codebase. Implements:
- pub/sub API migration to tick/sub framework (Breakage 1).
- tick_count index param adoption (Breakage 2).
- `overwrite_partition` adjustment if Breakage 3 flips.
- Wheel name update if Breakage 4 happens.

**Phase 3 — cutover:** mdata's refactor branch tests pass with the new wheel; they
merge + cut a new mdata release. claude-baseline-2026-05-07 wheel can be deprecated
(with grace period — exact policy TBD).

**Phase 4 — A/B feedback loop:** mdata reports any production-detected regressions
or surprises with the new pub/sub framework. If any axis (perf / compactness /
efficiency / ergonomics) shows claude-baseline measurably winning, the user can
re-open ADR 0001 with a new ADR (ADR 0002) — but the default state is "main's
tick/sub is canonical and durable."

---

## Tentative timeline

These dates are *targets*, not commitments. They calibrate against Sprint 2 v2
actuals and may shift based on Sprint 3-5 port pace.

| Sprint | Est. wrap | mdata action |
|---|---|---|
| Sprint 2 v2 (current) | 2026-05-07 (today, ~end of day) | No action |
| Sprint 3 (additive feature port wave 1) | 2026-05-14 to 2026-05-21 | No action; mdata sign-off on this breakage report |
| Sprint 4 (port wave 2 + clippy + benchmarks) | 2026-05-21 to 2026-05-28 | mdata refactor branch starts (parallel to Sprint 4) |
| Sprint 5 (bench rebaseline + wheel cut) | 2026-05-28 to 2026-06-04 | Receive wheel + breakage report; complete refactor branch |
| Sprint 6+ | TBD | Cutover + grace period for claude-baseline |

If mdata's refactor budget can't accommodate this window, escalate to user to
discuss extending the Phase 0 grace period.

---

## Open questions for mdata

To be answered when this report is delivered:

1. **What does mdata import?** Confirm: `import chili` (matches claude-2's pyproject.toml)
   or `import chili_pie` (claude's CLAUDE.md project state pre-pivot stale claim)?
2. **Does mdata use `engine.broker_eod()`?** If yes, what does the equivalent
   replacement look like in tick/sub-framework terms?
3. **Can mdata supply matched workload signatures** for the Sprint 5 A/B benchmark?
   (msg/s rate, subscriber count, payload size distribution.)
4. **Refactor budget — can mdata accommodate the Sprint 4-5 timeline?** If
   tighter, what's the constraint?
5. **Wheel-naming preference?** Default is to keep `chili`. If mdata has a strong
   preference for staying on `chili-pie` or for picking up upstream's rename, share
   it.

---

## Cross-references

- Pivot brief: `../sim/sprint_2_dispatch_brief_2026-05-07.md`
- ADR 0001 (canonical pub/sub decision): `../decisions/0001-pub-sub-canonical-model.md`
- Reverse-direction inventory: `../research/claude_only_features_inventory_2026-05-07.md`
- Sprint 1 forward inventory: `../research/main_vs_claude_inventory_2026-05-06.md`
- Pre-pivot bench baseline: `../history/bench_claude_baseline_2026-05-07/pre_pivot_state.md`
- mdata wishlist (the operational ask this report responds to):
  `~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`
- Iteration lesson driving the pivot: `../standards/iteration_lessons.md` lesson 4
  ("Cherry-pick conflict accumulation — invert the merge direction").
