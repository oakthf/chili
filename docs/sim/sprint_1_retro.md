# Sprint 1 retro — Strategic research + main↔claude inventory (research / scaffold)

**Wrap:** 2026-05-07 (~00:10 SGT)
**Predicted:** 22–35 pp (uncalibrated band; widened from initial 17–27 after ratification specified serial subagents + deeper Part D)
**Actual:** ~25 pp (5h cache moved 10% → 35%)
**Variance:** −11% (low edge of band; delivered tight)
**Owner:** coordinator-solo + 3 serial general-purpose subagents (Parts A, B, C) + main thread (Parts D, E, synthesis)
**Plan reference:** [`sprint_1_dispatch_brief_2026-05-06.md`](sprint_1_dispatch_brief_2026-05-06.md)

---

## Scope shipped

All 6 deliverables landed; no scope cut, no scope added.

- Part A — `docs/research/q_kdb_landscape.md` (500 lines) — kdb+ history, current state, dated benchmark table, sourced strengths/weaknesses (`94c3c9d`).
- Part B — `docs/research/kdb_alternatives.md` (666 lines) — competitor catalog, taxonomy, chili-fit analysis (`0650358`).
- Part C — `docs/research/shakti_analysis.md` (403 lines) — Shakti / k9 deep dive, decomposition of Shakti's edge by axis, plus back-correction to Part A (`8714599`).
- Part D — `docs/research/main_vs_claude_inventory_2026-05-06.md` (~285 lines) — every claude..main commit classified, conflict surface predicted, pickup verdicts (`7b5530c`).
- Part E — `docs/history/sim/roadmap_2026-05-06.md` (~225 lines, moved to history 2026-05-07 post-pivot) — Sprints 2–12 sequence with pp bands and gating (`ed7a02c`).
- Synthesis — `docs/research/competitive_position_2026-05-06.md` (~200 lines) — read-this-first synthesis (`ed7a02c`).

Tests: 0 (research sprint; no Rust touched).
Bench delta: 0 (no hot path changes).

---

## Lessons (durable)

### 1. API divergence silently invalidates cherry-pick plans

**Rule.** Before drafting a cherry-pick / merge plan against an upstream branch, run a per-surface diff of `claude` vs the relevant upstream commits — especially for surfaces where claude has its OWN feature implementation. If claude already has a divergent shape (different signature, different lock model, different return type), cherry-picking will produce real line-level conflicts, NOT trivial whitespace. Surface "we have surface X, upstream has surface X under different shape" as a HEAVY conflict prediction in any inventory doc.

**Why.** Sprint 1 Part D, 2026-05-07. The mdata wishlist (`~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`) requested cherry-picking 5 commits from main into claude. A surface-level read suggested clean cherry-picks. A diff-by-diff inventory revealed THREE competing pub/sub models on claude (in-process Python `publish(ipc_bytes)`, cross-process TCP `publish(handle, bytes)`, AND now upstream's `publish(table, df)` from `7948744`). The wishlist's "cherry-pick clean" premise was wrong; the right path is an ADR + multi-sprint reconciliation. If we'd gone straight to Sprint 2 cherry-picks without the inventory pass, ~10-20pp of Sprint 2 would have been spent thrashing on merge conflicts and possibly shipping a broken pub/sub state.

**Apply where.** Any future sprint that proposes cherry-picking commits or merging upstream branches into a long-lived feature branch with its own divergent work. Especially load-bearing for the Sprint 8 "main → claude full merge" milestone — that sprint's brief MUST include a per-surface API-divergence audit before scoping the merge resolution work. Also applies cross-project: any time we adopt a feature from another project (mdata ← chili, chili ← upstream), check whether the destination already has a divergent implementation of the same surface area.

**Cost saved.** ~10-20pp of Sprint 2 thrashing avoided + risk of shipping a broken pub/sub state to mdata's production-adjacent tp/rdb refactor.

### 2. After API errors during subagent dispatch, check disk before retrying — the file may already exist

**Rule.** When a subagent returns an API 500 / timeout / network error after long-running work, before treating the result as lost and retrying, verify the actual filesystem state. Subagents often write to disk before returning their final response packet; the error may be at the response stage, not the work stage. Specifically: `ls -la <expected-output-path>` and `wc -l <expected-output-path>` BEFORE redispatching with the same prompt. If the file is there with reasonable content, you've saved a full subagent re-dispatch.

**Why.** Sprint 1 Part B, 2026-05-06. The first general-purpose subagent for the kdb+ alternatives catalog returned API 500 after ~12 minutes (76 tool uses). I retried the exact same prompt, burning another ~7pp on a second subagent. The second subagent immediately found `docs/research/kdb_alternatives.md` already on disk with 666 lines of well-cited content — written by the FIRST subagent before its API error. The first subagent's work was complete; only the response packet died. Recognizing the file was there could have saved ~5-7pp of the second dispatch (only the consistency review by main thread was strictly needed).

**Apply where.** Any subagent dispatch where the agent has filesystem-write authority and can write deliverables directly. Especially load-bearing for research subagents that produce single-file outputs and may run for 5+ minutes. Generalizes to: trust filesystem state over tool-result success/failure indicators when the tool is filesystem-mutating. Inverse case (subagent dispatched to a non-writing operation, e.g. a query) doesn't apply — there's nothing to verify on disk.

**Cost saved.** ~5-7pp per occurrence (the cost of a duplicate research subagent run). Will recur every time a long subagent errors on the response packet, which appears to happen non-trivially often.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| Part A — q/kdb+ landscape | ~5 | ~7 |
| Part B — kdb+ alternatives | ~5 | ~11 (counts both dispatches; first errored on response only, second confirmed quality) |
| Part C — Shakti deep-dive | ~3 | ~4 |
| Part D — main↔claude inventory | ~8-10 | ~2 (efficient main-thread; bash diff surveys + targeted file reads only) |
| Part E — roadmap | ~3 | ~1 |
| Synthesis — competitive_position | ~2 | ~1 |
| **Total** | **22–35** | **~25–26** |

**Position-in-band:** low edge. Drove by Part D landing at one-fifth of predicted (the fanned-out diff reads turned out to be unnecessary — `git show --stat` plus targeted `grep`s on claude's tree gave the inventory in ~2pp of main thread time, no Explore subagents needed). Part B was over-predicted because the first subagent's failure looked like a complete loss; the second dispatch was nearly free once the existing file was found. Part E and Synthesis are short docs that reference rather than duplicate the deep dives — no surprise on the low side.

**Calibration notes for Sprint 2:**
- **Subagent research (general-purpose, web-fetch authority):** 5–10pp per ~500-line output doc. Variance high — depends on web-source quality.
- **Subagent error budget:** ~5pp of failed-dispatch overhead seems plausible per subagent given the 1/3 failure rate this sprint. Build it into Sprint 2's prediction.
- **Main-thread inventory work:** much cheaper than predicted when scoped to "diffstat + targeted grep, not full diff read." Sprint 2's inventory of `b20177c` / `7948744` / `aa227b3` / `01c1227` / `3aeee62` cherry-pick conflicts will be similarly cheap.
- **Synthesis docs:** ~1pp each when they reference rather than duplicate deeper docs. Don't over-budget.

---

## What surprised

- **The serial-subagent quality win was real and concrete.** Part C's primary-source read of STAC SHK211203 caught Part A's "7×" press-release artifact. A 3-in-parallel dispatch would have produced two docs that contradict each other and a synthesis pass that didn't catch the discrepancy because both were "vendor-citable." Serial execution made cross-section verification possible.
- **Subagent API errors don't always mean lost work.** First time hitting this; the disk-check workaround is now an iteration_lesson.
- **Part D was 5x cheaper than predicted.** The deep-dive plan called for 8-10pp of fan-out diff reads. Reality: `git log --pretty=format:'%h %s'` + `git show --stat` + targeted greps on claude got the same answer in ~2pp. Lesson for Sprint 2's similar inventory work.
- **`docs/proposals/load_tree_namespaced_hdb.md` is exactly the gap that `aa227b3`'s recursive `load_par_df` would close.** mdata wrote that proposal explicitly because chili lacked the recursive walker. The wishlist commits address the proposal directly. Worth flagging for Sprint 3 — when `aa227b3` lands, the proposal moves to `docs/history/proposals/`.
- **chili's parse_cache (golden rule 6) is older than upstream's `9b65a50` LRU cache commit.** The CLAUDE.md golden rule predates the upstream commit by some weeks. This is the classic "claude shipped first, upstream later shipped a different version" pattern that drives the API-divergence iteration_lesson.
- **CLAUDE.md vs `pyproject.toml` discrepancy on `chili-pie`.** CLAUDE.md says "we stay on chili-pie because mdata/nxcar import it" but `crates/chili-py/pyproject.toml` has `name = "chili"`. Captured as Sprint 2 prep action item.

---

## Cross-references

- Plan: [`sprint_1_dispatch_brief_2026-05-06.md`](sprint_1_dispatch_brief_2026-05-06.md)
- Cadence metrics row: [`cadence_metrics.md`](cadence_metrics.md) row 1
- Iteration lessons promoted: [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md) — 2 entries (API divergence + post-error disk-check)
- Sprint 2 candidate: per `../history/sim/roadmap_2026-05-06.md` (moved to history 2026-05-07 post-pivot) — "mdata-foundation" cherry-picks. The original cherry-pick plan halted at Part A; Sprint 2 v2 pivot replaced it with claude-2-from-main-tip restart per `roadmap_2026-05-07.md`.
- Synthesis (read-this-first): [`../research/competitive_position_2026-05-06.md`](../research/competitive_position_2026-05-06.md)
- Companion deep dives: `q_kdb_landscape.md`, `kdb_alternatives.md`, `shakti_analysis.md`, `main_vs_claude_inventory_2026-05-06.md`.
