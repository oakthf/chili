# ADR-0001 — Pub/sub canonical model

**Date:** 2026-05-07 (drafted + ratified Sprint 2 v2 Part C)
**Status:** Accepted (user ratified 2026-05-07 alongside the pivot direction; this Part C entry records the formal ADR ratification on `claude-2`)
**Cutover commits:** Already in effect — claude-2 was forked from main tip on 2026-05-07 (`f8b6360`, tagged `main-pivot-2026-05-07`); main's tick/sub framework (commit `7948744`) is already on claude-2 as the only pub/sub surface. claude's models are parked-historical on `claude` branch (tagged `claude-baseline-2026-05-07`). Sprint 5 parallel-binary A/B comparison validates retirement.

---

## Context

At Sprint 2 v1 kickoff (cherry-pick plan, since superseded), `claude` carried **two
divergent pub/sub implementations** in addition to upstream `main`'s:

1. **In-process Python pub/sub** at `crates/chili-py/src/lib.rs:594-700` —
   `publish(topic, ipc_bytes)` returning per-topic seq i64; `subscribe(topics, callback)`
   registering Python callbacks via `Arc<Mutex<HashMap<...>>>`. Built for in-process
   nxcar/mdata callers.
2. **Cross-process TCP pub/sub** at `crates/chili-core/src/engine_state.rs:1103` —
   `publish(table, bytes: &[Vec<u8>])` iterating `topic_map` (i64 handles) and writing
   IPC bytes to subscriber TCP handles. Engine-level model, partial.

Upstream `main` (commit `7948744`) introduced a third model: `init_tick(schema, log_dir,
date)` + `publish(table, df: DataFrame)` + bundled `tick.pep` / `sub.pep` Pepper
scripts providing `.tick.upd` (write-tplog-then-broker-publish) and `.sub.init`
(replay-from-tplog then live subscribe). This is the canonical kdb+ tickerplant
topology.

Sprint 1 inventory (`main_vs_claude_inventory_2026-05-06.md` §2.6) flagged this as
the heaviest reconciliation surface in the codebase. Sprint 2 v1 brief deferred the
decision to a then-Sprint-4 ADR with three options:

- **(a)** Adopt upstream's tick/sub framework as canonical; retire claude's two models.
- **(b)** Keep claude's two models; port only the tplog durability contract from upstream.
- **(c)** Hybrid: upstream's `tick.pep` / `sub.pep` canonical Pepper-level + claude's
  `publish(ipc_bytes)` retained as low-level escape hatch for in-process callers.

Sprint 2 v1 was originally going to land Option (c) with measured-retirement (A/B
data gates the eventual retirement of claude's models). User direction 2026-05-07
ratified Option (c) at that point.

The 2026-05-07 pivot from cherry-pick to claude-2-from-main-tip changes the calculus:
claude-2 inherits upstream's tick/sub framework natively (it's just on `main`).
Claude's two models are now in the **claude-only-features inventory** (Sprint 2 v2
Part B) as candidates for re-implementation onto claude-2 — but only if mdata's
production needs justify the port cost.

---

## Decision

**Adopt upstream's tick/sub framework on `claude-2` as the canonical pub/sub model.
No in-tree backward-compatibility shim.**

Specifically:

1. `claude-2` ships **only** the upstream tick/sub surface: `init_tick` /
   `publish(table, df: DataFrame)` / `subscribe` / `tick.pep` / `sub.pep` /
   `.tick.upd` / `.sub.init`.
2. Claude's two pub/sub models (`publish(ipc_bytes)` and the cross-process TCP
   `publish(handle, bytes)`) go to the Sprint 2 v2 Part B inventory's
   **deliberately-retired** class **unless** Sprint 3-4 surfaces a concrete mdata
   blocker that justifies re-implementation onto claude-2.
3. **A/B comparison strategy: parallel binary builds.** Tag-based — build chili
   binary from `claude-baseline-2026-05-07` tag (claude branch tip at pivot time)
   AND from `claude-2` tip; run them in different physical or container locations
   under matched workloads; compare metrics (msg/s throughput, p50/p99
   publish→delivery latency, GIL-release behavior under N concurrent Python
   callers per chili's golden rule 5, memory/subscriber, lock contention,
   tplog write amplification). No in-tree A/B harness — independent binaries
   are simpler, more reproducible, and avoid the dual-implementation
   complexity that Option (c) would have imposed on claude-2.
4. mdata's existing callers (`engine.publish(topic, ipc_bytes)`,
   `engine.subscribe(topics, callback)`) refactor on their side per the breakage
   report (`docs/sync/mdata_breakage_report_2026-05-07.md`); claude-2 does NOT
   carry compatibility shims for their old shapes.

---

## Consequences

### Binds future work

- Sprint 3-4 ports do **not** include re-implementing claude's pub/sub by default.
  If Sprint 3 inventory + mdata's refactor experience flags a blocker that justifies
  port, that decision is **a new ADR**, not a silent decision.
- Tests for claude's pub/sub move to `docs/history/` reference status (not deleted
  on `claude` branch — that's parked-historical and immutable; deleted from
  `claude-2`'s test suite if tests existed only as integration tests for the retired
  shapes).
- mdata is a hard dependency on Sprint 3-4 timeline: their refactor must be ready
  by Sprint 5 wheel cut OR they stay on `claude-baseline-2026-05-07` wheel
  indefinitely. Sprint 3 brief confirms mdata's refactor branch is ready before
  starting; Sprint 5 brief gates wheel ship on mdata sign-off.

### Excludes

- **No in-tree dual-implementation.** Option (c) with measured-retirement (the
  pre-pivot direction) is **not adopted** in claude-2. The A/B measurement work
  shifts from in-tree (Option (c)'s Sprint 4.5 measurement-sprint) to parallel
  binary comparison (Sprints 3-5 as ports complete).
- **No silent retention of claude's pub/sub on claude-2.** If claude's models
  appear on claude-2 it must be via an explicit Sprint 3-4 port commit cited from
  this ADR's "binds future work" exception clause.

### Risks

1. **mdata refactor cost is mdata-side.** This ADR pushes complexity onto mdata.
   Mitigation: the breakage report is detailed; mdata stays on `claude-baseline-
   2026-05-07` wheel until they're ready to refactor.
2. **Some upstream tick/sub semantics may not match claude's pub/sub semantics
   1:1.** Likely surprises around: tplog write amplification (upstream's model
   writes tplog before broker publish; claude's in-process model didn't), GIL
   behavior (claude golden rule 5: 6.10× concurrent throughput depends on GIL
   release; upstream's tick/sub may or may not preserve this — verify in Sprint 3).
3. **A/B comparison may reveal upstream's tick/sub is slower** on chili-specific
   workloads. Mitigation: if claude's `publish(ipc_bytes)` measurably wins on a
   metric mdata cares about, re-open via a new ADR; that's the intended
   "retirement gate" preserved from the pre-pivot Option (c) plan, just executed
   via parallel binaries instead of in-tree dual implementation.

---

## Alternatives considered

- **Option (a) — adopt upstream's tick/sub; retire claude's models — chosen
  (this ADR).** Cleanest end state; aligns with vision (`project_chili_vision`
  memory: kdb+ replacement; tick/sub is the canonical kdb+ tickerplant
  topology).
- **Option (b) — keep claude's two models; port tplog durability from upstream as
  additive — rejected.** Diverges from upstream forever; recurring sync cost;
  doesn't match the kdb+-replacement vision.
- **Option (c) — hybrid (upstream Pepper-level + claude Python-level escape hatch)
  with measured-retirement — rejected as durable; collapsed into Option (a) by
  the 2026-05-07 pivot.** The pivot eliminated the cherry-pick path that made
  claude's models "free to keep alongside upstream's" — once claude-2 is forked
  from main tip, retaining claude's models requires explicit re-implementation
  effort. The user's direction at pivot time was "I don't need mdata code to keep
  working for now; I want a clean shape done here first" — which selects (a) over
  (c) at the architectural fork.
- **Defer ADR to Sprint 4 — rejected.** Sprint 2 v2 Part B inventory needs ADR 0001
  to classify claude's pub/sub features (deliberately-retired vs claude-only-needs-port);
  the inventory is a Sprint 2 deliverable; therefore the ADR must land in Sprint 2.

---

## Cross-references

- **Sprint 2 v2 brief:** `../sim/sprint_2_dispatch_brief_2026-05-07.md` Part C.
- **Sprint 1 inventory (forward direction):** `../research/main_vs_claude_inventory_2026-05-06.md` §2.6.
- **Sprint 2 v2 inventory (reverse direction):** `../research/claude_only_features_inventory_2026-05-07.md` (built Sprint 2 v2 Part B).
- **Iteration lesson driving the pivot:** `../standards/iteration_lessons.md` "Cherry-pick conflict accumulation — invert the merge direction."
- **Strategic frame:** `../research/competitive_position_2026-05-06.md`, `../research/shakti_analysis.md` §4.2 (pub/sub layer doesn't move the perf needle for the kdb+-replacement vision — confirms Option (a) is low-strategic-cost).
- **Mdata breakage report:** `../sync/mdata_breakage_report_2026-05-07.md` (held internal until Sprint 3 starts).
- **Project memories:** `project_chili_vision`, `project_chili_branch_model` (updated in Sprint 2 v2 Part E for the pivot).
- **Sprint 17 follow-up (2026-05-14):** `signal_eod` was rewritten to broadcast via `write_chili_ipc_msg(rw, &bytes, MessageType::Async)` rather than `sync()`, matching the existing `EngineState::publish` broker-upd path. The previous `sync()` route failed silently with `EvalErr("cannot sync for Publishing handle")` because `sync()`'s conn_type match has no `Publishing` arm — `signal_eod`'s Sync semantics were incorrect for a 1→N broadcast anyway. No semantic change to ADR 0001's canonical model; just a correctness fix that makes subscriber-side `eod` dispatch actually fire. See Sprint 17 retro for the localization trace + audit appendix C1 (H6 not in original hypothesis space).
