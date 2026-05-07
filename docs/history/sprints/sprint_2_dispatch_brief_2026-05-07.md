# Sprint 2 dispatch brief — claude-2 baseline + features inventory (PIVOT)

**Kickoff:** TBD — pending user ratification of this brief
**Owner:** coordinator-solo (main Claude); `Explore` subagent for Part B reverse-direction inventory
**Type:** pivot + research-heavy (1 git op, lots of audit/doc work; no Rust ports yet — Sprints 3+ do that)
**Predicted pp:** 8–14
**Plan reference:** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) (rewritten this sprint)
**ADR references:** ADR `0001-pub-sub-canonical-model.md` is authored in Part C of this sprint
**Supersedes:** [`../history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md`](../history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md) — the cherry-pick plan halted at Part A on 2026-05-07 (see iteration lesson "Cherry-pick conflict accumulation — invert the merge direction" in `docs/standards/iteration_lessons.md`)

---

## Sprint objective

Restart chili's working branch from upstream `main` tip as `claude-2`; produce an
authoritative reverse-direction inventory of all `claude`-only features that need to be
ported; ratify the canonical pub/sub model decision via ADR 0001; produce the mdata
breakage report (held internal until Sprint 3 mdata sign-off). **No Rust ports happen
this sprint** — the port work is scoped into Sprints 3-4.

**Binary success criterion:**

1. `claude-2` branch exists, forked from `main` tip; the existing pre-commit gate
   (`cargo fmt --check + clippy + test + uv run pytest from chili-py`) is run on bare
   `main` and the result is documented (green or red — both are acceptable; red on
   bare main becomes Sprint 3 input).
2. `claude` branch tip tagged `claude-baseline-2026-05-07` for reproducible historical
   binary builds (used in A/B comparison against claude-2 in Sprints 3-4).
3. `docs/research/claude_only_features_inventory_2026-05-07.md` enumerates every
   feature on `claude` that isn't on bare `main`, classified per the user's policy:
   **already-on-main**, **shape-divergent**, **claude-only-needs-port**,
   **deliberately-retired**.
4. ADR `docs/decisions/0001-pub-sub-canonical-model.md` ratified by user — the call
   per user direction 2026-05-07 is **adopt main's tick/sub framework as canonical;
   no in-tree shim**; A/B comparison done by parallel binary builds, not by dual
   implementations in claude-2.
5. `docs/sync/mdata_breakage_report_2026-05-07.md` complete; **NOT broadcast to
   mdata** until user ratifies and Sprint 3 starts.
6. New roadmap (`docs/sim/roadmap_2026-05-07.md`) replaces the cherry-pick arc with
   the port arc; original Sprint 8 (full merge) is deleted (accomplished structurally).
7. Sprint 2 retro + cadence_metrics row 2 + sprints_index update.
8. CLAUDE.md updated to reflect post-pivot reality: working branch is now `claude-2`;
   `claude` is parked-historical (immutable, reference only).

---

## Why now

The original Sprint 2 brief (cherry-pick `b20177c` + serde9 from `7948744` + partial
`3aeee62`) hit halt-criterion #1 in Part A on 2026-05-07. Cherry-pick of `b20177c`
produced 12 conflict regions in `crates/chili-py/src/lib.rs`, multiple > 30 lines,
largest 101 lines.

**Root cause:** all three planned wishlist commits were authored against upstream's
*pre-FFI-rewrite* chili-py shape; claude's `chili-py/src/lib.rs` is the
*post-FFI-rewrite* shape (merged from upstream into claude via `08fe588` on
2026-04-26). So all three cherry-picks would have hit the same divergence cost —
not a one-off conflict but a recurring tax on the cherry-pick approach.

**User-directed pivot (2026-05-07):**
- Park `claude` branch as historical reference (tagged + immutable; never deleted).
- Restart `claude-2` from `main` tip (claude-2 inherits all upstream features for free).
- Walk `claude` to compile claude-only features list; cross-check against claude-2.
- Re-implement claude-only features on top of claude-2 codebase (full re-implementation,
  not cherry-pick — the divergence makes cherry-pick infeasible).
- Produce mdata breakage report; mdata adapts code-side later (no backward-compat shim
  in claude-2 itself — clean shape first; mdata's `publish(ipc_bytes)` callers refactor
  to upstream's `publish(table, df)` shape on their side).
- A/B comparison done independently: build binaries from both branches, run parallel
  in different locations, compare metrics — no dual implementation in claude-2.

**Strategic upside:** this collapses the originally-Sprint-8 "main → claude full merge"
milestone into the present and eliminates recurring cherry-pick conflict cost from the
entire 12-sprint roadmap forward. Net pp savings vs the cherry-pick path: ~10-30pp
(see `docs/standards/iteration_lessons.md` "Cherry-pick conflict accumulation" entry
for the cost analysis).

---

## Scope — Part A: backup + branch creation + env fix + planning-doc copy

### A.1 Surface additions

**Tagging + branching:**
- `git tag claude-baseline-2026-05-07 claude` — immutable reference for historical
  binary build. Never deleted.
- `git tag main-pivot-2026-05-07 main` — immutable reference for the exact `main` tip
  at pivot time (provides reproducibility for "we forked at this commit").
- `git checkout main` followed by `git checkout -b claude-2` — creates the new
  working branch from current `main` tip.

**Planning + research doc copy (claude → claude-2).** claude-2 inherits the full
sprint-planning + research baseline from claude (claude is parked-historical; future
planning happens on claude-2). Use `git checkout claude -- <path>` for each:
- `docs/sim/` (briefs, retros, templates, cadence_metrics, sprints_index, roadmap_2026-05-07.md)
- `docs/research/` (Sprint 1 outputs — landscape, alternatives, Shakti, inventories, synthesis)
- `docs/decisions/` (README + ADR 0001)
- `docs/standards/` (iteration_lessons.md with all 4 entries)
- `docs/sync/` (decisions-needed, ideas)
- `docs/history/` (frozen historical docs including the superseded Sprint 2 v1 brief)
- `docs/bench/mdata-collab/` if not present on main
- `.claude/rules/` (sprint-cadence)
- `CLAUDE.md` — copied from claude THEN immediately rewrite branch policy + pre-commit
  gate sections to reflect post-pivot reality (claude-2 is working branch; gate uses
  `cargo test --workspace --exclude chili-py`); preserve all other sections.

**Bench backup (on claude-2):** create `docs/history/bench_claude_baseline_2026-05-07/`
and copy in all claude bench artifacts:
- `docs/bench/baseline.md`, `summary.md`, `phase{1..7,9}.md`
- `docs/bench/phase17_*.py` (3 files)
- `docs/bench/mdata-collab/` (whole subdir, if it stays at the original path on
  claude-2 it can stay; otherwise duplicate here)
- A `pre_pivot_state.md` snapshot — claude tip SHA, the two new tags' SHAs, test count,
  key bench numbers (parse_cache hit ns, GIL-released eval throughput, etc.)

**Environment fix (on claude-2; closes the env diagnostic from Sprint 2 v2 prep):**
- `.gitignore` append `.cargo/config.toml` (machine-specific paths must not commit).
- `.cargo/config.toml.example` (committed) — template with `<UV_PYTHON_PATH>`
  placeholders for `PYO3_PYTHON` + `DYLD_FALLBACK_LIBRARY_PATH`, plus a comment
  explaining the workspace feature unification footgun.
- `docs/dev_setup.md` (committed) — install uv, `uv python install 3.12`, copy
  `.cargo/config.toml.example` → `.cargo/config.toml`, replace placeholders; explain
  why `cargo test --workspace --exclude chili-py` is the working gate (chili-py's
  `extension-module` feature unifies into pyo3 across the workspace and disables
  `-lpython` linker flags); pytest from chili-py runs separately.
- `CLAUDE.md` Pre-commit gate section — change `cargo test` to
  `cargo test --workspace --exclude chili-py`; add 1-line pointer to `docs/dev_setup.md`.
- `Taskfile.yml` `test:` task — `cargo test --workspace --exclude chili-py` (matches
  CLAUDE.md gate).

**Pre-commit gate run on claude-2:**
- After config landing: `cargo fmt --check + clippy --all-targets -- -D warnings +
  test --workspace --exclude chili-py` should be GREEN (env fix in place).
- Plus `cd crates/chili-py && uv run maturin develop && uv run pytest` for the
  Python side.
- Document the outcome in `pre_pivot_state.md`. If anything else fails, **document
  but don't fix** — failures become Sprint 3 input.

### A.2 Implementation hints

- Verify the `main` branch exists locally before checkout: `git branch -a | grep main`.
- If `cargo test` fails on bare main (likely on some surface — main has been moving),
  capture the failures verbatim into `docs/history/bench_claude_baseline_2026-05-07/main_baseline_state.md`
  for Sprint 3 reference.
- Set `git config user.email "claude-code@chili.local"` is already the project default
  per identity drift fix (2026-05-07); confirm before first commit on claude-2.
- The `claude` branch retains the `claude` name; we do NOT rename it. The new branch
  is literally named `claude-2`.

### A.3 Storage / schema

None. Branch creation only.

### A.4 Tests

- Document test count on bare main (vs claude's 209: 165 Rust + 44 Python).
- Document bench numbers on bare main where the bench infrastructure lands cleanly.
- These numbers become inputs to Sprint 3's port priorities.

### A.5 Estimated pp

**2-3pp.** Mostly git ops + doc copy + env-fix paperwork. The env diagnostic itself
was already done during Sprint 2 v2 prep (the `.cargo/config.toml [env]` +
`--exclude chili-py` finding); Part A just productionizes it. The pre-commit gate
run on claude-2 should now be GREEN end-to-end.

---

## Scope — Part B: claude-only features inventory (reverse direction)

### B.1 Surface additions

New file: `docs/research/claude_only_features_inventory_2026-05-07.md` (the
*reverse-direction* counterpart of Sprint 1's `main_vs_claude_inventory_2026-05-06.md`,
which surveyed main → claude pickup needs).

For every feature/surface on `claude` since fork point `d7a748b`, classify against
`claude-2` (which is now at main tip):

| Class | Meaning | User policy |
|---|---|---|
| **already-on-main** | claude-2 has this feature natively (e.g. `08fe588` FFI rewrite came FROM main, so claude-2 inherits it free) | adapt naturally to claude-2's version; no port work |
| **shape-divergent** | both have it but with different signature/lock model/return type (e.g. `parse_cache`, `overwrite_partition` vs `write_partition(overwrite=…)`) | use claude-2's shape; mark claude's shape for A/B benchmark |
| **claude-only-needs-port** | feature exists only on claude (e.g. Int64 quantization, GIL-release pattern, structured exception hierarchy, mimalloc allocator) | re-implement on top of claude-2 codebase in Sprints 3-4 |
| **deliberately-retired** | feature being abandoned per user direction (e.g. claude's in-process `publish(ipc_bytes)` per ADR 0001) | mdata-side migration required; document in breakage report |

Cross-reference against CLAUDE.md golden rules (parse_cache 385ns, GIL release, Int64
quantization, edition 2024, polars version pin) — these are load-bearing invariants
that claude-2 must preserve or beat after ports complete.

Output structure (sections):
1. Inventory methodology (how each surface was identified; commands run)
2. Class-1 features (already-on-main): table + commit references
3. Class-2 features (shape-divergent): table + per-feature commentary on which shape
   wins, why, and what A/B comparison axis applies
4. Class-3 features (claude-only-needs-port): ranked by port priority + per-feature
   port complexity estimate + bench-gate need
5. Class-4 features (deliberately-retired): per-feature breakage notes for mdata
6. Cross-reference matrix to Sprint 1's `main_vs_claude_inventory_2026-05-06.md`
7. Open questions for user

### B.2 Implementation hints

- `git log d7a748b..claude --oneline` — chronological view of claude-only commits
- `git diff main..claude --stat` — surface-level delta
- For each surface candidate, `grep -rn '<surface>'` on both branches to confirm
  presence/absence
- `Explore` subagent best for the heavy lifting (cross-branch surveys; many independent
  greps); main thread reviews and writes the doc
- Pre-seed candidates from existing Sprint 1 inventory + CLAUDE.md golden rules:
  - parse_cache shape (golden rule 6)
  - GIL release around eval (golden rule 5)
  - Int64-quantized storage (golden rule 4)
  - structured exception hierarchy (Phase 13 / WL 3.3)
  - logger built-ins (`.log.{info,warn,debug,error}`)
  - mimalloc global allocator
  - in-process Python pub/sub (`publish(ipc_bytes)` / `subscribe(callback)`)
  - cross-process TCP pub/sub (`publish(handle, bytes)`)
  - `overwrite_partition` separate fn
  - `tick_count: Vec<i64>` shape (vs main's later refactor)
  - chili-py wheel name `chili 0.7.5` (vs main's mid-rename)
  - bench infrastructure (`docs/bench/phase{1..7,9}.md` files)

### B.3 Tests

N/A (research output).

### B.4 Estimated pp

**3-5pp.** Heavy `Explore` subagent + main-thread review/synthesis. Calibrates against
Sprint 1's Part D (which was 6pp on the forward-direction inventory; this is the same
shape, slightly smaller scope since we already have Sprint 1's data to cross-reference).

---

## Scope — Part C: ADR 0001 — pub/sub canonical model

### C.1 Surface additions

New file: `docs/decisions/0001-pub-sub-canonical-model.md`.

**Decision (per user direction 2026-05-07):**
- **Adopt upstream's tick/sub framework on claude-2 as the canonical pub/sub model.**
  This is `init_tick(schema, log_dir, date)` + `publish(table, df: DataFrame)` +
  bundled `tick.pep` / `sub.pep` providing `.tick.upd` (write-tplog-then-broker-publish)
  and `.sub.init` (replay-from-tplog then live subscribe) per upstream commit `7948744`.
- **No in-tree backward-compatibility shim.** Claude-2 ships only the canonical tick/sub
  surface. mdata's existing `publish(ipc_bytes)` callers refactor on their side.
- **A/B comparison strategy: parallel binary builds.** Tag-based: `claude-baseline-
  2026-05-07` builds claude's binary; `claude-2` tip builds the new binary. Run them
  in different locations under the same workload; compare metrics (msg/s throughput,
  p50/p99 publish→delivery latency, GIL-release behavior under N concurrent Python
  callers, memory/subscriber, lock contention). Independent comparison — no in-tree
  A/B harness needed (which collapses the originally-Sprint-4.5 measurement-sprint scope).
- **Claude's two pub/sub models go to the inventory's "deliberately-retired" class
  unless mdata's tp/rdb refactor surfaces a need to re-implement them on claude-2.**
  Decision deferred to per-feature port discussion in Sprint 3-4 — likely outcome:
  retire both.

### C.2 Implementation hints

- Use the convention from `docs/decisions/README.md`: Title, Date, Status, Cutover
  commits, Context, Decision, Consequences, Alternatives considered.
- Cross-reference `docs/research/competitive_position_2026-05-06.md` (strategic frame),
  `docs/research/shakti_analysis.md` §4.2 (pub/sub layer doesn't move the perf needle
  for the kdb+-replacement vision — so we lose nothing by adopting upstream's shape),
  and Sprint 1's main↔claude inventory §2.6 (the three competing models analysis).
- Status: **Accepted** at sprint wrap (after user ratifies the brief and the ADR
  contents).

### C.3 Tests

N/A.

### C.4 Estimated pp

**1-2pp.** ADR authoring is a defined-template doc-writing task.

---

## Scope — Part D: mdata breakage report

### D.1 Surface additions

New file: `docs/sync/mdata_breakage_report_2026-05-07.md`.

**Critical:** this report is **NOT broadcast to mdata until user ratifies and Sprint 3
starts.** mdata stays on the current chili wheel (claude-baseline) until we have a
Sprint 3-tested claude-2 wheel ready.

Contents:
- One-paragraph summary of the pivot direction (claude → claude-2; `main`-aligned base).
- Functional features mdata gains on claude-2 (recursive `load_par_df` from `aa227b3`,
  upstream's tick/sub framework, multi-subscriber broadcast, etc.).
- Breaking API changes vs the current chili wheel (claude-baseline-2026-05-07):
  - `engine.publish(topic, ipc_bytes)` → `engine.publish(table, df: DataFrame)` —
    mdata's tp callers refactor.
  - `engine.subscribe(topics, callback)` → `.sub.init` Pepper-script subscribe pattern
    via `sub.pep` — mdata's rdb callers refactor.
  - Possibly `engine.overwrite_partition(...)` → `engine.write_partition(..., overwrite=True)` —
    contingent on Sprint 2 inventory's per-feature call.
  - Other surfaces flagged by Part B inventory.
- Recommended migration sequence:
  1. mdata stays on claude-baseline wheel (current behavior; no action).
  2. Sprint 3-4 ports the additive claude-only features onto claude-2.
  3. Sprint 5 publishes `claude-2`-built wheel as `chili 0.8.0-claude2.1` (or
     similar — naming TBD with the `project_chili_naming_watch` memory).
  4. mdata receives the breakage report + new wheel; mdata refactors callers; cuts
     over.
- Open timeline: tentative; concrete dates set after Sprint 3 starts and we have
  feature-port pp actuals.

### D.2 Implementation hints

- File goes in `docs/sync/` per docs-lifecycle (sync-with-other-project territory).
- Reference Sprint 1's `main_vs_claude_inventory_2026-05-06.md` §3 cross-reference
  table for the wishlist alignment context.
- User decides when to share with mdata team; not autonomous.

### D.3 Estimated pp

**1-2pp.** Constrained doc-writing.

---

## Scope — Part E: roadmap rewrite + retro + cadence_metrics + sprints_index + CLAUDE.md

### E.1 Surface additions

- `git mv docs/sim/roadmap_2026-05-06.md docs/history/sim/roadmap_2026-05-06.md` —
  preserve the original sequencing as historical reference.
- New `docs/sim/roadmap_2026-05-07.md`:
  - Replaces Sprints 2-4-8 cherry-pick + full-merge arc with Sprint 2-5 port arc.
  - Sprints 6+ continue with original-roadmap features (deep housekeeping, bench
    suite v0, perf passes, KDB-X CE comparison, Pepper conformance, Iceberg eval) but
    on the claude-2 base.
  - Each sprint sized; gate dependencies preserved; 5-sprint deep-housekeeping
    cadence enforced (Sprints 6 and 11).
  - Notes Sprint 8 from old roadmap is **deleted** (full merge accomplished by pivot).
  - Notes Sprint 4 (pub/sub ADR) is **collapsed into Sprint 2 Part C**.
  - Notes Sprint 4.5 (A/B measurement) is **collapsed into Sprints 3-5**
    (A/B happens as port verification on parallel binary builds).
- `docs/sim/sprint_2_retro.md` per `_retro_template.md`:
  - Records Part A halt (cherry-pick conflict accumulation; Sprint 2 v1 superseded).
  - Records pivot decision and ratification.
  - Records Parts A-E actuals.
  - Promotes the iteration lesson if not yet promoted (it's already in
    `iteration_lessons.md` from this commit batch — note that in the retro).
- `docs/sim/cadence_metrics.md` row 2 appended.
- `docs/sim/sprints_index.md` — Sprint 2 row updated to "Wrapped (awaiting
  ratification)" with notes about supersession + reference to v1 brief in history.
- `CLAUDE.md` updates:
  - Branch policy section: rewrite to reflect post-pivot reality. Add `claude-2`
    as the active working branch; `claude` is parked-historical (immutable; never
    deleted; tagged `claude-baseline-2026-05-07`); `main` continues as upstream
    read-only mirror.
  - Project state line: refresh to claude-2 baseline (date pin, version, test count
    on bare main).
  - Docs map: update roadmap pointer to `roadmap_2026-05-07.md`; add reverse-direction
    inventory pointer; add ADR 0001 pointer.
  - Verify size budget ≤ 200 lines per `~/.claude/rules/claude-md-housekeeping.md`.
- Memory update: `~/.claude/projects/-Users-oakadmin-code-chili/memory/project_chili_branch_model.md`
  — note pivot direction; record pivot date 2026-05-07; tag references.

### E.2 Tests

- CLAUDE.md size budget check.

### E.3 Estimated pp

**2-3pp.** Roadmap rewrite is the heaviest doc work; retro + index + memory + CLAUDE.md
are mechanical updates.

---

## Out of scope (defer)

- **Any actual Rust feature ports onto claude-2** — Sprints 3-4.
- **A/B benchmark runs** — Sprints 3-5 as ports complete; comparison binaries built
  from `claude-baseline-2026-05-07` and `claude-2` tip respectively.
- **mdata team coordination/notification** — blocked until user ratifies the
  breakage report and Sprint 3 starts.
- **Bench rewrites/rebaseline** — Sprints 3-5.
- **Test rewrites for new shapes** — Sprints 3-5.
- **Rename pickup** (any of `a0a42f6` / `778cac0` / `98586cf` / similar in-flight
  upstream renames) — held per `project_chili_naming_watch.md` memory; verdict on
  the post-pivot wheel name (`chili 0.8.0-claude2.1` or otherwise) lands in Sprint
  2 Part E.
- **Deletion of `claude` branch** — never. Permanent reference.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `git tag claude-baseline-2026-05-07` on claude tip | git op |
| 2 | `git tag main-pivot-2026-05-07` on main tip | git op |
| 3 | `claude-2` branch created from main | git op |
| 4 | Pre-commit gate output on bare main captured | doc artifact |
| 5 | `docs/history/bench_claude_baseline_2026-05-07/` populated on claude-2 | doc copy |
| 6 | `docs/research/claude_only_features_inventory_2026-05-07.md` | new doc |
| 7 | `docs/decisions/0001-pub-sub-canonical-model.md` ratified | new ADR |
| 8 | `docs/sync/mdata_breakage_report_2026-05-07.md` (held internal) | new doc |
| 9 | `docs/sim/roadmap_2026-05-07.md` | new doc |
| 10 | Old roadmap moved to `docs/history/sim/roadmap_2026-05-06.md` | doc move |
| 11 | `docs/sim/sprint_2_retro.md` | new (post-sprint) |
| 12 | `docs/sim/cadence_metrics.md` row 2 | edit (post-sprint) |
| 13 | `docs/sim/sprints_index.md` Sprint 2 → "Wrapped (awaiting ratification)" | edit (post-sprint) |
| 14 | `CLAUDE.md` rewritten for post-pivot branch policy | edit |
| 15 | `project_chili_branch_model.md` memory updated | edit |

---

## Lead allocation

- **Coordinator-solo (main Claude)** for Parts A, C, D, E.
- **`Explore` subagent** for Part B inventory — single dispatch; deep cross-branch
  survey of claude-only features. Budget ~2-4pp. Subagent outputs the inventory
  draft; main thread reviews and finalizes.
- **No `code-reviewer` subagent** this sprint — no Rust changes to review.
- **No worktrees** — Parts A creates the new branch; subsequent parts work on
  `claude-2` once it exists. Sequential.
- **SHUTDOWN_SIGNAL discipline** — same as Sprint 1: check before each major part
  (A → B → C → D → E). Watchdog daemon writes signal at 5h ≥ 90%; baseline at
  Sprint 2 kickoff = 2% (fresh window post-/compact).

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~5-7pp into the sprint, after Parts A and B land):

- Part A — branch created, both tags landed, pre-commit gate on bare main captured?
- Part B — features inventory complete? Surface count vs prediction (~12 candidates)?
  Any features discovered that weren't in the pre-seeded list?
- ETA to wrap.

State current 5h-pp delta + absolute % at every checkpoint and at wrap.

### Halt-and-escalate criteria

1. **Scope-blowing finding** — Part B reveals MORE claude-only features than expected,
   pushing port-sprint pp budget beyond Sprints 3-4. If port count > 15 features OR
   cumulative port pp predicted > 30, halt and rescope (may require Sprint 3 split
   into 3a/3b).
2. **Plan-pivot finding** — bare `main` pre-commit gate fails catastrophically (e.g.
   cargo test red on multiple test files, not just isolated breakage). Investigate;
   may require Sprint 2 to also stabilize main as a side quest, OR halt and
   re-baseline against an earlier main commit.
3. **User-decision needed** — ADR 0001 surfaces a sub-decision the user hasn't
   ratified (e.g., specific naming for the post-pivot wheel; whether to retain
   in-process pub/sub at all).
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 6pp. Predicted Sprint 2
   ≤ 14pp; baseline 2%; should never trigger. **If it does trigger, full halt + WIP
   commit + cron resume per `~/.claude/rules/shutdown-protocol.md`.**

---

## Wrap (per ceremony)

- Pre-commit gate green on `claude-2` (since no Rust changed this sprint, this should
  be the same state as bare main after the branch fork).
- Sprint 2 v2 retro authored.
- Cadence_metrics row 2 appended.
- Sprints_index updated to "Wrapped (awaiting ratification)".
- CLAUDE.md project state + branch policy + docs map refreshed.
- Memory updated.
- ADR 0001 ratified by user before Part C marks complete.
- Mdata breakage report drafted but **NOT delivered** (held until Sprint 3 start +
  user ratification).
- HALT until user ratifies the full sprint wrap.

---

## Pp accounting reference

**Sprint 1 actuals as the calibration anchor:** ~25pp research-heavy.

**Sprint 2 v1 actual:** ~1.5pp burnt before halt (cherry-pick aborted; tree restored
to `0f040fe`). The v1 brief authoring + ratification cost (~2pp) is treated as
sunk-cost allocated to the v2 brief authoring (this commit batch).

**Sprint 2 v2 predicted:** 8-14pp.

- Part A: 1-2pp (git ops + doc copy)
- Part B: 3-5pp (heavy `Explore` subagent + main-thread synthesis)
- Part C: 1-2pp (ADR authoring)
- Part D: 1-2pp (breakage report doc)
- Part E: 2-3pp (roadmap rewrite + retro + sprints_index + CLAUDE.md + memory)

If actuals come in materially under (≤6pp), the pivot itself was cheaper than
expected — useful calibration for the rest of the port arc.

If materially over (≥17pp), Part B's reverse-direction inventory was harder than
predicted. Likely cause: more claude-only features than the pre-seeded list. The
next iteration_lessons entry would be about reverse-direction inventory cost
calibration.

The "git op error budget" for Part A is ~0.5pp — `git tag` and `git checkout -b`
are mechanical; if they fail, something is weird (uncommitted changes, weird
HEAD state) and needs to halt-and-investigate, not retry-blindly.

---

## Cross-references

- **Old brief (superseded):** `docs/history/sprints/sprint_2_dispatch_brief_2026-05-07_superseded.md`
- **New roadmap (this sprint):** `docs/sim/roadmap_2026-05-07.md`
- **Sprint 1 inventory (forward direction, main → claude):** `docs/research/main_vs_claude_inventory_2026-05-06.md`
- **New inventory (reverse direction, this sprint Part B):** `docs/research/claude_only_features_inventory_2026-05-07.md`
- **Companion synthesis:** `docs/research/competitive_position_2026-05-06.md`
- **Cadence rule:** `.claude/rules/sprint-cadence.md`
- **Iteration lessons (load-bearing for this sprint):** `docs/standards/iteration_lessons.md` —
  the "Cherry-pick conflict accumulation — invert the merge direction" entry is the
  prime motivation for this sprint
- **Project memories:** `project_chili_vision`, `project_chili_branch_model`
  (will be updated this sprint), `project_chili_naming_watch`
- **Shutdown protocol:** `~/.claude/rules/shutdown-protocol.md`
