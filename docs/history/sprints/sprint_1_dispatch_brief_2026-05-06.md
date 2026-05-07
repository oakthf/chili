# Sprint 1 dispatch brief — strategic research + main↔claude inventory (research / scaffold)

**Kickoff:** TBD — pending user ratification of this brief
**Owner:** coordinator-solo (main Claude) + research subagents serial (one at a time, A → B → C → D → E)
**Type:** research / scaffold (no production code changes)
**Predicted pp:** 22–35 (uncalibrated — first sprint under cadence; band widened from 17–27 after 2026-05-06 ratification specified serial subagent execution + deeper Part D analysis; will tighten on Sprint 2)
**Plan reference:** user directive 2026-05-06 (open the floor for vision-driven research)
**ADR references:** none yet — Sprint 1 may surface ADR candidates as deliverable

---

## Sprint objective

Establish the strategic-research foundation chili needs to credibly pursue the
"open-source kdb+ replacement" vision (per `project_chili_vision.md` memory). Produce
durable artifacts that future sprints can reference: a kdb+ landscape brief, a
competitive-positioning doc, a Shakti analysis, an mdata-wishlist cross-reference
that disambiguates "already-shipped on claude / available on main / gap", and a
proposed 6–12-sprint roadmap.

**Binary success criterion:** at sprint wrap, the user can read one doc
(`docs/research/competitive_position_2026-05-06.md`) and answer two questions
unambiguously — (a) "where does chili stand vs kdb+ / Shakti / open-source
alternatives today, on which axes?" and (b) "what is the next 6–12 sprint sequence
that gets us closest to vision?" Implementation work (cherry-picks, perf
optimizations) is **not** in scope; it lands in Sprint 2+ informed by these docs.

---

## Why now

- The cadence machinery just stood up (Sprints folder, templates, iteration_lessons,
  ADR convention) — a research sprint is the lowest-risk way to **dogfood the
  cadence** before doing anything that touches production code paths.
- mdata is blocked on a wishlist of 4 cherry-picks from main; we cannot prioritize
  that work intelligently without first knowing **what else** is on main worth
  pulling in (22 commits on main since the d7a748b fork point — only 5 are in the
  wishlist; the other 17 deserve at least a triage pass).
- The user's strategic vision (project_chili_vision memory) raises the bar from
  "support mdata" to "outperform kdb+ / Shakti." This bar can only be defended with
  benchmark data we don't yet have in writing. Sprint 1 puts that data on paper so
  Sprint 2+ optimization claims are defensible.
- "Compactness / lightweight" is a stated optimization axis — we need a baseline
  measurement (binary size, deps tree, build time) before optimizing anything, or
  we'll celebrate placebo wins.

---

## Scope — Part A: kdb+ / q landscape brief

### A.1 Surface additions

New deliverable: `docs/research/q_kdb_landscape.md` (~3–5pp of agent time). Sections:

- **History.** Arthur Whitney (APL → A → K → q → kdb+ at Kx Systems → Shakti at his
  new co). One paragraph each.
- **Current state.** Kx ownership (FD Technologies), licensing model, target market
  (HFT, capital markets), market share signal where defensible. Not vendor copy —
  cite primary sources or date the claim.
- **Benchmarks.** Published kdb+ numbers we can cite — STAC-M3, Whitney's own
  white-paper figures, third-party reproductions where they exist. Tabulate
  benchmark name → metric → kdb+ figure → date. We will compare chili against
  these in later sprints.
- **What kdb+ is good at vs bad at.** No editorializing — list the canonical
  praises (raw latency, splay/parted layout, q's terseness for streaming queries)
  and complaints (license cost, q learning curve, vendor lock-in, single-node
  scale ceilings).

### A.2 Implementation hints

Subagent-led: spawn `general-purpose` agent with web-fetch authority to gather
primary sources. Main thread does final synthesis pass to ensure consistency with
the rest of Sprint 1's docs. Cite every figure with URL + retrieval date.

---

## Scope — Part B: competitive analysis (open + closed-source kdb+ alternatives)

### B.1 Surface additions

New deliverable: `docs/research/kdb_alternatives.md` (~3–5pp). Two starting indices
(per `reference_external_research_targets.md` memory):

- https://www.timestored.com/kdb-guides/kdb-alternatives
- https://opensourcealternative.to/alternativesto/kdb+

For each material competitor, capture: project name, license, language layer (if
any), storage format, primary use case, last-active commit date, public benchmarks
where available. Group by family: column-store DBs (DuckDB, ClickHouse, QuestDB),
timeseries DBs (InfluxDB, TimescaleDB, Druid), array-language successors (k9,
Shakti, Klong, ngn/k), and dataframe engines (Polars, DataFusion, Vaex).

Where chili shares an architectural lineage (Polars / Arrow / Parquet), call it
out — we inherit some of the parent project's positioning.

### B.2 Implementation hints

Subagent-led parallel with Part A. Use `general-purpose` agent. The output is a
table + 1–2 paragraphs per competitor — not a feature checkbox doc. Lead with the
**positioning question** ("what does chili offer that this competitor doesn't, and
vice versa?") to keep length bounded.

---

## Scope — Part C: Shakti analysis

### C.1 Surface additions

New deliverable: `docs/research/shakti_analysis.md` (~2–4pp).

Shakti is closed-source but public benchmarks and architecture hints leak via
Whitney's talks and Shakti white papers. Capture what's known:

- Lineage from k9 / k7 — interpreter design choices that affect performance.
- Published benchmarks (cite source + date — Shakti's own marketing OR third-party).
- Open-source approximations (ngn/k, Klong, k9-style implementations) — where the
  **techniques** are visible even if Shakti's binary isn't.
- Strategic implication for chili: are there design choices Shakti made that we
  should adopt or deliberately reject?

### C.2 Implementation hints

Subagent-led parallel with A and B. Highest research uncertainty (most info is
secondary or vendor-marketing — be skeptical). Don't claim Shakti benchmarks
without dated source citations.

---

## Scope — Part D: mdata wishlist + main↔claude cross-reference

### D.1 Surface additions

New deliverable: `docs/research/main_vs_claude_inventory_2026-05-06.md` (~3–5pp).

Primary tasks (main thread, not subagent-delegated — needs deep familiarity with
chili's commit semantics):

1. **Enumerate** the 22 commits on main since the d7a748b fork point. For each:
   one-line summary + classification (feature / refactor / fix / dep / ci / docs).
2. **Cross-reference** against work already on `claude`. The mdata wishlist names 5
   specific commits (`b20177c`, `01c1227`, `aa227b3`, `7948744`, `3aeee62`); we must
   verify these are NOT yet on claude (likely true, but confirm) and surface any
   half-overlap (a feature we partially implemented on claude that conflicts with
   the upstream version — exactly the "watch for half-done overlap" risk the user
   flagged).
3. **Triage** the other 17 commits: pickup-now / pickup-later / skip. The 4 wishlist
   commits are P0; the other 17 need a verdict each.
4. **Conflict prediction.** For each "pickup" commit, predict conflict surfaces
   against `claude`-only work (especially the 2026-04-26 FFI rewrite).

### D.2 Implementation hints

Use `git log d7a748b..main --oneline` as the entry point; use `git diff
claude...main -- <file>` for conflict prediction. The Explore subagent can be
delegated to read individual commit diffs in parallel if it speeds things up — but
synthesis of the inventory stays on main thread.

---

## Scope — Part E: Roadmap proposal (6–12 sprints)

### E.1 Surface additions

New deliverable: `docs/sim/roadmap_2026-05-06.md` (~2–3pp).

Proposed sequence covering:

- **Sprint 2 (P0):** Cherry-pick the 4 mdata-blocking wishlist commits. Validation
  per the wishlist's "Section 3" gate (`test_tick_sub.py` green, `test_engine.py`
  green, FFI tests still pass, version bump to e.g. `0.7.6-claude.1`).
- **Sprint 3+ (P1):** The other "pickup-now" upstream commits identified in Part D.
- **Sprint N (research-driven):** Performance work. Each gap surfaced in Parts A/B/C
  becomes a candidate sprint with an explicit benchmark-target clause.
- **Sprint M (strategic):** The full main → claude reconciliation arc per
  `project_chili_vision`. Probably 2–3 sprints away once the wishlist is delivered
  and the conflict prediction in Part D is empirically validated.
- **Optimization candidates** the chili author has surfaced in past upstream
  refactors — if our git log reveals a pattern, propose a "preemptive refactor"
  sprint.

### E.2 Implementation hints

Each sprint in the roadmap gets a one-line abstract + predicted-pp band. Don't write
full briefs — those happen at each sprint's kickoff per the cadence rule.

---

## Out of scope (defer)

- **Any actual cherry-pick or merge work** — Sprint 2 territory. If we touch the
  working tree this sprint, we've scope-crept; the docs are the deliverable.
- **Benchmark execution.** Reading kdb+'s published numbers is in scope; running
  STAC-M3 against chili is out of scope (huge sprint of its own; Sprint M+).
- **Refactors of `claude`-branch code.** Even if Part D's analysis surfaces an
  obvious target, defer to its own sprint.
- **CLAUDE.md status-block date pin / test count refresh.** Stale-state hygiene
  but not load-bearing for Sprint 1; bundle into a small chore commit at sprint
  wrap if convenient, otherwise next sprint.
- **A new ADR** unless one is unavoidable. Sprint 1 may surface ADR candidates;
  authoring them is out of scope (too much ceremony for week-1 of cadence).

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `docs/research/q_kdb_landscape.md` | new |
| 2 | `docs/research/kdb_alternatives.md` | new |
| 3 | `docs/research/shakti_analysis.md` | new |
| 4 | `docs/research/main_vs_claude_inventory_2026-05-06.md` | new |
| 5 | `docs/research/competitive_position_2026-05-06.md` | new (synthesis of 1–4) |
| 6 | `docs/sim/roadmap_2026-05-06.md` | new |
| 7 | `docs/sim/sprint_1_retro.md` | new (post-sprint) |
| 8 | `docs/sim/cadence_metrics.md` row 1 | edit (post-sprint) |
| 9 | `docs/sim/sprints_index.md` row updated to "Wrapped / Ratified" | edit (post-sprint) |
| 10 | `CLAUDE.md` Docs map: add `docs/research/` pointer | edit (modest, ≤ 200 lines budget intact) |

---

## Lead allocation

**Coordinator-solo + serial subagents (one at a time, queue order A → B → C → D → E):**

- `general-purpose` subagent → Part A (q/kdb+ landscape). Budget ~5pp.
  - Read; main thread reviews returned draft, stores at `docs/research/q_kdb_landscape.md`.
- `general-purpose` subagent → Part B (competitive analysis). Budget ~5pp.
  - Same pattern.
- `general-purpose` subagent → Part C (Shakti). Budget ~4pp.
  - Same pattern. Higher uncertainty due to closed-source target.
- Main Claude (sequential, deep): Part D (inventory). Budget ~8–10pp.
  - **Per 2026-05-06 ratification:** read **every** one of the 22 `claude..main`
    commits' diffs, not just the 5 wishlist commits. For each: one-line summary,
    feature/refactor/fix/dep/ci/docs classification, **predicted conflict surface
    against `claude`** with cited file paths, and pickup-now / pickup-later / skip
    verdict. Use the `Explore` subagent inside Part D to read individual commit
    diffs in parallel (Explore is read-only, parallel-safe, low-cost — designed
    for fanned-out file reads). Synthesis stays on main thread.
- Main Claude (sequential, after Part D): Part E (roadmap). Budget ~3pp.
- Main Claude (final synthesis): `docs/research/competitive_position_2026-05-06.md`
  pulling A/B/C/D/E into one read-this-first doc. Budget ~2pp.

**Why serial over parallel** (per 2026-05-06 ratification): user prioritizes quality
of subagent output over wall-clock compression. Serial execution lets main thread
review each draft fully before queueing the next subagent — early subagent output
can inform later subagents' prompts.

Each subagent returns a draft markdown doc; main thread does final consistency edit
pass. No subagent is authorized to commit; main thread stages and commits. No
worktrees needed.

**SHUTDOWN_SIGNAL discipline:** before dispatching each subagent, main thread checks
`~/.claude/SHUTDOWN_SIGNAL` and `~/.claude/rate_limit_cache.json` per the protocol
in `~/.claude/rules/shutdown-protocol.md`. The watchdog daemon writes the signal at
5h ≥ 90%. If signal fires mid-sprint, main thread + any in-flight subagent halt per
shutdown protocol (WIP note, commit, CronCreate at reset+stagger, halt-but-stay-in
session).

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~10–14pp into the sprint), post a short status:

- Subagent A/B/C return state — drafts in hand, partial, or blocked?
- Part D inventory state — any conflict surface uglier than predicted?
- Part E roadmap draft state.
- ETA to wrap.

Halt-and-escalate criteria:

1. **Scope-blowing bug** — discovered issue would push actual-pp >150% of predicted
   (e.g. >40pp). Most likely failure: subagent burns budget gathering low-quality
   web sources and we have to pivot to primary-source-only synthesis.
2. **Plan-pivot finding** — Part D surfaces that mdata's wishlist commits already
   conflict deeply with claude's FFI rewrite (in which case the cherry-pick approach
   in the wishlist is wrong and we'd need to escalate that to the user before
   Sprint 2 plans land).
3. **User-decision needed** — e.g. an ADR-worthy choice surfaces (e.g. "should
   chili adopt the k9 interpreter dispatch pattern?") that needs explicit
   ratification before encoding.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 15pp. The 90%
   `SHUTDOWN_SIGNAL` write is the hard backstop; don't reach it. **First sprint:
   uncalibrated** — extra vigilance at the 80% mark is required since I have no
   chili-specific empirical baseline yet.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets
  -- -D warnings && cargo test`. **Should be no-op for a docs-only sprint** —
  Sprint 1 doesn't touch Rust. If the gate goes red, that's a regression to surface
  separately.
- Test-count delta: `0` (no test changes).
- Author retro at `docs/sim/sprint_1_retro.md` — predicted/actual/variance, what
  surprised, lessons. Promote any high-cost incident lesson to
  `docs/standards/iteration_lessons.md` per §10.3.
- Append row to `docs/sim/cadence_metrics.md` (10 fields).
- Update `docs/sim/sprints_index.md` to show Sprint 1 in the "Wrapped (awaiting
  ratification)" state, with link to retro.
- HALT until user ratifies.

---

## Pp accounting reference

**No prior chili sprints under this cadence.** Calibration is from scratch.

Closest available comparable: mdata's `cadence_metrics.md` rows for documentation /
research / planning sprints under the team-Max plan. Per
`~/.claude/rules/work-metrics.md`, "small/design-only sprints land 25–50% under
initial estimates." If that holds, this sprint may land closer to the low end
(~12–18pp actual vs 17–27pp predicted) — but the absence of chili-specific data
and the multi-subagent dispatch pattern make the variance band wider than usual.

Sprint 1 expected at the **mid-band** of 17–27, capped above by subagent quality
risk (research depth varies; we may need second-pass agent runs if drafts return
shallow).

This sprint's actuals are themselves a **calibration anchor** for Sprint 2 — that's
part of why dogfooding the cadence on a research sprint is the right opener.

---

## Cross-references

- Roadmap (forthcoming this sprint): `docs/sim/roadmap_2026-05-06.md`
- mdata wishlist: `~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`
- Vision memory: `~/.claude/projects/-Users-oakadmin-code-chili/memory/project_chili_vision.md`
- Branch model memory: same dir, `project_chili_branch_model.md`
- Cadence rule: `.claude/rules/sprint-cadence.md`
- Token-budget methodology: `~/.claude/rules/work-metrics.md`
