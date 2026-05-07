> **SUPERSEDED 2026-05-07** — never executed past Part A's halt finding. The cherry-pick
> approach hit halt-criterion #1 on the first commit (`b20177c`): conflict surface in
> `crates/chili-py/src/lib.rs` was 12 regions with multiple > 30 lines (largest 101),
> caused by all three planned wishlist commits being authored against upstream's
> pre-FFI-rewrite chili-py while claude carries the post-FFI-rewrite shape (`08fe588`,
> 2026-04-26). User-directed pivot: park `claude` branch, restart `claude-2` from
> `main` tip, port claude-only features onto the new base. See the active brief
> at `docs/sim/sprint_2_dispatch_brief_2026-05-07.md` and the
> "cherry-pick conflict accumulation" lesson in
> `docs/standards/iteration_lessons.md`.

# Sprint 2 dispatch brief — mdata-foundation cherry-picks (implementation)

**Kickoff:** TBD — pending user ratification of this brief
**Owner:** coordinator-solo (main Claude); `code-reviewer` subagent for diff review at sprint wrap
**Type:** implementation (real Rust code lands; no Rust touched in Sprint 1)
**Predicted pp:** 5–9 (calibrated against Sprint 1's research-heavy ~25pp; Sprint 2 is implementation-heavy with bounded scope; subagent error budget reduced because most work is main-thread)
**Plan reference:** [`roadmap_2026-05-06.md`](roadmap_2026-05-06.md) Sprint 2 row; [`../research/main_vs_claude_inventory_2026-05-06.md`](../research/main_vs_claude_inventory_2026-05-06.md) §4 verdicts
**ADR references (if any):** none authored this sprint; first ADR (`docs/decisions/0001-pub-sub-canonical-model.md`) is Sprint 4 territory

---

## Sprint objective

Land the three "clean cherry-pick" deliverables from the mdata wishlist into the
`claude` branch and rebuild the chili-py wheel with a lockfile-detectable version
bump. Specifically: (A) `b20177c` TCP listener extraction + Python binding,
(B) the serde9 nested-MixedList deserialization fix surgically extracted from
`7948744` (the rest of `7948744` is Sprint 4 territory per the pub/sub ADR),
(C) `engine.stats()` + `MissingParCondErr` partial cherry-pick from `3aeee62`
(dropping the `write_partition(overwrite=…)` portion to preserve claude's separate
`overwrite_partition` function).

**Binary success criterion:** `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` is green AND `pytest crates/chili-py/tests/test_engine.py` passes (the 42 EngineState surface tests landed by `b20177c` per the mdata wishlist Section 3) AND the chili-py wheel builds with version bumped from `0.7.5` to `0.7.6-claude.1` so mdata's lockfile detects the rebuild AND `engine.stats()` returns sensible runtime statistics on a smoke-tested EngineState.

**Out-of-scope rebuild gates from the wishlist:** `pytest test_tick_sub.py` is **NOT** expected to pass this sprint — it depends on the pub/sub portion of `7948744` which is Sprint 4 territory. mdata will be told this directly.

---

## Why now

- mdata is operationally blocked on the wishlist's foundation commits. The TCP listener (`b20177c`) is the foundation commit upstream-side for everything else they need. Landing it first unblocks their next planning cycle.
- Sprint 1's inventory pass de-risked the cherry-picks: each Sprint 2 commit was independently classified as "clean" (b20177c), "surgical extraction needed" (serde9 from 7948744), or "partial pickup" (3aeee62 minus write_partition). The hard cases (full `aa227b3` + `7948744` pub/sub) are deferred to Sprints 3 and 4 explicitly.
- The cadence convention's first implementation sprint. Sprint 1 was research-only and didn't exercise the pre-commit gate. Sprint 2 is the first sprint that runs the full `cargo fmt + clippy + test` ceremony on real changes — it's the first practical test of the cadence machinery on production-adjacent code.
- The chili-py wheel hasn't been rebuilt since the 2026-04-26 FFI-merge (`08fe588`). A version bump is overdue regardless of mdata's wishlist; their lockfile-detection request just adds a pinning constraint.

---

## Scope — Part A: cherry-pick `b20177c` (TCP listener + Python binding)

### A.1 Surface additions

The upstream commit:
- Extracts the existing ~80-line TCP listener block from `crates/chili-bin/src/main.rs` into a new method `EngineState::start_tcp_listener(port: u16, ...) -> SpicyResult<...>` in `crates/chili-core/src/engine_state.rs`.
- Adds two PyO3 bindings: `start_tcp_listener` and `stats` (the latter is part of `3aeee62` upstream but `b20177c` exposes it through the Python layer).
- Adds the `start_tcp_listener()` and `stats()` methods to the Python `ChiliEngine` wrapper in `crates/chili-py/chili/engine.py` with Args/Returns docstrings.
- Adds a `dev-py-binding` task to `Taskfile.yml` for the `maturin develop` workflow.
- Fixes a redundant `.map(|a| a)` identity iterator (clippy `map_identity`).

### A.2 Implementation hints

Per inventory §2.4, this is the cleanest of the wishlist commits — claude's `main.rs` carries the same ~80-line shape. Conflicts should be limited to:
- The import boundary in `chili-bin/main.rs` (where the extracted block used to live)
- The new method position in `engine_state.rs` (probably no real conflict — append at end of impl block)
- `chili-py/src/lib.rs` PyO3 binding additions (line position only)
- `chili-py/chili/engine.py` Python wrapper additions

`git cherry-pick b20177c` first; resolve any auto-detected conflicts; verify build with `cargo build` BEFORE running tests; iterate.

### A.3 Storage / schema (if applicable)

None. `b20177c` is purely a code-organization refactor + new API surface.

### A.4 Tests

`b20177c` ships `crates/chili-py/tests/test_engine.py` (42 tests on the EngineState surface). All must pass post-cherry-pick. `cargo test` and `cargo clippy --all-targets -- -D warnings` must remain green.

---

## Scope — Part B: surgical extraction of serde9 fix from `7948744`

### B.1 Surface additions

`7948744` is the "big" wishlist commit. We are extracting **only** the serde9 portion:

- Fix `crates/chili-core/src/serde9.rs` so nested `MixedList` deserializes from the *current* offset rather than a hardcoded byte 16. This is the bug that causes IPC topic misregistration on tplog replay > 1 batch (per the wishlist's wording).

We are **NOT** extracting the pub/sub portion (init_tick / publish(df) / subscribe / tick.pep / sub.pep / job scheduler / memory monitor) — that is Sprint 4 territory pending the canonical-model ADR.

### B.2 Implementation hints

Strategy: do NOT `git cherry-pick 7948744` — the diff is too broad and would pull in pub/sub work. Instead:
- `git show 7948744 -- crates/chili-core/src/serde9.rs > /tmp/serde9_fix.patch`
- Inspect the patch; verify it touches *only* serde9.rs and possibly the serde9 test file. If it touches anything pub/sub-shaped, narrow further.
- `git apply /tmp/serde9_fix.patch` from the claude tip; resolve conflicts.
- Add a regression test for the bug if one wasn't included.

If the upstream commit message about the bug is ambiguous, read the test that 7948744 added (likely a serde9 round-trip test for nested MixedList). Use that test as the spec.

### B.3 Storage / schema (if applicable)

The fix changes the on-the-wire IPC bytes layout interpretation, NOT the on-disk Parquet layout. claude's CLAUDE.md golden rule 4 (Int64-quantized storage) is unaffected. No schema migration concern.

### B.4 Tests

The serde9 fix is correctness, not performance. Required test addition: a round-trip test for nested MixedList that fails on the pre-fix code and passes after. Goes in `crates/chili-core/src/serde9.rs` test module or a sibling tests/ file.

---

## Scope — Part C: partial pickup of `3aeee62` (stats + MissingParCondErr; drop overwrite= folding)

### C.1 Surface additions

From `3aeee62`, take:
- `EngineState::stats()` method that returns runtime statistics (lazy mode, REPL language, partitioned dataframe count, parse cache length).
- `MissingParCondErr` error variant in `crates/chili-core/src/errors.rs`.
- Python binding for `engine.stats()` in `crates/chili-py/src/lib.rs` (already partially present per `b20177c` — coordinate ordering).

Do **NOT** take:
- The `write_partition(overwrite=…)` flag addition. Claude has a separate `overwrite_partition()` function (`chili-py/src/lib.rs:520`) that mdata calls. Folding overwrite into write_partition would create two competing APIs; preserve claude's separate-function shape per inventory §2.3.

### C.2 Implementation hints

This is a partial cherry-pick. Strategy:
- `git show 3aeee62 --stat` to see the file scope.
- `git format-patch -1 3aeee62 --stdout > /tmp/3aeee62.patch`
- Edit the patch to remove hunks that touch `write_partition` overwrite-related code (the function-signature change + caller updates).
- `git apply /tmp/3aeee62-trimmed.patch`.
- Verify `overwrite_partition` is still callable + behaves the same; if any test in claude exercises it, run that test specifically.

If `b20177c` (Part A) already brought in the `stats()` Python binding, Part C only needs the Rust-side `stats()` method + `MissingParCondErr`. Order Parts A and C accordingly.

### C.3 Storage / schema (if applicable)

`MissingParCondErr` is an error variant — not a schema change. Stats output is read-only.

### C.4 Tests

`engine.stats()` returns a struct/dict; add a Python-side smoke test that calls it on a fresh `ChiliEngine` and verifies the returned shape (`lazy_mode: bool`, `repl_lang: str`, `par_df_count: int`, `parse_cache_len: int`).

---

## Scope — Part D: wheel rebuild + version bump

### D.1 Surface additions

- `crates/chili-py/pyproject.toml` — bump `version = "0.7.5"` to `version = "0.7.6-claude.1"` (post-PEP-440 prerelease form so pip install does not auto-upgrade past it).
- `crates/chili-py/Cargo.toml` — sync version field with pyproject.toml.
- `Cargo.lock` regenerated by `cargo build`.
- `CHANGELOG.md` — add an entry for `0.7.6-claude.1` describing the cherry-picks.
- Build wheel via `cd crates/chili-py && uv run maturin develop --release` (per CLAUDE.md "Common commands").

### D.2 Implementation hints

The version bump must satisfy the wishlist's request: "please bump to e.g. `0.7.6-claude.1` so mdata's lockfile detects the rebuild." Use the exact form `0.7.6-claude.1`.

`crates/chili-py/pyproject.toml [project].name` stays as `chili` (per `project_chili_naming_watch.md` memory + CLAUDE.md project state line — user direction 2026-05-07: hold the rename pending upstream's official release notes).

### D.3 Storage / schema (if applicable)

None.

### D.4 Tests

After rebuild: `cd crates/chili-py && uv run pytest`. All 44+ tests must pass. The wishlist's Section 3 gate (`test_engine.py` green from `b20177c`) is satisfied here.

---

## Scope — Part E: project-state refresh + retro + cadence-metrics row

### E.1 Surface additions

- `CLAUDE.md` Project State block — refresh date pin (`2026-04-26` → `2026-05-07`), refresh version line, refresh test count to reflect the new `test_engine.py` tests (44 baseline + 42 from b20177c = ~86 Python tests; verify exact count from pytest output), refresh "Open items" pointer.
- `docs/sim/sprint_2_retro.md` per `_retro_template.md`.
- `docs/sim/cadence_metrics.md` row 2 appended.
- `docs/sim/sprints_index.md` row 2 → `Wrapped (awaiting ratification)`.
- mdata-side communication: a one-liner to the user summarizing what landed (TCP listener + serde9 + stats; pub/sub deferred to Sprint 4) so the user can pass the message to mdata.

### E.2 Tests

`CLAUDE.md` size budget — must stay ≤ 200 lines per `~/.claude/rules/claude-md-housekeeping.md`. Currently 116; refresh adds ~3 lines net = ~119. Within budget.

---

## Out of scope (defer)

- **Full `7948744` pub/sub framework** — Sprint 4 (with ADR + Option-c-with-measured-retirement implementation).
- **`aa227b3` recursive `load_par_df` + multi-subscriber + bounds** — Sprint 3.
- **`01c1227` tick_count refactor (helper for aa227b3)** — Sprint 3.
- **`write_partition(overwrite=…)` folding** — never (claude keeps separate `overwrite_partition` function per inventory §2.3).
- **`98fbd7f` PyLazyFrame** — flagged for "pickup-later"; check actual need before adding.
- **`2286dec` parking_lot lock refactor** — bench-gated; deferred to Sprint 7's perf-pass-1 territory.
- **Rename pickup** (any of `a0a42f6` / `778cac0` / `98586cf`) — held per `project_chili_naming_watch.md`.
- **CI / docs commits** (`f8b6360` musllinux, `00cda45` README, etc.) — not load-bearing for mdata; pickup if they apply trivially during conflict resolution, otherwise skip.
- **Any new feature work or refactors of claude-only code** — Sprint 2 is cherry-pick-only. Refactor temptations get logged in `docs/sync/ideas.md` with a tag.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `git cherry-pick b20177c` resolved + landed on claude | git op |
| 2 | serde9 surgical fix from `7948744` landed on claude | git op |
| 3 | partial cherry-pick of `3aeee62` (stats + MissingParCondErr; no overwrite folding) | git op |
| 4 | `crates/chili-py/pyproject.toml` + `Cargo.toml` version bumped to `0.7.6-claude.1` | edit |
| 5 | `CHANGELOG.md` entry for `0.7.6-claude.1` | edit |
| 6 | Rebuilt chili-py wheel (`maturin develop --release`) | build artifact |
| 7 | `cargo fmt + clippy + test` green | gate |
| 8 | `pytest` green; `test_engine.py` 42 tests passing | gate |
| 9 | `engine.stats()` smoke test demonstrating the new API | new test |
| 10 | `serde9` regression test for the nested-MixedList fix | new test |
| 11 | `CLAUDE.md` project state refreshed | edit |
| 12 | `docs/sim/sprint_2_retro.md` | new (post-sprint) |
| 13 | `docs/sim/cadence_metrics.md` row 2 | edit (post-sprint) |
| 14 | `docs/sim/sprints_index.md` Sprint 2 → "Wrapped (awaiting ratification)" | edit (post-sprint) |
| 15 | mdata communication summary (one-liner; user delivers) | text |

---

## Lead allocation

**Coordinator-solo (main Claude) for the cherry-picks themselves** — implementation work is sequential by nature (each cherry-pick depends on the previous one's resolution); subagent dispatch would not parallelize meaningfully.

**`code-reviewer` subagent at sprint wrap** for an independent diff review of the staged commits before pre-commit gate. Single dispatch; expected to surface any obvious regression or risky change. Budget ~1pp.

**No worktrees** — sequential cherry-picks on `claude` directly. If a single cherry-pick goes wrong, `git reset --hard HEAD~N` rolls back (per the iteration_lessons.md "hard rollback > manual revert" entry from the prior sprint — this is exactly the case it's written for).

**SHUTDOWN_SIGNAL discipline** — same as Sprint 1: check before each major phase (Part A, B, C, D, E). Watchdog daemon writes the signal at 5h ≥ 90%; we're starting from ~35% post-Sprint-1.

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed (~3-5pp into the sprint, after Parts A and B land):

- Part A (b20177c) — clean cherry-pick? Did it land without conflict resolution > 1pp?
- Part B (serde9 surgical extraction) — did the patch trim cleanly to serde9.rs only? Any leakage into pub/sub territory?
- Part C status — started or not?
- Test gate state — green?
- ETA to wrap.

Halt-and-escalate criteria:

1. **Scope-blowing bug** — discovered issue would push actual-pp >150% of predicted (>13.5pp). Most likely failure: `b20177c` conflict resolution touches the FFI-rewrite surface (`08fe588` content) in a way that requires reasoning about three commits' semantics simultaneously. If conflict surface > 30 lines per file, halt.
2. **Plan-pivot finding** — sprint premise contradicted by mid-sprint discovery. Most likely: the surgical serde9 extraction is impossible because the bug fix is entangled with pub/sub-shaped helpers in upstream's diff. If the patch can't be cleanly trimmed, escalate to user — Option (a) take the full `7948744` (advancing Sprint 4 work into Sprint 2) vs Option (b) defer the serde9 fix entirely until Sprint 4.
3. **User-decision needed** — mostly already addressed in this brief (Option-c-with-measured-retirement, naming hold, overwrite separate-fn preservation). If a new architectural choice surfaces (e.g. b20177c assumes upstream's `chili-sauce` package layout), halt.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 15pp. Starting from ~35%; budget should have 65pp of headroom; predicted Sprint 2 ≤ 9pp; should never trigger this. **If it does trigger, something has gone seriously wrong** — full halt + WIP commit + cron resume per `~/.claude/rules/shutdown-protocol.md`.

State current 5h-pp delta + absolute % at every checkpoint and at wrap.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test`. **Should run cleanly** since prior `chore(clippy)` cycles closed all known lint sources; new code may surface fresh ones — if so, fix in a follow-up commit, don't suppress.
- Python-bindings wrap: `cd crates/chili-py && uv run maturin develop --release && uv run pytest`. All 86+ tests must pass.
- Bench delta documented IF any of (parse_cache, scan, eval, load_par_df, write_partition) hot paths were touched. Sprint 2 should NOT touch them, but verify with `cargo bench` if any test_engine.py tests reveal regression.
- Test-count delta documented in retro: probably +42 (b20177c's test_engine.py) + 1 (stats smoke test) + 1 (serde9 regression test) = +44 vs Sprint 1's baseline. Verify exact count.
- Author retro at `docs/sim/sprint_2_retro.md` per template.
- Append row to `docs/sim/cadence_metrics.md`.
- Update `docs/sim/sprints_index.md` to "Wrapped (awaiting ratification)".
- Promote any high-cost incident lesson to `docs/standards/iteration_lessons.md`.
- Compose mdata communication summary.
- HALT until user ratifies.

---

## Pp accounting reference

**Sprint 1 actuals as the calibration anchor:**

- Sprint 1 was research-heavy: ~25pp predicted, ~25pp actual. Three subagents at ~5-7pp each + main thread ~6pp.
- Sprint 2 is implementation-heavy with NO subagent dispatch except `code-reviewer` at the end. Main-thread cherry-pick work in Sprint 1's Part D was ~2pp; Sprint 2's three cherry-picks should each be similar (~1-2pp each per cherry-pick + resolution + smoke test) plus build + retro overhead.

Sprint 2 expected at the **mid-band of 5-9pp**, capped above by:
- Conflict resolution depth on `b20177c` (Part A) if the FFI rewrite touched the same lines (low-probability per inventory §2.4).
- Surgical patch trim on serde9 (Part B) if it doesn't extract cleanly (medium-probability per inventory §2.6).

If actuals come in materially under (≤4pp), the calibration data point says implementation sprints are cheaper than research; if materially over (≥12pp), the conflict surfaces were uglier than the inventory predicted.

The cherry-pick error budget is reduced from Sprint 1's ~5pp (subagent failures) — main-thread work doesn't have that failure mode. Replace it with a "git cherry-pick reset budget" of ~1-2pp for a worst-case `git reset --hard` if a cherry-pick goes badly mid-sequence (the iteration_lesson is in place for exactly this scenario).

---

## Cross-references

- Roadmap: [`roadmap_2026-05-06.md`](roadmap_2026-05-06.md) — Sprint 2 row.
- Inventory: [`../research/main_vs_claude_inventory_2026-05-06.md`](../research/main_vs_claude_inventory_2026-05-06.md) — §2.4 (b20177c clean), §2.6 (serde9 extraction from 7948744), §2.3 (3aeee62 partial pickup), §4 verdicts.
- Synthesis: [`../research/competitive_position_2026-05-06.md`](../research/competitive_position_2026-05-06.md) — strategic frame.
- mdata wishlist (the operational asks this sprint addresses): `~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md` Sections 2 (commits), 3 (validation gate), 5 (what mdata will build with the rebuild).
- Cadence rule: [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md).
- Shutdown protocol: `~/.claude/rules/shutdown-protocol.md`.
- Iteration lessons (load-bearing for this sprint): `../standards/iteration_lessons.md` — the "hard rollback" lesson is the canonical recovery strategy if a cherry-pick goes wrong.
- Project memories: `project_chili_vision`, `project_chili_branch_model`, `project_chili_naming_watch`.
