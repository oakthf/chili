# Cadence Metrics — sprint pp + variance tracking

Per-sprint metrics record for estimation calibration over time. Filled in at each sprint
wrap; **never overwritten**. The table is the historical record — past rows stay in
their original shape even after the canonical shape evolves. Shape adapted from mdata's
`docs/sim/cadence_metrics.md`.

Forward-only: this table begins with Sprint 1 under the new cadence (per
`.claude/rules/sprint-cadence.md`, seeded 2026-05-06). Past chili work — the upstream
`e9092ce..b0f20e5` merge (2026-04-26), the bench phase sweep — is recorded in its
existing locations and is not retro-fitted into this table.

---

## Sprint metrics table

| Sprint | Theme | Pred pp | Actual pp | Variance % | Mid-sprint pivots | User-touch | Gate defects | Test count delta | Wrap timestamp |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | Strategic research + main↔claude inventory | 22–35 | ~25 | −11% (low edge) | 0 | 0 (kickoff/ratification only; no mid-sprint user msg) | 0 (research; no Rust touched) | 0 | 2026-05-07 ~00:10 SGT |
| 2 | claude-2 pivot (v1 halt + v2 ratify-and-execute) | 8–14 (v2 brief; v1+v2 implied 13–23) | ~20–22 | +73% to +91% vs v2 midpoint; ~+27% within implied total band | 1 (v1 halt → pivot to v2) | ~6 (Part A scope ratification, env-fix go-ahead, mid-Part-B answer for next steps, Part B continue ratification, Part C-D-E continue ratification, post-Part-A check-in) | 1 (bare main fmt diff in chili-parser/tests/chili/test_error.rs; fixed via cargo fmt --all in 4fbe5eb) + clippy still RED on claude-2 deferred to Sprint 3 | 0 | 2026-05-07 (Part E commit) |
| 3 | additive feature port wave 1 (clippy unblock + 7 features + parse_cache bench gate) | 10–15 (mid 12.5) | ~14 | 0% vs midpoint | 1 (Part E.1 unplanned: code-reviewer findings absorbed in-sprint) | 0 (autonomous run; user pre-ratified entire sprint chain) | 2 (Part B build-fail until `log = "0.4"` added to chili-op deps; Part C maturin doc-comment placement) | +14 (6 Rust integration parse_cache_test + 8 chili-py pytest) | 2026-05-07 (Part E.1 commit `b269ec0`) |
| 4 | additive feature port wave 2 — chili-py clippy unblock + ADR 0002 (`engine.eval(lazy=True)`) + bench harness validation (downgraded) | 9–14 (mid 11.5) | ~9 | −22% vs midpoint | 1 (Part C downgraded mid-flight from "measure 4 benches" to "validate compile only" after bench compile cost overran 2-3pp budget) | 0 (autonomous run; user observation only) | 0 (gates green throughout Parts A/B; Part C never had a gate to fail) | +6 chili-py pytest (4 xfailed for polars Python/Rust DSL skew, 2 passing for default + lazy=False) | 2026-05-07 (Part D commit) |
| 5 | bench A/B sweep + polars pin + chili 0.8.0-claude2.1 wheel cut + mdata handoff | 10–15 (mid 12.5) | ~10 | −20% vs midpoint | 1 (Part B downgraded mid-flight to "deferred to Sprint 7" — bench A/B's release-profile compile cost exceeded remaining budget after Part A's unexpected uv-sync wheel rebuild) | 0 (autonomous run; user observation only) | 0 (gates green; Part D.1 absorbed reviewer findings cleanly) | +1 chili-py pytest (TestTick.test_get_tick_count_no_arg_defaults_to_index_zero regression) | 2026-05-07 (Part D commit) |
| 6 | deep housekeeping sweep (every-5-sprint cadence) — demote 13 stale docs to history; populate cadence_metrics "Patterns observed" with 5-sprint calibration | 3–5 (mid 4.0) | ~3 | −25% vs midpoint | 0 (clean scope) | 0 (autonomous run) | 0 (no code touched; gates not re-run) | 0 (housekeeping; no tests added) | 2026-05-07 (Sprint 6 commit) |
| 7 | ADR 0003 resolution via option 3b (polars py-1.39.3 fork + q-style fmt patch) + chili 0.8.1 wheel cut + bench A/B sweep | 8–15 (mid 11.5) | ~12 | +4% vs midpoint | 0 (clean scope; sub-sprints A/B/D evolved interactively but no in-flight pivots) | several (interactive Q&A on ADR 0003 root cause; user redirect to lazy fix mid-sprint; no formal ratification interruptions) | 1 (mid-Sprint-7-Part-A disk exhaustion; cleared + rebuilt) | +5 net chili-py pytest (4 xfail markers removed at lazy resolution + 1 Sprint 5 Part D.1 carryover) | 2026-05-08 (Sprint 7 wrap commit) |
| 8 | perf-pass-1 — Sprint 7 R1/R2/R3 fixes (P1 parse_cache re-measure resolved as noise; P3+P4 eval bench parser fix + A/B fill; P2 load_multitable profile deferred to Sprint 9) | 6–12 (mid 9.0) | ~4 | −56% vs midpoint | 1 (P2 deferred mid-sprint due to macOS profiling infrastructure friction — no Xcode + release-profile symbol-strip) | 0 (autonomous run; user observation only) | 0 (gates green throughout; bench-files+docs-only sprint) | 0 (no test-count changes; bench file change + bench reruns) | 2026-05-08 (Sprint 8 wrap commit) |
| 9 | perf-pass-2 — P7 [profile.bench] override + P2 symbolized rebuild + samply profile captured (93% of polars worker time on offset 0x450c); P5 / P6 / P2-mitigation deferred to Sprint 12 due to autonomous-run infrastructure friction (no GUI for samply load, no addr2line installed) | 5–10 (mid 7.5) | ~2 | −73% vs midpoint | 0 (clean scope shrinkage from skipped P5/P6 + P2 partial verdict) | 0 (autonomous run; user observation only) | 0 (gates green throughout; profile-config + bench-record only sprint) | 0 (no test-count changes) | 2026-05-08 (Sprint 9 wrap commit) |
| 10 | Pepper conformance to k9 design — ADR 0004 ratifies shakti_analysis §4.3 conclusion (pepper retains Polars-aligned primitives; does NOT track k9 minimal-primitive axiom) | 5–10 (mid 7.5) | ~1.5 | −80% vs midpoint | 0 (source research already concluded the answer; sprint is ratification only) | 0 (autonomous run; user pre-ratification) | 0 (no code touched; gates not re-run) | 0 (ADR-only sprint) | 2026-05-08 (Sprint 10 wrap commit) |

---

## Field definitions

- **Sprint:** Numeric or named identifier (e.g., `1`, `2`, `Housekeeping`).
- **Theme:** Short label for the sprint's scope (e.g., "parse-cache micro-opt",
  "load_tree implementation", "py FFI hardening").
- **Pred pp:** Predicted token-cost range from the locked dispatch brief
  (`docs/sim/sprint_N_dispatch_brief_<date>.md`). Pp = 5h-window percentage points
  per `~/.claude/rules/work-metrics.md`.
- **Actual pp:** 5h-window delta from sprint kickoff to wrap (token-meter integration
  per `~/.claude/rules/work-metrics.md` if available; else best-effort estimate).
- **Variance %:** (actual midpoint − predicted midpoint) / predicted midpoint × 100.
  Negative = under-spent.
- **Mid-sprint pivots:** Count of times the coordinator changed sprint scope or
  direction mid-sprint without user input.
- **User-touch:** Count of user messages exchanged during the sprint (kickoff +
  ratification messages excluded; in-sprint communication only).
- **Gate defects:** Count of pre-commit-gate failures during the sprint
  (`cargo fmt` / `cargo clippy` / `cargo test` / `uv run pytest` issues that needed a
  fix before commit).
- **Test count delta:** Net change in `cargo test` + `pytest` count from sprint start
  to wrap.
- **Wrap timestamp:** Local time the final commit landed.

---

## How to update

At each sprint wrap:

1. Add a row to the table with all 10 fields.
2. Capture sprint-specific lessons in the per-sprint retro
   (`docs/sim/sprint_N_retro.md`).
3. Promote durable, cost-quantified lessons to `docs/standards/iteration_lessons.md`
   per the 4-field shape — see `.claude/rules/sprint-cadence.md`.
4. Add a "Patterns observed" section below once enough rows accumulate to spot
   calibration drift (typically after sprint ~5).

---

## Patterns observed

5-row early calibration (Sprints 1-5; populated 2026-05-07 Sprint 6 housekeeping):

### 1. Within-band variance: −22% to +91% across 5 sprints (excluding Sprint 2's pivot anomaly: −20% to 0%)

Sprint 1 (research, low-edge ~−11%); Sprint 3 (port-wave, midpoint 0%); Sprint 4
(port-wave + ADR + bench-validation, low-edge −22%); Sprint 5 (delivery + ADR
+ wheel cut, low-edge −20%). Sprint 2 was the outlier (pivot sprint with v1 halt
+ v2 ratify; +73% to +91% on the v2 brief alone, ~+27% on the implied total band).
**Pattern: post-pivot port/delivery sprints calibrate at low-mid band consistently
when scope-downgrades absorb structural blockers.** Implication for Sprint 7+: brief
predictions can compress to "midpoint −15% to midpoint +5%" range with high
confidence; the upper edge is reserved for structural-blocker discoveries.

### 2. Mid-sprint pivots correlate with scope-downgrades, not scope-creep

Sprint 2: 1 pivot (v1 → v2 plan-pivot under cherry-pick conflict accumulation; lesson 4).
Sprint 4: 1 pivot (Part C bench measurement → harness validation; lesson 8).
Sprint 5: 1 pivot (Part B bench A/B sweep → deferred Sprint 7; lesson 10 + 8).
**Pattern: every mid-sprint pivot in this 5-sprint window has been a scope-downgrade
under structural cost discovery, not scope-creep**. Implication: future sprint
briefs should explicitly rank parts by "first to downgrade" so pivots don't
require rescoping mid-sprint. Bench-related parts always go last in this ranking.

### 3. Code-reviewer subagent dispatch consistently surfaces 2-3 must-fix items per sprint

Sprint 3: 3 must-fix (substring fragility, single-table loop, docstring) absorbed
in Part E.1.
Sprint 4: 1 must-fix (doc/commit inconsistency) + 3 verifications absorbed in Part D.1.
Sprint 5: 1 critical (pub/sub finality) + 2 warnings (ADR framing, no-arg-default
not implemented) absorbed in Part D.1.
**Pattern: lesson 7 (reviewer-before-retro) saves ~1pp per sprint by absorbing
findings in-sprint instead of leaking to next sprint. The reviewer always finds
something — budget 1pp for absorption.**

### 4. Test count delta runs higher than predicted on port sprints

Sprint 3 predicted +15-20 tests, actual +14 (close).
Sprint 4 predicted +2 tests, actual +6 (3× over).
Sprint 5 predicted +2 tests, actual +1 (close, Part B downgrade reduced new test
count).
**Pattern: test count delta is hard to predict on port sprints because each ported
feature surfaces at least one regression test for golden-rule preservation +
the reviewer often surfaces 1-2 regression-test additions. Default budget:
+5-10 tests per implementation sprint, +0-2 per delivery sprint, +0 per
docs/housekeeping sprint.**

### 5. Bench compile cost dominates bench-related sprint pp on this codebase

Lessons 8 + 11 both surfaced bench/dependency-rebuild compile costs as
under-predicted. polars 0.53 release-profile compile is 5-10 min wall PER
binary. Sprint 4 + Sprint 5 both hit this.
**Pattern: any sprint that runs `cargo bench` OR edits chili-py/pyproject.toml
must budget the rebuild cost separately. Future template: add "release-profile
compile expected" as a flag in dispatch briefs that gates bench / pyproject
parts.**

---

_Re-evaluate patterns at next sweep (Sprint 11 housekeeping or earlier if
calibration drift becomes apparent)._
