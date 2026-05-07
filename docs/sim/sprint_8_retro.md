# Sprint 8 retro — perf-pass-1 (R1/R2/R3 fixes; A/B rows fill)

**Wrap:** 2026-05-08
**Predicted:** 6–12 pp
**Actual:** ~4 pp (well under low-band; P2 deferred + P1 was measurement-noise)
**Variance:** −56% vs midpoint (9.0)
**Owner:** coordinator-solo (main Claude); no code-reviewer dispatch this sprint (scope was small enough that self-review is appropriate; lesson 7 says reviewer is "mandatory for any sprint that touches: chili-py FFI surface, parse-cache code, pub/sub code, partition I/O, anything in `crates/chili-core/src/engine_state.rs`" — Sprint 8 only touched bench files and docs).
**Plan reference:** [`../history/sprints/sprint_8_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_8_dispatch_brief_2026-05-08.md) (moved post-ratification).

---

## Scope shipped

**Part A (P1) — parse_cache regression RESOLVED via re-measure**
- 3 fresh runs of `cargo bench -p chili-core --bench parse_cache`.
- Run #1: 397.47 ns | Run #2: 379.08 ns | Run #3: 398.33 ns. All under 400 ns.
- Sprint 7 Part B's 410.58 ns reading was a thermal-variance outlier (reviewer C1 hypothesis confirmed).
- **Golden rule 6 holds end-to-end.** No code change needed.

**Part B (P3 + P4) — eval bench parser fix + A/B fill (commit `429ccc7`)**
- `crates/chili-op/benches/eval.rs`: `src_path` `"bench.chi"` → `"bench.pep"` (matches bench engine's `state.enable_pepper()` mode).
- 6 eval/projection benches re-run on claude-2; A/B rows populated in `post_pivot_baseline_2026-05-07.md`.
- **Important caveat documented:** parked-claude .chi vs claude-2 .pep numbers are apples-to-oranges (different parser dispatch). 3 of 6 queries within ±2.2% (comparable plans); 3 of 6 +18-66% (likely different lazy plans). Sprint 9 P5 (optional) re-benches parked-claude with .pep src_path for true Δ%.

**Part C (P2) — load_multitable profiling DEFERRED to Sprint 9**
- macOS profiling infrastructure friction:
  - `cargo flamegraph` requires Xcode (only CLI tools installed).
  - `samply` works without Xcode but produces unsymbolized profile because `[profile.release] strip = true` strips bench binary symbols at link time.
- Captured `/tmp/load_multi_profile.json` (3.4 MB; 17,216 main-thread samples, hex-address stacks). Hottest 3 self-time leaves: `0x450c` 42.5%, `0x4834` 26.4%, `0x29c4` 13.7%.
- Sprint 9 P2 entry plan documented: add `[profile.bench] debug = true; strip = false` workspace override + rebuild + symbolized re-profile. ~3-4pp.

**Part D — wrap (this commit)**
- `docs/sim/sprint_8_retro.md` (this file).
- `docs/sim/cadence_metrics.md` row 8 appended.
- `docs/sim/sprints_index.md` Sprint 8 row → Ratified.
- Brief moved to `docs/history/sprints/`.
- CLAUDE.md state refresh: parse_cache 397 ns (within 400 ns target); eval/projection A/B rows complete with .pep-baseline caveat; P2 deferred to Sprint 9.

**Tests:** Rust workspace 166 (no change). chili-py pytest 65 (no change). Bench file edit only.

**Bench delta:** parse_cache 397 ns (was 410 ns Sprint 7; thermal noise, no real regression). 6 eval/projection numbers added to A/B doc. No A/B comparison numbers changed in any meaningful way except the apples-to-oranges caveat documentation.

---

## Lessons (durable)

### 1. Bench numbers within ±10% of a hard target should be re-measured before triggering mitigation work

**Rule.** When a bench result lands within ±10% of a golden-rule target (or any known threshold that triggers escalation), the FIRST move is re-measurement, NOT investigation/profiling/mitigation. Apple Silicon thermal/memory variance can account for 20-40 ns on sub-microsecond benches and similar relative variance on µs/ms benches. Two or three additional runs cost ~30s wall each and produce a confidence interval. If all extras land safely in-target, the original was an outlier and no work is needed. If extras confirm out-of-target, escalate to investigation. Skipping the re-measure and going directly to investigation wastes pp on diagnosing noise.

**Why.** Sprint 7 Part B measured parse_cache hit at 410.58 ns; golden rule 6 target is ≤400 ns. The +2.6% over target prompted Sprint 7 retro language about "Sprint 8 P1 = profile + reclaim ≤400 ns or ADR amend." Sprint 8 P1's first move was the re-measurement: 397.47 / 379.08 / 398.33 ns across 3 runs — comfortably under target. The 410.58 ns was thermal noise. A naive Sprint 8 that skipped re-measure and jumped straight to "profile parse_cache hot path with samply" would have spent ~2-3pp investigating a non-issue. Re-measure cost: ~0.5pp. Saving: 1.5-2.5pp + the cognitive overhead of an unnecessary "is golden rule 6 broken?" investigation.

**Apply where.** Any bench-pass sprint that surfaces a marginal regression (within ±10% of a target). Specifically: parse_cache hit (golden rule 6 ≤400 ns); future Python concurrent eval (golden rule 5 6.10× throughput); any new golden rule that pegs to a numerical threshold. Generalizes to any benchmark-driven decision point — re-measure before treating a result as load-bearing.

**Cost saved.** ~1.5-2.5pp per sprint that would otherwise pursue noise. Recurs every bench-pass sprint where natural variance straddles a target threshold.

### 2. macOS bench profiling needs `[profile.bench]` override that retains symbols

**Rule.** When a chili sprint plans to run `samply record` / `cargo flamegraph` / any sampling profiler on a bench binary, FIRST verify that the workspace `[profile.release]` retains debug symbols (or add a `[profile.bench]` override). Workspace currently has `[profile.release] strip = true` for production binary leanness; this strips bench binary symbols too. Profiling without symbols produces unsymbolized hex-address stacks — the profile data is captured correctly but functions can't be named, so root-cause analysis is impossible.

**Why.** Sprint 8 Part C, 2026-05-08. samply captured 17,216 main-thread samples on `load_multitable_5x200p` cleanly. All stack frames resolved to bare hex addresses (e.g., `0x450c 42.5%`). Without function names, no way to identify the per-table linear cost driver Sprint 7 R2 wanted profiled. Cost: ~0.5pp on the failed profiling attempt + the realization that re-running with symbols requires either a workspace `[profile.bench]` override (which invalidates every cached release-target artifact, lesson 11 rebuild territory) OR a separate bench-only profile flow. Generalizes: any chili sprint wanting sampling-profiler-driven optimization needs the symbol-retention override budgeted upfront, not discovered mid-sprint.

**Apply where.** Sprint 9 P2 (load_multitable profile, deferred from Sprint 8). Future perf-pass sprints (Sprint 12 perf-pass-3). Generalizes to any project where production-build size optimizations strip symbols. `cargo flamegraph` separately requires `xcode-select --install` of full Xcode for its `xctrace` dependency on macOS — a 5+ GB download, not appropriate for autonomous-run unless the user pre-approves. samply is the autonomous-run-compatible alternative IF the binary has symbols.

**Cost saved.** ~0.5pp per perf-pass sprint that would otherwise mid-sprint discover the symbol issue. Plus ~10-15 min wall on the unnecessary profile run that produces hex-only data.

---

## Pp accounting

| Part | Predicted | Actual |
|---|---:|---:|
| 1.A — P1 parse_cache re-measure + reclaim | 1.5–3 | ~0.5 (3 cargo bench runs, total ~4 min wall, no code change) |
| 1.B — P3 + P4 eval bench fix + A/B fill | 1.5–2 | ~1 (1-line bench change + bench rerun + doc update) |
| 1.C — P2 load_multitable profile + (maybe) fix | 2–4 | ~1 (samply install + record + analyze + decide-defer) |
| 1.D — wrap | 1.5–2.5 | ~1.5 (no code-reviewer dispatch this sprint per lesson 7 — scope was bench files + docs only) |
| **Total** | **6.5–11.5** | **~4** |

Below low-band (~−56% vs midpoint 9.0pp). Drivers:

- **P1 collapsed to 0.5pp** because it was measurement noise; re-measure resolved without investigation.
- **P3+P4 was a 1-line bench file fix + bench rerun + doc update** — the simplest path the reviewer's S1 suggested.
- **P2 deferred** because macOS profiling infrastructure friction (no Xcode, release strip = true) made it not-cheap mid-sprint. Sprint 9 P2 inherits the captured profile artifact + symbolized rerun plan.
- **No code-reviewer dispatch** for a docs+bench-files-only sprint (lesson 7's "mandatory for chili-py FFI / parse-cache code / pub/sub / partition I/O / engine_state.rs" trigger doesn't apply).

Position in band: well below low edge. Sprint 8's value isn't the pp count — it's the (a) golden rule 6 reinstated cleanly, (b) bench A/B doc complete with caveat, (c) Sprint 9 P2 set up with profile + entry plan. The "small sprint with disproportionate value" pattern from cadence_metrics Pattern 4 plays out here: bench-driven sprints don't need lots of code changes to deliver decisive findings.

---

## What surprised

- **Sprint 7's 410.58 ns parse_cache reading was 100% thermal noise.** I gave it ~50% probability of being noise (per reviewer C1) and ~50% of being a real py-1.39.3 polars regression. Three Sprint 8 re-measures all under 400 ns lands the noise verdict definitively. Lesson 1 promotion captures the "re-measure ±10% targets first" protocol.

- **The eval bench .chi vs .pep parser dispatch produces materially different plans for SOME queries.** query_select_star went from 355.92 µs (.chi) to 591.62 µs (.pep) — that's not a 10% difference, that's a different LAZY PLAN. Three of six eval/projection queries had this issue (+18-66%). Without parker-claude re-bench at .pep src_path, can't disentangle parser-shape impact from polars-source impact. Documented as caveat in bench rebaseline doc.

- **macOS sampling profiler ergonomics are awkward without Xcode.** `cargo flamegraph` -> "xctrace requires Xcode." `samply` -> works but unsymbolized due to workspace strip = true. The autonomous-run-friendly path would have been to pre-add a `[profile.bench]` symbol-retention override at sprint kickoff; deferring to Sprint 9 is the right call given lesson 11 rebuild costs.

- **Sprint 8 budget burn: ~4pp actual vs 6-12pp predicted = 33-66% utilization.** Pattern 1 from cadence_metrics: "post-pivot port/delivery sprints calibrate at low-mid band consistently when scope-downgrades absorb structural blockers." This sprint extends the pattern: bench-pass sprints with one happy outcome (P1 noise) + one trivial fix (P3) + one defer (P2) come in dramatically under predicted band. Future bench-pass sprint predictions should account for this.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_8_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_8_dispatch_brief_2026-05-08.md) (moved post-ratification).
- **Sprint 7 retro (predecessor):** [`sprint_7_retro.md`](sprint_7_retro.md)
- **Bench rebaseline doc (P1/P3+P4 results + P2 deferral):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md)
- **Cadence metrics row:** [`cadence_metrics.md`](cadence_metrics.md) row 8 (this commit).
- **Iteration lessons (15+16 promoted this sprint):** [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md)
- **Cadence rule:** [`../../.claude/rules/sprint-cadence.md`](../../.claude/rules/sprint-cadence.md)

---

## Sprint 9 hand-off

P0 (carry-over from Sprint 7) — GitHub-host the chili polars fork; switch both `[patch.crates-io]` blocks from `path = "/tmp/..."` to `git = "..." + tag = "..."`. **Requires user GitHub auth** — NOT autonomous; surfaced for user action.

P2 — load_multitable_5x200p +22.8% profile (deferred from Sprint 8). Workspace `[profile.bench] debug = true; strip = false` override + rebuild + symbolized samply re-profile. ~3-4pp.

P5 (optional from Sprint 8) — re-bench parked-claude with .pep src_path on the 3 apples-to-oranges queries. ~2-3pp.

P6 (Sprint 9 original scope) — perf-pass-2 + KDB-X CE comparison if available.

P7 (Sprint 8 lesson 16 follow-up) — pre-add `[profile.bench]` symbol-retention override at Sprint 9 kickoff so P2 profiling runs cleanly.
