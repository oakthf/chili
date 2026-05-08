# Sprint 13 retro — `load_par_df` hot path optimization (REVERTED — 0pp gain)

**Wrap:** 2026-05-09
**Predicted:** 9–13 pp
**Actual:** ~3 pp
**Variance:** −67% vs midpoint (11)
**Owner:** coordinator-solo (main Claude); no code-reviewer dispatch (changes reverted before reaching reviewer step).
**Plan reference:** [`../history/sprints/sprint_13_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_13_dispatch_brief_2026-05-08.md) + audited proposal `docs/proposals/perf_alternatives_post_mimalloc_2026-05-08.md` §A.4.

---

## Wrap status: REVERTED — no code change shipped

**Binary success criterion (from brief):** `load_multitable_5x200p ≤ 1.65 ms`. **Result: 1.9264 ms (FAILED).**

Per brief rollback criterion ("if a step bench-gates < 5pp gain on its target bench, revert before proceeding"), Part A.1 was reverted. Sprint 13 ships **zero code change** to `claude-2`.

The dispatch brief at commit `77ab60c` remains in place as audit trail; the implementation commit `55e2ba2` was hard-reset.

---

## Sprint structure

This sprint is a one-arc execution with two bench iterations:

1. **Part A.1 v1 (regressed)** — applied A.2.6 + A.2.8 + P1.3, ran bench. Found a **+60% regression** across all three load benches. Root cause: my `enumerate_dir` helper eagerly materialized a `Vec<(PathBuf, String, bool)>` calling `entry.metadata()` (stat syscall) per entry, replacing the old code's lazy `any()` short-circuit.
2. **Part A.1 v2 (fixed)** — replaced the eager enumeration with a lazy scan-and-accumulate pattern (break early on first partition file found; otherwise accumulate for namespace recursion). Re-bench showed regression recovered to baseline ±5% — but **no net gain** on the target bench.
3. **Halt and revert** — applied brief halt criterion 2 (plan-pivot finding) + rollback criterion (<5pp gain).

Part B (polars feature-flag audit) was completed as audit-only — no code change. `simd` requires nightly Rust + release rebuild; `streaming` is already implicitly active via chili-op's `ipc` feature. Documented as future Sprint 14+ ADR territory.

---

## Scope shipped

**Code:** none. All changes reverted via `git reset --hard HEAD~1` post-bench.

**Docs:**

- `docs/sim/sprint_13_dispatch_brief_2026-05-08.md` (commit `77ab60c`) — Sprint 13 brief, retained for audit trail.
- This retro.

**Tests:** unchanged. Pre-Sprint-13 baseline: 166 Rust + 65 chili-py pytest. Post-revert: identical.

**Bench delta:** 0 (revert restored baseline).

**Polars feature flag audit (Part B) findings (no action this sprint):**

- `simd` is opt-in for polars-core (`simd = ["arrow/simd", "polars-compute/simd"]`). polars's `nightly` feature transitively pulls in `polars-arrow/nightly` — likely needs nightly Rust toolchain. Not in Sprint 13 scope.
- `streaming` is not a top-level polars feature; what exists is `new_streaming` which is auto-enabled by `ipc`, `json`, `scan_lines` features (all already on in chili-op). No change needed.
- The audited proposal's framing of A.2.3 as "compile-time only, no code change" was overly optimistic about toolchain implications.

---

## Lessons (durable)

### 1. Don't replace lazy `any()` short-circuit with eager enumeration

**Promotion candidate.** Iterator-based `any()` over `fs::read_dir().filter_map(|e| e.ok())` short-circuits as soon as a match is found — typically 1 metadata syscall per directory. Eagerly materializing the iterator into `Vec<(PathBuf, String, bool)>` (calling `entry.metadata()` per entry) replaces that with N metadata syscalls. For a 2000-entry directory, that's a 2000× overhead specifically for the partition-file detection step.

**Why.** Sprint 13 Part A.1 v1 regressed `load_cold_2000p` from 5.02 ms → 8.10 ms (+60%) because `enumerate_dir` eagerly stat-syscalled every entry just to populate a `Vec` whose first element was checked by `any()`. The lazy fix (scan iterator + break early) recovered to baseline ±5%.

**Apply where.** Any refactor that replaces an iterator-based predicate (`.any()`, `.all()`, `.find()`) with an intermediate `.collect::<Vec<_>>()`. Especially load-bearing in directory traversal, parse_cache lookups, and any code that does `metadata()` / `to_string_lossy()` per entry.

**Cost saved.** ~1pp wasted on the v1 regressed bench round (compile + bench + diagnose). Future-occurrence estimate: same per recurrence. Worth promoting to a durable rule once the second occurrence happens (or earlier if the cost-attributability is clear; arguable now).

### 2. Speculative optimization claims need profile-evidence verification

**Already covered by `~/.claude/rules/verify-before-claim.md` + project memory `feedback_speculation_pattern.md`.** Sprint 13 is the first execution under those rules, and the rules worked: I flagged P1.1 as unverified in the brief + audit (per `verify-before-claim`). The bench result confirms: P1.1's premise (chili-side schema-batch reduces the regression) was not borne out empirically.

**Forward implication.** Sprint 14+ perf-pass items must pre-quantify "what fraction of the target bench's runtime is attributable to the proposed-fix surface area" via profile, not via inference. The Sprint 12 P2 partial profile gave 17.7% main-thread Box::new — but that 17.7% is small relative to the 93.1% polars-internal worker kernel that dominates the regression. Targeting the smaller share couldn't recover the larger source.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---:|---:|
| Brief authoring + commit | 1.0 | ~1.0 |
| Baseline bench capture | 1.0 | ~0.5 (already in flight; mostly wall-time) |
| Part A.1 v1 implementation (A.2.6 + A.2.8 + P1.3) | 4–6 | ~1.0 |
| Part A.1 v1 bench → REGRESSION discovered | 0.5 | ~0.5 |
| Part A.1 v2 lazy-scan fix | 0.5 | ~0.5 |
| Part A.1 v2 re-bench → no gain on target | 0.5 | ~0.5 |
| Part B feature-flag audit (no code change) | 0.5–1 | ~0.3 |
| Revert + retro + cadence row + sprints_index | 1–2 | ~1 (this) |
| (Skipped: P1.1 batch schema reads, code-reviewer dispatch, P0/P3.2/P3.4 — out of scope) | (3–5 of buffer) | 0 |
| **Total** | **9–13** | **~3** |

Below low-band (~−67% vs midpoint 11.0). Driver: bench result's plan-pivot finding triggered halt at Part A wrap.

Position in band: well below low. Pattern continues: Sprint 13 fits the autonomous-run perf-pass shape (Sprints 8/9/10/12: median ~3pp actual against 5–12pp predicted; cadence_metrics pattern 6).

---

## What surprised

- **The audit appendix's caution on P1.1 was load-bearing.** I cited the audit's "the +22.8% regression is dominated by polars-internal worker kernels" in the brief itself, then proceeded with the implementation. The bench result is exactly what the audit predicted: chili-side optimizations don't reach the polars-internal share. The audit was right; I should have heeded it more strongly.
- **My v1 regression was caused by replacing iterator `.any()` with eager `Vec` collection** — a textbook anti-pattern, and exactly the kind of mistake that gets caught by careful code review. Caught it via bench measurement (lesson 7 reviewer-before-retro would also have caught it pre-bench, but I didn't dispatch reviewer for the v1 scope).
- **The fix (lazy scan + break + accumulate-on-no-match)** is genuinely a correctness improvement (single dir-open in namespace-heavy HDB layouts) but doesn't move the bench needle on the bench fixtures (which have no namespaces). The bench corpus doesn't reflect mdata's real workload. Could matter on real HDBs but unverified — not shipping speculative correctness wins.
- **Sprint 13 was the first session to operationally apply the new `verify-before-claim.md` + `self-audit-on-plans.md` rules.** The discipline worked: I flagged P1.1 as unverified in advance + the bench verified. The forfeiture of "unverified speculative optimization" is exactly what the rules buy.

---

## Cross-references

- **Audited proposal:** [`../proposals/perf_alternatives_post_mimalloc_2026-05-08.md`](../proposals/perf_alternatives_post_mimalloc_2026-05-08.md) §A.1 (where I correctly flagged P1.1 as unverified).
- **Sprint 12 P2 partial symbolization:** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 12 P2 partial symbolization" — the 17.7% main-thread Box::new + 93.1% polars-internal worker kernel attribution.
- **Cadence metrics row 13:** [`cadence_metrics.md`](cadence_metrics.md).
- **Sprints index:** [`sprints_index.md`](sprints_index.md).
- **Brief (now historical):** [`../history/sprints/sprint_13_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_13_dispatch_brief_2026-05-08.md) (post-ratification move).
- **Reverted commit:** `55e2ba2` (held in reflog locally; not in current branch tree).

---

## Sprint 14 hand-off

The audited proposal's recommended Sprint B (concurrent throughput) is now the highest-leverage no-blocker work:

- **P3.2b** — Release GIL on `load_par_df` (`chili-py/src/lib.rs:532` doesn't call `py.detach()` today; the write path already does). Concurrent multi-table loads from threaded Python callers — mdata's REST workers benefit.
- **A.2.2** — Release `vars` write-lock around heavy DataFrame ops in `upsert`/`insert` (`engine_state.rs:277-382`). mdata's RDB ingest path benefit.
- **A.2.4** — Parquet codec tuning. Small (~1-2pp); could fold opportunistically.
- **P3.4** — Categorical mapping cache + ADR. ADR territory.

Sprint 14 should explicitly ADD a profile-evidence prerequisite per Lesson 2 above: before the implementation phase, capture a profile of the target workload (concurrent throughput on threaded Python harness) to confirm GIL-hold is actually the bottleneck in the bench shape we're optimizing for. The Sprint 13 lesson is "don't optimize without profile evidence on the same shape we're benching."

The polars-internal kernel dominance (`0x450c` 93.1% of worker time) **remains unaddressed** post-Sprint-13 and is the single largest remaining perf opportunity. It requires the user-driven P0 (GitHub-host the polars fork) to land before Sprint C (PGO + symbolized polars rebuild) can act on it. Until then, chili-side optimizations are bounded to the small share of runtime they can reach.
