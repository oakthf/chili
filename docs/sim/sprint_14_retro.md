# Sprint 14 retro — release GIL on direct-FFI `load_par_df` + `clear_par_df` (P3.2b)

**Wrap:** 2026-05-09
**Predicted:** 5–9 pp
**Actual:** ~5 pp
**Variance:** −29 % vs midpoint (7) — at low band edge
**Owner:** coordinator-solo + `code-reviewer` subagent dispatch (Part C, lesson 7).
**Plan reference:** [`../history/sprints/sprint_14_dispatch_brief_2026-05-09.md`](../history/sprints/sprint_14_dispatch_brief_2026-05-09.md)

---

## Wrap status: BINARY SUCCESS — bench gate PASSED, reviewer ship-as-is

**Binary success criterion (from brief):** `concurrent_load_direct` N=4 ≥
12,000 calls/s on the chili-py bench fixture (Sprint 13.5 baseline:
4,841 cps flat × N ∈ {1,2,4,8}). **Result: 12,987 cps at N=4 — 8.2 %
above target, +168.3 % vs pre-change baseline.**

The post-change `concurrent_load_direct` shape now matches
`concurrent_load` (fn_call-released path) on every N ∈ {1,2,4,8} within
±1.5 %. Sprint 14 closed the FFI-symmetry gap.

---

## Scope shipped

**Code:**

- `crates/chili-py/src/lib.rs:531-548` (commit `<wrap>`) — wrapped
  `engine.load_par_df` + `engine.clear_par_df` in `py.detach(...)`.
  Added `Python<'_>` parameter to both methods (mirrors `eval`,
  `fn_call`, `get_var`, `import_source_path`). `hdb_path: &str` cloned
  to `String` for `'static` closure. Doc-comments reference the state
  audit.

**Docs:**

- `docs/bench/post_pivot_baseline_2026-05-07.md` — new "Sprint 14 —
  P3.2b implementation A/B" section with full A/B table.
- This retro.
- `docs/sim/cadence_metrics.md` — row 14 appended.
- `docs/sim/sprints_index.md` — Sprint 14 row → Wrapped.
- `CLAUDE.md` — state line refresh.

**Tests:** unchanged. Pre/post Sprint 14: 166 Rust + 65 chili-py pytest. ✓

**Bench delta:** see post_pivot_baseline_2026-05-07.md §"Sprint 14".

---

## Reviewer findings (Part C — `code-reviewer` subagent)

Dispatched per lesson 7 (FFI surface change). Findings:

| Item | Verdict |
|---|---|
| `Send` constraint on closure | OK — `to_owned()` clone makes `&str` into `'static String` |
| `&self` capture in `move` closure | OK — pattern matches `eval`, `fn_call`; `EngineState: Send + Sync` |
| Error propagation | OK — `py.detach(...)?; Ok(())` mirrors pre-change semantics |
| `Python<'_>` parameter | OK — pyo3 injects for free; consistent with siblings |
| `to_owned()` necessity | OK — minimal correct change |
| `EngineState::load_par_df` body unchanged | OK — state audit GREEN verdict still valid |
| `§5.2` doc reference | MINOR — confirmed §5.2 of state audit covers `clear_par_df` ("same shape; Sprint 14 P3.2b should release GIL on both for symmetry") |
| `check_fork()` position | OK — fork guard fires while GIL held, before `py.detach` |

Subagent token usage: 31,288 tokens, 44 s wall. Verdict: "ship after 1
minor confirmed; everything else correct." Minor confirmed during
review (no edit needed).

---

## Lessons (durable)

### 1. The dev-profile bench is a misleading reference; always A/B against release-profile builds matching the baseline

**Rule.** When validating an FFI change against a Sprint 13.5-style
release-wheel baseline, always rebuild the post-change as a release
wheel via `maturin build --release` and install in a clean venv. Do
NOT rely on `maturin develop`'s dev-profile install for an A/B that
will be cited against the baseline.

**Why.** Sprint 14 Part B initially ran the bench against the dev-
profile install (the natural output of `maturin develop`) and saw
`concurrent_load_direct` N=4 = 8,883 cps — well below the 12K target.
The shape WAS structurally correct (matched `concurrent_load`'s shape
on every N), but the absolute numbers were ~0.55× of the release-
profile baseline because the dev profile is unoptimized + debuginfo.
Building the release wheel and re-running gave 12,987 cps at N=4 —
target met by 8.2 %.

The dev bench was *diagnostically* useful (confirmed the GIL release
worked — both paths reached the same lock-contention shape), but the
*pass/fail* gate had to be evaluated against release. Burning ~1 pp
on the dev-profile detour was avoidable.

**Apply where.** Any sprint whose binary success criterion cites a
Sprint-13.5-style or earlier release-profile baseline. The dev
profile is fine for "did the change compile + behavior change in the
expected direction?" but not for "did we cross the gate threshold?"

**Cost saved.** ~1 pp per recurrence (one extra bench run on dev,
plus the wall-time of recognizing the discrepancy + scheduling the
release wheel build). Worth a durable rule. Promotion candidate;
single occurrence so far so hold for second observation before
promoting to `iteration_lessons.md`.

### 2. (no second durable lesson this sprint — small implementation, well-bounded scope, audit-already-done meant zero structural surprises)

The largest theme is "structural verification ahead of time pays off":
Sprint 13.5's state audit (Part D, GREEN verdict) and bench infra
(Part A) gave Sprint 14 everything it needed to ship a 6-line FFI
change with confidence. The reviewer dispatch found no CRITICAL/MAJOR;
the bench gate passed on first release-wheel run. This is the inverse
of Sprint 13's experience and validates the Sprint 13.5 retro lesson 1
(measurement-evidence first, implementation second).

---

## Pp accounting

| Item                                                          | Predicted | Actual |
|---------------------------------------------------------------|----------:|-------:|
| Brief authoring + commit                                      | 1.0       | ~0.7 |
| Part A implementation (2 method wraps + doc-comments)         | ~2        | ~0.5 (small surgical change) |
| `cargo fmt + clippy + test --workspace --exclude chili-py` gate | 0.3     | ~0.3 |
| Part B `maturin develop` + dev-profile bench (lesson 1 detour) | 1.5      | ~1.0 (lesson 1 surfaced; ~1 pp wasted on dev profile) |
| Part B `maturin build --release` + clean-venv install + A/B   | 1.0       | ~0.8 (3.59 s incremental compile; lesson 8 floor avoided) |
| Part C code-reviewer dispatch + finding fold-in (no edit needed) | 1.5    | ~0.8 (subagent: 31K tokens / 44 s wall + minor confirmation) |
| Part D wrap (bench section + retro + cadence + index + brief move + CLAUDE.md) | 1.5 | ~1.0 |
| **Total**                                                     | **5–9**   | **~5** |

Below mid-band (mid 7), at low band edge. Driver: small implementation
+ no structural surprises. This sprint is the inverse-shape of
Sprint 13's revert (which over-budgeted the 9–13 pp band against an
implementation that hit zero gain). Sprint 14 right-sized.

Pattern position in `cadence_metrics.md`: comparable to **Sprint 4**
(predicted 9–14, actual ~9 — 22 % below mid-band) or **Sprint 5**
(predicted 10–15, actual ~10 — 20 % below mid-band). The
"narrow-FFI-change with audit-already-done" pattern consistently
delivers at the low band edge.

---

## What surprised

- **The dev-profile bench was disorienting.** Saw 8,883 cps at N=4
  initially and almost flagged as "below target." Recognized within
  ~1 minute that the dev-profile compile would not match release-
  profile baseline absolute numbers. Lesson 1 above codifies the
  rule.
- **The two paths now have IDENTICAL N=4 throughput within ±1 %.**
  Pre-Sprint-14: `concurrent_load` 13,135 cps vs `concurrent_load_direct`
  4,841 cps (170 % gap). Post-Sprint-14: 13,114 vs 12,987 cps (1.0 %
  gap). The `par_df.write()` lock is the actual bottleneck on both
  paths now; the GIL was never the binding constraint for the fn_call
  path, and after Sprint 14 it's not the binding constraint for the
  direct-FFI path either.
- **The reviewer dispatch was cheap.** 31K tokens / 44 s wall. The
  finding-density was low (6 OK, 2 MINOR, 0 CRITICAL/MAJOR), which
  matches the small-and-pattern-mirroring shape of the change. Lesson
  7 (reviewer-before-retro) holds — even when findings are sparse,
  the dispatch is cheap insurance against the structural-defect-that-
  bench-doesn't-catch case.
- **`maturin build --release` was 3.59 s incremental** (vs Sprint 13.5
  Part B.2's 5 m 48 s from-scratch). The cargo target dir caching
  paid off — only `chili-py` itself recompiled.
- **No wheel cut for mdata.** The brief (Part D wrap, CLAUDE.md
  update) noted a wheel re-cut was OPTIONAL since mdata's
  `load_partitioned_df` already routes through fn_call (Sprint 13.5
  lesson 2). Confirmed at wrap: no mdata-visible bug; no urgency to
  ship 0.8.3.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_14_dispatch_brief_2026-05-09.md`](../history/sprints/sprint_14_dispatch_brief_2026-05-09.md) (post-ratification move)
- **Cadence metrics row 14:** [`cadence_metrics.md`](cadence_metrics.md)
- **Sprints index:** [`sprints_index.md`](sprints_index.md)
- **State audit (gate condition for this sprint):** [`../sync/load_par_df_state_audit.md`](../sync/load_par_df_state_audit.md) — GREEN verdict still valid (engine-state body unchanged).
- **Bench A/B (Part B output):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 14"
- **Sprint 13.5 retro (the readiness gate):** [`sprint_13.5_retro.md`](sprint_13.5_retro.md)
- **Implementation commits:** `35c215c` (brief), `<wrap-commit>` (Part A code + Parts B/D docs + retro)
- **Related artifacts (uncommitted, /tmp):** `/tmp/sprint_14_post_change_concurrent.json` (dev profile A/B; lesson 1 trigger), `/tmp/sprint_14_post_release_concurrent.json` (release-profile A/B; bench gate PASSED), `/tmp/sprint_14_post_dist/chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl` (release wheel; not delivered to mdata).

---

## Sprint 15 hand-off

**Sprint 15 = A.2.4 Parquet codec tuning** (deferred from Sprint 13.5
per user direction). Scope:

- Expose `ParquetWriteConfig` (or equivalent) as a new public API on
  `engine.write_partitioned_df` accepting `compression`, `row_group_size`,
  `data_page_size`, etc.
- Coordinate with mdata: their write-side ingest path is the primary
  consumer; surface a public API change requires mdata sign-off on
  semantic shape.
- Bench A/B: write_partition criterion bench against post-change codec
  options. Target: ZSTD-3 vs current default (likely Snappy or no
  compression) — measure write-throughput delta + on-disk size delta.
- ADR territory: any default-codec change is a Storage Schema decision
  per CLAUDE.md golden rule 4.
- Predicted pp: 6–10 (more involved than Sprint 14; new public API +
  potential ADR + cross-project coordination).

**Out of scope for Sprint 15:**

- A.2.2 vars-write-lock release (descoped indefinitely; reopen only
  with profile evidence).
- P3.4 Categorical mapping cache (deferred indefinitely;
  categorical_eval bench Δ 0.4 %).
- Polars-internal kernel optimization (blocked on user P0).

**User-driven backlog status (no change from Sprint 13.5):**

- (P0) GitHub-host the polars fork — still open.
- (P1) KDB-X CE comparison — pending GA.
- (P2) mdata sign-off on 0.8.1 (now superseded by 0.8.2) delivery —
  awaiting.
- (P3) Sprint 13 P2 Box::new mitigation — deferred indefinitely per
  Sprint 13 lesson 2.

---

## Open question for user (informational, not blocking)

**Should Sprint 14's GIL-release change ship as a wheel for mdata?**

Three positions:

1. **No wheel** (current default). The change benefits direct-FFI
   callers; mdata's `load_partitioned_df` uses fn_call (already GIL-
   released). No user-visible bug fix, no urgency.
2. **Cut a 0.8.3 wheel and notify mdata.** Frame: "FFI symmetry
   improvement; not a bug fix. Use only if you bypass the
   `load_partitioned_df` Python wrapper." mdata can ignore.
3. **Cut 0.8.3 only when bundled with another change** (e.g., Sprint
   15's `ParquetWriteConfig`). Defers the wheel-cut wall-time cost
   to a more meaningful release.

Recommendation: **option 3** — defer to Sprint 15. No user-visible
benefit alone; bundle the version bump.