# Sprint 20 retro — main → claude-2 merge (lean refactors adopted, mdata superset preserved)

**Wrap:** 2026-05-18 ~23:21 (gate-green; awaiting user ratification)
**Predicted:** 7–16 pp, mid ~11 (post-audit#1+#2 revision; original brief 6–12)
**Actual:** ~12–15 pp (best-effort; upper band — driven by the auto-merge-cascade chain: 5 silent inconsistencies not in the 16 conflicts, each surfaced only by compile/test, plus the over-checkpointing detour the user corrected)
**Variance:** ~ +10–35% vs post-audit mid (upper band, as the audit's "expect ≥12" warning predicted)
**Owner:** coordinator-solo + 2 pre-execution audit rounds (3-agent then 2-agent) + the user (3 behavioral corrections → 3 new memories)

## Scope shipped (commit: the Sprint-20 merge commit on `claude-2`)

- Real `git merge main` (3 commits `9dfa4d2`/`43faf44`/`ef4bfb2`). 16 conflicts resolved per the audited A.2 table.
- **Adopted (take-main):** lean refactors + `rotate_handle` (dormant, G4-DISJOINT) + 6 AA test/bench + `roll_tick_test.rs` split into `roll_tick_test.rs`(claude-2 guard)+`rotate_handle_test.rs`(main); io.rs/util.rs/benches/common/built_in_fn.rs lean 7-arg wpar.
- **Preserved (mdata superset):** `publish_via_handle` (2b), `roll_tick`/`init_tick(date)` (2c), GR4 helpers + M-1 (eager `eval()` no auto-dequant; true-lazy ADR-0002/0003 kept — corrected the brief's wrong "take main's bare eval" which would have regressed ADR-0003), full-family `py-1.39.3` polars pin (decision #4; Cargo.toml auto-resolved correctly as audit predicted).
- **Dropped:** `overwrite_partition` alias (2d), wpar `compression`/`row_group_size` kwargs (#3 → ADR-0005 superseded-in-place).
- **Committed guards:** `TestM1EagerNoAutoDequant` (M-1 contract, Sprint-19 lesson #1); roll_tick/rotate_handle/publish_via_handle suites green.
- Gate: fmt 0 / clippy 0 (no upstream conformance needed this merge) / `cargo test --workspace --exclude chili-py` all-ok / `maturin develop` 0 / pytest **90 passed 0 failed**.

## Lessons (durable)

- **Lesson 19 promoted** (`iteration_lessons.md`): take-main of a refactored API silently breaks every *auto-merged* consumer; the conflict set is a floor, the gate is the enumerator; a failing *preserved*-surface test after an API take-main is presumptively a cascade, not a regression (the "query_plan regression" red herring → it was the 9-arg `built_in_fn.rs` `wpar`).
- 3 user-level memories written: `verifier-must-be-executed` (the `-- fmt_test` false-green that survived 2 audits), `no-unblocked-stops` (over-checkpointing reframed as "rigor"), and Lesson 19's auto-merge-cascade.

## Pp accounting

| Item | Pred | Actual |
|---|---|---|
| Brief + 2 audit rounds + appendices | ~3 | ~3 |
| Step-0 reconstruction + verify (caught the `-- fmt_test` bug) | ~1 | ~1.5 |
| 16 conflict resolutions (engine_state brace surgery, --ours+surgery py) | ~4 | ~4 |
| 5 auto-merge-cascade fixes (Cargo.toml×2, io/util/bench, partition_filter, built_in_fn wpar) | (unbudgeted) | ~3–5 |
| Part B docs + M-1 guard + retro/cadence | ~1.5 | ~1.5 |
| Over-checkpointing detour (user-corrected) | 0 | ~1 |

## What surprised

- **5** auto-merge cascades (not 0–1). The audit's atomic-#3 set enumerated only conflicted files; the auto-merged consumers (built_in_fn.rs wpar registration especially) were the real cost. → Lesson 19.
- The brief's "2a take main's bare eval" was **wrong** vs decision #4 (would regress ADR-0003); caught by reading both sides, not the audits.
- Cargo.toml `[patch]` "silent BLOCKER #4" was a *false alarm* (git auto-resolved to claude-2 correctly) while the *real* blocker was the `/tmp` stub (Step-0) — audit#1 correctly inverted these.
- 1 false alarm (mimalloc `#[global_allocator]` "violation") correctly dismissed by verifying claude-2 baseline before escalating.

## Cross-references

- Brief + 2 audit appendices + decisions: `docs/sim/sprint_20_dispatch_brief_2026-05-18.md`
- mdata contract + execution outcome: `docs/sync/mdata_chili_2026-05-18_main_merge_signoff.md`
- ADR-0005 (superseded), iteration_lessons Lesson 19, `vendor/polars-core/README.md` (verifier-bug fixed)
