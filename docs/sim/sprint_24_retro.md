# Sprint 24 retro — main port (claude-2 ≡ main 0.9.0+)

**Wrap:** 2026-05-25
**Predicted:** 4-7 pp (mid 5, per the brief's `Pp accounting reference`)
**Actual:** ~5-7 pp (mid-band; on target)
**Variance:** ~0% (within predicted)
**Owner:** coordinator-solo
**Plan reference:** `docs/history/sprints/sprint_24_dispatch_brief_2026-05-25.md` (post-move)

---

## Scope shipped

**Pre-impl gate (deliverables #0/#1 per brief):**
- Pre-flight: verified polars-core-patch URL has our q-style fmt patch (closes 6-month P0 backlog)
- Pre-flight: enumerated 8 code files + 7 test files in the deletion footprint
- Sprint 24 brief (commit `1744a41`)
- mdata pre-impl notification via bus (`chili-sprint-24-mainport-20260525`, event 18926)
- Safety tag: `claude-2-pre-sprint-24` (rollback point)

**Part A — merge (commit `da6b1a4`):**
- `git merge --no-ff main` brought 8 author commits (f6bccd1+5cfc096+588de78+26b437e+74acdc6+cc954d2+fb4455d+3d12995)
- Conflict resolution: prefer-ours on .gitignore + CHANGELOG.md; prefer-main on all source files (engine_state, eval, utils, side_effect_fn, broker, df, tick.pep, Cargo.toml's, lib.rs's, engine.py)
- Folded Part B deletions INTO this commit (mechanically necessary — auto-merged code referenced deleted methods)
- Deleted 12 files (3 source + 9 test): external_fn.rs, upd_notify.rs, external_dispatcher.rs, external_fn_test.rs, flush_handle_test.rs, upd_notify_test.rs, eval_str_test.rs, rotate_handle_test.rs, test_eval_str.py, test_publish_via_handle.py, test_push_model.py, test_register_fn.py, test_roll_tick.py, test_tplog_flush.py
- Manual clippy fixes (Rust 1.95.0): collapsible_if + needless_return in main's new engine_state.rs code

**Part B residual cleanup (commit `1a9dbdd`):**
- Took main's func.rs (drop Func::external_name field + Func::new_external)
- Took main's serde9.rs (drop W3 inline comment)
- Took main's chili-core/Cargo.toml (drop crossbeam-channel + libc deps)
- Took main's chili-op/Cargo.toml + sub.pep + README + test_engine.py
- Net diff vs main: 7 files changed, 40 insertions, 227 deletions

**Part C — version bump:** NO-OP (main brought 0.9.0 already; workspace + chili-py + pyproject all at 0.9.0)

**Part D — gate:**
- `cargo fmt --all -- --check`: OK
- `cargo clippy --all-targets -- -D warnings`: OK
- `cargo test --workspace --exclude chili-py`: **189 passed, 0 failed** (was 215 on claude-2-pre-sprint-24; -26 reflects deleted tests)
- `uv run pytest`: **72 passed, 0 failed** (was 108 on claude-2 0.8.9; -36 reflects deleted test files)
- Matched-shell A/B bench (per Sprint 23 L21): 0.8.9 N=1=1264 cps + N=4=3160 cps vs 0.9.0 N=1=1272 cps (+0.7%) + N=4=3155 cps (-0.2%) — both within ±1% of 0.8.9 cps comparison; within ±5% gate

**Part E — wheel + delivery:**
- `dist/chili_sauce-0.9.0-cp310-abi3-macosx_11_0_arm64.whl` — sha256 `ee85a079cee12531d211a4426fb3fa793176fe918acd0ce566f4c91082d585f4`
- `docs/sync/mdata_chili_2026-05-25_0.9.0_delivery.md` — handoff with API-substitution table, bench numbers, 5 acceptance asks

**Part F — wrap:**
- This retro
- `docs/sim/cadence_metrics.md` row 24 appended
- Brief moved to `docs/history/sprints/`
- ADR-0006 + ADR-0007 marked Superseded with rationale
- CLAUDE.md project state rewritten (post-merge state)

---

## Lessons (durable)

### 1. When merging upstream, fold structural-coupling deletions INTO the merge commit, not after

**Rule.** When `git merge upstream → fork` and the upstream's auto-merged source files reference methods/types being deleted as part of the merge intent (because the upstream removed them or the fork-only-additions are no longer compatible), DELETE those orphaned files / types in the merge commit itself. Don't try to keep them "for the audit-trail delete-commits" because the intermediate state won't compile.

**Why.** Sprint 24's brief planned 6 sequential delete commits AFTER the merge for clean audit trail (one per logical chunk: push-model / W3 / etc.). When I tried that, the merge commit alone couldn't pass cargo test — the auto-merged main version of `engine_state.rs` lacked methods like `set_resume_cursor` that claude-2's `chili-py/src/lib.rs` still referenced. I had to fold ALL deletions into the merge commit. The "B.X sequential delete commits" became a single residual-cleanup commit (Part B `1a9dbdd`).

**Apply where.** Future main → fork merges where the merge intent includes deleting fork-only divergence. Recognize the structural coupling: if the merge's auto-resolved code is incompatible with the fork's diverged code, the deletions must accompany the merge in the same commit OR be staged before via `git rm` then merged.

**Cost saved.** ~30 min saved on the second attempt by recognizing the coupling earlier vs trying to split deletions across commits.

### 2. Concurrent maturin builds destroy bench-environment quiescence

**Rule.** Bench gates (per Sprint 23 L21 matched-shell A/B) MUST run in a quiescent system — no maturin build, no other heavy CPU jobs running concurrently. Schedule the bench AFTER the build completes (or before it starts), never overlapping.

**Why.** Sprint 24 Part D ran the 0.8.9 bench while a maturin release build was running concurrently in the background. Result: N=1=58cps with p99=128ms (vs the expected ~1100cps with p99=2ms — 20x throughput collapse + 60x latency spike). The matched-shell A/B methodology (Sprint 23 L21) was protected against snapshot-vs-snapshot noise but NOT against actively-saturated-CPU during the measurement. After build completion, fresh quiescent bench returned 1272cps — confirming the contention was the noise source, not real perf.

**Apply where.** Every sprint with a bench gate. Add to the wrap checklist: "before bench, confirm no background builds (`ps aux | grep -E 'cargo|maturin|rustc' | grep -v grep`)".

**Cost saved.** Would have triggered false halt-and-escalate per criterion 1 (GR5 regression — concurrent_eval N=1 from 1100 → 58 = -95%) without the L21 + matched-A/B methodology catching the contention via the unrealistic p99. ~15 min of investigation saved per sprint.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| Pre-flight (polars-fork verify + footprint enumeration) | 0.5pp | ~0.5pp |
| Brief drafting + mdata notification | 0.5pp | ~0.5pp |
| Part A merge + conflict resolution + clippy fixes | 2-3pp | ~3pp (more conflicts than predicted; folded Part B deletions) |
| Part B residual cleanup | 1pp | ~0.5pp |
| Part C version bump | 0.5pp | 0pp (main brought 0.9.0; NO-OP) |
| Part D gate + matched-A/B bench | 1pp | ~1pp (bench re-run due to concurrent-build contention) |
| Part E wheel cut + delivery doc | 1pp | ~1pp |
| Part F retro + cadence + brief→history + ADRs Superseded + CLAUDE.md | 1pp | ~1pp |
| **Total** | **4-7 (mid 5)** | **~5-7** (mid-band, on target) |

On target despite the merge being more conflict-heavy than predicted (saved by Part C being a NO-OP). The "fold deletions into the merge" mechanical realization was the only mid-sprint pivot (predicted 6 sequential delete commits → 1 merged-into-merge + 1 residual cleanup).

---

## What surprised

- **polars-core-patch URL has our q-style fmt patch verbatim** — pre-flight verified via direct git clone. Author's hosted fork at v0.53.0 contains commit 6c64273 with our exact patch (referenced as `from hinmeru/polars-core-patch 6c64273` in our claude-2 vendor patch). This closes the 6-month P0 backlog (no more /tmp clone fragility) cleanly. Better than expected.
- **Concurrent maturin build during bench dropped N=1 throughput from 1272 → 58 cps** (-95%) — significant lesson 2 above.
- **Author shipped 8 commits over a single day** addressing 6 of 7 properties from my notes_to_chili_author dialogue + cc954d2 importing our Python test suite. The pace of upstream adoption surprised both me and (per Revision A) mdata.
- **Part B deletions had to be folded into the merge commit** mechanically — the auto-merged main code referenced deleted symbols. Sprint cadence brief assumed 6 sequential delete commits; reality was 1 large merge + 1 residual cleanup. Net same surface, simpler git history.
- **0.8.9 wheel was orphaned** — we cut it for W3 but mdata never adopted (Revision A predates pin). Sunk cost.

---

## Cross-references

- Plan: `docs/history/sprints/sprint_24_dispatch_brief_2026-05-25.md` (post-move)
- mdata Revision A (authorization source): `docs/sync/mdata_architecture_handoff_2026-05-24.md`
- Author dialogue: `docs/sync/notes_to_chili_author_2026-05-25*.md` (4 docs)
- Delivery: `docs/sync/mdata_chili_2026-05-25_0.9.0_delivery.md`
- ADRs marked Superseded: `docs/decisions/0006-async-upd-notification-ffi.md`, `docs/decisions/0007-w3-python-callable-bridge.md`
- Pre-flight reproducers (verified the gap): `docs/sync/reproducers/q1_publish_path.py`, `w1_fsync_handle.py`, `q2_v2_post_author_fixes.py`, `q2_v3_three_recovery_cases.py`, `q2_v4_post_truncate.py`
- Safety tag (rollback point): `claude-2-pre-sprint-24` = `1744a41`
- Cadence metrics row: `docs/sim/cadence_metrics.md` (row 24)
- Sprint 23 retro (immediately prior): `docs/sim/sprint_23_retro.md`
- Sprint 20 retro (closest prior merge-from-main shape): `docs/sim/sprint_20_retro.md`
