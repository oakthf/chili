# Sprint 24 dispatch brief — main port (claude-2 ≡ main 0.9.0+)

**Kickoff:** 2026-05-25
**Owner:** coordinator-solo
**Type:** deletion + forward-port (no new features)
**Predicted pp:** 4-7 (mid 5)
**Plan reference:** `docs/sync/notes_to_chili_author_2026-05-25_update3.md` §"Move-to-v0.9 plan — accelerated"
**Audit:** light (deletion-sprint; technical verifications cover the design)

---

## Sprint objective

Forward-port `main` (v0.9.0 with the chili-author's 8 morning + afternoon commits closing all gaps) into `claude-2` AND remove the claude-2-unique features that became redundant per mdata's Revision A reframe + author's commits. End state: **claude-2 ≡ main 0.9.0+** with M-1 test guard + claude-team-specific tooling preserved.

**Binary success criterion:** `git diff main..claude-2 -- crates/` shows ONLY:
1. `crates/chili-py/tests/test_engine.py::TestM1EagerNoAutoDequant` (the M-1 invariant guard)
2. Any `chili.scale` Python facade we lift GR4 helpers into

Plus: full pre-commit gate green, 0.9.0 wheel cut, mdata delivery doc shipped.

---

## Why now

- chili-author shipped 8 commits over 2026-05-25 closing 12 of 13 features we built (1 is a test guard, kept at zero cost).
- mdata's Revision A explicitly authorized deletion of D-1/D-2/D-3, W3, GR4 etc.
- Each chili main release going forward costs us manual rebase work for zero downstream benefit if we don't sync now.
- mdata's 24h Pipeline X soak completes today; they're poised to bump from 0.8.8 → 0.9.0+ (skipping 0.8.9 which mdata never adopted).
- The polars-core-patch URL (single-fork-on-GitHub) closes our 6-month P0 backlog item about /tmp/polars-py-1.39.3 local clone fragility — **pre-flight verified** the URL carries our q-style fmt patch.

---

## Scope — Part A: merge main → claude-2

**Approach:** `git merge --no-ff main` (the canonical "user uploads upstream state" pattern per CLAUDE.md branch policy). NOT cherry-pick (which caused the 2026-05-07 pivot incident per iteration_lessons.md L4).

Expected conflicts in:
- `crates/chili-core/src/engine_state.rs` (both branches modify heavily)
- `crates/chili-core/src/lib.rs` (re-exports)
- `crates/chili-core/src/eval.rs` (W3 branch on claude-2; eval_op String parse on main)
- `crates/chili-core/src/utils.rs` (push-model is_upd_shape on claude-2; SyncFile + prepare_file_writer on main)
- `crates/chili-core/src/side_effect_fn.rs` (eval_str builtin on claude-2; .handle.fsync on main)
- `crates/chili-core/Cargo.toml` (crossbeam-channel + libc on claude-2)
- `crates/chili-py/src/lib.rs` (many push-model + W3 + flush_tplog + publish_via_handle + GR4 methods)
- `crates/chili-py/chili/engine.py` (same)
- `crates/chili-py/chili/src/tick.pep` (different rollLog impls)
- `Cargo.toml` (polars-py-1.39.3 patches vs polars-core-patch URL)
- `crates/chili-py/Cargo.toml` (patches block)
- `CLAUDE.md` (project state — most lines stale post-merge)
- `CHANGELOG.md`

**Resolution strategy:**
- **prefer-main** for: engine_state.rs, lib.rs (re-exports), eval.rs, utils.rs, side_effect_fn.rs, tick.pep, chili-py/src/lib.rs, chili-py/chili/engine.py, Cargo.toml `[patch.crates-io]`, chili-py/Cargo.toml
- **prefer-ours** for: docs/, .cross_comms config, .claude/ tooling, CLAUDE.md (initial-keep then rewrite), CHANGELOG.md (manual merge)
- **manual** for: workspace `[workspace.package] version` (will bump to 0.9.0 in Stage C anyway), Cargo.lock

After merge resolution: claude-2 has main's canonical code + our docs + the M-1 test (which is in test_engine.py — overlap, may need manual carry-over).

---

## Scope — Part B: sequential delete commits

Six logical chunks, one commit each (for clean audit trail). Each commit must leave the gate green.

| Chunk | Deletes | Tests deleted | ADR action |
|---|---|---|---|
| B.1 push-model | `chili-core/src/upd_notify.rs`; `is_upd_shape` in utils.rs; UpdEvent/upd_notify_fd/drain_upds/resume_cursor in engine_state.rs + chili-py FFI; `crossbeam-channel` + `libc` from chili-core/Cargo.toml | upd_notify_test.rs; test_push_model.py | mark ADR-0006 Superseded |
| B.2 W3 register_fn | `chili-core/src/external_fn.rs`; `external_name` field on Func; W3 branch in eval.rs; serde9 W3 comment; chili-py/src/external_dispatcher.rs; register_fn/unregister_fn FFI | external_fn_test.rs; test_register_fn.py | mark ADR-0007 Superseded |
| B.3 flush_tplog | engine_state.rs flush_tplog method; chili-py FFI; engine.py wrapper (replaced by main's fsync_handle) | flush_handle_test.rs; test_tplog_flush.py | n/a |
| B.4 publish_via_handle | engine_state.rs publish_via_handle method; chili-py FFI; engine.py wrapper | test_publish_via_handle.py | n/a |
| B.5 roll_tick | engine_state.rs roll_tick native method; chili-py FFI; engine.py wrapper (mdata uses roll_tick_log from main now) | roll_tick_test.rs (claude-2's), test_roll_tick.py refs to native | n/a |
| B.6 GR4 helpers | chili-py engine.py set_column_scale / clear_column_scales / _apply_column_scales; possibly lift to a `chili.scale` Python facade (or just delete if not needed for v1-36) | refs in test_engine.py | n/a (M-1 test stays in test_engine.py) |

After Part B: claude-2 source code ≡ main source code + M-1 test + docs.

---

## Scope — Part C: version bump + Cargo.lock + pyproject.toml sync

Per Lesson 14:
- `Cargo.toml` workspace.package → `version = "0.9.0"`
- `crates/chili-py/Cargo.toml` → `version = "0.9.0"`
- `crates/chili-py/pyproject.toml` → `version = "0.9.0"`
- `Cargo.lock` follows (one workspace-version row each crate)

---

## Scope — Part D: full pre-commit gate

```
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace --exclude chili-py
cd crates/chili-py && uv run maturin develop --release && uv run pytest
```

Plus matched-shell A/B bench per Sprint 23 L21 — `concurrent_eval` N=1/N=4 vs the prior 0.8.9 wheel. Assert 0.9.0 within ±5% of 0.8.9 (loose tolerance for a major restructure).

Expected test counts after merge + deletes:
- Rust workspace: 215 (current claude-2) → ~150 (drop external_fn=5, upd_notify=~6, flush_handle=~4, roll_tick=~5 since the test is replaced by main's, fn_call_i64=~3 if also deleted)
- chili-py pytest: 108 (current claude-2) → ~70-80 (drop test_register_fn=8, test_push_model=~15, test_tplog_flush=~5, test_publish_via_handle=~3; main brought ~70 over per cc954d2)

---

## Scope — Part E: wheel cut + mdata delivery

- `cd crates/chili-py && uv run maturin build --release -o dist`
- sha256
- Write `docs/sync/mdata_chili_2026-05-25_0.9.0_delivery.md` — clean v0.9.0 wheel based on author's main + mdata Revision A reframe + M-1 test guard preserved
- Acceptance asks: sha-verify, 769-suite regression, ratify v1-36 plan against 0.9.0
- Bus notification at delivery time (NOT during pre-impl since mdata's Revision A already authorizes the deletion plan)

---

## Scope — Part F: wrap ceremony

- `docs/sim/sprint_24_retro.md` — actual pp + lessons
- `docs/sim/cadence_metrics.md` — row 24
- `git mv docs/sim/sprint_24_dispatch_brief_2026-05-25.md docs/history/sprints/`
- Mark `docs/decisions/0006-async-upd-notification-ffi.md` and `docs/decisions/0007-w3-python-callable-bridge.md` as **Superseded** with rationale (mdata Revision A user-of-chili reframe + author's main 0.9.0 commits closing all gaps)
- Update `CLAUDE.md` project state (most lines need rewrite)
- Cleanup vendor/polars-core/ — mark patch file historical (polars-core-patch URL is now the canonical source)
- Cleanup `/tmp/polars-py-1.39.3` references in CLAUDE.md / dev_setup.md (no longer needed)

---

## Out of scope (defer)

- **No new chili-core features.** Pure deletion + forward-port.
- **No upstream PRs.** Author has clearly stated his positioning (standalone-first); claude-2 stays on the embedded-friendly extensions only if mdata needs them.
- **No mdata-side migration code.** mdata writes their own v1-36 migration; claude-2 just ships the wheel.
- **No async upstream wishlist follow-up.** ADR-0006 push-model is Superseded; W4/W5 async surface is mdata-side work per their Revision A.

---

## Deliverables

| # | Artifact | Type | Order |
|---|---|---|---|
| 1 | `.cross_comms/outbox/<key>.json` — pre-impl notification to mdata | new | pre-impl |
| 2 | Merge commit `merge: main → claude-2 — Sprint 24 main port` | new | Part A |
| 3 | `feat(claude-2): Sprint 24 B.1 — drop push-model (ADR-0006 Superseded)` | commit | Part B.1 |
| 4 | `feat(claude-2): Sprint 24 B.2 — drop W3 register_fn (ADR-0007 Superseded)` | commit | Part B.2 |
| 5 | `feat(claude-2): Sprint 24 B.3 — drop flush_tplog (use main fsync_handle)` | commit | Part B.3 |
| 6 | `feat(claude-2): Sprint 24 B.4 — drop publish_via_handle (use sync(h, tuple))` | commit | Part B.4 |
| 7 | `feat(claude-2): Sprint 24 B.5 — drop roll_tick native (use main roll_tick_log)` | commit | Part B.5 |
| 8 | `feat(claude-2): Sprint 24 B.6 — drop GR4 helpers (lift to chili.scale facade)` | commit | Part B.6 |
| 9 | `chore: bump version 0.8.9 → 0.9.0 + Cargo.lock sync` | commit | Part C |
| 10 | `docs(claude.md): rewrite project state for post-Sprint-24 main-port state` | commit | Part C/F |
| 11 | `dist/chili_sauce-0.9.0-cp310-abi3-macosx_11_0_arm64.whl` | new | Part E |
| 12 | `docs/sync/mdata_chili_2026-05-25_0.9.0_delivery.md` | new | Part E |
| 13 | `docs/sim/sprint_24_retro.md` + cadence row | new | Part F |
| 14 | `docs/sim/sprint_24_dispatch_brief_2026-05-25.md` → `docs/history/sprints/` | move | post-ratification |

---

## Lead allocation + audit shape

**Coordinator-solo.** No subagent fanout.

Audit: **light self-audit** (this brief). Skipped 3-agent audit because (a) the technical verifications across notes_to_chili_author updates 1/2/3 already cover the design space, (b) the work is mechanical deletion + a well-precedented merge, (c) each delete commit is independently reversible.

If anything novel surfaces during execution, halt-and-escalate per criterion 3.

---

## Mid-checkpoint plan

After Part A (merge complete + gate green), post a short status:
- Did the merge resolve cleanly?
- Are all 6 deletion targets present in the merged tree?
- Pre-impl bench baseline number captured?

Halt-and-escalate criteria:
1. **Merge surfaces unexpected functional conflict** — e.g., a main-side change that breaks an unrelated claude-2 feature
2. **polars-core-patch incompatibility** — if `cargo build` fails despite the pre-flight check
3. **mdata signals objection** on the bus before execution completes
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 7pp

---

## Wrap ceremony

- Pre-commit gate green (cargo fmt/clippy/test --workspace --exclude chili-py + uv run pytest)
- Bench delta within ±5% of 0.8.9 (loose tolerance — major restructure)
- 0.9.0 wheel cut + sha256 recorded + delivery doc written
- ADR-0006 + ADR-0007 marked Superseded
- CLAUDE.md project state rewritten
- Retro at docs/sim/sprint_24_retro.md
- Append row to docs/sim/cadence_metrics.md
- Move this brief to docs/history/sprints/
- HALT until user ratifies

---

## Pp accounting reference

Closest comparable sprints:
- **Sprint 20 (main → claude-2 merge for lean refactors)** — predicted 7-16 mid 11, actual 12-15. Similar SHAPE (merge from main); different DIRECTION of net work (Sprint 20 was preserve-mdata-superset; Sprint 24 is delete-mdata-superset).
- **Sprint 22 (W1+W2 wishlist)** — predicted 7-13, actual ~9-11. Comparable for ceremony overhead (wheel + delivery + ADR refs + retro + cadence).
- **Sprint 23 (W3)** — predicted 13-17, actual ~12-14. Comparable for surface span (chili-core + chili-py + tests + docs + wheel).

Sprint 24 expected at the **lower band** (~4-7) because:
- Pure deletion work — no design iteration
- No new features = no new tests to write
- Audit pre-empted by the exhaustive verifications already done

Capped above by: merge conflict resolution time (could escalate if more conflicts than expected); CLAUDE.md rewrite (significant lines)

---

## Cross-references

- mdata Revision A: `docs/sync/mdata_architecture_handoff_2026-05-24.md`
- chili-author confirmation: `docs/sync/notes_to_chili_author_2026-05-25_update3.md`
- Gap analysis (origin): `docs/sync/upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md`
- ADRs to mark Superseded: `docs/decisions/0006-async-upd-notification-ffi.md`, `docs/decisions/0007-w3-python-callable-bridge.md`
- Pre-flight verification of polars-core-patch URL: this brief, "Why now" §5
- Branch policy: CLAUDE.md "Branch policy" section
