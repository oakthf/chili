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
| 11 | deep housekeeping #2 (every-5-sprint sweep; 2 proposals demoted; CLAUDE.md state refresh; 10-row pattern deltas in cadence_metrics) | 3–5 (mid 4.0) | ~1.5 | −63% vs midpoint | 0 (clean scope; doc tree already in good shape) | 0 (autonomous run) | 0 (no code touched) | 0 (housekeeping; no tests added) | 2026-05-08 (Sprint 11 wrap commit) |
| 12 | perf-pass-3 + Iceberg eval (final sprint per roadmap) — Sprint 9 P2 partial symbolization (17.7% Box::new on main thread identified; polars-internal kernels still unresolved); Iceberg recommendation: defer to consumer-demand-driven sprint | 6–12 (mid 9.0) | ~3 | −67% vs midpoint | 0 (clean scope; deferred items remain deferred) | 0 (autonomous run) | 0 (no code touched) | 0 (research + partial profile sprint) | 2026-05-08 (Sprint 12 wrap commit) |
| 13 | `load_par_df` hot path optimization (A.2.6 + A.2.8 + P1.3 + A.2.3 audit + P1.1 deferred) — REVERTED. Implementation correct but 0pp gain on `load_multitable_5x200p` target (1.92→1.93 ms; within noise). +22.8% vs parked-claude regression remains because it's dominated by polars-internal worker kernel `0x450c` (93.1%), not the chili-side 17.7% Box::new the optimization targeted. v1 had a +60% regression bug (eager `Vec::collect` with `entry.metadata()` per entry replaced lazy iterator `any()` short-circuit); v2 fixed but still 0pp gain. Per brief rollback criterion (<5pp gain → revert). | 9–13 (mid 11) | ~3 | −73% vs midpoint | 1 (Part A.1 v1 → v2 fix mid-sprint after 60% regression bench result) | 1 (user just gave "let's start sprint A" go signal; no in-sprint user touches) | 0 (gate stayed green at every step; the regression was pure perf, not a gate failure) | 0 (revert restored pre-Sprint-13 test count of 166) | 2026-05-09 (Sprint 13 wrap commit) |
| 13.5 | Bench infrastructure + state audit (pre-Sprint-14 measurement) — NEW `tests/bench_concurrent.py` (4 shapes) + NEW `categorical_eval.rs` (P3.4 evidence: 0.4% Δ → defer indefinitely) + 0.8.2 baseline + claude-2 HEAD wheel baseline (≡ 0.8.2 within ±5%) + Rust criterion baselines (parse_cache 377 ns ≤ 400 ns golden rule 6 PASS; load_par_df identical to Sprint 13 post-revert) + samply concurrent_load_direct profile (92.5% kernel time = GIL contention; halt threshold ≥40% cleared decisively) + `load_par_df_state_audit.md` GREEN. **NO chili source changes.** All 9 Sprint 14 readiness gates green. Surfaced lesson 2 finding: `load_partitioned_df` already routes through GIL-released `fn_call` path; only direct-FFI callers benefit from Sprint 14 P3.2b. | 8–12 (mid 10) | ~10 | 0% vs midpoint | 0 (clean scope; A.2.2 pre-descoped, A.2.4 pre-deferred to Sprint 15) | 1 (kickoff "/compact + proceed" go-signal; no in-sprint user touches) | 0 (no chili source touched; gates stayed green) | 0 (bench-files-only sprint; pre/post 166 Rust + 65 chili-py pytest unchanged) | 2026-05-09 (Sprint 13.5 wrap commit) |
| 14 | P3.2b — release GIL on direct-FFI `engine.load_par_df` + `clear_par_df` (lib.rs:531-548 `py.detach` wrap; +`Python<'_>` param + `String::to_owned` for Send closure). Bench gate PASSED: `concurrent_load_direct` N=4 = **12,987 cps** (Sprint 13.5 baseline 4,841 cps flat × N → +168.3% at N=4; ≥12K target met by 8.2%). Direct-FFI shape now matches `concurrent_load` (fn_call path) on every N within ±1.5%; both paths now share the `par_df.write()` Phase 2 lock-contention boundary. `code-reviewer` subagent dispatched (lesson 7, FFI surface): 6 OK + 2 MINOR + 0 CRITICAL/MAJOR; ship-as-is. | 5–9 (mid 7) | ~5 | −29% vs midpoint (low band edge) | 0 (clean scope; small surgical FFI change) | 1 ("ratified 13.5 retro. let's move on" kickoff; no in-sprint user touches) | 0 (gates green throughout) | 0 (no test-count changes; behavior preserved) | 2026-05-09 (Sprint 14 wrap commit) |
| 15 | A.2.4 — Parquet `compression` + `row_group_size` public API (`write_partitioned_df` + `overwrite_partition` keyword-only kwargs; new `pub ParquetWriteConfig` struct in chili-op; `wpar` FFI arg_num 7→9; `parse_compression_name` helper). ADR 0005 documents default-codec preservation + future-change protocol. Bundled Sprint 14 + Sprint 15 into **0.8.3 wheel** at `dist/chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl`. Byte-equivalence regression check PASS (sha256 9682bed9... matches 0.8.2; size 1105 bytes). Codec correctness verified at parquet metadata level (4 codecs benched: snappy/zstd/lz4_raw/uncompressed within ±1.5% wall, on-disk 5878/11073/11048/18655 bytes — zstd 1.88× better ratio than snappy at zero CPU cost). Sprint 14 regression check: `concurrent_load_direct` N=4 on 0.8.3 wheel = 13,169 cps (≥12K, GIL release intact). 3-agent parallel audit pre-execution (3 majors found + folded). `code-reviewer` post-impl (2 minors found + folded: removed `"none"` codec alias footgun + `util.rs` doc comment "Snappy"→"ZSTD"). **Lesson 1 PROMOTED to user-level** (`~/.claude/rules/verify-before-claim.md` "External dependency defaults" bullet): external-dependency defaults are load-bearing claims (default codec was empirically ZSTD, not Snappy as brief assumed; 3 audit agents inherited the wrong premise). Portable copy at `~/team/verify-before-claim.md` for cross-user adoption. | 7–11 (mid 9, post-audit) | ~9 | 0% vs midpoint | 1 (ZSTD-not-Snappy default-codec discovery mid-implementation; reframed test assertions + ADR 0005) | 1 ("let's continue with Sprint 15 and bundle the result before wheel cut" kickoff; no in-sprint user touches) | 1 (CHILI_SYNTAX env-var leak in eval_test.rs — pre-existing latent test ordering bug surfaced by Sprint 15 recompile shuffling parallel scheduling; fixed in 1 line) | +7 chili-py pytest (TestParquetWriteConfig 6 + TestOverwritePartition mirror 1; Rust unchanged 166) | 2026-05-09 (Sprint 15 wrap commit) |
| 16 | mdata wishlist v1 bundle — **P0** `engine.flush_tplog()` (ReadWrite-trait extension Option β + `Handle.bytes_since_flush: AtomicU64` write-tracking in 3 file:// `sync()` branches + `EngineState::flush_handle` + PyO3 binding looking up `.tick.msgHandle`; GIL released around fsync). **P3** `engine.add_at_time()` (PyO3 wrapper around `.job.addAtTime` with tz-aware Python datetime → chili-local-wall-clock-as-UTC-ns conversion + nullary-fn invocation convention; surfaced + fixed 2 pre-existing chili bugs: `job.rs:96 next_run_time: 0 → start_time` so jobs defer, and pyo3-chrono UTC-vs-local offset). **P2** Pepper `::` null-literal disambiguation (`Op("::")` → `Expr::Nil` via `.or` branch on `lit` production in both parser_chili + parser_pepper; mdata wishlist Q2 lock-in narrowed scope from "accept `;` everywhere" to specifically `::; <ident>` ambiguity). 3-agent parallel audit pre-execution (2 CRITICAL + 4 MAJOR + 4 MINOR + 1 NIT found and folded: blanket-impl coherence rework on Part A, "wrong fix site" correction on Part C, NameError-vs-None guard, wheel-cut deliverable, every-5-sprint housekeeping trigger). mdata clarification cycle (4 Qs, locked-in via reply): Q1 tplog-only fsync, Q2 narrower-than-text bug shape, Q3 reversed Option-A→B (saves Sprint 17 ~10pp), Q4 full xfail test source provided. **0.8.4 wheel** at `dist/chili_sauce-0.8.4-cp310-abi3-macosx_11_0_arm64.whl` (sha256 `6e724eef6b526372d82b14fb2c7f6ae0eafb482e2067005f9ba79f3839451f87`). Delivery doc at `docs/sync/mdata_chili_2026-05-13_0.8.4_delivery.md`. | 10.7–18.2 (mid 14, post-audit) | ~14 | 0% vs midpoint | 2 (Part B chili-time tz convention discovery + Part B `next_run_time` chili-side bug — both fixed in-sprint) | several (initial wishlist read; Q1-Q4 clarification draft + delivery confirmation; mdata reply review; kickoff ratification) | 1 (transient: cargo test process lock-up from 6 stale `cargo test` invocations across the day; resolved by `kill -9`) | +2 Rust integration (`flush_handle_test.rs`) + +11 chili-py pytest (3 tplog_flush + 4 add_at_time + 4 pepper_syntax) | 2026-05-13 (Sprint 16 wrap commit) |
| 17 | mdata wishlist v1 P1 bundle (closes wishlist v1) — **Part B P1 publish_remote**: `engine.publish_via_handle(h, table, df)` thin one-shot wrapper over `sync()` (~3pp; per Q3 lock-in Option B, mdata owns RemoteTpClient). **Part A P1 eod-dispatch**: rewrite `signal_eod` from `sync()`-based loop to Async fire-and-forget `write_chili_ipc_msg(rw, &bytes, MessageType::Async)` matching `EngineState::publish`. Bug was H6 (NOT in original audit hypothesis space H1-H5): `sync()` conn_type match has no `Publishing` arm → every signal_eod call returned `EvalErr("cannot sync for Publishing handle")` and disconnected the subscriber, silently suppressing all EOD broadcasts. Localized in 2 instrumentation cycles. 3-agent parallel audit pre-execution (2 CRITICAL + 1 MAJOR + 5 MINOR + 1 OPPORTUNITY folded into appendix: H2 hypothesis disproved by eval.rs:125 reading, 4 chili-py API mismatches in original A.2 test, py_to_spicy→spicy_from_py_bound correction, Rust integration test dropped C5, sync() not fire-and-forget C4). Code-reviewer dispatched post-Part-A per G2 spirit: 1 MAJOR folded (redundant post-loop disconnect_handle); 1 WARNING acknowledged as pre-existing convention (publish + signal_eod both hold handle.write() across TCP writes). ADR 0001 cross-reference added per audit O2. **0.8.5 wheel** at `dist/chili_sauce-0.8.5-cp310-abi3-macosx_11_0_arm64.whl` (sha256 `62e809129827d9f2514e5f5cbb506161f1281f1e7a4e3abd1a9e56f67efb5bf2`). Delivery doc at `docs/sync/mdata_chili_2026-05-14_0.8.5_delivery.md`. mdata wishlist v1 closed on chili side. | 11–25 (mid 18, post-audit) | ~12 | −33% vs midpoint (low-edge band; H6 localized faster than budgeted hypothesis exploration) | 0 (clean scope; audit pre-folded C5 Rust integration test drop pre-impl) | 1 (Sprint 17 ratification "ratify, please proceed"; no in-sprint user touches) | 0 (gates green throughout) | +4 chili-py pytest (2 publish_via_handle round-trip + error path; 2 eod dispatch acceptance + O1 multi-message regression) + 0 Rust (C5 audit dropped Rust integration test) | 2026-05-14 (Sprint 17 wrap commit) |
| 18 | roll_tick atomic tplog segment-rollover (mdata wishlist v2 P0; thread `mdata-chili-eod-upd-race-2026-05-15`) — `EngineState::roll_tick(log_dir, segment_label)` holds `handle.write()` across open-next → fsync-old → **same-id** writer swap; replaces the racy `.tick.createLog` close-then-reopen at the boundary; no Python drain barrier. `prepare_file_writer` extracted from `open_handle`'s `file://` arm (audit CRITICAL-1; single source of truth, behaviorally unchanged — reviewer-verified). Generic opaque caller-owned `segment_label` (user directive; not date-bound; UHF size/count-roll-ready). 3-agent pre-exec audit (2 CRITICAL [open_handle file-prep extraction; `sync():1121` `rw:None` framing] + 6 MAJOR + OPP-1 + flagged the overdue every-5-sprint housekeeping) + post-impl code-reviewer (1 MAJOR failed-fsync zero-byte-artifact doc note folded). Red-first teeth harness surfaced a SECOND silent legacy failure mode — `set_handle:874` id-reuse → wrong-segment misplacement — beyond verdict-(b) gap-loss; design D fixes both. Two verify-before-claim self-catches at impl (`tick()` is `+= inc` cumulative not per-segment-reset; tplog tick slot is fixed `0` not handle id) → dissolved the brief's "deferred cross-segment-seq decision" (it carries over by construction). **0.8.6 wheel** + delivery doc `docs/sync/mdata_chili_2026-05-16_0.8.6_delivery.md` + cross-comms reply draft (user-confirm before send) + ADR 0001 Sprint-18 cross-ref. | 11–20 (mid 15, post-audit) | ~16 | +7% vs post-audit mid (upper-mid band) | 0 (red-first finding refined scope, not a pivot) | 1 (open_handle-approach AskUserQuestion; no other in-sprint user touches) | ~3 (fmt nits auto-fixed; clippy `collapsible_if`; 3 Tier-2 test-harness fixes — all fixed in-sprint, none shipped) | +17 (10 Rust integration `roll_tick_test` [3 teeth + 7 roll_tick]; 7 chili-py pytest `test_roll_tick`) | 2026-05-16 (Sprint 18 wrap commit) |

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

---

### 10-row deltas (Sprint 11 housekeeping update — 2026-05-08)

Five additional sprints (6, 7, 8, 9, 10) since the initial 5-row pass. New patterns:

### 6. Autonomous-run macOS perf-pass + research-synthesis sprints have a structural pp ceiling

Sprints 6-10 actuals: 3, 12, 4, 2, 1.5 (median 3pp). Predicted bands were 3-5, 8-15, 6-12, 5-10, 5-10 (median midpoint 9.0pp). Actual / predicted ratio: 33%, 80%, 33%, 27%, 17%. **Pattern: every autonomous-run sprint after Sprint 7 came in below 50% of predicted band**, driven by:
- Sprint 6 (housekeeping): scope shrinkage on already-clean docs tree.
- Sprint 8 (perf-pass-1): P1 was thermal noise (lesson 15); P3+P4 trivial bench fix; P2 deferred.
- Sprint 9 (perf-pass-2): symbolization infrastructure-blocked (lesson 17); P5/P6 deferred.
- Sprint 10 (ADR sprint): research already concluded; ADR is documentation.

**Implication for Sprint 12+ predictions:** roadmap-default 5-10pp bands for perf-pass / research-synthesis sprints overshoot consistently. Recalibrate to 2-5pp range for autonomous-run on this sprint shape. Implementation sprints (Sprints 3, 4, 5, 7) calibrate at the predicted band.

### 7. Mid-sprint pivot count remains stable across all 10 sprints

Total mid-sprint pivots: 5 (Sprint 2 v1→v2, Sprint 4 Part C downgrade, Sprint 5 Part B downgrade, Sprint 7 Part A disk crisis, Sprint 8 P2 deferral). All five were scope-downgrades under structural cost discovery — pattern 2 from the original 5-row analysis still holds at 10 rows.

### 8. Lesson promotion rate: 17 lessons across 10 sprints

Lessons 1-2 from Sprint 1; lessons 3 from Sprint 1 onboarding incident; lesson 4 from Sprint 2 pivot; lesson 5 from Sprint 2 v2 wrap conversation; lessons 6-7 from Sprint 3; lessons 8-9 from Sprint 4; lessons 10-11 from Sprint 5; (no lessons Sprint 6 housekeeping); lessons 12-14 from Sprint 7; lessons 15-16 from Sprint 8; lesson 17 from Sprint 9; (no lessons Sprint 10). Average ~1.5 lessons per sprint; implementation sprints typically promote 2-3 lessons; housekeeping/research-synthesis typically 0-1.

### 9. Test count delta calibration update

Cumulative chili-py pytest: 0 (pre-Sprint-3) → 65 passing + 0 xfailed (post-Sprint-9). Net +65 over 7 sprints touching chili-py = 9.3 tests per port-or-feature sprint average. Implementation sprints add 5-15 tests; bench-or-docs-only sprints add 0-2.

### 10. The "user-driven step" backlog has accumulated

Items from Sprint 7+ that the autonomous run cannot complete:
- Sprint 7+ P0: GitHub-host the chili polars fork (replace `path = "/tmp/..."` with `git = "..." + tag = "..."` in both Cargo.toml patch blocks).
- Sprint 9 P6: KDB-X CE comparison (requires interactive registration / EULA).
- Sprint 9 lesson 17 follow-up: `cargo install addr2line` OR `dsymutil` setup for symbolic resolution; OR `xcode-select --install` for `cargo flamegraph`.

These compound: until the GitHub-host migration (P0) lands, every fresh chili clone breaks at `cargo build` because `/tmp/polars-py-1.39.3` doesn't survive reboot. Sprint 12 should NOT proceed without P0 resolution OR the chili-py wheel is the only build-able artifact (which is fine for mdata but not for any future chili contributor).
