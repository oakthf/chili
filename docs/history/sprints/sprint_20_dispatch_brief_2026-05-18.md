# Sprint 20 dispatch brief — main → claude-2 merge: adopt hinmeru's lean refactors, preserve the mdata superset

**Kickoff:** BLOCKED until mdata signs off on M-1 + M-2 in `docs/sync/mdata_chili_2026-05-18_main_merge_signoff.md` (caller-side eager scaling; `overwrite_partition` rename). Do NOT execute the merge before sign-off.
**Owner:** coordinator + **mandatory 3-agent pre-execution audit** (Explore + code-reviewer + planner) — per `.claude/rules/sprint-cadence.md` (touches FFI + storage + parse-cache surfaces) and `~/.claude/rules/self-audit-on-plans.md` (subagent-heavy + ≥3 code artifacts).
**Type:** integration / upstream merge — real `git merge main`, NOT cherry-pick (iteration_lessons lesson 4). This IS the FFI-rewrite-divergence surface that caused the 2026-05-07 pivot — highest-risk merge class.
**Predicted pp:** 6–12 (mid ~9, pre-audit). Calibration: Sprint 19 (2 commits, *disjoint* surfaces, low-risk) ran 3–6 pred → ~6 actual. Sprint 20 is 3 commits, **16 git conflicts + 1 silent semantic Cargo.toml conflict + selective hunk surgery on the divergence surface** — strictly larger.
**Plan reference:** `docs/sync/mdata_chili_2026-05-18_main_merge_signoff.md` (locked per-item decisions) + this session's decision trail.
**ADR references:** ADR-0002 + ADR-0003 **preserved** (decision #4 — true-lazy-across-FFI kept); ADR-0005 **superseded** (decision #3 — `compression`/`row_group_size` kwargs removed; ADR-0005 must be amended/historised in Part B).

---

## Sprint objective

Merge `main`'s 3 new commits into `claude-2` via a real `git merge main`, resolving every conflict per the locked decision table so that **hinmeru's lean refactors + new tests are adopted wherever they don't regress an mdata-load-bearing surface, while claude-2's mdata superset (`publish_via_handle`, `roll_tick`, the full-family `py-1.39.3` polars pin / ADR-0003) is preserved**, and the two agreed caller-contract changes (M-1 eager scaling, M-2 `overwrite_partition`) land cleanly with their doc fallout.

The 3 commits (`606d1cc..main`):
- `9dfa4d2` feat: rotate_handle, query_plan, add_at_time, lazy eval, GIL-release (38 files, +1736/−209).
- `43faf44` feat: roll_tick_log + `init_tick(date→filename)` rename (engine.py, tick.pep).
- `ef4bfb2` refactor: remove `overwrite_partition` + `publish_via_handle` wrappers (engine.py, CHANGELOG, test_engine.py).

**Binary success criterion (all must hold):**
1. `git merge main` completed on `claude-2` (merge commit; `main` untouched; no remote ops).
2. Full pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py` + `cd crates/chili-py && uv run maturin develop && uv run pytest`.
3. **Zero regression of preserved surfaces:** committed end-to-end tests for `publish_via_handle` + `roll_tick` (Rust + pytest) green, unchanged signatures (`roll_tick(log_dir, segment_label)`, `init_tick(.., date)`).
4. **M-1 lands correctly:** eager `eval()` returns raw Int64 (no `_apply_column_scales`); a committed test asserts the new contract; lazy path unchanged.
5. **ADR-0003 stays resolved:** `fmt_test.rs` (q-style `0D00:00:00.000001000`) green; workspace `[patch.crates-io]` is claude-2's full-family `py-1.39.3` block — NOT main's `polars-core` git line.
6. **Codec unchanged:** a written-Parquet codec assertion shows ZSTD (golden rule 4 storage byte-stable).
7. Parse-cache golden rule 6 unaffected (confirm by inspection; bench if any hot-path hunk touched).

---

## Why now

- All per-item decisions are locked (user, this session) and captured in the sign-off doc; only mdata's M-1/M-2 acknowledgement gates execution.
- Letting upstream accumulate compounds this exact divergence surface (the pivot lesson). 3 commits now ≪ N commits later.
- The sign-off doc already isolates the only two mdata-visible changes; everything else is internal.

---

## Scope — Part A: the merge + conflict resolution

### A.1 Mechanism

Recommended: execute in a **git worktree** (`isolation: worktree`) so a botched merge never dirties the primary tree (which holds untracked `docs/sync/*` + `bus/*`). `git merge main`; resolve per A.2; `git add` only resolved paths; never `git add -A` (git-commit-hygiene). Cherry-pick forbidden (lesson 4). On unrecoverable conflict → `git merge --abort`, escalate (halt trigger 3).

### A.2 Conflict-resolution table (verified this session against claude-2 @ `48a4b68`)

| # | Path(s) | Class | Resolution |
|---|---|---|---|
| 1 | 6 add/add test+bench (`roll_tick_test.rs`, `categorical_eval.rs`, `bench_concurrent.py`, `test_add_at_time.py`, `test_pepper_syntax.py`, `test_subscriber_eod_dispatch.py`) | AA | **Take main** (`--theirs`), then re-add the claude-2-only assertions for preserved surfaces (see A.4). |
| 1 | `job.rs`, `eval_test.rs`, `chili-op/src/lib.rs`, `chili-op/src/io.rs` | UU | **Take main** per-hunk (lean refactors; `io.rs` consistent with decision #3). |
| 1 | `Cargo.lock`, `crates/chili-py/Cargo.lock` (DU) | UU/DU | Regenerate root `Cargo.lock` via a build; `crates/chili-py/Cargo.lock` stays **untracked** (`.gitignore`) — `git rm --cached` if main re-adds it. |
| 2a | `engine.py` (`eval`/`_apply_column_scales`/`set_column_scale`), `lib.rs`, `engine_state.rs` | UU | **Take main's bare `eval`** (drop `_apply_column_scales` auto-call). Keep `set_column_scale`/`clear_column_scales`/`_apply_column_scales` as **callable helpers** (mdata may still use them caller-side) but NOT auto-invoked by `eval`. M-1. |
| 2b | `engine_state.rs:~1446`, `lib.rs:~686`, `engine.py` py wrapper | UU | **Preserve claude-2** `publish_via_handle` (Rust + py). It is a claude-2-only addition; ensure main's `ef4bfb2` deletion hunk does NOT remove it (manual — it will look like a clean delete). |
| 2c | `engine.py` (`init_tick`, `roll_tick`), `tick.pep` | UU | **Preserve claude-2** `roll_tick(log_dir, segment_label)` + `init_tick(.., date)`. Do NOT adopt `roll_tick_log`/`.tick.rollLog`/`init_tick(filename)`. main's `43faf44` hunks are rejected for these symbols. Decide explicitly whether to ALSO keep `rotate_handle` (the `.handle.rotate` primitive) as dormant infra — default: take it (harmless, no caller) unless it conflicts with claude-2 roll_tick internals (ESCALATE if it does). |
| 2d | `engine.py` (`overwrite_partition`), `test_engine.py`, `CHANGELOG.md` | UU | **Take main** — drop the alias. `write_partitioned_df(overwrite=True)` retained. M-2. |
| 3 | `engine.py`/`lib.rs`/`io.rs` `write_partitioned_df` `compression`/`row_group_size` | UU | **Take main** — drop both kwargs. Codec stays ZSTD (verified: `polars-io 0.53.0 ParquetCompression::default()` = `Zstd(None)`). Loses row-group auto-clamp heuristic (perf-only) — acceptable per decision unless mdata flags its ADR-0005 bench (gate). |
| **4** | **workspace `Cargo.toml` `[patch.crates-io]`** | **SILENT — git does NOT flag it** | **Manual, mandatory.** git auto-merges this producing a file with BOTH claude-2's full-family local-path block AND main's `[patch.crates-io.polars-core] git=hinmeru/polars-core-patch.git` line = contradictory double-patch. **Resolve to claude-2's full-family `py-1.39.3` block; delete main's polars-core git line.** This is the single highest-risk step — a clean-looking merge silently un-resolves ADR-0003 if missed. Verify post-merge: `grep -n 'polars-core' Cargo.toml` shows ONLY the `/tmp/polars-py-1.39.3` path entry. |

### A.3 Storage / schema

No on-disk format change. M-1 is read-time only (storage stays Int64-quantized, golden rule 4). Codec stays ZSTD (criterion #6). `roll_tick`/tplog internals preserved unchanged (2c). No mdata wheel breaks pre-merge.

### A.4 Tests (Sprint-19 lesson #1 — MANDATORY committed e2e tests)

- **Preserved-surface guards (committed):** `roll_tick` (Rust `roll_tick_test.rs` + pytest), `publish_via_handle` (pytest) — must survive main's refactors with unchanged signatures. These are the highest-value gate (Sprint-19 lesson: wrapper-shape incompat is invisible to the Rust gate + regression suite).
- **M-1 contract test (committed, new):** assert eager `eval()` over a quantized table returns **Int64** (no auto-dequant); assert `set_column_scale` no longer affects `eval()` output; assert lazy path unchanged.
- **Codec assertion (committed or bench):** a written partition's Parquet codec == ZSTD.
- Adopt main's 6 new test/bench files; reconcile counts in the retro.

---

## Scope — Part B: doc + contract fallout

- **ADR-0005 superseded:** the `compression`/`row_group_size` public override is removed. Either amend ADR-0005 to "Superseded — kwargs removed Sprint 20; default ZSTD retained" or `git mv` to `docs/history/decisions/` with a date suffix + a one-line live pointer (docs-lifecycle rule). Update CLAUDE.md ADR list + golden-rule-4 note.
- **CLAUDE.md updates:** project-state line (post-Sprint-20 merge SHA, test counts, the "no compression kwargs" + "M-1 caller-side scaling" facts); ADR list; versions line if chili-py bumps (new/changed FFI surface → consider 0.8.6 → 0.8.7 per version-monotonicity, since `eval()` contract changed for the eager path — DECIDE in-sprint, flag to user).
- **Sign-off doc:** append the execution outcome + final dispositions; move to `docs/history/sync/` only after mdata acknowledges (it is a live contract until then).
- **iteration_lessons:** Sprint-19 lesson #2 (pre-budget clippy-conformance on adopted upstream) recurs here → promote it from candidate to ratified (cadence convention: promote on confirmed recurrence).

---

## Out of scope (defer)

- **D-1…D-4 push-model** — separate future sprint; this merge neither helps nor blocks it (verified). The proposal doc stays valid as-is.
- **Full-family polars GitHub-hosting (the standing P0)** — we KEEP claude-2's `/tmp` plan (decision #4); we do NOT adopt hinmeru's git-hosted polars-core as the solution and do NOT solve the hosting this sprint. Still tracked as the open P0.
- **Adopting `roll_tick_log`/`init_tick(filename)` as new capability** — explicitly rejected (mdata uses `roll_tick`); not a "port later" item.
- Wheel build + mdata acceptance round-trip — post-ratification follow-up, not in the merge sprint.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | merge commit on `claude-2` (`main` → `claude-2`) | new |
| 2 | resolved 16 conflicts + the silent `Cargo.toml` patch-block | edit |
| 3 | committed e2e guards: `roll_tick`, `publish_via_handle`, M-1 Int64-contract, codec | new/edit |
| 4 | ADR-0005 supersession (amend or historise + pointer) | edit/move |
| 5 | CLAUDE.md project-state / ADR / versions update | edit |
| 6 | sign-off doc execution-outcome appendix | edit |
| 7 | iteration_lessons: promote Sprint-19 clippy-conformance lesson | edit |
| 8 | `docs/sim/sprint_20_retro.md` + `cadence_metrics.md` row | new (post-sprint) |

---

## Lead allocation

Coordinator drives the merge in a **worktree** (isolation). Optional impl subagent for the mechanical Part-A hunks (the 6 AA test files + `io.rs`/`job.rs`) while coordinator owns the load-bearing 2a/2b/2c/4 hunks personally (do NOT delegate the silent Cargo.toml step or the preserve-surface hunks). Mandatory 3-agent pre-execution audit (below) before any `git merge` is run. Budget: ~1.5pp audit, ~5–9pp merge+tests+docs.

---

## Mid-checkpoint plan

At ~50% predicted-pp (≈ after Part-A conflicts resolved, pre-gate):
- Did `engine_state.rs`/`lib.rs` auto-merge as the audit predicted, or did the FFI divergence surface conflict deeper than the table assumes?
- Are `publish_via_handle` + `roll_tick` provably intact (signatures + committed tests pass)?
- Is the `Cargo.toml` patch-block resolved to claude-2's full-family block (grep-verified)?
- ETA to gate-green.

Halt-and-escalate criteria:
1. **Scope-blowing bug** — resolution would push actual > 150% predicted (e.g., `engine_state.rs` FFI surface conflicts beyond the A.2 table).
2. **Plan-pivot finding** — a preserved surface (2b/2c) cannot be cleanly retained against main's refactor without semantic change.
3. **User-decision needed** — e.g., chili-py version bump call (0.8.7?), or `rotate_handle` conflicts with claude-2 roll_tick internals, or mdata flags the ADR-0005 row-group bench.
4. **Watchdog** — 5h ≥ 80% AND remaining > 15pp.

---

## Wrap (per ceremony)

- Pre-commit gate green (full, incl. `uv run maturin develop && uv run pytest`).
- **Clippy-conformance pass on adopted upstream** pre-budgeted (~0.5pp; Sprint-19 lesson #2 — expected, not a defect).
- Committed e2e guards green (A.4); test-count delta documented.
- Bench delta only if a hot-path hunk was touched (parse-cache golden rule 6 — expected: untouched).
- ADR-0005 + CLAUDE.md + sign-off doc updated in the wrap commit set.
- `docs/sim/sprint_20_retro.md` + `cadence_metrics.md` row.
- Move Sprint-20 brief → `docs/history/sprints/` post-ratification.
- HALT until user ratifies.

---

## Pp accounting reference

| Item | Predicted pp |
|---|---|
| Brief + 3-agent audit + appendix | ~1.5 |
| Part-A mechanical conflicts (6 AA + io/job/locks) | ~1.5 |
| Part-A load-bearing hunks (2a/2b/2c + silent Cargo.toml) | ~3–5 |
| Committed e2e guards (A.4) | ~1–2 |
| Clippy-conformance on upstream | ~0.5 |
| Part-B doc fallout (ADR-0005, CLAUDE.md, iteration_lessons) | ~1–1.5 |
| **Total** | **~6–12 (mid ~9)** |

Compare vs `cadence_metrics.md`: Sprint 19 (same class, smaller) +33% over post-audit mid. Expect upper-band risk here — the divergence surface is exactly the pivot-cause surface; the audit must stress-test the A.2 "auto-merge" assumptions for `engine_state.rs`/`lib.rs`.

---

## Appendix — Independent audit (2026-05-18)

3 agents (Explore — trial-merge run; code-reviewer — claim verification; planner — sequencing) audited the draft. The coordinator then **independently re-verified the one contested item** (Explore vs code-reviewer disagreed on the Cargo.toml conflict) plus two second-order audit claims, per `verify-before-claim.md` RULE-7. Verification commands + results cited inline.

### Material corrections

1. **A.2 #4 (Cargo.toml) — DOWNGRADE BLOCKER → grep-verify. Brief overstated.** Independently re-run: `git merge --no-commit --no-ff main` → Cargo.toml is **not** in the conflicted set, has **no** conflict markers, and the merged file contains **only** claude-2's `[patch.crates-io]` full-family block (`polars-core = { path = "/tmp/polars-py-1.39.3/..." }`, line 83); the only `hinmeru/polars-core-patch` strings are explanatory **comments** (lines 67/69), not a live `[patch.crates-io.polars-core]` git stanza. `git merge --abort` restored the tree. **git auto-resolves ADR-0003 correctly with zero manual surgery.** Action reduces to: post-merge `grep -n 'polars-core' Cargo.toml` confirms the `/tmp` path line and no git line. (code-reviewer's "git produces both blocks" was inference; Explore's empirical run + this re-run refute it.)

2. **NEW — Step 0 BLOCKER (real; supersedes the false #4 concern): `/tmp/polars-py-1.39.3` is a broken empty stub.** Verified: `ls /tmp/polars-py-1.39.3/crates/polars-core/src/*.rs` → no matches; `crates/polars/Cargo.toml` missing. The Cargo.toml correctly preserves the full-family py-1.39.3 pin (correction 1) — but it points at a fork that does not exist on disk, so **`cargo build` (hence the entire pre-commit gate, hence the sprint) fails before the merge even starts.** **Step 0, mandatory, before any `git merge` or `cargo` command:** run the `vendor/polars-core/README.md` reconstruction protocol (`git clone --branch py-1.39.3 --depth 1 pola-rs/polars /tmp/polars-py-1.39.3` then `git apply vendor/polars-core/chili-port-py-1.39.3.patch`), verify with `cargo test --workspace --exclude chili-py -- fmt_test` green (q-style `0D00:00:00.000001000`). This is also halt-trigger-0.

3. **A.2 #3 (wpar) is NOT a clean per-hunk "take main" — it is a coordinated atomic multi-file removal.** Taking main for #3 requires, in one resolution unit: drop the `config` arg on the `write_partition_native` call site in `io.rs`; remove the `parse_compression_name` helper + `ParquetWriteConfig` struct (claude-2-only, ~`io.rs:297` carries a stale "Snappy in polars 0.53" comment — delete with it); AND remove the matching `test_engine.py` ADR-0005 classes (`TestParquetWriteDefaults`/`TestParquetWriteConfig`, ~`:553–692`). If `engine.py`/`lib.rs`/`io.rs` are taken but the `test_engine.py` hunks are missed, `uv run pytest` reddens mid-gate. Add this atomic-pairing note to #3.

4. **NEW pre-merge prerequisite — `rotate_handle` × `roll_tick` interaction (was an in-sprint ESCALATE; promote to pre-merge read).** `rotate_handle` is absent on claude-2 (grep → 0 hits). Before `git merge`, read `9dfa4d2`'s `engine_state.rs` diff to confirm `rotate_handle` does **not** share `prepare_file_writer`/writer-swap state with claude-2's Sprint-18 `roll_tick`. ~0.1pp; eliminates a plausible 2–3pp mid-merge abort (halt-trigger-2 scenario).

5. **NEW pre-merge USER decision — chili-py 0.8.6 → 0.8.7 (was "decide in-sprint").** M-1 changes the eager `eval()` public contract (auto-dequant removed). A third semantically-distinct 0.8.6 violates the version-monotonicity principle (CLAUDE.md already records two distinct 0.8.6 builds). This must be a user call **before** execution — it determines what the merge/wrap commits and CLAUDE.md state. Surfaced to user alongside the mdata sign-off gate.

### Cross-cutting gates (kickoff now has 4, was 1)

Execution is blocked until ALL hold: (G1) mdata signs off M-1+M-2 in the sign-off doc; (G2) user decides the 0.8.7 bump (correction 5); (G3) Step-0 polars-fork reconstruction green (correction 2); (G4) pre-merge `rotate_handle` diff read clears (correction 4). G3+G4 are coordinator-side and cheap; G1+G2 are external/user.

### Revised sequencing

Resolution order (fail-fast): **Step 0 reconstruction (corr. 2) → `git merge` → grep-verify Cargo.toml (corr. 1) → load-bearing hunks 2a/2b/2c (coordinator, never delegated) → atomic #3 multi-file removal (corr. 3) → mechanical #1 → gate.** Rollback: if the gate cannot be made green within halt-trigger-1 headroom, discard the worktree, file a scoped follow-up brief isolating the failing hunk — do not partial-commit.

### Sprint sizing

Revised band **7–16 pp, mid ~11** (was 6–12 / mid 9). planner: the 6 AA test files are not clean drop-ins (re-add step) + M-1 contract test + `publish_via_handle` guard = 3 non-trivial test-authoring chunks; plus Step-0 reconstruction and the #3 multi-file entanglement. code-reviewer found mid-9 defensible as a *floor*, not a ceiling. Treat ≥12 as the realistic upper, not the cap.

### Second-order audit corrections (RULE-7 — audit claims re-verified)

- code-reviewer C1 "corrected" `publish_via_handle` to `engine_state.rs:1495`. **Rejected — itself wrong.** `grep -n 'pub fn publish_via_handle' crates/chili-core/src/engine_state.rs` → `1446`. The brief's `~1446` was correct; the "correction" is an audit-introduced error (baseline-doc-audit Pattern 4) and is not propagated.
- Conflict count stated explicitly: **16 paths** (Explore-confirmed; the A.2 table groups them by region — enumeration complete, not missing files).

---

## Decisions resolved (2026-05-18, post-audit — user)

- **G2 RESOLVED — stay 0.8.6 this sprint; NO version bump; NO wheel delivery to mdata this sprint.** The 0.8.7 bump is **deferred** until BOTH this merge **and** the push-model D-1…D-4 work land; the bump + a single combined wheel delivery happen *together* after the push-model sprint. Supersedes appendix correction 5 and the Part-B "consider 0.8.6→0.8.7 in-sprint" line — both are now closed: do not bump, do not deliver.
  - **Binding safety condition (load-bearing — wrap MUST assert this):** mdata continues running its pinned **Sprint-18 0.8.6 wheel** (sha `8881337…`, unchanged). No post-Sprint-20 0.8.6 artifact is delivered to mdata. The next artifact mdata ever receives is **0.8.7** (merge + push-model, one clean superset). This keeps version-monotonicity intact in the *delivered* space.
  - **Doc-hygiene consequence (Part B, this sprint):** post-merge the on-disk `dist/chili_sauce-0.8.6-…whl` + `target/release/chili` change content again → CLAUDE.md's "two distinct 0.8.6 builds" note must update to *N* distinct dev builds, and explicitly state "0.8.6 label frozen for delivery at the Sprint-18 sha; dev builds diverge; 0.8.7 is the next delivered version." Part-B deliverable; not optional.
- **G1 — user is pinging mdata** for M-1+M-2 sign-off. Reminder folded for that ping: the **post-merge push-model sprint cannot scope until mdata answers Q1–Q5** (per `docs/sync/mdata_push_model_proposal_2026-05-17.md` — Q1 per-handle-seq, Q3 frame semantics, Q4 kill-9 durability, Q5 fork-safety). Bundle both asks (M-1/M-2 sign-off + Q1–Q5) into the one mdata ping so the next sprint isn't gated a second time.
- **Remaining kickoff gates:** G1 (mdata M-1/M-2 sign-off — with mdata), G3 (Step-0 polars-fork reconstruction — coordinator, on execution), G4 (`rotate_handle` pre-merge diff read — coordinator, on execution). **G2 resolved.** Brief deliverable #5/#6: the version bump is removed; CLAUDE.md update becomes the multi-0.8.6 doc-hygiene note above.

## mdata sign-off received + item-3 reclassified (2026-05-18)

mdata bundled-responded in `~/code/mdata/docs/sync/chili_wishlist_2026-05-17_push-model.md` (lines 105–151; mirrored into chili's canonical `docs/sync/mdata_push_model_proposal_2026-05-17.md`).

- **G1 SATISFIED.** M-1 ✅ signed off (mdata verified no eager consumer relies on `_apply_column_scales`; owns `db/quantize.py`). M-2 ✅ signed off — **no-op for mdata** (zero call sites; uses its own `StorageEngine.write_partition`). Preservation ✅ acked with concrete deps (`tp/remote_client.py:288` publish_via_handle, `tp/tickerplant.py:311` roll_tick, `polars==1.39.3` pin, ADR-0003 LazyFrame constraint).
- **⚠ A.2 #3 RECLASSIFIED — was "take main, no mdata action / perf-only"; it is a HARD SIGNATURE BREAK.** `src/mdata/db/storage.py:107-117` passes `compression=`/`row_group_size=` on **every** partition write → dropping them raises `TypeError` on mdata's hottest write path. (Evaluation miss on chili's side: "no action" was asserted without verifying mdata's call sites — should have been marked unverified. mdata's sign-off caught it; verify-before-claim lesson.) **Resolution = mdata's option (ii) hard-coordination** (NOT the (i) deprecation shim). Decision #3 ("take main", drop both kwargs) **stands unchanged** — chili still drops them; (ii) is satisfied **for free by G2**: no post-merge 0.8.6 wheel ever ships to mdata; the next and only artifact mdata receives is **0.8.7** (built post merge **+** push-model), by which point mdata has long since landed its trivial zero-behaviour `storage.py` kwarg-drop. (i) would add chili dead-kwarg API debt for zero benefit under G2.
  - **A.2 #3 disposition (replaces the "perf-only" note):** take main (drop kwargs + `parse_compression_name` + `ParquetWriteConfig` + the `test_engine.py` ADR-0005 classes, atomically per appendix correction 3). No shim. The break never reaches mdata pre-0.8.7 (G2). ADR-0005 row-group re-bench = mdata-owned post-merge follow-up (acked, not a blocker).
- **Merge is NOT gated on mdata's (ii) confirmation.** (ii) = "don't ship the wheel until coordinated"; G2 already guarantees no wheel ships until 0.8.7. So the Sprint-20 merge (which produces no delivered wheel) proceeds now; mdata's formal (ii)+0.8.7-only agreement sits on the **0.8.7-delivery** critical path, not the merge-execution path.
- **Remaining kickoff gates: G3 + G4 only** (both coordinator-side, cleared at execution). G1 satisfied; G2 resolved; item-3 resolved (ii).
- **Push-model gates Q1–Q5 all answered + accepted** (Q1=Path 1, Q2=chili discretion/lean (a), Q3=raw-as-sent, Q4=confirmed per-drain-ack, Q5=no-fork+defensive-close-on-exec; D-2 reword accepted; sizing accepted). → becomes **Sprint 21**, own dispatch brief, scoped post-Sprint-20-ratification. Not in Sprint-20 scope.

## Pre-execution audit #2 (2026-05-18) — post-mdata-signoff refresh

2-agent audit (Explore execution-readiness + G4 clearance; code-reviewer final-claim verification) on the finalized plan. Material results:

- **G4 RESOLVED → (a) DISJOINT (gate cleared).** Explore read `git show 9dfa4d2 -- crates/chili-core/src/engine_state.rs`: `rotate_handle` (9dfa4d2, ~:753–783) creates a NEW handle slot via `set_handle()`; claude-2's `roll_tick` (~:863+) swaps the writer inside an EXISTING handle under `handle.write()`. Only shared symbol is the stateless `prepare_file_writer`. No shared mutable state / no `tick_count` / `.tick.msgHandle` collision. **Take `rotate_handle` as a dormant addition; preserve `roll_tick` on the HEAD side. G4 closed — no remaining coordinator-side pre-merge gate.**
- **NEW BLOCKER — A.2 #3 atomic-removal set was INCOMPLETE.** `crates/chili-op/benches/write_partition.rs` (`:16,:24,:67`) imports `ParquetWriteConfig` + calls `write_partition_native` with it. Dropping the struct/helper without also removing/updating this bench fails `cargo clippy --all-targets -- -D warnings` (bench target) → reds the pre-commit gate. **A.2 #3 atomic unit is now: `engine.py` kwargs + `lib.rs` + `io.rs` (`ParquetWriteConfig` struct + `parse_compression_name` + the `config` call-site arg) + `chili-op/src/lib.rs:17` re-export + `chili-op/benches/write_partition.rs` (drop the codec A/B variants) + `test_engine.py` `TestParquetWriteConfig`/ADR-0005 classes — ALL in one resolution unit.**
- **C6 — Sprint-21 carry-forward (not Sprint-20):** Q1 dissolves the per-table-seq blocker but `UpdEvent.seq_lo/seq_hi` field semantics (carry chili's per-handle `tick_count` ordinal vs rename/drop to avoid collision with mdata's own row-level `seq`) is an explicit **Sprint-21 dispatch-brief must-decide** — recorded here so it isn't lost; out of Sprint-20 scope.
- **Reconfirmed:** 16 conflict paths stable (no drift); Step-0 patch (`vendor/polars-core/chili-port-py-1.39.3.patch`, 113 lines) targets `polars-core/src/fmt.rs`, protocol internally consistent; M-1 does not red the gate (`TestColumnScale` calls `_apply_column_scales` directly, not via `eval()`; main drops it, A.4 replacement test covers); `publish_via_handle` = `engine_state.rs:1446` (the "1495" was conflict-marker line inflation — appendix rejection stands); C5 merge-not-gated-on-mdata-(ii) TRUE.
- **Sole remaining blocker: Step-0 polars-fork reconstruction** (`/tmp/polars-py-1.39.3` is a stub). Mandatory first action; nothing else blocks. **Sprint-20 is execution-ready once Step-0 completes.**
