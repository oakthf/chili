# Sprint 19 dispatch brief — upstream merge: IPC remote query + chiz package imports (main → claude-2)

**Kickoff:** 2026-05-16 — user uploaded new upstream state into local `main`; `main` HEAD `606d1cc feat: add open_handle and sync for IPC remote queries` is 2 commits ahead of `claude-2` (merge-base `7ebb9196`).
**Owner:** coordinator-solo + mandatory 3-agent pre-execution audit (Explore + code-reviewer + planner)
**Type:** integration / upstream merge (touches the chili-py FFI + handle surface — dispatch brief mandatory per `.claude/rules/sprint-cadence.md`; real merge, NOT cherry-pick, per `iteration_lessons` lesson 4)
**Predicted pp:** 3–7 (mid 5, pre-audit)
**Plan reference:** branch policy in `CLAUDE.md` ("Merging: Only `main → claude-2`")
**ADR references:** none (no model change; ADR 0001 pub/sub unaffected — feature is request/response query, not pub/sub)

---

## Sprint objective

Merge `main`'s 2 new commits into `claude-2` via a real `git merge main`, resolving conflicts so **claude-2's Sprint-16/17/18 superset wins on its diverged surface (`sync`/`open_handle`/`roll_tick`/`prepare_file_writer`/tplog internals) while upstream's net-new capability is adopted verbatim where claude-2 has no competing change**.

The 2 commits:
- `5a6adc5` — chiz package imports (`import "@scope/pkg/mod"` → `$CHIZPATH` resolution); rewrites `import_source_path`, adds `resolve_package_import`/`resolve_package_version`, `chi_pkg_test.rs`, `serde_json` dep.
- `606d1cc` — IPC remote query: `engine_state.rs` adds `eval_call` to the `eval::` import + one `SpicyObj::I64(_) => eval_call(...)` arm to `fn_call`'s match; `engine.py` adds `open_handle()`/`sync()`; README IPC section.

**Binary success criterion (all must hold):**
1. `git merge main` completed (a merge commit on `claude-2`; `main` untouched, no remote ops).
2. Full pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py` + `uv run pytest`.
3. **Zero regression** to claude-2 FFI/handle work: `flush_handle_test`, `roll_tick_test` (Rust) + `test_publish_via_handle`, `test_subscriber_eod_dispatch`, `test_roll_tick`, `test_tplog_flush` (pytest) all green, unchanged counts.
4. **New capabilities work:** loopback `engine.open_handle("chili://…")` + `engine.sync(h, b"1+1")` → `2`; a `$CHIZPATH` package `import` resolves.
5. Parse-cache golden-rule 6 unaffected (no hot-path touched — confirm by inspection).

---

## Why now

- User manually uploaded the upstream state and explicitly asked to merge the IPC-remote-query feature; keeping `claude-2` current with upstream is the standing branch contract.
- Low-risk window: verified the 2 upstream commits touch **disjoint `engine_state.rs` functions** from claude-2's divergence (`fn_call`-match + `import_source_path` vs claude-2's `sync`/`open_handle`/`roll_tick`) — this is NOT the FFI-rewrite-conflict surface that caused the pivot. Merging now (2 commits) is far cheaper than letting upstream accumulate.

---

## Scope — Part A: the merge + conflict resolution

### A.1 Mechanism

`git checkout claude-2` (already there) → `git merge main`. **Never** commit to `main`; **never** `push/pull/fetch` (no remote). Cherry-pick is forbidden (lesson 4). Resolve each conflict per A.2, `git add` only the resolved paths, complete the merge commit.

### A.2 Conflict-resolution table (verified against current claude-2)

| Path | Expected | Resolution |
|---|---|---|
| `engine_state.rs` — `606d1cc` import + `fn_call` I64 arm | auto-clean | claude-2 `fn_call` match + `eval::{…}` import byte-identical to main-pre-`606d1cc`; `eval_call` exists `eval.rs:544` with the exact 6-arg sig. Adopt as-is. |
| `engine_state.rs` — `5a6adc5` `import_source_path` + new methods | auto-clean | claude-2 never modified `import_source_path`. Pure addition. |
| `chi_pkg_test.rs` | auto-clean | new file. |
| `chili-py/README.md` | auto-clean | claude-2 untouched since base. |
| `chili-py/chili/engine.py` | **conflict** | (a) append `open_handle()`/`sync()` AFTER claude-2's `roll_tick` (claude-2 has no such methods; author placed them after `subscribe`, where claude-2 has more). (b) `tick()` cosmetic-whitespace hunk: **keep claude-2's** `def tick(self, index: int = 0, inc: int = 1)` (superset), drop author's change. |
| `crates/chili-py/Cargo.lock` | **drop** | `606d1cc` adds it tracked; claude-2 `.gitignore:28` ignores it. `git rm --cached` if added; do not track. |
| `chili-core/Cargo.toml` + workspace `Cargo.lock` | **conflict** | add `serde_json` dep (`5a6adc5` needs it; absent on claude-2); regenerate `Cargo.lock` via a build. |
| `Cargo.toml` / `pyproject.toml` / `chili-py/Cargo.toml` versions | **conflict** | **keep claude-2's** (chili-py `0.8.6`, workspace `0.8.1`). Upstream's bumps are its own scheme; claude-2 owns wheel versioning (branch policy). |
| `Taskfile.yml` | **conflict** | take `606d1cc`'s removal of the `git add -A` + auto-commit from the version task (upstream improvement + removes an anti-hygiene `git add -A`), unless claude-2's Taskfile diverged there — 3-way will show. |

### A.3 Storage / schema

No on-disk format change. No tplog/handle-internal change. Int64-quantized convention untouched. The upstream feature is request/response query over an Outgoing chili:// handle (claude-2's `sync()` Outgoing branch already implements send+receive — verified; `eval.rs:454/641` already routes applied-I64→`sync()`). The `fn_call` I64 arm is additive enablement, not a behavior change to the diverged surface.

### A.4 Tests

Adopt `chi_pkg_test.rs` (upstream). No new claude-2 tests required for the merge itself; the new-capability smoke (criterion 4) is a verification step, not a committed test, unless the audit recommends a regression test for the `fn_call` I64 arm.

---

## Out of scope (defer)

- **mdata wheel delivery.** mdata's wishlist v2 P0 (`roll_tick`) already shipped as 0.8.6; IPC-remote-query was not requested by mdata. No 0.8.7 cut / delivery doc / cross-comms reply unless the user or mdata asks. (If cut later, it's a separate small task.)
- Any refactor of claude-2's `sync()`/`open_handle` to "align" with upstream — they are a deliberate superset; do not touch.
- Upstreaming anything `claude-2 → main` (branch policy forbids).
- `chiz` packaging tooling beyond what `5a6adc5` ships.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | Merge commit on `claude-2` (`git merge main`, conflicts resolved per A.2) | new |
| 2 | `chili-core/Cargo.toml` + `Cargo.lock` — `serde_json` dep | edit |
| 3 | `chili-py/chili/engine.py` — `open_handle()`/`sync()` adopted, claude-2 `tick()` kept | edit |
| 4 | Adopted upstream files (`import_source_path`, `resolve_package_*`, `chi_pkg_test.rs`, README) | merge |
| 5 | `docs/sim/sprint_19_retro.md` + `cadence_metrics.md` row + `sprints_index.md` row | new (post-sprint) |
| 6 | `CLAUDE.md` Project-state: workspace-version / merge-state / test-count refresh | edit |

---

## Lead allocation

Coordinator-solo. **Mandatory pre-execution 3-agent parallel audit** (Explore + code-reviewer + planner) per `self-audit-on-plans.md` — touches the FFI/handle surface, ≥ 3 work items, current-state claims. Appendix folded before `git merge`. No worktree (single linear sprint). Code-reviewer post-merge only if the audit flags a non-trivial resolution.

---

## Mid-checkpoint plan

After `git merge main` + conflict resolution, before the gate — post status:
- Did `engine_state.rs` (both upstream hunks) auto-merge clean as predicted? If a logic conflict appeared on the handle surface → **halt + escalate** (premise was "disjoint functions").
- serde_json + Cargo.lock regenerated; versions kept at claude-2's.
- ETA to gate.

Halt-and-escalate criteria:
1. Scope-blowing — a real logic conflict on `sync`/`open_handle`/tplog internals (contradicts the disjoint-surface premise).
2. Plan-pivot — `eval_call` signature or `fn_call` context turns out NOT byte-identical (audit must catch pre-exec).
3. User-decision — upstream version-scheme collision needs a versioning policy call.
4. Watchdog — 5h ≥ 80% (not expected; small sprint).

---

## Wrap (per ceremony)

- Pre-commit gate green (fmt + clippy `-D warnings` + `cargo test --workspace --exclude chili-py`).
- `cd crates/chili-py && uv run maturin develop && uv run pytest` (chili-py touched).
- Sprint-16/17/18 regression suites green, counts unchanged + `chi_pkg_test` added.
- New-capability smoke recorded (sync query round-trip; chiz import).
- Bench: no hot-path touch — confirm parse_cache criterion still ≤ 400 ns only if any chili-core recompile risk to it (none expected).
- Retro + cadence row + index row + CLAUDE.md state. Brief → `docs/history/sprints/` post-ratification.
- HALT until user ratifies.

---

## Pp accounting reference

- **Sprint 14** (small surgical FFI, `py.detach` 2 methods) — predicted 5–9, actual ~5. Closest comparable: small, surgical, FFI-adjacent.
- **2026-05-13 upstream merge** (commits 7ebb919..b91680f → claude-2) — prior `main → claude-2` precedent; mechanical.
- Sprint 19 expected **low band (~3–5)**: 2 commits, logic auto-clean (verified), conflicts mechanical (versions/placement/lockfile). Upper edge (7) reserved for: serde_json/Cargo.lock release-rebuild cost (lesson 8/11 — dep change triggers a rebuild) or an unforeseen handle-surface conflict.

---

## Cross-references

- Branch policy: `CLAUDE.md` "Branch policy" + `iteration_lessons` lesson 4 (cherry-pick → invert to merge).
- Upstream commits: `5a6adc5`, `606d1cc` (local `main`).
- Adjacent claude-2 work the merge must not regress: Sprint 17 `publish_via_handle`/`signal_eod` retro, Sprint 18 `roll_tick` retro.
- Prior upstream sync: CLAUDE.md note "post-2026-05-13 merge of upstream main, commits 7ebb919..b91680f".

---

## Appendix — Independent audit (2026-05-16)

3-agent parallel audit (Explore + code-reviewer + planner) per `self-audit-on-plans.md`. Original brief preserved. Contested findings independently re-verified by the coordinator (RULE-7 second-order); citations replayed.

### Material corrections

**MC-1 — `chili-core/Cargo.toml` is an add/add conflict, not just "add serde_json" (was A.2 row understated).** Verified `git show 5a6adc5 -- crates/chili-core/Cargo.toml`: it adds `serde_json = "1.0"` (**`[dependencies]`** — used by `resolve_package_version`'s `serde_json::from_str`, production code) **and** `tempfile = "3"` + `serial_test = "3"` (**`[dev-dependencies]`**, for `chi_pkg_test.rs`). claude-2 **already has `tempfile = "3"`** in `[dev-dependencies]` (Sprint 16, `flush_handle_test`). So the merge produces an add/add `[dev-dependencies]` conflict. **Resolution:** take `serde_json` (deps) + `serial_test` (dev-deps, required — `chi_pkg_test.rs` uses `#[serial]`); **dedupe `tempfile` to a single line** (keep one). Not "add serde_json" alone.

**MC-2 — the new `fn_call` I64 arm has an empty-args panic path; accept upstream verbatim.** Verified `eval.rs:641` `SpicyObj::I64(h) => state.sync(h, args[0])` indexes `args[0]` unconditionally. `606d1cc`'s arm routes `fn_call(name,&[])` (name→I64, zero args) → `eval_call` → that arm → panic, where claude-2's pre-merge `_ => Err(...)` returned a typed error. **Decision: accept upstream behavior as-is.** It is upstream's API on the exact surface we are converging; adding a claude-2-only guard would re-diverge (branch policy: adopt upstream net-new verbatim). Degenerate misuse path. Documented, not guarded.

**MC-3 — `chi_pkg_test.rs` is gate-safe (planner CRITICAL #1 resolved).** Verified `git show 5a6adc5:…/chi_pkg_test.rs`: each test does `tempfile::tempdir()` + `unsafe{env::set_var("CHIZPATH",…)}` + `env::remove_var` under `#[serial]`. Self-contained; no ambient env/fixture. `cargo test --workspace` will pass **provided `serial_test` is adopted (MC-1)**.

**MC-4 — version-conflict scope clarified (code-reviewer #6).** The only Cargo version conflict is root `Cargo.toml` `[workspace.package] version` (claude-2 `0.8.1` vs `5a6adc5` `0.8.2`) — **keep claude-2 `0.8.1`**. `chili-py/Cargo.toml` is `version.workspace = true` (no independent field, no conflict). `pyproject.toml` wheel version `0.8.6` is non-Cargo — keep claude-2's. The brief's "chili-py 0.8.6 Cargo version" was a conflation.

> **[Superseded 2026-05-16 by user decision — original MC-4 above preserved as audit trail.]** Keeping the workspace at `0.8.1` was a content/version *inversion*: claude-2 strictly supersets upstream `0.8.2` (94 commits ahead, `main` 0 ahead), so a version below upstream's misrepresents content. Resolution corrected: root `[workspace.package] version` → **`0.8.6`**, aligned to claude-2's deliverable line (the chili-py wheel was already `0.8.6`); workspace members (`version.workspace = true`) + chili-py now coherently `0.8.6`, eliminating the workspace-vs-wheel split. Also: MC-4's "chili-py/Cargo.toml is `version.workspace = true` (no independent field)" was inaccurate — it carries an explicit `version = "0.8.6"` (a real merge conflict, resolved). Build-verified neutral (gate green, 23 ok-blocks). Recorded in the Sprint-19 retro + cadence row.

**MC-5 — planner #5 REFUTED (second-order audit error).** The audit claimed every-5-sprint housekeeping is overdue (deferred through Sprints 16/17/18). **False.** Verified: commit `0ada8d8 docs(sprint-18): wrap — … every-5-sprint housekeeping` + Sprint 18 retro — Sprint 18 (this session) performed the full sweep (CLAUDE.md state, 0.8.5→history, lesson promotion, memory). Last sweep = **Sprint 18**; rows since = 0. **Housekeeping is NOT due at Sprint 19.** The agent missed it because Sprint 18's sweep was folded into the wrap row, not a standalone "deep housekeeping" row (Sprint 6/11 pattern). Recorded per Pattern 4 — the unverified "correction" is not propagated.

### Additional opportunities surfaced

**ADD-1 — `fn_call` I64-arm regression test (planner #3a, accepted).** Neither upstream nor claude-2 tests the I64 dispatch arm; criterion 3 ("zero regression") can't catch a future drop. Add one Rust integration test: a var resolving to a valid handle id, `fn_call`'d with a query arg, asserts the round-trip. ~1pp.

**ADD-2 — IPC-remote-query smoke → committed pytest (planner #3b, accepted).** Convert criterion 4 from a manual step to `crates/chili-py/tests/test_ipc_remote_query.py` (loopback `chili://` engine; `open_handle` + `sync(h, b"1+1") == 2`). Protects the headline feature + the diverged `sync()` surface against future merge regressions. ~0.5pp.

**ADD-3 — state 0.8.6 wheel byte-stability (planner #3c).** Add to wrap: the `dist/chili_sauce-0.8.6` wheel is a frozen artifact; this merge does not alter it or mdata's installed copy.

### Cross-cutting gates

- Post-`git merge`, BEFORE the gate: confirm the `eval::{…}` import did not drop `eval_call` and the `fn_call` I64 arm landed (Explore/code-reviewer: 3-way auto-clean because context is byte-identical to merge-base, but verify — cheap).
- Dedupe `tempfile` in `chili-core/Cargo.toml` during conflict resolution (MC-1) — a duplicate key is a hard Cargo error.
- `cargo build` once after resolving `Cargo.toml` (before `cargo test`) — faster, cleaner failure if a manifest edit is malformed (planner #2).
- Regression suites (Sprint 16/17/18) + ADD-1/ADD-2 must be green before commit.

### Revised sequencing

0. `git merge main` → resolve conflicts per A.2 **as corrected by MC-1/MC-4** (claude-2 superset on diverged surface; dedupe tempfile; keep claude-2 versions; drop gitignored `chili-py/Cargo.lock`).
1. Verify import/`fn_call` arm landed (cross-cutting gate).
2. `cargo build` (fetch/link serde_json + serial_test; regenerate `Cargo.lock`).
3. ADD-1 (`fn_call` I64 test) + ADD-2 (`test_ipc_remote_query.py`).
4. Full gate + `maturin develop` + `pytest` + Sprint-16/17/18 regression.
5. New-capability smoke (now ADD-2 covers the IPC half; chiz import via the adopted `chi_pkg_test.rs`).
6. Retro + cadence row + index + CLAUDE.md state. HALT for ratification. (No housekeeping — MC-5.)

### Sprint sizing

`serde_json` / `tempfile` / `serial_test` are **already transitively in the workspace `Cargo.lock`** (no cold compile / registry fetch) → link-only rebuild, low cost. Real upper-edge risk is a handle-surface conflict escalation (halt criterion 1), not dep rebuild. **Revised pp band: 3–6 (mid ~4.5)** (was 3–7), +~1.5pp absorbed for ADD-1/ADD-2. Comparable: Sprint 14 (~5, small surgical FFI).
