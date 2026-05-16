# Sprint 19 retro — upstream merge: IPC remote query + chiz package imports (main → claude-2)

**Wrap:** 2026-05-16
**Predicted:** 3–6 pp (mid 4.5, post-audit; revised from 3–7)
**Actual:** ~6 pp (best-effort; upper band — ADD-2 caught a broken headline feature → real adaptation work, not a clean merge; + upstream clippy-conformance; the disk-full block was wall-time/environmental, not token cost)
**Variance:** ~+33% vs post-audit mid (upper edge of band)
**Owner:** coordinator-solo + 3-agent pre-exec audit
**Plan reference:** `docs/history/sprints/sprint_19_dispatch_brief_2026-05-16.md`

---

## Scope shipped

- **Merge commit `f04e9e8`** — `git merge main` (real merge, not cherry-pick; branch policy). Absorbed `5a6adc5` (chiz package imports) + `606d1cc` (IPC remote query). `engine_state.rs` auto-merged clean (both upstream hunks: `fn_call` I64 arm + `eval_call` import; `import_source_path`/`resolve_package_*`). Conflicts resolved per the audited table: `engine.py` union; versions kept claude-2's (wheel `0.8.6`; workspace initially reverted to `0.8.1` per MC-4, then **amended post-wrap 2026-05-16 to `0.8.6`** — user flagged MC-4 as a content/version inversion: a strict superset must be ≥ upstream `0.8.2`; workspace+wheel now coherently 0.8.6, build-verified neutral); `serde_json`+`serial_test` adopted, `tempfile` deduped; `crates/chili-py/Cargo.lock` untracked (gitignored); `Cargo.lock` regenerated.
- **Fixup commit `b0b5f89`**:
  - `engine.py sync()` **adapted to claude-2** — upstream's `self.eval("pyHandle",[query])` is incompatible with claude-2's ADR-0002-diverged `eval()` (2nd positional = `src_path`). Routed via `fn_call` (606d1cc's own I64 arm → `eval_call` → `state.sync`). **ADD-2 caught this — the headline feature did not work as-shipped on claude-2.**
  - `engine_state.rs` **clippy-conformance** on upstream `5a6adc5` `resolve_package_version` (`ptr_arg` `&PathBuf`→`&Path`; 7× `collapsible_if` → edition-2024 let-chains). Behavior-preserving.
  - **ADD-1** `fn_call_i64_test.rs` (regression guard for the 606d1cc I64 arm) + **ADD-2** `test_ipc_remote_query.py` (loopback `open_handle`+`sync`).

Tests: **Rust +12** (182→194: `chi_pkg_test.rs` 11 adopted from 5a6adc5 + `fn_call_i64_test.rs` 1) · **pytest +2** (94→96: `test_ipc_remote_query.py`). Gate: Rust 23 ok-blocks / `pytest` 96 passed, 0 xfailed. **Zero regression** — all Sprint-16/17/18 suites green (criterion 3). Parse-cache golden rule 6 untouched.

---

## Lessons (durable)

### 1. Merging upstream net-new with a thin Python/FFI wrapper REQUIRES a committed end-to-end test of the headline feature

**Rule.** When a `main → claude-2` merge adopts an upstream feature whose surface includes a Python/FFI shim, you MUST add/keep a *committed* end-to-end test exercising that shim against claude-2's (possibly diverged) wrapper API. Never rely on the Rust gate, the regression suite, or a manual smoke — a wrapper-shape incompatibility is invisible to all three.
**Why.** Sprint 19: `606d1cc`'s `engine.py sync()` called `self.eval("pyHandle",[query])`. claude-2's `eval()` 2nd positional diverged to `src_path` (ADR 0002 `lazy`/`src_path`). Result: `TypeError`, headline feature **broken as-shipped on claude-2**. The Rust gate (23 ok-blocks) and the full Sprint-16/17/18 regression (94 passed) were ALL green — it's a Python-API-shape incompat, structurally invisible to them. Only ADD-2 (the audit-mandated committed IPC test, planner #3b — converted from a "manual smoke") caught it.
**Apply where.** Every upstream merge that adopts a feature with a `chili-py` wrapper or PyO3 method (the divergence surface that caused the 2026-05-07 pivot in the first place).
**Cost saved.** A broken headline feature shipped to `claude-2` (and potentially into a future mdata wheel). High. Vindicates `self-audit-on-plans.md`'s "convert smoke → committed test" audit move.

### 2. Upstream code routinely fails claude-2's stricter pre-commit gate — budget a clippy-conformance pass as a standard merge step

**Rule.** Every `main → claude-2` merge brief must pre-budget a "clippy-conformance pass on adopted upstream code" line. Upstream does not gate `clippy --all-targets -D warnings`; claude-2 does. Behavior-preserving conformance (let-chains, `&Path`, etc.) is expected, not a defect.
**Why.** Sprint 19: `5a6adc5`'s `resolve_package_version` shipped with `clippy::ptr_arg` + 7× `clippy::collapsible_if` — clean on `main`, hard-fail on claude-2's gate. ~8 lint errors fixed behavior-preservingly. (Same shape as the Sprint-18 self-caught `collapsible_if`.)
**Apply where.** Every future `main → claude-2` merge. Candidate — promote to `iteration_lessons.md` at the next merge that recurs it (cadence convention).
**Cost saved.** ~0.5pp/merge of "surprise gate-fail mid-merge"; prevents mistaking expected lint-conformance for a merge defect.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| Brief + 3-agent audit + appendix | ~1.5 | ~1.5 |
| `git merge` + conflict resolution (per audited table) | 1–2 | ~1.5 |
| Upstream clippy-conformance (5a6adc5) | (folded) | ~0.5 |
| ADD-1 + ADD-2 + sync() adaptation (ADD-2-caught) | ~1.5 | ~2 |
| Gate + regression + chili-py rebuild | ~0.5 | ~0.5 |
| **Total** | **3–6** | **~6** |

Upper edge. Driver: ADD-2 caught a genuine broken-headline-feature → a real adaptation (`sync()`→`fn_call`), making this *not* the clean mechanical merge predicted. The disk-full block was wall-time/environmental (surfaced to user, recovered) — not token cost. No mid-sprint pivot; premise ("disjoint surfaces") held — `engine_state.rs` auto-merged clean exactly as the audit verified.

---

## What surprised

- **`engine_state.rs` auto-merged perfectly clean** (both upstream hunks) — the audited disjoint-surface premise held exactly; all real conflicts were mechanical (versions/placement/lockfile), as predicted.
- **The headline feature was broken as-shipped on claude-2** (upstream `sync()` ↔ ADR-0002 `eval()` divergence). Not a conflict git could show — a silent semantic incompat in non-conflicting code. ADD-2 was the only safety net (→ Lesson 1).
- **Disk exhaustion mid `maturin develop`** — recurred (also Sprint 7). Known chili infra (dual target dirs + polars fork are huge). Environmental, recovered after user freed space; not a new durable rule — folds into the standing build-infra backlog.
- planner audit's "housekeeping overdue" was a **second-order error** (Sprint 18 already swept, commit `0ada8d8`); refuted in the appendix (MC-5). No housekeeping this sprint — correct.

---

## Cross-references

- Plan: `docs/history/sprints/sprint_19_dispatch_brief_2026-05-16.md` (incl. audit appendix MC-1..MC-5 + ADD-1/ADD-2)
- Cadence row: `docs/sim/cadence_metrics.md`
- Upstream: `5a6adc5`, `606d1cc` (local `main`); merge `f04e9e8`; fixups `b0b5f89`
- Branch policy: `CLAUDE.md` + `iteration_lessons` lesson 4 (cherry-pick → merge)
- Adjacent claude-2 work merge did not regress: Sprint 17 (`publish_via_handle`/`signal_eod`), Sprint 18 (`roll_tick`) retros
