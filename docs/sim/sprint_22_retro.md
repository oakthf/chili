# Sprint 22 retro — mdata wishlist 2026-05-23 (W1 + W2; W3 deferred)

**Wrap:** 2026-05-23
**Predicted:** 7–13 pp (mid 10, post-audit)
**Actual:** ~9–11 pp (within band, upper-mid edge)
**Variance:** ~0% vs predicted mid (post-audit-revised 8–11pp expected band)
**Owner:** coordinator-solo (code-reviewer subagent on impl per lesson 7)
**Plan reference:** `docs/history/sprints/sprint_22_dispatch_brief_2026-05-23.md`

---

## Scope shipped

**W1 (originally P0, then RESOLVED by mdata turn-9 self-discovery 2026-05-23 ~20:35 local) — `eval_str` builtin + sync() docstring update:**

- `pub fn eval_str` in `crates/chili-core/src/eval.rs` (parse + eval_ast + raw return; accepts `Str | Sym` since chili-py FFI converts Python str → SpicyObj::Symbol). Commit `00db7b7`.
- Registered in `SIDE_EFFECT_FN` (`side_effect_fn.rs`) — same shape as `evalc` / `evali` but with raw object return. Commit `00db7b7`.
- 6 Rust unit tests in `crates/chili-core/tests/eval_str_test.rs` (chili-syntax + pepper-syntax + Symbol-source + parse-error + non-string-arg + literal+assign). Commit `00db7b7`.
- 3 chili-py pytest in `crates/chili-py/tests/test_eval_str.py` — the MC-4 closure gate (E2E via chili:// TCP). Commit `00db7b7`.
- `sync()` docstring rewrite in `crates/chili-py/chili/engine.py` documenting str/bytes/tuple polymorphism (the actual W1 ask post-turn-9). Commit `<delivery>`.

**W2 (P0-highest per turn-9; no user-space workaround) — graceful bare-TCP-connect:**

- 14 `.unwrap()` + 1 `panic!` converted to `match` + `info!` log + `continue` in `engine_state.rs::start_tcp_listener` accept loop (lines 2598-2660) AND `validate_auth_token` (lines 2495-2541). Commit `00db7b7`.
- 3 Rust integration tests in `crates/chili-core/tests/tcp_listener_graceful_test.rs`: bare-connect-close × 10 + bad-version-byte burst + MC-13 <1ms latency assertion. Commit `00db7b7`.

**W3 — DEFERRED** with explicit re-evaluation gate (per audit MC-10). Delivery doc commits chili-side to opening a W3 design sprint when (a) mdata v1-36 cutover specifically blocks on it AND poll-on-variable proves insufficient, OR (b) chili-team has bandwidth for ADR + design sprint.

**Wheel + delivery:**

- `dist/chili_sauce-0.8.8-cp310-abi3-macosx_11_0_arm64.whl` (sha256 `e75dffb7c5621d3cc8c206828f8db2a6ff5a43442c53ad37b181c7914f963ddc`).
- Versions coherent at 0.8.8 across workspace `Cargo.toml`, `crates/chili-py/Cargo.toml`, `crates/chili-py/pyproject.toml` (lesson 14).
- Delivery doc `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md`.
- Brief moved to `docs/history/sprints/`.

**Tests:** +12 (6 Rust unit + 3 Rust integration + 3 chili-py pytest). Predicted +7 (mid). Actual delta higher because eval_str_test was 6 not 3 (cheap to add edge-case coverage; symbol-source-acceptance was a directly-implied verification after the StrOrSym fix).

**Bench delta:** none expected; no scan / eval / load_par_df / write_partition / parse-cache hot-path edits. parse_cache hit baseline (377 ns ≤ 400 ns golden rule 6) verified unchanged at gate.

---

## Lessons (durable)

### 1. **Cross-project doc updates can land mid-sprint via the user; re-read source-of-truth at every phase boundary, not just at kickoff.**

**Rule.** When a sprint references an external-project file (mdata's wishlist, vantage's bus protocol, etc.) and the file is editable by the user OR over the bus, re-read it at every phase boundary (impl-kickoff, post-impl-pre-wheel, post-wheel-pre-commit). Don't assume the brief's snapshot is canonical for the whole sprint.

**Why.** Sprint 22 kickoff: brief committed at `fb88a44` referenced turn-7 wishlist. The post-3-agent-audit appendix (MC-9 → MC-13) folded turn-7 priorities and naming. **Between brief commit and impl-start, the user (oak) prompted mdata to test bytes-form sync; mdata published turn-9 revision withdrawing W1.** I implemented W1 anyway based on the audit-folded turn-7 view. The post-impl code-reviewer subagent caught the discrepancy as a CRITICAL finding (commit `00db7b7` → reviewer flagged W1 as "unsolicited scope"). User chose Option C (ship anyway, document both paths). Real cost: ~2-3pp of W1 implementation work that was strictly speaking unnecessary (bytes-form was already the solution); savings: the named-tuple alias is now a documented part of the surface, future ergonomics win for mdata if they adopt it.

**Apply where.** Any sprint referencing a wishlist / proposal / spec file that lives in another project or in a bus thread. Specifically:
- Re-read `docs/sync/<wishlist>.md` at impl-kickoff (post-brief-commit) AND post-impl (pre-wheel-cut).
- Check `git diff <brief-commit-sha> -- <wishlist-path>` before each phase transition.
- Check the bus for new events on the wishlist's correlation_id at the same boundaries.

**Cost saved.** ~2-3pp of unsolicited-scope work avoidable if the impl-kickoff phase had re-read the wishlist. The post-impl reviewer caught it (saving downstream delivery-doc and mdata-acceptance churn), but a cheaper catch at impl-kickoff would have surfaced the user-decision question before any code was written. Cumulative cost across recurrence (similar pattern observed in mdata-side sprints per the verify-before-claim history) is multiple sprints' worth.

### 2. **`EngineState::initialize()` does NOT register chili-op's `BUILT_IN_FN`; chili-py's `ChiliEngine` ctor does. Rust integration tests in `crates/chili-core/tests/` must use builtin-free expressions, or add `chili-op` as a dev-dep.**

**Rule.** When writing a Rust integration test in `crates/chili-core/tests/` that exercises pepper-source-string evaluation, the engine returned by `EngineState::initialize()` does NOT have the arithmetic-and-comparison operators (`+`, `-`, `*`, `>`, etc.) loaded — those live in `chili-op::BUILT_IN_FN` and are registered by chili-py's `ChiliEngine::new` (`crates/chili-py/src/lib.rs:363`). Tests on `EngineState` directly must either (a) use builtin-free expressions (literals + variable assignment + variable lookup) or (b) make `chili-op` a dev-dependency and call `state.register_fn(&BUILT_IN_FN)`.

**Why.** Sprint 22 W1 first Rust test `eval_str_evaluates_pepper_source_and_returns_raw_object` used `"1 + 1"` (failed with `NameErr("+")`) then `"1 + 2"` (same failure). Diagnosed via reading the chili-py init path; resolved by switching to `"42"` / `"x: 100; x"` / `"a: 7; a"` expressions. End-to-end arithmetic verification was moved to the chili-py pytest (which has BUILT_IN_FN registered).

**Apply where.** Future Rust integration tests in `crates/chili-core/tests/` that exercise any pepper-eval primitive (`eval_for_console`, `eval_for_ide`, `eval_str`, or any custom eval-using test). Also: any future "test-fixture engine" helper added to a chili-core test crate.

**Cost saved.** ~1pp avoided per future Rust test that would otherwise repeat this diagnosis (Sprint 22 itself spent ~0.5pp re-deriving the chili-op-not-loaded asymmetry).

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| Polars-fork re-clone (lesson 11 / user-driven P0 backlog) | (not in brief) | ~0.5 pp (4s clone; recovery protocol worked first try) |
| W1 eval_str builtin + registration | 1.5 pp | ~1 pp |
| W1 Rust unit tests | 1 pp | ~1 pp (4 → 6 tests; cheap) |
| W1 closure-gate pytest + maturin develop rebuild | 1.5 pp | ~2 pp (+ ArgType::Str → StrOrSym fix mid-impl) |
| W2 panic-site conversion (14 + 1) | 2 pp | ~1.5 pp |
| W2 integration tests + <1ms latency assertion | 1.5 pp | ~1 pp |
| Pre-commit gate + maturin debug rebuild + chili-py pytest | 1 pp | ~1 pp |
| Code-reviewer subagent dispatch + fold | 1 pp | ~0.5 pp (reviewer surfaced CRITICAL turn-9 scope discrepancy; user-decision Option C) |
| `sync()` docstring rewrite (turn-9 deliverable) | (not in brief; added post-reviewer) | ~0.5 pp |
| Version bump + maturin --release wheel cut + sha256 + verify METADATA | 3 pp (lesson 11 floor) | ~3 pp |
| Delivery doc + retro + cadence-metrics row + brief→history | 1 pp | ~1 pp |
| Cross-comms reply via outbox (bridge fix live) | 0.5 pp | ~0.3 pp |
| **Total** | **7–13 (mid 10)** | **~9–11 pp** |

Position-in-band: upper-mid of the post-audit-revised 8–11pp band. Drivers: (a) `ArgType::Str` → `StrOrSym` fix surfaced mid-impl via the closure-gate pytest failure (+0.5pp); (b) reviewer-caught turn-9 wishlist drift triggered a user-decision question (+0.5pp, but cheap because Option C kept the work); (c) maturin release rebuild was 9m24s (lesson 11 — non-compressible).

---

## What surprised

- **The `ArgType::Str` rejecting `SpicyObj::Symbol` was the right correctness boundary** but the wrong choice for any builtin called via remote-sync, where the chili-py FFI universally converts Python `str → Symbol`. `evalc` and `evali` are immune because they're only invoked via the local `engine.eval()` Python path (which sends `SpicyObj::String` directly via lib.rs:401 `let args = SpicyObj::String(source.to_string())`), never via remote sync. Future: any new SIDE_EFFECT_FN intended for remote dispatch should default to `ArgType::StrOrSym` when the arg is a source-string.

- **The brief's audit appendix had the chili-py FFI type-bridge citation as `crates/chili-py/src/lib.rs:11`** — actually the extractor arm is at line 111. The `:11` line is a header comment table. The code-reviewer subagent caught the stale line citation as a m-3 MINOR; corrected in the eval.rs in-line comment during the docstring-edit phase.

- **Brief A.4 text said "file:// handle" for the W1 closure-gate pytest**, but file:// is write-only (tplog sequence file). The right setup is chili:// TCP (per `test_ipc_remote_query` convention). Documented in test_eval_str.py docstring. Likely a brief-write-time slip; not a load-bearing error.

- **mdata's turn-9 update arrived between my brief commit and impl-kickoff** without any explicit "the wishlist is updated" signal to me. I only caught it because the code-reviewer subagent (post-impl) re-read the wishlist file as part of its review. This is the source of lesson #1 above.

- **The vantage-bus bridge fix (commit `1800e72`) landed during the same session** (event 17667; I had reported the bug at event 17615 earlier in the session). End-to-end verified mid-sprint via a probe event (17679). MC-7 in the brief audit appendix had already pre-noted this transition; the standard outbox path was used for the Sprint 22 cross-comms reply. No additional friction.

- **The reviewer's MAJOR M-2 (validate_auth_token version<3 shutdown asymmetry)** is real but not a regression — the accept loop's `if !auth_info.is_authenticated` branch already does the shutdown. The asymmetry will read oddly to the next maintainer but is consistent with the pre-existing "validate_auth_token returns AuthInfo; caller decides shutdown" contract. Out of scope for Sprint 22; flag for future hygiene.

---

## Cross-references

- Brief: `docs/history/sprints/sprint_22_dispatch_brief_2026-05-23.md` (post-move)
- Wishlist (chili-side mirror): `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` (turn-9)
- Delivery doc: `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md`
- Cadence-metrics row: `docs/sim/cadence_metrics.md`
- Related retros: Sprint 21 (`docs/sim/sprint_21_retro.md`) — preceding mdata wishlist sprint; same shape (audit + multi-feature + wheel + delivery)
- Cross-project: mdata's `docs/sync/chili_wishlist_2026-05-23_remote-eval-surface.md` (turn-9 authoritative)
- Vantage-bus bridge bug — `vantage-bridge-optimistic-sent-bug-2026-05-23` correlation_id; fixed mid-sprint via vantage-cto commit `1800e72`.
