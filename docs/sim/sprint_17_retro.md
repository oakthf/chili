# Sprint 17 retro — mdata wishlist v1 P1 bundle (eod dispatch + publish_via_handle)

**Wrap:** 2026-05-14
**Predicted:** 11–25 pp (post-audit band; pre-audit was 10–22)
**Actual:** ~12 pp (low edge of post-audit band; midpoint was ~18)
**Variance:** −33% vs midpoint (under-spent, low-edge cadence)
**Owner:** coordinator-solo (no subagent fan-out; code-reviewer dispatched once post-Part-A per audit G2 spirit)
**Plan reference:** `docs/history/sprints/sprint_17_dispatch_brief_2026-05-14.md`

---

## Scope shipped

### Part B — `engine.publish_via_handle(h, table, df)` (mdata P1 publish_remote)

- `EngineState::publish_via_handle` (crates/chili-core/src/engine_state.rs) — thin wrapper over `sync()`, validates `df` is DataFrame + handle is Outgoing, builds `MixedList[Symbol("upd"), Symbol(table), df]`, dispatches via `sync()` Outgoing branch. (commit `0062c8e`)
- `EngineStatePy::publish_via_handle` PyO3 binding using `spicy_from_py_bound(&Bound<PyAny>)` + `py.detach` for GIL release per Sprint 14 P3.2b convention. (commit `0062c8e`)
- `ChiliEngine.publish_via_handle` Python wrapper. (commit `0062c8e`)
- Tests: 2 chili-py pytest in `crates/chili-py/tests/test_publish_via_handle.py` (round-trip via TCP loopback receiver loading sub.pep for `upd`; error path on bogus handle id → RuntimeError/InvalidHandleErr). (commit `0062c8e`)
- **NOT shipped** (per audit C5): Rust integration test. `Cursor<Vec<u8>>` doesn't impl `ReadWrite` without an explicit Send+Sync newtype, and the loopback pytest exercises the SAME marshalling path with strictly more realism (real TCP, real IPC). Honest accounting beats infrastructure investment.

### Part A — subscriber-side `eod` dispatch (mdata P1 eod-dispatch)

- **Bug localized as H6** — NOT in the original audit hypothesis space (H1 var visibility / H2 fn-body scope / H3 stack wiring / H4 message shape / H5 timing race were all explored; H6 was the actual cause). (commit `7b508bd`)
- **Root cause:** `signal_eod` (crates/chili-core/src/engine_state.rs:1230) called `self.sync(&h, args)` for each Publishing handle. `sync()`'s conn_type match (engine_state.rs:984-1132) has NO `Publishing` arm — every call returned `EvalErr("cannot sync for Publishing handle")` and the broadcast was silently dropped. mdata's failing test had been waiting on this for an indeterminate period.
- **Fix:** rewrite `signal_eod` to use the same Async fire-and-forget pattern as `EngineState::publish` (broker upd path, engine_state.rs:1192-1241): serialize message once via `serde9::serialize`, iterate Publishing handles, write each via `utils::write_chili_ipc_msg(rw, &bytes, MessageType::Async)`. (commit `7b508bd`)
- Tests: 2 chili-py pytest in `crates/chili-py/tests/test_subscriber_eod_dispatch.py` (acceptance — ports mdata's failing test verbatim with the 4 audit-C2 chili-py API fixes folded; O1 audit multi-message regression — verifies `stack.clear_vars()` preserves multi-message subscriber sequence integrity). (commit `7b508bd`)
- ADR 0001 cross-reference added per audit O2 (one-line follow-up note describing the signal_eod sync→async correctness fix). (commit `7b508bd`)
- Code-reviewer dispatched post-implementation per audit G2 spirit. 1 MAJOR finding folded (removed redundant post-loop `disconnect_handle` double-write). 1 WARNING acknowledged as pre-existing convention (publish + signal_eod both hold `handle.write()` across TCP writes — pre-Sprint-17 concern). 2 non-blocking suggestions.

### Wheel cut + handoff

- chili-py version 0.8.4 → 0.8.5 (`Cargo.toml` + `pyproject.toml`).
- `dist/chili_sauce-0.8.5-cp310-abi3-macosx_11_0_arm64.whl` built (sha256 captured in delivery doc).
- Handoff doc: `docs/sync/mdata_chili_2026-05-14_0.8.5_delivery.md`.

### Test count delta

- Rust: 172 → 172 (no Rust integration tests added per audit C5 decision)
- chili-py pytest: 85 → 87 (+2 Part B publish_via_handle) → 89 (+2 Part A eod dispatch acceptance + multi-message regression). **Net: +4 chili-py pytest.**

---

## Lessons (durable)

### 1. Hypothesis space should include "the calling site rejects this conn_type" before assuming the receiver is at fault

**Rule.** When debugging a "message X sent but Y never received" symptom across an IPC boundary, **add a hypothesis explicitly checking that the SEND path itself doesn't error out** before assuming the bug is at the receiver. Brief audit C1 listed H1 (var visibility), H2 (fn-body scope), H3 (stack wiring), H4 (message shape), H5 (timing race) — all assuming the message reached the receiver. The actual bug (H6) was upstream: `sync()` rejected the conn_type before any bytes hit the wire. The 4-agent audit chain inherited the "the message reaches the subscriber, where is dispatch broken?" framing from mdata's wishlist text + the chili-side code reading, and none of them tested the simpler null hypothesis: does the message leave at all?

**Why.** Sprint 17 Part A first instrumentation cycle showed:
1. `S17-SET_VAR` confirmed subscriber registered `eod` globally (rejects H1 baseline). ✓
2. `S17-HANDLE_SUB` confirmed publisher's handle promoted to Publishing. ✓
3. `S17-SIGNAL_EOD` logged "broadcasting to 1 Publishing handles". ✓
4. **No** `S17-IPC` subscriber-side log. ← red flag pointing UPSTREAM of the subscriber.
5. Second instrumentation pass: log `sync()`'s return value inside signal_eod. Immediately surfaced `"cannot sync for Publishing handle"`. H6 confirmed.

Brief framing said "instrument 3 points, identify H1/H4/H5." The reality required ONE additional instrumentation point to localize a 6th hypothesis. If H6 had been in the original audit's space, Part A would have been ~2pp (skip the H1-H5 framing entirely). Net: ~2pp saved on Sprint 17, but also more importantly the structural failure mode of inherited-wrong-framing across the audit chain (paired with verify-before-claim's 2026-05-09 incident on default codecs).

**Apply where.** Any debug-then-fix sprint sourced from an external project's failing test. The audit chain should ALWAYS include "the send path may not even leave the calling process" as a top-tier hypothesis before assuming receiver-side bugs. Especially binds when the test artifact comes from a downstream project (mdata wishlist + reply) and the audit agents inherit the downstream's framing.

**Cost saved.** Sprint 17 itself saved ~2pp by finding H6 quickly (one round of additional instrumentation, not five). Future sprints saved more: every "downstream project's IPC test fails on chili side" debug task gets this hypothesis baked into the first audit pass. Estimate ~3-5pp avoided per such future sprint × estimated 2-4 such sprints/year on the mdata wishlist arc = ~8-15pp/year.

### 2. Drop unmeasurable infrastructure investment when an alternate path proves the same surface

**Rule.** When a planned test (Part B Rust integration test) requires non-trivial infrastructure (Send+Sync newtype `impl ReadWrite` for `Cursor<Vec<u8>>`) AND an alternate path (chili-py pytest via TCP loopback) exercises the SAME code path with strictly MORE realism — drop the planned test. Don't double-instrument the same path for marginal coverage; honest test-count accounting beats infrastructure-tax inflation.

**Why.** Sprint 17 Part B dispatch brief listed a Rust integration test (1 deliverable). Audit C5 surfaced 3 blockers (Cursor doesn't impl ReadWrite, Handle struct visibility, cfg-test gate is golden-rule-adjacent). Brief was revised pre-implementation to drop the Rust test entirely; loopback pytest covers the marshalling path through real TCP + real PyO3, which is strictly more realistic than a mock. Sprint 17 Part B shipped in ~3pp instead of ~5pp (≈40% saving). The retained test count (2 pytest) is honest about what's load-bearing.

**Apply where.** Any future sprint where the dispatch brief's "tests" section lists multiple coverage paths through the same code surface. The audit should ask: "if path A covers the same code as path B but with more realism, do we need both?"

**Cost saved.** Sprint 17 Part B: ~2pp directly (no Rust integration test infra). Pattern generalizes: probably 1-2pp per future sprint that has a "mock vs realistic" tradeoff. Net ~3-5pp/year if the pattern recurs.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| K1+K2+K3 pre-kickoff gates | 0.2 | 0.2 (polars fork verified, rustc 1.95.0, maturin develop clean) |
| Part B chili-core + PyO3 + Python wrapper | 2.5–4 | ~2 (clean compile first try; pyo3 pattern matched fn_call/load_par_df) |
| Part B pytest (2 tests) | (inside B.1) | ~1 (schema-syntax false start: `\`symbol$()` rejected; switched to `set_var` of empty DataFrame) |
| Part A.2 port mdata test | 1.5–2.5 | ~1 (audit C2 fixes pre-folded; smooth) |
| Part A.3 instrument + localize | 2–4 | ~2 (initial 3-point instrument missed H6; added signal_eod return-value log; H6 surfaced in second cycle) |
| Part A.4 fix + code-reviewer + MAJOR fold | 2–8 | ~3 (signal_eod rewrite ~20 LOC; code-reviewer caught redundant disconnect_handle; trivial fold) |
| Part A.5 tests + O1 regression | 1.5–2.5 | ~1 (acceptance test already xpass after fix; multi-message regression ~20 LOC) |
| 0.8.5 wheel cut + delivery doc | 0.5–1 | ~1 (release build wall time + sha256 + delivery doc) |
| Retro + cadence + history move | 1–2 | ~1 (this section) |
| **Total** | **11–25 (mid 18)** | **~12** |

**Variance commentary.** Sprint 17 came in at the low edge of the post-audit band (~−33% vs midpoint). Three drivers:
- **Part B was simpler than budgeted** (~3pp actual vs 4-5pp predicted). The PyO3 + Rust + Python wrapper chain matched existing patterns exactly; no new design decisions.
- **Part A localized in ONE additional instrumentation cycle** rather than the budgeted 2-4pp of hypothesis-space exploration. The first instrument cycle showed "no subscriber-side activity" which collapsed the hypothesis space immediately to "the publisher never sent" → H6 found within ~30 minutes of compile-test-observe.
- **Audit C5 + C2 fold pre-folded several pp** (Rust integration test dropped; A.2 test code corrected before write rather than discover-then-fix during pytest).

Sprint 17 calibrates at the iteration_lessons.md pattern 6 (autonomous-run sprints ceiling ~5pp / median ~3pp) — but this was a "debug-then-fix" implementation sprint, not autonomous research. The low-edge variance suggests the audit framework continues to remove ~30-40% of inherent variance from audit-folded sprints.

---

## What surprised

- **H6 was a 6-line bug** (`sync()` match has no Publishing arm + signal_eod called sync) but the audit framework + ramping hypothesis space took 4 hypotheses (H1-H5) before reaching it. Lesson 1 documents this. The bug pre-dates Sprint 17 and was a latent eod-dispatch failure in any chili deployment that called `pub.eod` — including any prior Sprint pipeline that didn't have a strict eod-dispatch acceptance test.
- **`Cursor<Vec<u8>>` not being `ReadWrite` was a 1-second compile error** that the audit C5 spent ~600 words rationalizing. Worth noting: audits sometimes over-explain trivial constraints. The right shape might be to verify the constraint first (10 seconds) before generating long-form rationale.
- **The schema-syntax false start in pytest** (`\`symbol$()`) — neither chili NOR pepper grammar accepts q-style empty-typed-list syntax. The mdata wishlist showed this construct but it doesn't work in chili. Used `set_var` of an empty DataFrame instead. Worth a future docs commit clarifying chili/pepper's typed-empty-table construct (or lack thereof).
- **Pre-existing convention debt: `signal_eod` and `publish` both hold `handle.write()` across TCP writes.** Code-reviewer flagged this as a known scale-out concern. Captured in `docs/sync/ideas.md` (post-Sprint-17 housekeeping); not Sprint 17 scope.
- **mdata's wishlist explicitly tested for this bug** but their failing test had been xfail-strict for an indeterminate window. Worth noting: every wishlist with a "we've already written the failing test" pattern is a signal that the upstream bug has been latent for some time.

---

## Cross-references

- Plan: `docs/history/sprints/sprint_17_dispatch_brief_2026-05-14.md` (audited, with appendix)
- mdata wishlist source: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md`
- mdata reply (Q3 + Q4 lock-in): `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`
- Sprint 16 retro (P0+P3+P2 closing wishlist v1): `docs/sim/sprint_16_retro.md`
- Cadence metrics row: `docs/sim/cadence_metrics.md`
- ADR 0001 (Pub/sub canonical model, with Sprint 17 follow-up note): `docs/decisions/0001-pub-sub-canonical-model.md`
- Delivery doc: `docs/sync/mdata_chili_2026-05-14_0.8.5_delivery.md`
- Part B commit: `0062c8e`
- Part A commit: `7b508bd`
