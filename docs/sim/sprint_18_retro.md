# Sprint 18 retro — `roll_tick` atomic tplog segment-rollover (mdata wishlist v2 P0)

**Wrap:** 2026-05-16
**Predicted:** 11–20 pp (mid 15, post-audit re-pin from 10–18)
**Actual:** ~16 pp (best-effort; no kickoff rate-timeline anchor this session — methodology per `cadence_metrics.md` field def)
**Variance:** ~+7% vs post-audit mid (upper-mid band)
**Owner:** coordinator-solo + 3-agent pre-exec audit + post-impl code-reviewer
**Plan reference:** `docs/history/sprints/sprint_18_dispatch_brief_2026-05-16.md`

---

## Scope shipped

- **`EngineState::roll_tick(log_dir, segment_label)`** — atomic tplog segment rollover; holds `handle.write()` across open-next → fsync-old → same-id writer swap (`1b288e5`). Replaces the racy `createLog` close-then-reopen at the boundary; no Python drain barrier.
- **`prepare_file_writer`** extracted from `open_handle`'s `file://` arm (audit CRITICAL-1); `open_handle` refactored to call it — single source of truth, behaviorally unchanged (reviewer-verified + `flush_handle_test` green) (`1b288e5`).
- **`lib.rs` `#[pymethods]` + `engine.py` wrapper** — mirrors the Sprint-17 `publish_via_handle` chain (`check_fork` + owned args + `py.detach` + `map_spicy_error`) (`1b288e5`).
- **Generic opaque caller-owned `segment_label`** (user directive) — not date-bound; UHF size/count-triggered rolls supported.
- **0.8.6 wheel** + delivery doc `docs/sync/mdata_chili_2026-05-16_0.8.6_delivery.md` + cross-comms reply draft (user-confirm before send) + ADR 0001 Sprint-18 cross-ref note.

Tests: **+17** (10 Rust integration `roll_tick_test.rs`: 3 red-first teeth + 7 roll_tick incl. per-publisher SEQ-MONO zero-loss; 7 Python `test_roll_tick.py`: real-TCP concurrent, UHF 50-roll, idempotent, cutover-only, …). Rust 172→182; chili-py full suite 94 passed / 0 xfailed. No bench-gate path touched (parse-cache untouched; golden rule 6 holds).

Scope vs brief: delivered as scoped. The "deferred cross-segment-seq decision" (brief Out-of-scope) **dissolved** — see Lesson 2 (it carries over by construction; nothing for mdata to decide).

---

## Lessons (durable)

### 1. Red-first harness on a concurrency primitive teaches the failure space the analysis + audit miss

**Rule.** For a concurrency/correctness primitive, build the red (teeth) harness FIRST and run it against real pre-fix code before trusting the design analysis or the audit's failure-mode enumeration. Let the harness enumerate the failure space.
**Why.** Sprint 18: the design reply, my reasoning, AND all 3 audit agents identified ONE failure mode (gap-loss → `InvalidHandleErr`, mdata's verdict (b)). The first red run returned `Ok`, not `Err` — exposing a SECOND, silent mode none had predicted: `set_handle:874` allocates `1+max(keys)`, so a single-tplog tickerplant re-derives the freed id and a stale-id write *succeeds into the wrong segment* with no error. Only executing the red harness against real code surfaced it.
**Apply where.** Any sprint adding/altering a primitive whose correctness is "no concurrent observer sees an inconsistent state" — tplog, handle map, par_df, parse cache, pub/sub.
**Cost saved.** High. A different valid-looking design (e.g. chili's offered option (i) reorder) fixes mode 1 but NOT mode 2 — we'd have shipped a "fix" that still silently corrupted mdata's partitions. The red harness made the design-selection criterion concrete.

### 2. verify-before-claim binds a brief's *mechanism* claims, not only current-state facts — and the implementer is the last line (audit inherits brief framing)

**Rule.** A brief/ADR claim about HOW an existing mechanism behaves ("seq resets per segment", "indexed by handle id") is a load-bearing claim. RULE-7 it (read the function) before writing it. Audit agents inherit the brief's framing and will not catch it (documented 2026-05-09 incident); the implementer's first read of the cited code is the last line of defense.
**Why.** The Sprint 18 brief + audit appendix asserted seq "resets per segment ⇒ composite global seq" and a "per-handle `tick_count` slot indexed by `h`". Reading `EngineState::tick` showed `tick_count[index] += inc` (cumulative, not reset) and the tplog slot is the literal `0` (`tick.pep:6`). Both wrong; 3 audit agents inherited and propagated the framing. Caught only at implementation.
**Apply where.** Every brief/ADR/dispatch claim of the form "component X currently does Y". Reinforces `~/.claude/rules/verify-before-claim.md` + the `feedback_speculation_pattern` memory.
**Cost saved.** Prevented shipping a wrong "deferred decision" + wrong seq mental model to mdata (a bad cross-project reply round-trip + downstream mdata mis-design). Confirmed recurrence of the chronic speculation pattern → recommend promotion to `iteration_lessons.md` at the Sprint-18 housekeeping sweep.

### 3. Extract-and-share, never hand-roll, a load-bearing fn's internals (audit CRITICAL-1)

**Rule.** When a new code path needs an existing load-bearing function's internal behavior, extract a shared private helper and have both call it; never reimplement the internals at the new site.
**Why.** roll_tick's design D hand-rolled `open_handle`'s file-prep and silently omitted the `seek(SeekFrom::End(0))` (`engine_state.rs:758`) + conn_type detection — a data-corruption CRITICAL the audit caught pre-impl. `prepare_file_writer` extraction makes "the new caller forgot a side-effect" unrepresentable.
**Apply where.** Any sprint where a new method needs the guts of an existing load-bearing fn (handle/tplog/par_df/parse-cache).
**Cost saved.** A silent data-corruption CRITICAL (clobbered segment header on retry/restart).

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| Brief + 3-agent audit + appendix | 2–3 | ~3 |
| Lock-order verify + red-first harness (Task 1–2) | 2–3 | ~3 |
| roll_tick + prepare_file_writer + FFI/py (Task 3) | 3–5 | ~4 |
| Full Tier-1/Tier-2 matrix + gates (Task 4) | 3–5 | ~4 |
| Reviewer + fold (Task 5) | ~1 | ~1 |
| Part C wheel + delivery + reply + ADR | 1–3 | ~1 (wheel build wall-time, low token) |
| **Total** | **11–20** | **~16** |

Upper-mid band. Drivers: the red-first harness mid-finding (2nd failure mode) + the two verify-before-claim self-catches added depth but no rework (design D fixed both modes incidentally). No mid-sprint pivot. The mandatory every-5-sprint housekeeping is a **separate** wrap-phase cost (not in this 11–20), surfaced to the user at HALT.

---

## What surprised

- `set_handle` id-reuse (`1+max(keys)`) making a single-handle tickerplant re-derive the freed id — turned the legacy bug from "loss only" into "loss OR silent misplacement". Strengthened the mdata reply.
- chili `String` round-trips to Python `bytes` (not `str`) — only surfaced as a Tier-2 test-expectation bug; not a roll_tick issue. Worth knowing for future chili-py test authors.
- The 3-agent audit's MAJOR-2 (`tick_count` 1024-bound panic) was moot once code showed `tick()` bounds-checks internally AND the slot is fixed `0` — audit was right to flag, wrong on the mechanism (Lesson 2).

---

## Cross-references

- Plan: `docs/history/sprints/sprint_18_dispatch_brief_2026-05-16.md` (incl. audit appendix)
- Cadence metrics row: `docs/sim/cadence_metrics.md`
- Delivery: `docs/sync/mdata_chili_2026-05-16_0.8.6_delivery.md`
- Thread: `mdata-chili-eod-upd-race-2026-05-15`
- ADR 0001 Sprint-18 cross-ref note (cutover-only / independent of `signal_eod`)
- Companion: `sprint_17_retro.md` (`publish_via_handle` template), `sprint_16_retro.md` (`flush_tplog`/`sync_all` reused)
