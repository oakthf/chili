# mdata ← chili 0.8.6 wheel delivery

**Date:** 2026-05-16
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.8.6-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `8881337155b851b4ed4c1858eebaaa2a269b5ead176a92d13aea74cde3086ede`
**Replaces:** 0.8.5 (sha256 `62e809129827d9f2514e5f5cbb506161f1281f1e7a4e3abd1a9e56f67efb5bf2`)
**Thread:** `mdata-chili-eod-upd-race-2026-05-15`

---

## TL;DR

Sprint 18 ships **`engine.roll_tick(log_dir, segment_label)`** — the
atomic tplog segment-rollover primitive you requested (option ii). It
replaces the racy `engine.eod(d)` + `init_tick(.., d+1)` pair at the
segment boundary with a single call that holds chili's handle
write-lock across open-next → fsync → swap-writer (**same handle id**)
→ so a concurrent inbound `.tick.upd` is serviced by exactly one valid
handle and lands wholly in the old segment or wholly in the new one.

**You can delete the planned Python drain barrier.** The landing
estimate you asked for is now actual: it shipped this sprint, before
your v1-25.2 soak — no fallback barrier needed.

A finding from the red-first harness strengthened the diagnosis: the
legacy roll had **two** silent failure modes, not one —

1. **gap-loss** (your verdict (b)): a `.tick.upd` in the
   `close_handle`→`open_handle` window hits `InvalidHandleErr` and is
   dropped; and
2. **id-reuse misplacement**: `set_handle` (`engine_state.rs:874`)
   allocates `1+max(keys)`, so a single-tplog tickerplant (your exact
   topology) re-derives the *same* id after close — a stale-id write
   then *succeeds* but lands in the **wrong segment** with no error.

`roll_tick`'s atomic same-id swap fixes both: the swap point is the
exact, crisp day/segment boundary. Both are proven by deterministic +
randomized tests against the pre-fix path (red) and `roll_tick` (green).

---

## Answers to your 6 open questions (as-built, code-cited)

1. **Schema arg — reuse, slim signature.** Final shape:
   `roll_tick(log_dir, segment_label)`. `.tick.schema` is a persistent
   engine var set by the prior `init_tick`
   (`engine.py:461 set_var(".tick.schema", …)`); `roll_tick` does not
   touch it, so schema carries across the roll unchanged. No re-pass.

2. **Single vs split — single atomic call.** One `roll_tick`. No
   `close_log`/`open_log` split (less surface for either side to
   misorder; the atomicity is the whole point).

3. **EOD broadcast — NOT subsumed (your preference honored).**
   `roll_tick` is cutover-only. It does not call `signal_eod`. If you
   want a `(eod;d)` broadcast, call `engine.eod(d)` **first**, then
   `roll_tick(log_dir, next_label)`. Verified independent:
   `signal_eod` filters `ConnType::Publishing` and skips the
   `Sequence` tplog handle, so the two never interact. (For UHF
   intra-day rolls you typically won't call `eod` at all — another
   reason not to couple them.)

4. **Durability — yes, fsync-before-cutover, by construction.** Inside
   the held `handle.write()`, before the old writer is swapped out,
   `roll_tick` does `flush()` then `sync_all()` on the old segment
   (`ReadWrite::sync_all` = `fs::File::sync_all`, the same fsync that
   backs `flush_tplog()` from Sprint 16). The day-d tail is durable
   before the cutover completes — your PRD §5.1 requirement is met
   without a separate `flush_tplog()` call.

5. **Path/index — there is no chili-side index; caller owns the whole
   component.** Definitive answer to the original `<prefix>_<NNNN>` /
   `_0000` question: chili does plain string concat
   `log_dir + segment_label` (exactly as `init_tick` does
   `log_dir + date`). **`segment_label` is an opaque, caller-owned
   path component.** It is NOT date-bound — pass a date for daily
   rolls, or a zero-padded counter for size/count-triggered UHF rolls
   where a daily file is too large. mdata owns the monotonic increment
   and naming convention; chili never parses or generates it.

6. **Contract — raises on failure; idempotent; single-flight.**
   - Raises (`RuntimeError`) on: empty `segment_label`; unset/invalid
     `.tick.msgHandle`; next-segment open failure (**old segment left
     fully intact and writable** — failure-atomic, open-next-before-
     touch-old); old-segment fsync failure.
   - **Idempotent:** a repeat call once the live handle already points
     at `segment_label` is a safe no-op (your EodScheduler retry path).
   - **Single-flight expected:** do not invoke `roll_tick` concurrently
     with itself for the same handle. Your EOD is a single asyncio
     task, so this holds; concurrent calls are still serialized by the
     swap lock and re-checked, but the contract is single-flight.

---

## Resolves the "cross-segment seq" deferred decision — it's cumulative

Your request flagged a decision: does the in-log seq reset per segment
(composite global seq) or carry over? **It already carries over —
there is no decision to make.** chili's tick counter
(`tick.pep:6-7`, slot `0`) is advanced by `tick[0; validateSeq]`, and
`EngineState::tick` is `tick_count[0] += inc` (an increment, not a
set). A fresh segment ⇒ `validateSeq == 0` ⇒ the counter is
**unchanged** ⇒ the logical sequence is **monotonic across segments by
construction**. `roll_tick` replicates this exactly. Your SEQ-MONO
partition/seq-invariant holds across rolls with no per-segment reset
and no work on your side. (This corrects an earlier chili-side draft
assumption that seq reset per segment — verified false against
`engine_state.rs` `tick()`.)

## One operational note for your tplog monitors

If the old-segment fsync errors mid-roll, `roll_tick` returns `Err`
with the old segment still live (no swap) but a **zero-byte
next-segment file may exist on disk** (it was `create`-opened before
the fsync). Retry is correct (empty file ⇒ `validateSeq` 0 ⇒
idempotent open). Your tplog file-count / size probes should treat a
zero-byte trailing segment as "roll incomplete, retry pending", not as
a real segment.

---

## Install

```bash
uv pip uninstall chili-sauce
uv pip install /path/to/chili_sauce-0.8.6-cp310-abi3-macosx_11_0_arm64.whl
# pin the new hash: 8881337155b851b4ed4c1858eebaaa2a269b5ead176a92d13aea74cde3086ede
```

`roll_tick` is purely additive; 0.8.5 surface (`publish_via_handle`,
`flush_tplog`, `signal_eod` Async, `add_at_time`, …) is unchanged.

## Suggested mdata adoption (your call)

Replace, in `eod_fire`:

```python
self._engine.eod(d)                                # if you still want the (eod;d) broadcast
self._engine.roll_tick(self._log_dir, next_label)  # was: init_tick(empty_frames, prefix, nd)
```

`next_label` = whatever you increment (date string for daily; counter
for UHF). Drop the planned drain barrier. Your SEQ-MONO soak should
still empirically confirm, but the invariant now holds by construction.

---

## Cross-references

- Thread: `mdata-chili-eod-upd-race-2026-05-15` (your request
  `mdata_chili_roll_tick_request_20260515T175524Z.json`; chili
  analysis `chili-eod-upd-race-reply-e55aafa9…`).
- Sprint 18 dispatch brief + audit appendix:
  `docs/sim/sprint_18_dispatch_brief_2026-05-16.md`.
- ADR 0001 (pub/sub canonical) — Sprint 18 cross-ref note: `roll_tick`
  is cutover-only, does not touch `signal_eod`.
- Prior delivery: `docs/history/sync/mdata_chili_2026-05-14_0.8.5_delivery.md`
  (0.8.6 supersedes for mdata).
