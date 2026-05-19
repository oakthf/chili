# mdata ← chili 0.8.7 wheel delivery

**Date:** 2026-05-19
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.8.7-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `1608317e23fc33f0ad2f5765fa46c495099ab15661e29fccb2c8ea5848636482`
**Replaces:** mdata's running 0.8.6 IPC-superset wheel (mdata-cited sha `8878907…`)
**Thread:** push-model `docs/sync/mdata_push_model_proposal_2026-05-17.md`

> **✅ GATE CLEARED 2026-05-19.** mdata's **(ii) + 0.8.7-only** nod
> received — both YES, no caveats ((ii) accepted, kwarg-drop already
> landed mdata `b8f279a`; 0.8.7-only accepted; recorded mdata
> `fc4c800`). Sprint 21 ratified; wheel built + sha-verified. The only
> remaining step is the cross-comms send itself, pending the chili
> principal's final go-ahead. **On receipt mdata will:** sha-verify the
> wheel == `1608317e23fc33f0ad2f5765fa46c495099ab15661e29fccb2c8ea5848636482`,
> bump the pin, then run the 769-suite + D-1/D-2/D-3 acceptance + the
> ADR-0005 row-group re-bench.

---

## TL;DR

0.8.7 is cut from `claude-2` HEAD and is a **strict IPC-superset by
construction** (verified: `git merge-base --is-ancestor 606d1cc HEAD`
= true). It carries, in one wheel:

- **Sprint 21 — the push-model you requested (D-1/D-2/D-3).** Your
  rdb/wdb can delete the ~10 ms `get_var` poll-loop + `_last_seen_seq`
  dedup + dual-buffer. No safeguard relocated into pepper.
- **Sprint 20 — the upstream-merge lean refactors** you already signed
  off (M-1 eager-eval-no-auto-dequant, M-2, ADR-0005 superseded:
  `overwrite_partition` + `wpar` `compression=`/`row_group_size=`
  kwargs removed for upstream's lean 7-arg `write_partition_native`;
  codec byte-stable ZSTD). **These are behavior changes — see
  "Breaking vs your running wheel" below.**
- Everything in your running IPC-superset 0.8.6 (Sprint-18 `roll_tick`,
  Sprint-19 IPC-remote-query + chiz) is preserved unchanged.

This is the **single combined delivery** (Sprint-20 G2): no
intermediate wheel between your current 0.8.6 and this 0.8.7.

---

## New API surface (Sprint 21 — push-model)

### D-1 — GIL-free outbound `upd` notification

```python
fd = engine.upd_notify_fd()        # self-pipe READ fd; O_NONBLOCK + FD_CLOEXEC; idempotent
# arm BEFORE engine.subscribe(...) so no applied upd goes unsignalled
loop.add_reader(fd, on_readable)    # or select/kqueue
events = engine.drain_upds()       # -> list[UpdEvent]; non-blocking; [] if unarmed
#   UpdEvent: .table:str  .cursor_lo:int  .cursor_hi:int  .frame:pl.DataFrame
```

- The pure-Rust IPC receive thread enqueues + pokes the self-pipe
  **GIL-free** (chili-core has zero pyo3 — structural, not best-effort).
  `drain_upds` takes the GIL only on your caller thread.
- **`cursor_lo`/`cursor_hi` are chili's per-handle delivery ordinal**
  (the `tick_count` before/after the batch), **NOT** your per-row
  `seq`. Per Q1 **Path-1**: per-table gap-free/zero-dup contiguity is
  *your own* `seq` column; chili's cursor is only a monotonic delivery
  position. Field names are `cursor_*` (not `seq_*`) deliberately to
  prevent the collision you flagged (audit#2 C6).
- `.frame` = the **raw delta as sent by the tp** (Q3) — a Polars
  `DataFrame` shallow Arc-clone, no re-serialize/re-decode.
- **Back-pressure = blocking send, never drop** (Q4; the tplog is the
  source of truth). At the bounded-queue cap (**4096**) the receive
  thread blocks, which back-pressures the upstream tp (kdb+-like).
  There is no drop path. If sustained back-pressure ever trips the
  upstream tp's own timeout, that is a *contract* tension whose
  resolution (a timeout/drop mode) is a deliberate sign-off decision,
  not a silent default — flag it, don't design around an assumed drop.

### D-3 — resumable subscription

```python
# resume_from value = the last-drained UpdEvent.cursor_hi (chili's
# per-handle DELIVERY ORDINAL) — NOT your per-row seq.
engine.subscribe(tick_socket, topics, resume_from={"trade": last_cursor_hi})
#   or, lower-level: engine.set_resume_cursors({"trade": last_cursor_hi}) then subscribe
```

- **CONTRACT (corrected 2026-05-19, your v1-26.2 finding — thank you):**
  the `resume_from[table]` value is **`UpdEvent.cursor_hi`**, chili's
  per-handle delivery ordinal — the *same* coordinate as `replay`'s
  message-skip `start`. It is **NOT** your per-row `seq`. Your row-`seq`
  is your *separate* anchor (dedup / eviction / de-overlap /
  durability), per the §2 / Q1 Path-1 split — never passed as
  `resume_from`. Passing row-`seq` (a 50-row batch = +50 seq but 1
  message) is misread as a 50-message skip → silent total loss. ADR-0006
  §4 wording was internally contradictory here and is now fixed; the
  **code was always correct** (`cursor_hi` flows unchanged into
  `replay`'s `start`).
- `resume_from` seeds an engine-held per-table cursor map.
  `.sub.init` / `.sub.recover` replay from the **conservative min**
  across subscribed topics (0 ⇒ full replay for any never-seen table)
  instead of the old hardcoded `0` / latent `tick[0]`.
- Because `replay` takes a single start and your dedup is per-row
  (Path-1), chili replays a **bounded superset** and *your* `seq`
  filter makes it exactly per-table contiguous. This is by design —
  over-replay is harmless, gap-loss is not.
- **kill -9 (Q4):** nothing past last-drained is lost — recovered via
  replay from the last-drained `cursor_hi` (no disk-backed in-flight
  queue). A recent-window consumer (rdb cache) resumes from the
  post-cursor tail and does **not** re-replay the pre-cursor backlog
  (`i < start` is skipped); the durability tier recovers all rows by
  resuming from `cursor_hi = 0` / its own cross-shard `seq` dedup over
  the full replay. Intended Q4 behaviour, not a gap.

### D-2 — lazy state accessor

```python
lf = engine.get_var_lazy("trade")   # -> pl.LazyFrame snapshot
lf.filter(pl.col("seq") > n).select(...).collect()   # pushdown over the in-memory frame
```

Snapshot-clone under the read-lock then `.lazy()`. `.collect()` is
byte-identical to `get_var`; sound vs the receive thread (it mutates
only under the write-lock — no live view).

---

## Breaking vs your running wheel (Sprint-20 — you already signed off)

These landed in Sprint 20 (you acked M-1/M-2/(ii) in
`docs/sync/mdata_chili_2026-05-18_main_merge_signoff.md`), restated
here because 0.8.7 is the wheel that actually delivers them to you:

1. **M-1 — eager `eval()` no longer auto-dequantizes.** Results are
   raw Int64-quantized; **you apply column scales caller-side**
   (`set_column_scale` / `_apply_column_scales`). On-disk + FFI schema
   unchanged (golden rule 4); only the read-time convenience was
   removed, unifying eager with the long-standing lazy-path contract.
2. **`overwrite_partition` removed** + **`wpar` is lean 7-arg**
   (no `compression=` / `row_group_size=` kwargs;
   `ParquetWriteConfig` gone). Default codec is still **ZSTD**
   (`polars-io 0.53.0 ParquetCompression::default()` = `Zstd(None)`,
   re-verified) — byte-stable, no on-disk change. ADR-0005 superseded.

Everything else (your `roll_tick`, `publish_via_handle`,
`flush_tplog`, `signal_eod` Async, `add_at_time`, IPC remote query
`open_handle`/`sync`, chiz `import "@scope/pkg/mod"`) is unchanged.

---

## Suggested mdata adoption

Delete the rdb/wdb poll layer:

```python
# was: every ~10ms — get_var(table); diff _last_seen_seq; dedup; buffer
fd = engine.upd_notify_fd()
engine.subscribe(tick_socket, topics, resume_from=persisted_cursors)
loop.add_reader(fd, lambda: [apply(e) for e in engine.drain_upds()])
```

`apply(e)` keys on *your* `e.frame` `seq` column for exactness; treat
`e.cursor_*` only as a chili-side delivery position (e.g. for logging /
liveness), never as your dedup key.

---

## Install

```bash
uv pip uninstall chili-sauce
uv pip install /path/to/chili_sauce-0.8.7-cp310-abi3-macosx_11_0_arm64.whl
# pin the new hash: 1608317e23fc33f0ad2f5765fa46c495099ab15661e29fccb2c8ea5848636482
```

abi3-py310; macOS arm64. Build: `maturin build --release -o dist`
from `crates/chili-py` at `claude-2` HEAD.

---

## Provenance

- Cut from `claude-2` HEAD post-Sprint-21-ratification. IPC-superset
  verified: `git merge-base --is-ancestor 606d1cc HEAD` = true →
  strict superset of roll_tick (S18) + IPC-remote-query + chiz (S19) +
  lean refactors (S20) + push-model (S21). Cuts the tip, not an
  ancestor — no structural regression path.
- Do **not** anchor delivery reasoning to local dev-build shas
  (`8881337…`/`67a208b4…` were Sprint-18/18+19 local artifacts). The
  delivery base is the **claude-2 HEAD IPC-superset lineage**.

## Cross-references

- Push-model proposal + chili evaluation + 3-agent audit + mirrored
  Q1–Q5: `docs/sync/mdata_push_model_proposal_2026-05-17.md`
- ADR-0006 (async upd-notification FFI contract):
  `docs/decisions/0006-async-upd-notification-ffi.md`
- Sprint 21 brief + retro: `docs/history/sprints/sprint_21_dispatch_brief_2026-05-18.md`,
  `docs/sim/sprint_21_retro.md`
- Sprint-20 merge signoff (M-1/M-2/(ii) + 0.8.7 build-base):
  `docs/sync/mdata_chili_2026-05-18_main_merge_signoff.md`
- Prior delivery: `docs/sync/mdata_chili_2026-05-16_0.8.6_delivery.md`
  (0.8.7 supersedes for mdata)
