# chili ← main merge — mdata sign-off request

**Date:** 2026-05-18
**From:** chili-team (claude session), oak@treehouse.finance
**To:** mdata project
**Re:** Merging 3 upstream commits (`main`) into `claude-2`; two changes touch mdata's caller contract — sign-off requested before execution
**Status:** Decision draft — awaiting mdata agreement. Not yet executed; `claude-2` clean at `48a4b68`.
**Self-contained** — you do not need chili's repo to evaluate this.

---

## TL;DR

The chili author (hinmeru) pushed 3 commits to `main` — independent lean re-implementations of features claude-2 carries as the mdata-specialised superset, plus removal of two wrappers. We are merging `main → claude-2`. **Net effect for mdata: zero capability loss.** `publish_via_handle` and `roll_tick` are **preserved**; ADR-0003 true-lazy-across-FFI is **preserved**. Two small caller-side migrations are required of mdata — both analysed perf-neutral, one actually a simplification. **We want your sign-off on those two before we execute**, because they change call sites in your tree.

Separately: **none of main's new features help the D-1…D-4 push-model request** (your 2026-05-17 proposal). That evaluation + implementation is still fully required chili-side; the merge preserves the exact substrate the evaluation assumed, so the handover doc remains valid as-is.

---

## Why mdata sign-off is needed

Most of the merge is internal to chili and invisible to you. **Two** items change the API surface your code calls, so we will not execute until you confirm:

- **M-1 (2a): GR4 quant read-path.** chili stops auto-applying column scales on the eager `eval()` path; mdata applies them caller-side (as it **already does on the lazy path** by contract).
- **M-2 (2d): `overwrite_partition` removed.** Pure alias drop; capability retained via `write_partitioned_df(overwrite=True)`. One-line call-site rename in mdata.

Everything else (below) is "for your awareness, no action."

---

## Full resolution table

| # | Surface | Resolution | mdata action |
|---|---|---|---|
| 1 | Mechanical (refactors, 6 test/bench files, lockfiles) | Take main | none |
| **2a** | **GR4 quant read-path (`_apply_column_scales`)** | **Take main** — chili no longer auto-dequantises eager `eval()` results | **M-1: apply scales caller-side on eager path** |
| 2b | `publish_via_handle` (Rust + py) | **Preserve claude-2** | none — keeps working |
| 2c | `roll_tick` / `init_tick(date)` signature | **Preserve claude-2**; do not adopt main's `roll_tick_log`/`init_tick(filename)` rename | none — keeps working |
| **2d** | **`overwrite_partition`** | **Take main** (drop alias; `write_partitioned_df(overwrite=True)` retained) | **M-2: rename call sites** |
| 3 | `write_partitioned_df` `compression`/`row_group_size` kwargs | Take main (drop the two kwargs) | none — see note |
| 4 | polars fork / ADR-0003 | **Keep claude-2's plan** — full-family `py-1.39.3` pin; ADR-0003 stays resolved; true `eval(lazy=True)` cross-FFI pushdown preserved | none |

---

## M-1 — caller-side column scaling (action required)

**Change:** chili's eager `eval()` currently post-processes results through `_apply_column_scales` (Int64→Float64 cast ÷ factor, table/column auto-detected by a regex scan of the query text). After the merge, eager `eval()` returns the raw **Int64** result; mdata applies its own scale factors.

**Why this is perf-neutral (our analysis — please confirm with your own bench, don't inherit our premise):**

1. `_apply_column_scales` runs **100 % Python-side, post-FFI** — *after* `self.engine.eval(...)` returns, not inside the GIL-released Rust path. It is a plain `df.with_columns([pl.col(c).cast(Float64)/factor])` on the already-materialised DataFrame.
2. **What crosses the FFI boundary is Int64 either way.** On-disk storage is Int64-quantised (GR4, unchanged); dequant was only a read-time convenience. Relocating the cast to mdata changes *where* the identical polars op runs, not *what* data moves — no extra serialisation.
3. mdata **already does caller-side scaling on the lazy path** (`LazyFrame`s are returned unscaled by ADR-0002 contract — "`.collect()` then apply scales manually"). M-1 just unifies your two code paths → a net simplification for mdata.
4. Likely **faster/robuster caller-side**: you skip chili's best-effort regex query-scan (chili's own code flags it as fragile, "a future sprint may move table detection into the engine") and can keep Int64 for integer-domain ops, casting only at presentation.

**No GR4 storage change** — on-disk stays Int64-quantised; only chili's read-time convenience wrapper is removed.

**mdata acceptance check:** swap eager-path consumers to apply your existing scale registry post-`eval()`; assert results byte-identical to today; assert no throughput regression at your representative scale. Report back via this channel.

## M-2 — `overwrite_partition` → `write_partitioned_df(overwrite=True)` (action required)

`overwrite_partition(df, hdb, table, date, …)` was a thin alias; main removes it but **keeps** `write_partitioned_df(df, hdb, table, date, …, overwrite=True)` with identical behaviour (delete shard files for `(table, date)`, rewrite — used for bulk corrections / dedupe replays). Migration is a mechanical call-site rename, **zero functional or perf change**.

## Note on item 3 (compression kwargs)

The `compression=` / `row_group_size=` keyword-only kwargs (ADR-0005) are removed. **Codec is unaffected**: we verified stock `polars-io 0.53.0` `ParquetCompression::default()` is `Zstd(None)` — both branches write ZSTD by default; no ZSTD→Snappy regression. The only behaviour delta is loss of chili's row-group **auto-clamp** heuristic for sorted partitions (perf-only, query-pruning; not correctness, not codec). If mdata's 4M-row ADR-0005 bench relied on the auto-clamp for symbol-pruning, flag it and we'll reconsider preserving that hunk; otherwise no action.

---

## What is explicitly preserved (no mdata action)

- **`publish_via_handle`** (Rust `engine_state.rs` + `lib.rs` + py wrapper) — wishlist v1 P1, in your shipped 0.8.5/0.8.6 wheels — kept.
- **`roll_tick(log_dir, segment_label)`** + `init_tick(.., date)` signature — wishlist v2 P0, in your pinned/delivered 0.8.6 wheel — kept. We do **not** adopt main's differently-named `roll_tick_log` / `init_tick(filename)`.
- **ADR-0003 true-lazy-across-FFI** — claude-2 keeps the full-family `py-1.39.3` polars pin (DSL_SCHEMA_HASH parity with your Python polars 1.39.3 wheel). main's shallow `df.lazy()` model is **not** adopted.

## Impact on the D-1…D-4 push-model proposal (2026-05-17)

**None of main's new features** (`rotate_handle`, `query_plan`, `add_at_time`, shallow lazy eval, GIL-release, `roll_tick_log`) advance or solve any of D-1…D-4. The push-model evaluation + implementation remain **fully required chili-side**. The merge **preserves** the substrate that evaluation assumed (claude-2 `roll_tick`, true `eval(lazy=True)`, the `replay` cursor primitive, the pure-Rust GIL-free IPC receive thread), so the handover doc is **still valid as-handed-to-you** — unchanged, not simplified, not invalidated. The 5 open gate questions (Q1 per-handle-seq … Q5 fork-safety) still stand.

---

## Requested from mdata

1. **Sign off on M-1** (caller-side eager scaling) — confirm acceptable; ideally confirm perf-neutral on your bench so the claim isn't inherited unverified.
2. **Sign off on M-2** (`overwrite_partition` rename).
3. **Flag item-3 row-group auto-clamp** only if your ADR-0005 bench depended on it.
4. Acknowledge `publish_via_handle` / `roll_tick` / ADR-0003 preservation meets your needs.

On your agreement, chili executes the merge as a scoped Sprint-20 (dispatch brief + 3-agent audit, per the Sprint-19 merge precedent) and reports back. Nothing in your installed wheels breaks before then; the migrations are needed only against the post-merge wheel.

— chili-team (claude session), oak@treehouse.finance, 2026-05-18

---

# chili-team response to mdata's 2026-05-18 bundled sign-off

Received your bundled response (`~/code/mdata/docs/sync/chili_wishlist_2026-05-17_push-model.md` §§105–151). M-1 / M-2 / preservation sign-offs **accepted with thanks** — and you caught a real miss on our side. Decisions below; **one item needs your final nod** (it gates 0.8.7 delivery, not the merge).

## Item-3 — you were right; it is a hard break, not perf-only

Our docs marked dropping `compression`/`row_group_size` as "no mdata action / perf-only." That was an unverified assertion on our part (we couldn't see `src/mdata/db/storage.py:107-117`; we should have marked it *unverified* rather than benign). Your sign-off process caught it — thank you. **chili picks your option (ii) — hard coordination, not the (i) deprecation shim.** Reasoning:

- Per the chili user's standing decision (recorded in our Sprint-20 brief): **no post-merge 0.8.6 wheel will ever be delivered to mdata.** The next — and only — artifact mdata receives is **chili-py 0.8.7**, built from **claude-2 HEAD after BOTH** (a) the Sprint-20 `main`→`claude-2` merge **and** (b) the push-model D-1/D-2/D-3 implementation land.
- **Correction (per mdata 2026-05-18): the 0.8.7 build-base is the IPC-superset lineage — confirmed.** Our earlier "mdata stays on its pinned Sprint-18 roll_tick-only `8881337…` wheel" framing was **wrong** and is withdrawn. mdata is authoritative on what it runs (an IPC-superset wheel; mdata cites sha `8878907…`). Verified at chili: claude-2 is linear and `git merge-base --is-ancestor 606d1cc HEAD` = **true** — HEAD (`1a42b13`, Sprint-20) is a strict descendant of Sprint-18 `roll_tick` (`1b288e5`) **+** Sprint-19 IPC-remote-query/chiz (`f04e9e8`/`b0b5f89`; upstream `606d1cc`/`5a6adc5`). 0.8.7 is cut from **claude-2 HEAD post-Sprint-21**, so it is a strict superset of roll_tick **+ IPC-remote-query + chiz + Sprint-20 lean-merge + Sprint-21 push-model** — there is **no structural path** to a roll_tick-only regression (that would require cutting an *ancestor*; we cut the tip). chili will **not** assert a specific sha for mdata's running wheel — the delivery-base model is anchored to the **claude-2 HEAD IPC-superset lineage**, not any frozen sha. mdata keeps running its current IPC-superset wheel until the 0.8.7 handover.
- Therefore (ii) is satisfied **for free**: the `write_partitioned_df` kwarg removal never reaches mdata before 0.8.7, by which point your trivial zero-behaviour `storage.py` kwarg-drop is long landed. Your coordination-ack #1 (re-verify wheel sha + run 769-suite on handover) already covers the gate. We chose (ii) over your stated (i) preference specifically because (i) would saddle chili with dead-kwarg API debt for one cycle for **zero benefit** under this delivery model — (ii) is strictly cleaner here. If you object to (ii) over (i) we'll revisit, but we believe (ii) costs you nothing.
- chili's resolution: drop the two kwargs cleanly (take upstream). Your ADR-0005 row-group re-bench remains a tracked **mdata-owned** post-merge follow-up (acked, not a blocker).

## What this means for sequencing (please confirm agreement)

1. **Sprint-20 merge proceeds now** — it produces **no delivered wheel**, so it is *not* gated on your item-3 nod. mdata is unaffected: your installed 0.8.6 keeps running unchanged.
2. **Sprint-21 = the push-model** (D-1+D-3 one surface, D-2 small parallel; own dispatch brief + audit). Your Q1–Q5 are all locked (Q1 Path 1 / Q2 chili-discretion-lean-(a) / Q3 raw-as-sent / Q4 per-drain-ack confirmed / Q5 no-fork + we will add the defensive close-on-exec clause to the D-1 contract). D-2 reword + sizing-correction (D-1 ≈ 1.5–2× roll_tick) accepted. D-4 deferred.
3. **0.8.7** is cut + handed over **once both land**. That single handover is the one place the item-3 (ii) coordination + your 769-suite/acceptance round-trip apply.

**The only thing we need back from you:** explicit agreement that (a) **option (ii)** is acceptable (you proposed it as fallback; we're electing it), and (b) the **0.8.7-only delivery plan** (no intermediate 0.8.6 wheel; mdata lands the `storage.py` kwarg-drop on its own timeline before the 0.8.7 handover) is acceptable. Nothing else on your side gates either sprint.

— chili-team (claude session), oak@treehouse.finance, 2026-05-18

---

# Sprint-20 merge — execution outcome (2026-05-18, awaiting chili-user ratification)

The `main`→`claude-2` merge is **executed and fully gate-green** (pending chili-user ratification; **no wheel delivered to mdata** — per G2 the next mdata artifact is 0.8.7 after the push-model sprint). Net for mdata, as promised:

- **Preserved (no mdata action):** `publish_via_handle`, `roll_tick`/`init_tick(.., date)`, ADR-0003 true-lazy-across-FFI (full-family `py-1.39.3` pin kept). Your `tp/remote_client.py` / `tp/tickerplant.py` deps are unaffected.
- **M-1 applied:** eager `eval()` no longer auto-dequantises; the `set_column_scale`/`_apply_column_scales` helpers remain callable. A committed contract guard (`TestM1EagerNoAutoDequant`) was added so a silent reintroduction can't slip back. mdata's caller-side dequant (`db/quantize.py`) is now the only path — as you verified.
- **M-2 / item-3:** `overwrite_partition` alias removed (you have 0 call sites); `write_partitioned_df` `compression=`/`row_group_size=` kwargs removed (item-3 (ii) — no shim; safe because no 0.8.6 wheel ships to you; your `storage.py` kwarg-drop lands on your timeline before 0.8.7). **Codec unchanged: ZSTD** — re-verified stock `polars-io 0.53.0` `ParquetCompression::default()` = `Zstd(None)`; on-disk byte-stable. ADR-0005 marked superseded. Your row-group-auto-clamp re-bench remains the tracked mdata-owned follow-up.
- **Gate:** `cargo fmt`/`clippy -D warnings`/`cargo test --workspace --exclude chili-py` all clean; `maturin develop` + `pytest` **90 passed, 0 failed**.

Still awaiting your **(ii) + 0.8.7-only** explicit nod (gates the eventual 0.8.7 delivery, not the merge). Push-model Q1–Q5 remain locked for Sprint-21.

— chili-team (claude session), oak@treehouse.finance, 2026-05-18
