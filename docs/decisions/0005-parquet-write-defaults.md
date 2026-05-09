# ADR-0005 — Parquet write defaults + override semantics

**Date:** 2026-05-09 (drafted Sprint 15).
**Status:** Accepted.
**Cutover:** None — current default behavior is preserved byte-equivalently. Sprint 15 ships only the user-controllable override path; no on-disk format change.
**Supersedes:** None.
**Related:** [Sprint 15 dispatch brief](../sim/sprint_15_dispatch_brief_2026-05-09.md); [`docs/sync/load_par_df_state_audit.md`](../sync/load_par_df_state_audit.md) (covers Sprint 14 GIL-release audit; this ADR is the write-side equivalent for Sprint 15).

---

## Context

Sprint 15 (A.2.4) introduces user-controllable Parquet write options on
`engine.write_partitioned_df` and `engine.overwrite_partition`:

- `compression: Optional[str]` — codec name
  (`snappy` / `zstd` / `lz4_raw` / `uncompressed` / `gzip` / `brotli`,
  case-insensitive).
- `row_group_size: Optional[int]` — row group size override.

Both default to `None`, which preserves pre-Sprint-15 behavior.

This ADR documents the override semantics and (importantly) the
**actual default codec** as observed empirically against the 0.8.2
shipped wheel.

## Empirical finding (Sprint 15 lesson — verify-before-claim)

The Sprint 15 dispatch brief and its independent audit both ASSUMED the
default codec was Snappy ("default stays Snappy" was repeated language).
**This assumption was wrong.**

When implementing the public API, a smoke test against the 0.8.2 wheel
(`uv pip install dist/chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl`)
revealed that polars 0.53's `ParquetCompression::default()` is **Zstd**,
not Snappy. The 0.8.2 wheel was already shipping Zstd-compressed
parquet output.

This was discovered at the moment a `test_compression_zstd_smaller_than_default`
test failed because zstd produced byte-identical output to "default" —
the codec selection was working correctly; the assumption about which
codec was default was wrong.

Independent verification:

```python
import pyarrow.parquet as pq
shard = ".../2024.01.01_0000"  # written via 0.8.2 default args
pq.ParquetFile(shard).metadata.row_group(0).column(0).compression
# => 'ZSTD'
```

Sprint 13 lesson 2 ("speculative optimization claims need profile-evidence
verification") generalizes here: any claim about *current* behavior of
an external dependency is a "load-bearing claim" per `verify-before-claim.md`,
and the 5-minute cost to verify before drafting was skipped. The audit
agents inherited the same wrong premise from the brief and didn't
re-verify it. Both the audit + the brief should have run a `pq.metadata`
check before stating "Snappy is the default."

## Decision

### 1. Default codec

The default codec is **whatever `polars::io::parquet::write::ParquetCompression::default()`
returns at compile time**. As of polars 0.53.0 (current pin), this is
**Zstd with default level** (zstd library default ≈ 3).

### 2. Default row_group_size

The default `row_group_size` is determined by chili's existing
auto-sizing logic in `crates/chili-op/src/io.rs::write_partition_native`:

- When `sort_columns` is non-empty: clamp `df.height() / 16` into
  `[1024, 32768]`. Targets ≈ 16 row groups per partition for
  effective row-group-stats pruning during `where symbol=X` queries.
- When `sort_columns` is empty: polars' default (262144). No
  per-row-group selectivity is expected from this path.

### 3. Override semantics

Users opt in to non-default behavior via:

```python
engine.write_partitioned_df(
    df, hdb, "table", date,
    compression="zstd",            # or any of the supported codec names
    row_group_size=1024,            # any positive integer
)
```

Both kwargs are keyword-only (the `*` in the Python signature). Old
positional callers continue to work unchanged.

`overwrite_partition` exposes the same kwargs with the same semantics
(audit MAJOR finding — overwrite_partition parity gap).

### 4. Mixed-codec HDB read transparency

Polars' Parquet reader is codec-agnostic per file. A partition written
with `compression="zstd"` is read correctly alongside Snappy- or
LZ4-encoded neighbors. Mixed-codec HDBs are supported transparently
on the read path; chili does not enforce same-codec-per-table on write.

### 5. Future default-codec change protocol

Any change to the default codec triggers **golden rule 4 territory**
(storage schema decision):

- Requires mdata sign-off (or any other downstream consumer's sign-off).
- Requires a documented rewrite path for existing HDBs that rely on
  the old default.
- Requires an ADR amending or superseding this one.

Polars version bumps (currently pinned to 0.53.0) may incidentally
change `ParquetCompression::default()`. **Such bumps are NOT
default-codec changes per this ADR's intent**, but the version-bump
sprint should explicitly call out the new default in its retro for
contributors who track the implicit baseline.

### 6. Provisional aspects

The current FFI threading uses **two trailing positional optional args**
on the `wpar` chili built-in (compression-name symbol + row_group_size
i64, with `Null` sentinels = preserve default). This is a stopping
point, NOT a permanent design.

When ≥3 Parquet write options are needed (e.g. `data_page_size`,
`bloom_filter`, `dictionary` enable/disable, `statistics` granularity),
the right design is a **struct-shaped FFI** (`SpicyObj::Map` variant
or named-options struct over the FFI boundary). Sprint 16+ scope.

The reviewer audit (Sprint 15 Part D) flagged this as MINOR — not a
blocking concern for shipping the 2-arg version; the next sprint that
needs a 3rd option should plan the struct refactor first.

## Why now

- Sprint 13.5 + 14 closed the FFI-symmetry GIL-release gap on the read
  side; Sprint 15 closes the analogous "user-can't-control-write-codec"
  gap on the write side.
- mdata's write-side ingest path is the primary external consumer; a
  user-controllable codec enables ZSTD/LZ4/Snappy experiments without
  forking chili.
- Bundling 0.8.3 wheel cut means Sprint 14 (FFI symmetry GIL release)
  + Sprint 15 (write codec API) ship together; one delivery touch to
  mdata instead of two.

## Why not …

### Why not change the default to Snappy?

(The question presumed a Snappy default; the actual default IS Zstd —
the question is moot. But for completeness:)

Snappy is faster to write but produces ~30 % larger files than Zstd on
typical OHLCV-shape data. mdata's storage budget is the binding
constraint, not write-throughput, so the implicit Zstd default has
been net-positive — it just wasn't documented as such. This ADR
fixes the documentation gap.

### Why not expose a struct-shaped option from day 1?

Three reasons:

1. **YAGNI.** Sprint 15's primary mdata ask is codec selection, not
   bloom filters or dictionary encoding. Two args cover the ask.
2. **Implementation cost asymmetry.** Adding two positional args is a
   ~2 pp change. Adding a struct-shaped FFI requires a `SpicyObj::Map`
   variant (or a Rust-side struct-decoder for named args), which is a
   ~5 pp change with a wider blast radius.
3. **Provisional commitment.** This ADR explicitly calls the positional
   approach a stopping point. The next sprint that needs a 3rd option
   plans the struct refactor first; future-Claude reading this ADR
   has the explicit "do not extend with arg 10" instruction.

### Why not write an ADR for golden rule 4 itself?

Golden rule 4 (Int64-quantized storage) is captured as a numbered rule
in `CLAUDE.md`. ADR territory begins at "schema-affecting decisions"
above golden rule 4. This ADR (codec selection) is a **subset** of
golden rule 4's scope: the on-disk format (parquet) and dtypes
(Int64-quantized prices) are unchanged; only the byte-level
compression of those bytes is configurable.

## Consequences

### Positive

- mdata can override the codec for storage-budget-sensitive workloads.
- Documents the actual default (Zstd, not Snappy) — fixes a
  pre-existing assumption gap.
- Establishes the override-semantics contract for future Parquet write
  options.

### Negative

- Two positional optional args on the chili `wpar` built-in is not
  the cleanest FFI shape. Calls to `wpar` from chili / pepper scripts
  must now pass 9 args (with `Null` sentinels for unused options) —
  the chili-py wrapper handles this transparently.
- Polars version bumps may silently change the default codec. The ADR
  documents the version-bump protocol but doesn't prevent the change;
  contributors must be aware.

### Neutral

- Mixed-codec HDBs work transparently; no read-side change required.
- The Sprint 15 byte-equivalence regression check passes against the
  0.8.2 wheel for the canonical 2-row fixture (sha256
  `9682bed9ee1dca29a6da1d78932a0f1948146a1454d8fb23c56cb01b65271f61`,
  size 1105 bytes). See `crates/chili-py/tests/test_engine.py::TestParquetWriteConfig::test_default_args_byte_equivalent_to_0_8_2`.

## Cross-references

- Sprint 15 dispatch brief: [`../sim/sprint_15_dispatch_brief_2026-05-09.md`](../sim/sprint_15_dispatch_brief_2026-05-09.md)
- Sprint 15 retro (post-wrap): [`../sim/sprint_15_retro.md`](../sim/sprint_15_retro.md)
- Bench evidence (Part B): [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 15"
- Implementation: `crates/chili-op/src/io.rs` (ParquetWriteConfig + write_partition + write_partition_native), `crates/chili-op/src/util.rs` (write_parquet_to_filepath_with_options), `crates/chili-py/chili/engine.py` (write_partitioned_df + overwrite_partition kwargs).
- Polars 0.53 source: `/tmp/polars-py-1.39.3/crates/polars-parquet/src/parquet/compression.rs` for codec-routing reference.
- Iteration lessons: lesson 7 (reviewer-before-retro — Sprint 15 Part D), Sprint 13 lesson 2 (verify-before-claim — surfaced the Snappy-vs-Zstd default-codec finding mid-implementation).
