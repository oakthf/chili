# chili → mdata wheel delivery, 2026-05-09 (Sprint 14 + Sprint 15 bundle)

**Wheel artifact:** `dist/chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl`
**Branch:** `claude-2` tip
**Predecessor wheel:** `chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl`
(2026-05-08 delivery; Sprint 12 hotfix removed mimalloc to fix the
pyarrow co-load segfault).
**Status:** 0.8.3 ready for install. **NOT a bug fix** — default behavior
preserved byte-equivalently with 0.8.2. Adoption optional.

---

## TL;DR

0.8.3 ships **two additive changes** with NO behavior change at the
default code path:

1. **Sprint 14 (FFI symmetry — GIL release on direct-FFI):**
   `engine.engine.load_par_df` and `engine.engine.clear_par_df` now
   release the GIL during execution (via `py.detach`). Brings them in
   line with `eval`, `fn_call`, `get_var`, `import_source_path` —
   which already released the GIL.

   **Practical impact for mdata: NONE.** mdata's `load_partitioned_df`
   wrapper at `chili.engine.ChiliEngine.load_partitioned_df` routes
   through `fn_call("load", ...)` which has been GIL-released since
   0.8.0. Sprint 14 only affects callers that bypass the wrapper and
   reach the FFI directly (advanced use cases, future bench harnesses).
   See [`sprint_14_retro.md`](../sim/sprint_14_retro.md) §"Sprint 15
   hand-off — Open question for user" for the full positioning.

2. **Sprint 15 (new ParquetWriteConfig API):**
   `engine.write_partitioned_df` and `engine.overwrite_partition` now
   accept two **keyword-only** args:

   ```python
   engine.write_partitioned_df(
       df, hdb_path, table, date,
       sort_columns=None,
       rechunk=False,
       overwrite=False,
       *,                              # keyword-only barrier
       compression=None,                # NEW — codec name
       row_group_size=None,             # NEW — row group size override
   )
   ```

   `compression` accepts: `"snappy"` / `"zstd"` / `"lz4_raw"` /
   `"uncompressed"` / `"gzip"` / `"brotli"` (case-insensitive).
   `None` (the default) preserves Sprint 14 / 0.8.2 behavior
   byte-equivalently.

   **Practical impact for mdata: optional.** If you're storage-budget-
   sensitive, the default (ZSTD — see ADR 0005) is already the best
   compression ratio at zero CPU cost. If you want to experiment with
   LZ4 (~same wall, ~1.7× larger files than zstd) for some workloads,
   you can opt in via `compression="lz4_raw"`. Otherwise, no change.

---

## Byte-equivalence regression check (load-bearing)

Sprint 15 ships a chili-py pytest test
(`TestParquetWriteConfig::test_default_args_byte_equivalent_to_0_8_2`)
that asserts post-0.8.3 default-args output is sha256-identical to the
0.8.2 wheel for the canonical 2-row fixture:

```
shard:  2024.01.01_0000
size:   1105 bytes
sha256: 9682bed9ee1dca29a6da1d78932a0f1948146a1454d8fb23c56cb01b65271f61
```

Verified at 0.8.3 wheel cut. **Default behavior is preserved.** Existing
mdata HDBs written under 0.8.2 (or earlier 0.8.0/0.8.1 patched-via-0.8.2)
do not require any rewrite or migration; mixed-codec HDBs are read
transparently by polars regardless of per-file codec.

---

## ADR 0005 — what's documented

`docs/decisions/0005-parquet-write-defaults.md` documents:

1. **Default codec is ZSTD**, not Snappy as the brief originally
   assumed. Verified empirically against the 0.8.2 wheel (which was
   already shipping ZSTD).
2. **Override semantics** — `None` preserves default; explicit
   string opts in.
3. **Mixed-codec HDB read transparency** — polars handles it
   automatically.
4. **Future default-codec change protocol** — any change requires
   mdata sign-off (golden rule 4 territory).
5. **Provisional aspects** — the current 2-positional-arg FFI shape is
   a stopping point; struct-shaped FFI is the long-term direction
   (Sprint 16+).

---

## Install protocol for mdata

**Same wheel-only protocol as 0.8.2 (per CLAUDE.md lesson 14 —
NEVER editable installs).**

```sh
# In mdata's clean venv:
uv pip uninstall chili-sauce
uv pip install /path/to/dist/chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl
# Verify:
python -c "import chili; print(chili.__file__)"
# Should show site-packages/chili/__init__.py — not an editable path.
```

The 0.8.3 wheel is byte-stream-self-contained; chili dev on the
`claude-2` branch does not affect mdata's installed wheel.

---

## ABI / feature delta (Sprint 14 → Sprint 15 → 0.8.3)

| Surface | 0.8.2 | 0.8.3 |
|---|---|---|
| `engine.eval` | GIL-released | GIL-released (unchanged) |
| `engine.fn_call` | GIL-released | GIL-released (unchanged) |
| `engine.engine.load_par_df` (direct FFI) | **GIL-held** | **GIL-released** (Sprint 14) |
| `engine.engine.clear_par_df` (direct FFI) | **GIL-held** | **GIL-released** (Sprint 14) |
| `engine.load_partitioned_df` (Python wrapper) | GIL-released via fn_call | GIL-released via fn_call (unchanged) |
| `engine.clear_partitioned_df` (Python wrapper) | GIL-released via fn_call | GIL-released via fn_call (unchanged) |
| `engine.write_partitioned_df` | 7 positional args | 7 positional args + **2 keyword-only kwargs** (`compression`, `row_group_size`) |
| `engine.overwrite_partition` | 6 positional args | 6 positional args + **2 keyword-only kwargs** (mirror of `write_partitioned_df`) |
| `engine.engine.write_partition` (`wpar` FFI) | 7 args | **9 args** (compression-name + row_group_size positional optional) |
| Default Parquet codec | ZSTD (empirically) | ZSTD (unchanged; documented in ADR 0005) |

**No removals. No semantic changes at default code path.** `wpar`'s
FFI arg_num bump 7 → 9 only affects callers reaching the chili-script
built-in directly; the Python wrapper handles the new args
transparently via `None` sentinels.

---

## Concurrent-throughput evidence (Sprint 14 + bundled in 0.8.3)

For mdata workers that DO bypass the Python wrapper and reach the FFI
directly (`engine.engine.load_par_df`), Sprint 14 unblocks concurrency
on this path:

| Shape | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| `concurrent_load_direct` 0.8.2 (GIL-held) | 4857 cps | 4821 | 4841 | 4839 |
| `concurrent_load_direct` 0.8.3 (GIL-released) | 4811 cps | 8742 | **13169** | 8721 |
| Δ at N=4 | | | **+172 %** | |

Pre-Sprint-14: `concurrent_load_direct` was flat at ~4845 cps regardless
of N (perfectly GIL-serialized; p99 latency stacked linearly with N).
Post-Sprint-14: scales 1 → 1.8 → 2.7× to N=4 then regresses at N=8 due
to `par_df.write()` lock contention (same shape as the GIL-released
`fn_call` path mdata's wrapper already uses).

For mdata's `load_partitioned_df` callers: this is informational only.
The fn_call path has always been GIL-released; Sprint 14 only closed
the symmetry gap for direct-FFI callers.

---

## Codec evidence for mdata sizing decisions (Sprint 15)

On a representative 1000-row × 3-column fixture (sym Categorical, close
Int64, volume Int64 — analogous to OHLCV):

| Codec | Wall (ms) | On-disk size (bytes) | Compression ratio |
|---|---:|---:|---:|
| zstd (default) | 9.10 | 5,878 | **3.17× smaller than uncompressed** |
| snappy | 9.10 | 11,073 | 1.69× |
| lz4_raw | 9.09 | 11,048 | 1.69× |
| uncompressed | 7.84 | 18,655 | 1.00× |

**Zstd is the right default** at zero wall-time penalty vs Snappy / LZ4
on this fixture. mdata's storage budget is the binding constraint, not
write throughput; explicit override is unlikely to net-help unless your
workload is materially different.

For larger writes (mdata's typical 5M-row partitions), the per-codec
deltas may shift — Sprint 15 didn't bench at that scale. If you want a
larger-fixture codec A/B before committing to a codec change for
specific tables, that's a Sprint 16+ scope ask.

---

## Sign-off contract

mdata: please confirm one of the following after install:

1. **Smoke pass** — `python -c "import chili; e = chili.ChiliEngine();
   df = ...; e.write_partitioned_df(df, '/tmp/x', 't', '2024.01.01')"`
   round-trips. No further action.
2. **Adoption** — you decide to opt in to non-default `compression=` /
   `row_group_size=` for some tables. Document the per-table choice
   in mdata's storage-config doc; ADR 0005 §5 binds chili to NOT
   change the default without mdata sign-off, but mdata is free to
   opt in to non-default per call site.
3. **Issue surfaced** — file a row in
   `docs/sync/decisions-needed.md` (chili-side) with the reproducer.

For 0.8.2 → 0.8.3, no security advisory, no behavior change at default,
no migration required.

---

## Cross-references

- ADR 0005: [`../decisions/0005-parquet-write-defaults.md`](../decisions/0005-parquet-write-defaults.md)
- Sprint 14 retro: [`../sim/sprint_14_retro.md`](../sim/sprint_14_retro.md)
- Sprint 15 retro: [`../sim/sprint_15_retro.md`](../sim/sprint_15_retro.md)
- Bench evidence: [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §§ "Sprint 14", "Sprint 15"
- Previous delivery (0.8.2 hotfix): [`mdata_chili_2026-05-08_delivery.md`](mdata_chili_2026-05-08_delivery.md), [`mdata_chili_2026-05-08_pyarrow_response.md`](mdata_chili_2026-05-08_pyarrow_response.md)
- chili-py wrapper code: `crates/chili-py/chili/engine.py:221-260` (write_partitioned_df), `:326-360` (overwrite_partition)
- Chili-side codec parsing: `crates/chili-op/src/io.rs::parse_compression_name`