"""Phase 17 profiling: where does Q11 time actually go?

Breaks down the Q11 query (`select last close by symbol from ohlcv_1d where date within [...]`)
into discrete steps and measures each:

  1. Partition predicate extraction (chili overhead)
  2. File discovery / glob (filesystem)
  3. Parquet scan (I/O + deserialization)
  4. Group_by + agg (compute)
  5. Arrow IPC serialization (chili→Python bridge)
  6. IPC deserialization (Python-side polars)

Uses the 20 fixture partitions to profile. Ratios should be representative
even though absolute times will scale differently on the full 250-partition HDB.

Run:
    crates/chili-py/.venv/bin/python docs/bench/phase17_profile.py
"""
from __future__ import annotations

import io
import os
import shutil
import tempfile
import time
from pathlib import Path

import polars as pl
import pyarrow as pa

FIXTURE_DIR = Path("docs/bench/mdata-collab/fixtures")
FILES = sorted(FIXTURE_DIR.glob("ohlcv_1d_*_0000"))
N_ITERS = 20


def bench(name: str, fn, n: int = N_ITERS) -> float:
    fn()  # warmup
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)
    times.sort()
    med = times[len(times) // 2]
    return med


def main():
    n_files = len(FILES)
    total_rows = sum(pl.read_parquet(f, columns=["symbol"]).height for f in FILES)
    print(f"Fixture: {n_files} partitions, {total_rows:,} rows")
    print(f"{'='*70}")
    print()

    # -----------------------------------------------------------------------
    # Step 1: File discovery (glob pattern matching)
    # -----------------------------------------------------------------------
    tmpdir = tempfile.mkdtemp(prefix="chili-profile-")
    tbl_dir = os.path.join(tmpdir, "ohlcv_1d")
    os.makedirs(tbl_dir)
    shutil.copy(str(FIXTURE_DIR / "ohlcv_1d_schema"), os.path.join(tbl_dir, "schema"))
    for f in FILES:
        name = f.name.replace("ohlcv_1d_", "")
        shutil.copy(str(f), os.path.join(tbl_dir, name))

    def step_glob():
        return sorted(Path(tbl_dir).glob("*_0000"))

    t_glob = bench("1. File glob", step_glob)
    discovered = step_glob()
    print(f"  1. File glob (discover {len(discovered)} files):  {t_glob:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 2: Parquet scan → LazyFrame (metadata only, no data read)
    # -----------------------------------------------------------------------
    def step_scan_lazy():
        return pl.scan_parquet(discovered)

    t_scan_lazy = bench("2. scan_parquet (lazy, metadata)", step_scan_lazy)
    print(f"  2. scan_parquet (lazy plan, no I/O):     {t_scan_lazy:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 3: Collect schema only (no data)
    # -----------------------------------------------------------------------
    def step_schema():
        return pl.scan_parquet(discovered).collect_schema()

    t_schema = bench("3. collect_schema", step_schema)
    print(f"  3. collect_schema (metadata only):       {t_schema:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 4: Full scan → collect all rows (I/O + deser)
    # -----------------------------------------------------------------------
    def step_scan_collect_all():
        return pl.scan_parquet(discovered).collect()

    t_scan_all = bench("4a. scan + collect (all cols)", step_scan_collect_all)
    print(f"  4a. scan + collect ALL columns:          {t_scan_all:.2f} ms")

    def step_scan_collect_projected():
        return pl.scan_parquet(discovered).select(["symbol", "close"]).collect()

    t_scan_proj = bench("4b. scan + collect (projected)", step_scan_collect_projected)
    print(f"  4b. scan + collect projected (sym+close): {t_scan_proj:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 5: Group_by + agg on pre-collected DataFrame
    # -----------------------------------------------------------------------
    df_all = pl.scan_parquet(discovered).collect()
    df_proj = pl.scan_parquet(discovered).select(["symbol", "close"]).collect()

    def step_groupby_all():
        return df_all.group_by("symbol", maintain_order=True).agg(pl.col("close").last())

    def step_groupby_proj():
        return df_proj.group_by("symbol", maintain_order=True).agg(pl.col("close").last())

    t_gb_all = bench("5a. group_by+last (all cols, eager)", step_groupby_all)
    t_gb_proj = bench("5b. group_by+last (projected, eager)", step_groupby_proj)
    print(f"  5a. group_by + last (all cols in mem):   {t_gb_all:.2f} ms")
    print(f"  5b. group_by + last (projected in mem):  {t_gb_proj:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 6: End-to-end lazy (scan → groupby → collect)
    # -----------------------------------------------------------------------
    def step_e2e_lazy():
        return (
            pl.scan_parquet(discovered)
            .group_by("symbol", maintain_order=True)
            .agg(pl.col("close").last())
            .collect()
        )

    def step_e2e_lazy_proj():
        return (
            pl.scan_parquet(discovered)
            .select(["symbol", "close"])
            .group_by("symbol", maintain_order=True)
            .agg(pl.col("close").last())
            .collect()
        )

    t_e2e = bench("6a. E2E lazy (all cols)", step_e2e_lazy)
    t_e2e_proj = bench("6b. E2E lazy (projected)", step_e2e_lazy_proj)
    print(f"  6a. E2E lazy scan→groupby→collect:       {t_e2e:.2f} ms")
    print(f"  6b. E2E lazy projected→groupby→collect:  {t_e2e_proj:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 7: E2E lazy with streaming
    # -----------------------------------------------------------------------
    def step_e2e_streaming():
        return (
            pl.scan_parquet(discovered)
            .group_by("symbol", maintain_order=True)
            .agg(pl.col("close").last())
            .collect(streaming=True)
        )

    def step_e2e_streaming_proj():
        return (
            pl.scan_parquet(discovered)
            .select(["symbol", "close"])
            .group_by("symbol", maintain_order=True)
            .agg(pl.col("close").last())
            .collect(streaming=True)
        )

    try:
        t_stream = bench("7a. E2E streaming (all cols)", step_e2e_streaming)
        t_stream_proj = bench("7b. E2E streaming (projected)", step_e2e_streaming_proj)
        print(f"  7a. E2E streaming (all cols):            {t_stream:.2f} ms")
        print(f"  7b. E2E streaming (projected):           {t_stream_proj:.2f} ms")
    except Exception as e:
        print(f"  7. Streaming mode failed: {e}")

    # -----------------------------------------------------------------------
    # Step 8: Arrow IPC serialization (DataFrame → bytes)
    # -----------------------------------------------------------------------
    result_df = step_e2e_lazy()

    def step_ipc_serialize():
        buf = io.BytesIO()
        result_df.write_ipc_stream(buf)
        return buf.getvalue()

    t_ser = bench("8. IPC serialize", step_ipc_serialize)
    ipc_bytes = step_ipc_serialize()
    print(f"  8. Arrow IPC serialize ({len(ipc_bytes):,} bytes): {t_ser:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 9: Arrow IPC deserialization (bytes → DataFrame)
    # -----------------------------------------------------------------------
    def step_ipc_deserialize():
        return pl.from_arrow(pa.ipc.open_stream(io.BytesIO(ipc_bytes)).read_all())

    t_deser = bench("9. IPC deserialize", step_ipc_deserialize)
    print(f"  9. Arrow IPC deserialize:                {t_deser:.2f} ms")

    # -----------------------------------------------------------------------
    # Step 10: Full chili eval (if available)
    # -----------------------------------------------------------------------
    print()
    try:
        import chili

        engine = chili.Engine(pepper=True)
        engine.load(tmpdir)

        # Q11 shape — need a date range covering all partitions
        # Fixtures span 2021-2025, use a wide range
        def step_chili_q11():
            return engine.eval(
                "select last close by symbol from ohlcv_1d "
                "where date within 2021.01.01 2025.12.31"
            )

        def step_chili_q11_multi_agg():
            return engine.eval(
                "select last open, last high, last low, last close by symbol from ohlcv_1d "
                "where date within 2021.01.01 2025.12.31"
            )

        t_chili = bench("10a. chili eval Q11", step_chili_q11)
        t_chili_multi = bench("10b. chili eval Q11 (multi-agg)", step_chili_q11_multi_agg)
        print(f"  10a. chili eval Q11 (last close by sym): {t_chili:.2f} ms")
        print(f"  10b. chili eval Q11 (4-col agg):         {t_chili_multi:.2f} ms")

        # Chili overhead = chili_eval - polars_e2e
        overhead = t_chili - t_e2e_proj
        print(f"  --> chili overhead vs polars native:      {overhead:.2f} ms ({overhead/t_chili*100:.0f}%)")

        engine.close()
    except Exception as e:
        print(f"  10. chili eval failed: {e}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print()
    print(f"{'='*70}")
    print("BREAKDOWN (projected path, most relevant to Q11):")
    print(f"  File glob:           {t_glob:6.2f} ms")
    print(f"  Parquet I/O + deser: {t_scan_proj:6.2f} ms")
    print(f"  Group_by + last:     {t_gb_proj:6.2f} ms")
    print(f"  IPC serialize:       {t_ser:6.2f} ms")
    print(f"  IPC deserialize:     {t_deser:6.2f} ms")
    print(f"  ─────────────────────────────")
    est_total = t_glob + t_scan_proj + t_gb_proj + t_ser + t_deser
    print(f"  Estimated total:     {est_total:6.2f} ms")
    print(f"  Actual E2E lazy:     {t_e2e_proj:6.2f} ms")
    print()

    # Extrapolate to 250 partitions
    scale = 250 / n_files
    print(f"EXTRAPOLATION to 250 partitions (~{int(total_rows * scale):,} rows):")
    print(f"  Parquet I/O (linear): {t_scan_proj * scale:6.0f} ms")
    print(f"  Group_by (linear):    {t_gb_proj * scale:6.0f} ms")
    print(f"  E2E lazy (linear):    {t_e2e_proj * scale:6.0f} ms")
    print(f"  Actual Q11 (mdata):       862 ms (from STATUS.md)")

    shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
