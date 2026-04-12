"""Phase 17 evaluation: does sort-before-groupby help on pre-sorted partitioned data?

Uses the 20 ohlcv_1d fixture partitions (each internally sorted by symbol via
Phase 10's sort_columns). Tests three group_by strategies:

  A. group_by_stable (current chili approach — hash-based, preserves order)
  B. sort("symbol") → group_by (sort then hash — tests if polars detects sorted input)
  C. sort("symbol") → group_by with set_sorted_flag (explicit sorted hint)

Also tests with and without projection pushdown (select only symbol + close vs all cols).

Run from the chili repo root:
    cd ~/Desktop/repos/chili
    uv run --directory crates/chili-py python docs/bench/phase17_sort_groupby_bench.py
"""
from __future__ import annotations

import time
from pathlib import Path

import polars as pl

FIXTURE_DIR = Path("docs/bench/mdata-collab/fixtures")
FILES = sorted(FIXTURE_DIR.glob("ohlcv_1d_*_0000"))
assert len(FILES) >= 20, f"Expected 20+ fixtures, got {len(FILES)}"

N_ITERS = 10  # warm iterations per strategy


def scan_all() -> pl.LazyFrame:
    """Scan all fixture partitions as a single LazyFrame."""
    return pl.scan_parquet(FILES)


def scan_projected() -> pl.LazyFrame:
    """Scan with projection pushdown — only symbol + close."""
    return pl.scan_parquet(FILES).select(["symbol", "close"])


def bench(name: str, fn, n: int = N_ITERS) -> float:
    """Run fn() n times, return median ms."""
    # Warmup
    fn()
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)
    times.sort()
    med = times[len(times) // 2]
    print(f"  {name:55s} {med:8.1f} ms (median of {n})")
    return med


def main():
    # Check data shape
    lf = scan_all()
    df = lf.collect()
    n_rows = df.height
    n_cols = df.width
    n_symbols = df["symbol"].n_unique()
    n_files = len(FILES)
    print(f"Data: {n_rows:,} rows × {n_cols} cols, {n_symbols:,} unique symbols, {n_files} partitions")
    print(f"Rows per partition: ~{n_rows // n_files:,}")
    print()

    # Check if data is sorted by symbol within each file
    for f in FILES[:3]:
        part = pl.read_parquet(f)
        is_sorted = part["symbol"].to_list() == sorted(part["symbol"].to_list())
        print(f"  {f.name}: sorted by symbol = {is_sorted}")
    print()

    # -----------------------------------------------------------------------
    # Strategy A: current chili approach — group_by_stable (hash)
    # -----------------------------------------------------------------------
    print("=== Full scan (all columns) ===")

    bench("A: group_by_stable → last", lambda: (
        scan_all()
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    ))

    # -----------------------------------------------------------------------
    # Strategy B: sort then group_by
    # -----------------------------------------------------------------------
    bench("B: sort(symbol) → group_by_stable → last", lambda: (
        scan_all()
        .sort("symbol")
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    ))

    # -----------------------------------------------------------------------
    # Strategy C: sort + set_sorted_flag then group_by
    # -----------------------------------------------------------------------
    bench("C: sort(symbol) + sorted_flag → group_by → last", lambda: (
        scan_all()
        .sort("symbol")
        .with_columns(pl.col("symbol").set_sorted())
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    ))

    # -----------------------------------------------------------------------
    # Strategy D: group_by (non-stable, may exploit sorted input)
    # -----------------------------------------------------------------------
    bench("D: sort(symbol) + sorted_flag → group_by (non-stable) → last", lambda: (
        scan_all()
        .sort("symbol")
        .with_columns(pl.col("symbol").set_sorted())
        .group_by("symbol")
        .agg(pl.col("close").last())
        .collect()
    ))

    print()
    print("=== Projected scan (symbol + close only) ===")

    bench("A: group_by_stable → last (projected)", lambda: (
        scan_projected()
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    ))

    bench("B: sort → group_by_stable → last (projected)", lambda: (
        scan_projected()
        .sort("symbol")
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    ))

    bench("C: sort + flag → group_by_stable → last (projected)", lambda: (
        scan_projected()
        .sort("symbol")
        .with_columns(pl.col("symbol").set_sorted())
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    ))

    bench("D: sort + flag → group_by (non-stable) → last (projected)", lambda: (
        scan_projected()
        .sort("symbol")
        .with_columns(pl.col("symbol").set_sorted())
        .group_by("symbol")
        .agg(pl.col("close").last())
        .collect()
    ))

    # -----------------------------------------------------------------------
    # Strategy E: chili eval (if available)
    # -----------------------------------------------------------------------
    print()
    print("=== Chili eval (baseline) ===")
    try:
        import chili
        import tempfile, shutil, os

        # Build a temp HDB from fixtures
        tmpdir = tempfile.mkdtemp(prefix="chili-bench-")
        tbl_dir = os.path.join(tmpdir, "ohlcv_1d")
        os.makedirs(tbl_dir)
        # Copy schema + partitions
        shutil.copy(str(FIXTURE_DIR / "ohlcv_1d_schema"), os.path.join(tbl_dir, "schema"))
        for f in FILES:
            # Extract date part: ohlcv_1d_2021.04.06_0000 → 2021.04.06_0000
            name = f.name.replace("ohlcv_1d_", "")
            shutil.copy(str(f), os.path.join(tbl_dir, name))

        engine = chili.Engine(pepper=True)
        engine.load(tmpdir)

        bench("chili: select last close by symbol from ohlcv_1d", lambda: (
            engine.eval("select last close by symbol from ohlcv_1d")
        ))

        # Wider query
        bench("chili: select by symbol from ohlcv_1d (all cols)", lambda: (
            engine.eval("select last open, last high, last low, last close by symbol from ohlcv_1d")
        ))

        engine.close()
        shutil.rmtree(tmpdir)
    except Exception as e:
        print(f"  chili not available: {e}")


if __name__ == "__main__":
    main()
