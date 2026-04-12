"""Phase 17 profiling kit — run on the REAL mdata HDB (250+ partitions).

This script profiles Q11 (`select last close by symbol`) and tests whether
a two-pass approach (symbol-column scan → targeted partition read) can beat
the current full-scan + hash-groupby baseline.

Results go into:
    ~/Desktop/repos/chili/docs/bench/mdata-collab/benchmarks/phase17_profile_results.json

Chili's Claude will read that file to decide whether to implement the
optimization in Rust.

Usage (from mdata repo root):
    # Step 1: rebuild chili-py if needed
    cd ~/Desktop/repos/chili/crates/chili-py && uv run maturin develop --release
    cd ~/Desktop/repos/mdata

    # Step 2: run this script
    uv run python ~/Desktop/repos/chili/docs/bench/mdata-collab/artifacts/phase17_profiling_kit.py

    # Step 3: results are written automatically to the drop zone
"""
from __future__ import annotations

import json
import os
import platform
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Configuration — adjust if HDB path differs
# ---------------------------------------------------------------------------

HDB_ROOT = Path(os.environ.get("MDATA_HDB", "data/hdb"))
TABLE = "ohlcv_1d"
TABLE_DIR = HDB_ROOT / TABLE

# Date range for Q11 — 1 full year
DATE_START = "2024.01.02"
DATE_END = "2024.12.31"

# Output location (chili collab drop zone)
OUTPUT_DIR = Path.home() / "Desktop/repos/chili/docs/bench/mdata-collab/benchmarks"
OUTPUT_FILE = OUTPUT_DIR / "phase17_profile_results.json"

N_ITERS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def discover_partitions(table_dir: Path, start: str, end: str) -> list[Path]:
    """Find partition files in [start, end] date range."""
    all_files = sorted(table_dir.glob("*_0000"))
    # Filter by date range (filename format: YYYY.MM.DD_0000)
    result = []
    for f in all_files:
        name = f.name  # e.g. "2024.01.02_0000"
        date_part = name.split("_")[0]  # "2024.01.02"
        if start <= date_part <= end:
            result.append(f)
    return result


def bench(name: str, fn, n: int = N_ITERS) -> dict:
    """Run fn() n times, return stats dict."""
    fn()  # warmup
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        result = fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)
    times.sort()
    return {
        "name": name,
        "median_ms": round(times[len(times) // 2], 2),
        "min_ms": round(times[0], 2),
        "max_ms": round(times[-1], 2),
        "p90_ms": round(times[int(len(times) * 0.9)], 2),
        "n_iters": n,
        "result_rows": result.height if isinstance(result, pl.DataFrame) else None,
    }


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

def strategy_baseline_all_cols(files: list[Path]) -> pl.DataFrame:
    """Current chili approach: scan all → group_by_stable → last (all cols)."""
    return (
        pl.scan_parquet(files)
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    )


def strategy_baseline_projected(files: list[Path]) -> pl.DataFrame:
    """Scan all with projection → group_by_stable → last."""
    return (
        pl.scan_parquet(files)
        .select(["symbol", "close"])
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    )


def strategy_two_pass(files: list[Path]) -> pl.DataFrame:
    """Two-pass: read symbol col from all files, then read only needed files."""
    # Pass 1: read only symbol column from every file, track file index
    latest: dict[str, int] = {}
    for i, f in enumerate(files):
        syms = pl.read_parquet(f, columns=["symbol"])["symbol"].to_list()
        for s in syms:
            latest[s] = i  # overwrite: last file index wins

    # Group by target partition
    part_syms: dict[int, list[str]] = defaultdict(list)
    for sym, idx in latest.items():
        part_syms[idx].append(sym)

    # Pass 2: read projected data only from needed partitions
    frames = []
    for idx in sorted(part_syms.keys()):
        syms = part_syms[idx]
        df = pl.read_parquet(files[idx], columns=["symbol", "close"])
        df = df.filter(pl.col("symbol").is_in(syms))
        frames.append(df)

    return pl.concat(frames)


def strategy_chili_eval(hdb_path: str, date_start: str, date_end: str):
    """Full chili eval pipeline."""
    import chili

    engine = chili.Engine(pepper=True)
    engine.load(hdb_path)

    def run():
        return engine.eval(
            f"select last close by symbol from {TABLE} "
            f"where date within {date_start} {date_end}"
        )

    return engine, run


# ---------------------------------------------------------------------------
# Profile individual steps
# ---------------------------------------------------------------------------

def profile_steps(files: list[Path]) -> dict:
    """Break down the query into discrete steps and measure each."""
    steps = {}

    # Step 1: File glob
    def step_glob():
        return sorted(TABLE_DIR.glob("*_0000"))
    steps["glob"] = bench("File glob", step_glob)

    # Step 2: Scan + collect all cols
    def step_scan_all():
        return pl.scan_parquet(files).collect()
    steps["scan_collect_all"] = bench("Scan + collect (all cols)", step_scan_all)

    # Step 3: Scan + collect projected
    def step_scan_proj():
        return pl.scan_parquet(files).select(["symbol", "close"]).collect()
    steps["scan_collect_projected"] = bench("Scan + collect (projected)", step_scan_proj)

    # Step 4: Group_by on pre-loaded data
    df_proj = pl.scan_parquet(files).select(["symbol", "close"]).collect()
    def step_groupby():
        return df_proj.group_by("symbol", maintain_order=True).agg(pl.col("close").last())
    steps["groupby_last"] = bench("Group_by + last (in-memory)", step_groupby)

    # Step 5: Pass 1 only (symbol col from all files)
    def step_pass1():
        return pl.scan_parquet(files).select("symbol").collect()
    steps["pass1_symbol_col"] = bench("Pass 1: symbol col (all files)", step_pass1)

    # Step 6: Pass 1 per-file (loop overhead)
    def step_pass1_loop():
        latest = {}
        for i, f in enumerate(files):
            syms = pl.read_parquet(f, columns=["symbol"])["symbol"].to_list()
            for s in syms:
                latest[s] = i
        return latest
    steps["pass1_loop"] = bench("Pass 1: per-file loop", step_pass1_loop)

    return steps


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Phase 17 Profiling Kit")
    print(f"{'=' * 70}")
    print(f"HDB root:    {HDB_ROOT}")
    print(f"Table:       {TABLE}")
    print(f"Date range:  {DATE_START} to {DATE_END}")
    print()

    if not TABLE_DIR.exists():
        print(f"ERROR: {TABLE_DIR} does not exist")
        print("Set MDATA_HDB env var to point to the HDB root directory")
        sys.exit(1)

    files = discover_partitions(TABLE_DIR, DATE_START, DATE_END)
    print(f"Partitions in range: {len(files)}")

    if not files:
        print("ERROR: no partition files found in range")
        sys.exit(1)

    # Data shape
    first_df = pl.read_parquet(files[0], columns=["symbol"])
    last_df = pl.read_parquet(files[-1], columns=["symbol"])
    total_rows = sum(pl.read_parquet(f, columns=["symbol"]).height for f in files)
    all_symbols = set()
    for f in files:
        all_symbols.update(pl.read_parquet(f, columns=["symbol"])["symbol"].to_list())

    print(f"Total rows:  {total_rows:,}")
    print(f"First partition symbols: {first_df['symbol'].n_unique():,}")
    print(f"Last partition symbols:  {last_df['symbol'].n_unique():,}")
    print(f"Total unique symbols:    {len(all_symbols):,}")
    print()

    # Two-pass partition analysis
    print("--- Two-pass partition analysis ---")
    latest: dict[str, int] = {}
    for i, f in enumerate(files):
        syms = pl.read_parquet(f, columns=["symbol"])["symbol"].to_list()
        for s in syms:
            latest[s] = i
    needed_indices = set(latest.values())
    print(f"Pass 1 reads: {len(files)} files (symbol col only)")
    print(f"Pass 2 reads: {len(needed_indices)} files (projected data)")
    print(f"I/O reduction: {(1 - len(needed_indices) / len(files)) * 100:.1f}%")
    print(f"Needed partition indices: {sorted(needed_indices)}")
    print()

    # Step-by-step profiling
    print("--- Step-by-step profiling ---")
    steps = profile_steps(files)
    for key, s in steps.items():
        print(f"  {s['name']:45s} {s['median_ms']:8.2f} ms")
    print()

    # Strategy benchmarks
    print("--- Strategy benchmarks ---")
    results = {}

    r = bench("A: Baseline (all cols)", lambda: strategy_baseline_all_cols(files))
    results["baseline_all"] = r
    print(f"  {r['name']:45s} {r['median_ms']:8.2f} ms  ({r['result_rows']} rows)")

    r = bench("A': Baseline (projected)", lambda: strategy_baseline_projected(files))
    results["baseline_projected"] = r
    print(f"  {r['name']:45s} {r['median_ms']:8.2f} ms  ({r['result_rows']} rows)")

    r = bench("B: Two-pass (symbol scan → targeted read)", lambda: strategy_two_pass(files))
    results["two_pass"] = r
    print(f"  {r['name']:45s} {r['median_ms']:8.2f} ms  ({r['result_rows']} rows)")

    # Chili eval
    try:
        engine, chili_fn = strategy_chili_eval(str(HDB_ROOT), DATE_START, DATE_END)
        r = bench("C: Chili eval (full pipeline)", chili_fn)
        results["chili_eval"] = r
        print(f"  {r['name']:45s} {r['median_ms']:8.2f} ms  ({r['result_rows']} rows)")
        engine.close()
    except Exception as e:
        print(f"  C: Chili eval failed: {e}")
        results["chili_eval"] = {"name": "chili_eval", "error": str(e)}

    # Correctness verification
    print()
    a = strategy_baseline_projected(files).sort("symbol")
    b = strategy_two_pass(files).sort("symbol")
    correct = a.height == b.height
    if correct:
        mismatches = (a["close"] != b["close"]).sum()
        correct = mismatches == 0
        print(f"Correctness: {a.height - mismatches}/{a.height} values match")
    else:
        print(f"WARNING: row count mismatch baseline={a.height} vs two_pass={b.height}")

    # Write results
    output = {
        "timestamp": datetime.now().isoformat(),
        "machine": platform.node(),
        "python_version": sys.version,
        "polars_version": pl.__version__,
        "hdb_path": str(HDB_ROOT),
        "table": TABLE,
        "date_range": [DATE_START, DATE_END],
        "n_partitions": len(files),
        "total_rows": total_rows,
        "n_unique_symbols": len(all_symbols),
        "first_partition_symbols": first_df["symbol"].n_unique(),
        "last_partition_symbols": last_df["symbol"].n_unique(),
        "two_pass_partitions_needed": len(needed_indices),
        "two_pass_io_reduction_pct": round((1 - len(needed_indices) / len(files)) * 100, 1),
        "correctness_verified": correct,
        "step_profile": {k: v for k, v in steps.items()},
        "strategy_benchmarks": results,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
