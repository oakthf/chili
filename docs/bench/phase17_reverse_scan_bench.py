"""Phase 17 Option 3: reverse-scan with early termination for `select last X by Y`.

Instead of reading all partitions and hash-groupby-ing 2.5M rows, scan partitions
in reverse date order and stop once we've seen every symbol. For ohlcv_1d where
most symbols appear every day, this typically reads 1-3 partitions instead of 250.

Simulates three approaches using the 20 fixture partitions:
  A. Current: scan all → group_by_stable → last (baseline)
  B. Reverse-scan: scan newest-first, deduplicate, stop when stable
  C. Single-partition: just read the last partition (lower bound on speed)

Run:
    crates/chili-py/.venv/bin/python docs/bench/phase17_reverse_scan_bench.py
"""
from __future__ import annotations

import time
from pathlib import Path

import polars as pl

FIXTURE_DIR = Path("docs/bench/mdata-collab/fixtures")
FILES = sorted(FIXTURE_DIR.glob("ohlcv_1d_*_0000"))
assert len(FILES) >= 20, f"Expected 20+ fixtures, got {len(FILES)}"

N_ITERS = 10


def bench(name: str, fn, n: int = N_ITERS) -> tuple[float, object]:
    """Run fn() n times, return (median_ms, last_result)."""
    result = fn()
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        result = fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)
    times.sort()
    med = times[len(times) // 2]
    print(f"  {name:55s} {med:8.2f} ms (median of {n})")
    return med, result


def strategy_a_baseline() -> pl.DataFrame:
    """Current approach: scan all partitions → group_by → last."""
    return (
        pl.scan_parquet(FILES)
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    )


def strategy_a_projected() -> pl.DataFrame:
    """Current with projection: only read symbol + close."""
    return (
        pl.scan_parquet(FILES)
        .select(["symbol", "close"])
        .group_by("symbol", maintain_order=True)
        .agg(pl.col("close").last())
        .collect()
    )


def strategy_b_reverse_scan() -> pl.DataFrame:
    """Reverse-scan: newest partition first, stop when no new symbols."""
    seen_symbols: set[str] = set()
    result_frames: list[pl.DataFrame] = []

    for f in reversed(FILES):
        df = pl.read_parquet(f, columns=["symbol", "close"])
        new_symbols = set(df["symbol"].to_list()) - seen_symbols

        if not new_symbols and result_frames:
            # No new symbols — all remaining partitions are covered
            break

        if result_frames:
            # Only keep rows for symbols we haven't seen yet
            df = df.filter(pl.col("symbol").is_in(list(new_symbols)))

        result_frames.append(df)
        seen_symbols.update(new_symbols)

    # Concat and take first per symbol (first = latest since we went newest-first)
    combined = pl.concat(result_frames)
    return combined.group_by("symbol", maintain_order=True).agg(pl.col("close").first())


def strategy_b_reverse_scan_lazy() -> pl.DataFrame:
    """Reverse-scan with lazy reads for each partition."""
    seen_symbols: set[str] = set()
    result_frames: list[pl.DataFrame] = []
    partitions_read = 0

    for f in reversed(FILES):
        df = pl.scan_parquet(f).select(["symbol", "close"]).collect()
        partitions_read += 1
        new_symbols = set(df["symbol"].to_list()) - seen_symbols

        if not new_symbols and result_frames:
            break

        if result_frames:
            df = df.filter(pl.col("symbol").is_in(list(new_symbols)))

        result_frames.append(df)
        seen_symbols.update(new_symbols)

    combined = pl.concat(result_frames)
    return combined.group_by("symbol", maintain_order=True).agg(pl.col("close").first())


def strategy_c_last_partition() -> pl.DataFrame:
    """Lower bound: just read the last partition."""
    return (
        pl.scan_parquet(FILES[-1])
        .select(["symbol", "close"])
        .collect()
    )


def main():
    # Data info
    total_rows = 0
    for f in FILES:
        df = pl.read_parquet(f, columns=["symbol"])
        total_rows += df.height
    last_df = pl.read_parquet(FILES[-1], columns=["symbol"])
    first_df = pl.read_parquet(FILES[0], columns=["symbol"])

    print(f"Partitions: {len(FILES)}")
    print(f"Total rows: {total_rows:,}")
    print(f"Symbols in first partition (oldest): {first_df['symbol'].n_unique():,}")
    print(f"Symbols in last partition (newest):  {last_df['symbol'].n_unique():,}")

    # Check symbol universe stability
    all_symbols = set()
    for f in FILES:
        df = pl.read_parquet(f, columns=["symbol"])
        all_symbols.update(df["symbol"].to_list())
    print(f"Total unique symbols across all partitions: {len(all_symbols):,}")

    # How many partitions does reverse-scan actually read?
    seen = set()
    parts_read = 0
    for f in reversed(FILES):
        df = pl.read_parquet(f, columns=["symbol"])
        new = set(df["symbol"].to_list()) - seen
        parts_read += 1
        seen.update(new)
        print(f"  Partition {parts_read} ({f.name}): +{len(new)} new symbols, total seen: {len(seen)}")
        if not new and parts_read > 1:
            print(f"  → Early termination after {parts_read} partitions (of {len(FILES)})")
            break

    remaining = all_symbols - seen
    if remaining:
        print(f"  WARNING: {len(remaining)} symbols NOT covered by reverse scan!")
    else:
        print(f"  All {len(all_symbols)} symbols covered.")
    print()

    # Benchmark
    print("=== Benchmarks ===")
    _, result_a = bench("A: scan all → group_by_stable → last", strategy_a_baseline)
    bench("A': scan all projected → group_by_stable → last", strategy_a_projected)
    _, result_b = bench("B: reverse-scan → early termination", strategy_b_reverse_scan)
    bench("B': reverse-scan (lazy reads)", strategy_b_reverse_scan_lazy)
    _, result_c = bench("C: last partition only (lower bound)", strategy_c_last_partition)

    # Verify correctness: A and B should produce same result
    a_sorted = result_a.sort("symbol")
    b_sorted = result_b.sort("symbol")
    print()
    print(f"Result A: {result_a.height} rows")
    print(f"Result B: {result_b.height} rows")
    print(f"Result C: {result_c.height} rows")

    if a_sorted.height == b_sorted.height:
        matches = (a_sorted["close"] == b_sorted["close"]).sum()
        print(f"A vs B close values match: {matches}/{a_sorted.height}")
        if matches != a_sorted.height:
            # Show mismatches
            diff = a_sorted.with_columns(
                b_sorted["close"].alias("close_b")
            ).filter(pl.col("close") != pl.col("close_b"))
            print(f"Mismatches: {diff.head(5)}")
    else:
        print(f"WARNING: row count mismatch A={a_sorted.height} vs B={b_sorted.height}")
        a_syms = set(a_sorted["symbol"].to_list())
        b_syms = set(b_sorted["symbol"].to_list())
        print(f"  Symbols in A not in B: {len(a_syms - b_syms)}")
        print(f"  Symbols in B not in A: {len(b_syms - a_syms)}")


if __name__ == "__main__":
    main()
