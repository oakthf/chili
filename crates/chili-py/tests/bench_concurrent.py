"""B10 — Concurrent Python client benchmark.

Measures how many queries/sec N Python threads can drive through a single
chili Engine. This is the PRIMARY signal for proposal A (GIL release):

    pre-A:  ~1× single-thread throughput regardless of N (GIL serializes)
    post-A: ~N× single-thread throughput (bounded by core count)

Also doubles as a correctness check: every thread issues an identical query
and we assert all results match.

Run manually:
    pytest crates/chili-py/tests/bench_concurrent.py -v -s
    python crates/chili-py/tests/bench_concurrent.py --runs 10 --threads 8 --queries 500 --save post-phase6

Output JSON written to ``target/chili-bench/concurrent_clients.json`` with
the baseline label passed via ``--save``.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

import chili


QUERY = "select from ohlcv_1d where date>=2024.01.02, date<=2024.01.08"
EXPECTED_ROWS = 15  # 5 dates × 3 symbols in fixture below


def _make_df(symbols: list[str], day: date) -> pl.DataFrame:
    ts = datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=timezone.utc)
    n = len(symbols)
    return pl.DataFrame(
        {
            "symbol": symbols,
            "timestamp": [ts] * n,
            "price": [float(i + 1) for i in range(n)],
            "volume": [float((i + 1) * 100) for i in range(n)],
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("us", "UTC")))


def _build_fixture() -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="chili_b10_"))
    hdb = tmp / "hdb"
    hdb.mkdir()
    writer = chili.Engine(pepper=True)
    symbols = ["AAPL", "MSFT", "SPY"]
    dates = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ]
    for d in dates:
        writer.wpar(_make_df(symbols, d), str(hdb), "ohlcv_1d", d.strftime("%Y.%m.%d"))
    return hdb


def _run_queries(engine: chili.Engine, n: int) -> int:
    """Run ``n`` queries sequentially. Returns total row count (correctness check)."""
    total = 0
    for _ in range(n):
        df = engine.eval(QUERY)
        total += df.height
    return total


def _bench_concurrent(engine: chili.Engine, n_threads: int, n_queries: int) -> float:
    """Return wall-clock seconds for ``n_threads`` × ``n_queries`` total queries."""
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(_run_queries, engine, n_queries) for _ in range(n_threads)]
        totals = [f.result() for f in concurrent.futures.as_completed(futures)]
    elapsed = time.perf_counter() - t0
    # Correctness: every thread should have summed to n_queries * EXPECTED_ROWS
    expected = n_queries * EXPECTED_ROWS
    for t in totals:
        assert t == expected, f"correctness failure: got {t}, expected {expected}"
    return elapsed


@pytest.fixture(scope="module")
def loaded_engine() -> chili.Engine:
    hdb = _build_fixture()
    engine = chili.Engine(pepper=True)
    engine.load(str(hdb))
    return engine


def test_concurrent_correctness(loaded_engine: chili.Engine) -> None:
    """Sanity check: 4 threads × 50 queries must all agree."""
    elapsed = _bench_concurrent(loaded_engine, n_threads=4, n_queries=50)
    # No throughput assertion — that's for --save. Just correctness.
    assert elapsed > 0


def _cli_main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--save", type=str, default=None, help="baseline label for output JSON")
    args = parser.parse_args()

    hdb = _build_fixture()
    engine = chili.Engine(pepper=True)
    engine.load(str(hdb))

    # Warm-up: ensure the first polars collect has paid its fixed costs.
    _run_queries(engine, 20)

    # Single-thread baseline
    single_times = []
    for _ in range(args.runs):
        t0 = time.perf_counter()
        _run_queries(engine, args.queries)
        single_times.append(time.perf_counter() - t0)

    # N-thread concurrent
    concurrent_times = []
    for _ in range(args.runs):
        concurrent_times.append(_bench_concurrent(engine, args.threads, args.queries))

    single_p50 = statistics.median(single_times)
    conc_p50 = statistics.median(concurrent_times)
    # Speedup = serial_per_thread_total / concurrent_wall / n_threads
    # Concurrent wall should be roughly single-thread time if GIL is released.
    # Speedup = n_threads * single_p50 / conc_p50
    total_queries_serial = args.queries * args.threads
    serial_est = single_p50 * args.threads  # extrapolate serial baseline
    speedup = serial_est / conc_p50

    print()
    print(f"=== Chili concurrent bench ===")
    print(f"threads={args.threads} queries_per_thread={args.queries} runs={args.runs}")
    print(f"single-thread p50 ({args.queries} queries): {single_p50*1000:.1f} ms")
    print(f"serial-est  p50 ({args.threads}×{args.queries} queries): {serial_est*1000:.1f} ms")
    print(f"concurrent  p50 ({args.threads}×{args.queries} queries): {conc_p50*1000:.1f} ms")
    print(f"throughput (q/s): single={args.queries/single_p50:.0f}, concurrent={total_queries_serial/conc_p50:.0f}")
    print(f"speedup: {speedup:.2f}× (ideal = {args.threads:.1f}×)")

    if args.save:
        out_dir = Path(__file__).parent.parent.parent.parent / "target" / "chili-bench"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"concurrent_clients_{args.save}.json"
        payload = {
            "label": args.save,
            "threads": args.threads,
            "queries_per_thread": args.queries,
            "runs": args.runs,
            "single_thread_p50_s": single_p50,
            "concurrent_p50_s": conc_p50,
            "speedup": speedup,
            "throughput_qps_single": args.queries / single_p50,
            "throughput_qps_concurrent": total_queries_serial / conc_p50,
            "single_thread_all_runs_s": single_times,
            "concurrent_all_runs_s": concurrent_times,
        }
        out_file.write_text(json.dumps(payload, indent=2))
        print(f"wrote {out_file}")

    return 0


if __name__ == "__main__":
    sys.exit(_cli_main())
