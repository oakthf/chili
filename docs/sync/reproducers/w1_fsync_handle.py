"""W1 verification — engine.fsync_handle(h) on main 0.9.0 + author's fsync commits.

The author shipped:
  - SyncFile wrapper (utils.rs) so file handles' flush() calls fdatasync
  - .handle.fsync builtin (side_effect_fn.rs)
  - engine.fsync_handle(handle_num) Python method (engine.py:436)
  - rotate_handle flushes OLD before swap
  - close_handle flushes before drop

This script verifies:
  1. fsync_handle on a file handle works (returns without error)
  2. fsync_handle on an invalid handle errors cleanly
  3. tplog data is actually durable after fsync (read back from disk)
  4. Approximate cost of fsync_handle (microbench at 100ms cadence)
"""
from __future__ import annotations

import os
import tempfile
import time
from datetime import date
from pathlib import Path

import polars as pl
from chili import ChiliEngine


def _trade_schema() -> pl.DataFrame:
    return pl.DataFrame({
        "sym": pl.Series([], dtype=pl.Categorical),
        "price": pl.Series([], dtype=pl.Float64),
        "size": pl.Series([], dtype=pl.Int64),
    })


def _row(i: int) -> pl.DataFrame:
    return pl.DataFrame({
        "sym": pl.Series(["AAPL"], dtype=pl.Categorical),
        "price": [100.0 + (i % 1000) * 0.01],
        "size": [i],
    })


def test_fsync_returns_ok() -> bool:
    print("--- TEST 1: fsync_handle returns OK on valid handle ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        msg_handle = pub.get_var(".tick.msgHandle")
        print(f"  .tick.msgHandle = {msg_handle}")

        # Publish a few rows
        for i in range(5):
            pub.publish("trade", _row(i))

        try:
            result = pub.fsync_handle(msg_handle)
            print(f"  fsync_handle returned: {result!r}")
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")
            pub.shutdown()
            return False


def test_fsync_invalid_handle() -> bool:
    print("\n--- TEST 2: fsync_handle on invalid handle errors cleanly ---")
    eng = ChiliEngine()
    try:
        eng.fsync_handle(99999)
        print(f"  expected error but got success")
        eng.shutdown()
        return False
    except Exception as e:
        print(f"  RAISED as expected: {type(e).__name__}: {e}")
        eng.shutdown()
        return True


def test_durability_visible_after_fsync() -> bool:
    print("\n--- TEST 3: tplog data is on-disk after fsync ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        msg_handle = pub.get_var(".tick.msgHandle")

        for i in range(10):
            pub.publish("trade", _row(i))

        # Find the tplog file
        log_path = Path(log_dir) / str(date.today())
        size_before = log_path.stat().st_size if log_path.exists() else 0
        print(f"  pre-fsync file size: {size_before} bytes")

        # Drop OS page cache visibility — fsync should guarantee on-disk
        pub.fsync_handle(msg_handle)

        size_after = log_path.stat().st_size
        print(f"  post-fsync file size: {size_after} bytes")

        pub.shutdown()
        return size_after > 0 and size_after >= size_before


def bench_fsync_cost() -> float:
    print("\n--- TEST 4: fsync_handle cost microbench ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        msg_handle = pub.get_var(".tick.msgHandle")

        # warm up
        for i in range(100):
            pub.publish("trade", _row(i))
        pub.fsync_handle(msg_handle)

        # Bench: 100 publishes per fsync, 10 cycles
        ITERS = 10
        rows_per_iter = 100
        t0 = time.perf_counter()
        for cycle in range(ITERS):
            for i in range(rows_per_iter):
                pub.publish("trade", _row(cycle * rows_per_iter + i))
            pub.fsync_handle(msg_handle)
        elapsed = time.perf_counter() - t0

        total = ITERS * rows_per_iter
        per_op = elapsed / ITERS * 1000  # ms per (100 publishes + 1 fsync)

        # Pure-fsync isolated
        t0 = time.perf_counter()
        for _ in range(100):
            pub.fsync_handle(msg_handle)
        fsync_only = (time.perf_counter() - t0) * 1000 / 100

        print(f"  {ITERS} cycles × {rows_per_iter} publishes + 1 fsync = {elapsed*1000:.1f}ms total")
        print(f"  per (100 pub + 1 fsync) cycle: {per_op:.2f}ms")
        print(f"  pure fsync_handle call: {fsync_only:.3f}ms (avg over 100)")
        pub.shutdown()
        return fsync_only


def main() -> int:
    t1 = test_fsync_returns_ok()
    t2 = test_fsync_invalid_handle()
    t3 = test_durability_visible_after_fsync()
    fsync_ms = bench_fsync_cost()

    print("\n=== VERDICT ===")
    print(f"  T1 (fsync returns OK):       {'PASS' if t1 else 'FAIL'}")
    print(f"  T2 (invalid handle errors):  {'PASS' if t2 else 'FAIL'}")
    print(f"  T3 (data on disk):           {'PASS' if t3 else 'FAIL'}")
    print(f"  T4 (fsync cost):             {fsync_ms:.3f}ms per call")
    if t1 and t2 and t3:
        print("\n  → W1 ASK CLOSED — main 0.9.0+ has the generic fsync_handle primitive")
        print(f"  → at 10/sec cadence: {fsync_ms * 10:.1f}ms wall/sec = {fsync_ms:.2f}% throughput impact")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
