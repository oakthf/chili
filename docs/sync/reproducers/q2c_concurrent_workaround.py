"""Q2c — workaround the str/sym bug + run the real concurrent atomicity test.

After confirming Bug 1 (roll_tick_log broken from Python str), apply the
bytes-everywhere workaround and run the structural atomicity test:
  - publisher emitting at ~10 kHz on background thread
  - main thread calls roll_tick_log mid-stream
  - count: rows published vs rows accounted for in (old + new tplog files)
  - any mismatch = real atomicity gap (rows dropped or split incorrectly)
"""
from __future__ import annotations

import socket
import tempfile
import threading
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


def test_baseline_bytes_workaround() -> bool:
    print("--- BASELINE with bytes-everywhere workaround ---")
    with tempfile.TemporaryDirectory() as log_dir:
        # Pass bytes to init_tick so .tick.msgLog ends up as String type
        pub = ChiliEngine(pepper=True)
        try:
            pub.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=(log_dir + "/").encode(),  # bytes
                filename=b"segment_001",            # bytes
            )
        except Exception as e:
            print(f"  init_tick(bytes) RAISED: {e}")
            pub.shutdown()
            return False

        for i in range(3):
            pub.publish("trade", _row(i))
        try:
            pub.roll_tick_log((log_dir + "/").encode(), b"segment_002")
            print("  roll_tick_log(bytes) succeeded")
            pub.publish("trade", _row(99))
            print("  post-roll publish succeeded")
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  roll_tick_log(bytes) RAISED: {e}")
            pub.shutdown()
            return False


def test_concurrent_atomicity() -> tuple[int, int, list[int]]:
    print("\n--- CONCURRENT (publisher running during roll, bytes workaround) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=(log_dir + "/").encode(),
            filename=b"segment_001",
        )

        published = 0
        stop = threading.Event()
        publish_errors: list[Exception] = []

        def publisher():
            nonlocal published
            i = 0
            while not stop.is_set():
                try:
                    pub.publish("trade", _row(i))
                    published += 1
                    i += 1
                except Exception as e:
                    publish_errors.append(e)
                    break
            print(f"  publisher: stopped after {published} rows")

        t = threading.Thread(target=publisher, daemon=True)
        t.start()

        time.sleep(0.5)
        before_roll = published
        print(f"  rolling at {published} rows...")
        try:
            pub.roll_tick_log((log_dir + "/").encode(), b"segment_002")
            print(f"  roll_tick_log returned successfully")
        except Exception as e:
            print(f"  roll RAISED: {type(e).__name__}: {e}")
            stop.set()
            pub.shutdown()
            return (0, 0, [])

        time.sleep(0.5)
        after_roll = published
        stop.set()
        t.join(timeout=2.0)

        files = sorted(Path(log_dir).glob("segment_*"))
        sizes = [f.stat().st_size for f in files]
        print(f"  before_roll: {before_roll}; after_roll: {after_roll}; total_published: {published}")
        print(f"  segment files: {[(f.name, sz) for f, sz in zip(files, sizes)]}")
        if publish_errors:
            print(f"  publish errors: {publish_errors[:3]}")
        pub.shutdown()
        return (before_roll, after_roll - before_roll, sizes)


def main() -> int:
    base = test_baseline_bytes_workaround()
    before, after, sizes = test_concurrent_atomicity()

    print("\n=== VERDICT ===")
    print(f"  Baseline with bytes:  {'PASS' if base else 'FAIL'}")
    if base:
        print("  → roll_tick_log IS usable on main 0.9.0 via bytes-everywhere workaround")
        print("  → BUG: from-Python default (str args) crashes; this should be fixed upstream")
    print(f"  Concurrent:           pre_roll={before}; post_roll={after}; segments={len(sizes)}")
    print("  Files exist?", sizes)
    if base and sum(sizes) > 0:
        # Soft check — chili's tplog file format is binary; we can't easily count rows
        # from outside. The strongest signal we can get without a tplog reader: did
        # both files materialize with non-zero content? Did the post-roll publishes
        # land somewhere?
        if len(sizes) >= 2 and all(s > 0 for s in sizes):
            print("  → Both segments have content (suggests no total drop)")
            print("  → Still NO guarantee of zero-drop without seq-tail walk")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
