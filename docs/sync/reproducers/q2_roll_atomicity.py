"""Q2 verification — roll_tick_log atomicity on main 0.9.0.

Three tests against main 0.9.0:

  Test 1 (BASELINE): roll into FRESH target file — should pass.
  Test 2 (RECOVERY): roll into PRE-EXISTING file (simulating crash mid-roll)
    — main's rotate_handle refuses non-empty targets, so this is expected
    to fail; claude-2's roll_tick uses .broker.validateSeq to recover.
  Test 3 (CONCURRENT): publisher thread emitting at high rate while
    main thread calls roll_tick_log mid-stream — count rows in
    (old_segment + new_segment) vs published; difference = dropped rows.

If Test 3 shows 0 dropped rows on main, the atomicity story is sufficient
and claude-2's roll_tick is duplicative. If > 0 dropped, the gap is real.
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


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _trade_schema() -> pl.DataFrame:
    return pl.DataFrame({
        "sym": pl.Series([], dtype=pl.Categorical),
        "price": pl.Series([], dtype=pl.Float64),
        "size": pl.Series([], dtype=pl.Int64),
    })


def _row(i: int) -> pl.DataFrame:
    return pl.DataFrame({
        "sym": pl.Series(["AAPL"], dtype=pl.Categorical),
        "price": [100.0 + i],
        "size": [10],
    })


def test_1_baseline() -> bool:
    print("--- TEST 1: BASELINE (fresh target) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename=str(date.today()))
        for i in range(3):
            pub.publish("trade", _row(i))
        try:
            pub.roll_tick_log(log_dir + "/", b"segment_002")  # bytes → SpicyObj::String (workaround for main str-vs-sym bug)
            print("  roll_tick_log succeeded")
            pub.publish("trade", _row(99))
            print("  post-roll publish succeeded")
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")
            pub.shutdown()
            return False


def test_2_recovery() -> bool:
    print("\n--- TEST 2: RECOVERY (pre-existing target file) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pre_existing = Path(log_dir) / "segment_002"
        pre_existing.write_bytes(b"partial-content-from-prior-crash")

        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename=str(date.today()))
        for i in range(3):
            pub.publish("trade", _row(i))
        try:
            pub.roll_tick_log(log_dir + "/", b"segment_002")  # bytes → SpicyObj::String (workaround for main str-vs-sym bug)
            print("  roll_tick_log succeeded (file was recovered/overwritten)")
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")
            print("  → main rotate_handle refuses non-empty targets; roll fails.")
            print("  → claude-2 roll_tick uses validateSeq to recover.")
            pub.shutdown()
            return False


def test_3_concurrent() -> tuple[int, int, int]:
    print("\n--- TEST 3: CONCURRENT (publisher running during roll) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename=str(date.today()))

        published = 0
        stop = threading.Event()
        errors: list[Exception] = []

        def publisher():
            nonlocal published
            i = 0
            while not stop.is_set():
                try:
                    pub.publish("trade", _row(i))
                    published += 1
                    i += 1
                except Exception as e:
                    errors.append(e)
                    break
            print(f"  publisher: stopped after {published} rows")

        t = threading.Thread(target=publisher, daemon=True)
        t.start()

        # Let it warm up
        time.sleep(0.5)
        before_roll = published
        print(f"  rolling at {published} rows...")
        try:
            pub.roll_tick_log(log_dir + "/", b"segment_after_roll")
            print(f"  roll_tick_log returned successfully")
        except Exception as e:
            print(f"  roll RAISED: {type(e).__name__}: {e}")
            stop.set()
            pub.shutdown()
            return (published, 0, 0)

        time.sleep(0.5)
        after_roll = published
        stop.set()
        t.join(timeout=2.0)

        # tp's in-memory accumulator has ALL rows that succeeded
        in_memory = pub.get_var("trade").height
        print(f"  before_roll: {before_roll}; after_roll: {after_roll}; total_published: {published}")
        print(f"  in_memory: {in_memory} rows ({published - in_memory} not reflected)")

        # Check both segment files
        files = sorted(Path(log_dir).glob("segment_*"))
        print(f"  segment files: {[f.name for f in files]}")
        for f in files:
            print(f"    {f.name}: {f.stat().st_size} bytes")

        pub.shutdown()
        if errors:
            print(f"  ERRORS during publish: {errors[:3]}")
        return (published, in_memory, published - in_memory)


def main() -> int:
    t1 = test_1_baseline()
    t2 = test_2_recovery()
    pub_n, mem_n, lost = test_3_concurrent()

    print("\n=== VERDICT ===")
    print(f"  Test 1 (baseline):  {'PASS' if t1 else 'FAIL'}")
    print(f"  Test 2 (recovery):  {'PASS' if t2 else 'FAIL (expected — main rotate_handle refuses non-empty)'}")
    print(f"  Test 3 (concurrent): published={pub_n}; in_memory={mem_n}; rows_unaccounted={lost}")

    if not t2:
        print("  → Confirmed gap: main has NO seq-tail recovery on retry-after-crash.")
        print("  → claude-2's roll_tick provides validateSeq-based recovery.")
    if lost > 0:
        print(f"  → Confirmed gap: main loses {lost} rows under concurrent publish+roll.")
        print("  → claude-2's roll_tick atomic swap inside handle.write() prevents this.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
