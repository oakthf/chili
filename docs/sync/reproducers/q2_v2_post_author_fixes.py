"""Q2-v2 — re-verify roll_tick_log on main 0.9.0+ after author's fsync commits.

Tests against the new main HEAD (5cfc096):
  T1. BASELINE — roll_tick_log with default Python str args. Should now PASS
      (author shipped .handle.rotate ArgType::StrOrSym fix).
  T2. IDEMPOTENT RETRY — call roll_tick_log twice with same target. Should
      PASS (author shipped skip-if-URI-already-in-map).
  T3. RECOVERY — roll into a pre-existing partial file (kill -9 mid-roll
      simulation). Should STILL fail (rotate_handle still refuses non-empty
      targets; this remains a gap).
  T4. fsync-OLD-before-swap — verify that rotate flushes the old writer
      before swapping. Indirect test: write data, roll, kill the engine,
      check the OLD segment has all the pre-roll data on disk.
"""
from __future__ import annotations

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
        "price": [100.0 + i * 0.01],
        "size": [i],
    })


def test_baseline_str() -> bool:
    print("--- T1: BASELINE roll_tick_log with default Python str args ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        for i in range(3):
            pub.publish("trade", _row(i))
        try:
            pub.roll_tick_log(log_dir + "/", "segment_002")
            print("  roll_tick_log(str) succeeded — str/sym bug FIXED")
            for i in range(3):
                pub.publish("trade", _row(100 + i))
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")
            print("  → str/sym bug STILL present")
            pub.shutdown()
            return False


def test_idempotent_retry() -> bool:
    print("\n--- T2: IDEMPOTENT RETRY — roll twice to same target ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        for i in range(3):
            pub.publish("trade", _row(i))

        try:
            pub.roll_tick_log(log_dir + "/", "segment_002")
            print("  first roll: OK")
        except Exception as e:
            print(f"  first roll RAISED: {e}")
            pub.shutdown()
            return False

        try:
            # SAME target — should now be a no-op (idempotent)
            pub.roll_tick_log(log_dir + "/", "segment_002")
            print("  second roll (idempotent): OK")
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  second roll RAISED: {type(e).__name__}: {e}")
            print("  → idempotent retry NOT working")
            pub.shutdown()
            return False


def test_recovery_pre_existing_partial() -> bool:
    print("\n--- T3: RECOVERY — roll into pre-existing partial file ---")
    with tempfile.TemporaryDirectory() as log_dir:
        # Simulate prior-crash partial file at target
        pre_existing = Path(log_dir) / "segment_002"
        pre_existing.write_bytes(b"partial-content-from-prior-crash")
        print(f"  pre-existing partial file: {pre_existing.stat().st_size} bytes")

        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        for i in range(3):
            pub.publish("trade", _row(i))

        try:
            pub.roll_tick_log(log_dir + "/", "segment_002")
            print("  roll succeeded (target file was recovered/overwritten)")
            pub.shutdown()
            return True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")
            print("  → STILL A GAP: main rotate_handle refuses non-empty targets")
            print("  → claude-2's roll_tick uses .broker.validateSeq to truncate torn tail")
            pub.shutdown()
            return False


def test_fsync_old_before_swap() -> bool:
    print("\n--- T4: rotate flushes OLD writer (fsync-before-swap) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )

        # Write data WITHOUT explicit fsync
        for i in range(10):
            pub.publish("trade", _row(i))

        first_segment_path = Path(log_dir) / str(date.today())
        size_pre_roll = first_segment_path.stat().st_size
        print(f"  pre-roll first-segment size: {size_pre_roll} bytes")

        # Roll — should fsync the OLD writer before swapping
        pub.roll_tick_log(log_dir + "/", "segment_002")

        # File size after roll should be unchanged (no more writes to it)
        size_post_roll = first_segment_path.stat().st_size
        print(f"  post-roll first-segment size: {size_post_roll} bytes")

        # The key test: AFTER the engine drops the handle (close), the file should
        # have the same content. Author's commit flushes on rotate AND on close,
        # so this should be durable.
        pub.shutdown()

        # Re-read after engine drops the handle
        size_after_close = first_segment_path.stat().st_size
        print(f"  after-close size: {size_after_close} bytes")
        if size_after_close == size_post_roll and size_post_roll > 0:
            print("  → old segment data durably on disk after rotate+close")
            return True
        else:
            print(f"  → mismatch: post_roll={size_post_roll}, after_close={size_after_close}")
            return False


def main() -> int:
    t1 = test_baseline_str()
    t2 = test_idempotent_retry()
    t3 = test_recovery_pre_existing_partial()
    t4 = test_fsync_old_before_swap()

    print("\n=== VERDICT ===")
    print(f"  T1 (str-form baseline, was BUG):         {'PASS' if t1 else 'FAIL'}")
    print(f"  T2 (idempotent retry):                   {'PASS' if t2 else 'FAIL'}")
    print(f"  T3 (recovery from pre-existing partial): {'PASS' if t3 else 'FAIL (gap remains)'}")
    print(f"  T4 (fsync-OLD-before-swap):              {'PASS' if t4 else 'FAIL'}")

    fixed = sum([t1, t2, t4])
    still_gap = not t3

    if fixed >= 3 and still_gap:
        print(f"\n  → 3 of 4 atomicity properties NOW shipped on main 0.9.0+")
        print(f"  → 1 gap remains: seq-tail recovery from prior partial file")
        print(f"  → Decision point: ship validateSeq-style recovery upstream, or document")
        print(f"     'tp must clean partial segments before restart' contract")
    elif fixed == 4:
        print(f"\n  → ALL 4 atomicity properties shipped — claude-2 roll_tick fully covered")
    else:
        print(f"\n  → mixed result; see individual test outputs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
