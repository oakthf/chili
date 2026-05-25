"""Q2 v3 — grade main 0.9.0@588de78's rotate_handle against three recovery cases.

Author's 588de78 ('rotate_handle allows non-empty files and preserves tick count')
removes the ConnType::New guard so rotate_handle can target existing files.
This closes the simple "previous segment already exists" case the author
tested in his roll_tick_test.rs::rotate_handle_accepts_non_empty_file.

But there are three distinct pre-existing-file states a real kill-9-mid-roll
can leave behind. Grade rotate_handle against each:

  T3a — GARBAGE FILE: target file has content but NO valid sequence header
    Example: stray write from another process; partial truncated file
    Expected behavior debate: refuse OR overwrite? Authors' new code accepts
    it (ConnType::File) and APPENDS, leaving a corrupt non-sequence file.

  T3b — VALID SEQUENCE FILE, CLEAN TAIL: target is a properly-closed sequence
    file (header + complete records, no torn write at tail)
    Author's new test covers this. Should append cleanly.

  T3c — TORN TAIL: target is a valid sequence file with a torn record at the
    tail (partial bytes from a kill-9 mid-write)
    This is the load-bearing crash-recovery case. Without seq-tail validation
    + truncation (validateSeq + set_len in claude-2's roll_tick), new writes
    append AFTER the torn bytes, producing an unparseable file.

Each test reports rotate outcome AND whether the resulting file is parseable.
"""
from __future__ import annotations

import struct
import tempfile
from datetime import date
from pathlib import Path

import polars as pl
from chili import ChiliEngine


SEQ_HEADER = bytes([255, 0, 0, 0]) + bytes([0, 0, 0, 0])  # 8 bytes


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


def _classify_file(path: Path) -> str:
    """Return classification: empty / no-seq-header / seq-clean / seq-torn / chili-replay-OK."""
    if not path.exists() or path.stat().st_size == 0:
        return "empty"
    with path.open("rb") as f:
        head = f.read(4)
    if head != bytes([255, 0, 0, 0]):
        return f"no-seq-header (head={head!r})"
    return f"seq-header-present (size={path.stat().st_size})"


def _try_replay(path: Path) -> tuple[bool, str]:
    """Try to read the tplog through chili's subscribe — proxy for 'is this file usable?'.
    Returns (ok, info). We approximate via re-opening the file as a fresh handle and
    checking validateSeq via fn_call('.broker.validateSeq', [path, false]).
    """
    eng = ChiliEngine(pepper=True)
    try:
        n = eng.fn_call(".broker.validateSeq", [str(path), False])
        eng.shutdown()
        return True, f"validateSeq returned: {n}"
    except Exception as e:
        eng.shutdown()
        return False, f"validateSeq RAISED: {e}"


def _publish_n(pub: ChiliEngine, n: int) -> tuple[int, str | None]:
    """Publish n rows, returning (succeeded, last_error_msg)."""
    ok = 0
    last_err: str | None = None
    for i in range(n):
        try:
            pub.publish("trade", _row(i))
            ok += 1
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            break
    return ok, last_err


def test_3a_garbage_file() -> tuple[bool, str]:
    print("\n--- T3a: GARBAGE pre-existing file (no valid seq header) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        target = Path(log_dir) / "segment_002"
        target.write_bytes(b"partial-content-from-prior-crash")
        print(f"  garbage file: {_classify_file(target)}")

        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        result = _publish_n(pub, 3)

        try:
            pub.roll_tick_log(log_dir + "/", "segment_002")
            print("  rotate: SUCCESS (no error)")
            rotate_ok = True
        except Exception as e:
            print(f"  rotate RAISED: {e}")
            rotate_ok = False
            pub.shutdown()
            return rotate_ok, "rotate failed"

        result = _publish_n(pub, 3)
        pub.shutdown()

        print(f"  result file: {_classify_file(target)}")
        ok, info = _try_replay(target)
        print(f"  replay-check: {info}")
        return ok, info


def test_3b_clean_sequence_file() -> tuple[bool, str]:
    print("\n--- T3b: VALID sequence file with CLEAN tail (author's test case) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        # Build a valid sequence file by using a separate publisher
        builder = ChiliEngine(pepper=True)
        builder.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename="seed",
        )
        result = _publish_n(builder, 5)
        builder.shutdown()  # graceful — fsyncs

        target = Path(log_dir) / "seed"
        print(f"  pre-existing valid seq file: {_classify_file(target)}")

        # Now: pub starts fresh on a different log, then rotates INTO the existing seed
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename="origin",
        )
        result = _publish_n(pub, 3)

        try:
            pub.roll_tick_log(log_dir + "/", "seed")
            print("  rotate: SUCCESS")
        except Exception as e:
            print(f"  rotate RAISED: {e}")
            pub.shutdown()
            return False, "rotate failed"

        result = _publish_n(pub, 3)
        pub.shutdown()

        print(f"  result file: {_classify_file(target)}")
        ok, info = _try_replay(target)
        print(f"  replay-check: {info}")
        return ok, info


def test_3c_torn_tail() -> tuple[bool, str]:
    print("\n--- T3c: VALID sequence file with TORN tail (kill -9 mid-record) ---")
    with tempfile.TemporaryDirectory() as log_dir:
        # Build a valid sequence file, then truncate at a mid-record offset
        builder = ChiliEngine(pepper=True)
        builder.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename="torn_seed",
        )
        result = _publish_n(builder, 10)
        builder.shutdown()

        target = Path(log_dir) / "torn_seed"
        full_size = target.stat().st_size
        torn_size = full_size - 17  # mid-record cut
        print(f"  built valid file: {full_size} bytes; truncating to {torn_size} (torn tail)")
        with target.open("r+b") as f:
            f.truncate(torn_size)
        print(f"  after truncate: {_classify_file(target)}")

        # Verify torn file via validateSeq directly
        eng = ChiliEngine(pepper=True)
        try:
            seq_count = eng.fn_call(".broker.validateSeq", [str(target), False])
            print(f"  validateSeq on torn file BEFORE rotate: {seq_count}")
            print(f"    → validateSeq IS the recovery mechanism — note the resulting size:")
            print(f"    size after validateSeq: {target.stat().st_size} bytes (was {torn_size})")
        except Exception as e:
            print(f"  validateSeq BEFORE rotate RAISED: {e}")
        eng.shutdown()

        # Re-truncate (validateSeq may have rewound)
        with target.open("r+b") as f:
            f.truncate(torn_size)
        print(f"  re-truncated to {torn_size} for the rotate test")

        # Now rotate into the torn file
        pub = ChiliEngine(pepper=True)
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename="origin2",
        )
        result = _publish_n(pub, 3)

        try:
            pub.roll_tick_log(log_dir + "/", "torn_seed")
            print("  rotate INTO torn file: SUCCESS (rotated)")
        except Exception as e:
            print(f"  rotate RAISED: {e}")
            pub.shutdown()
            return False, "rotate failed"

        result = _publish_n(pub, 3)
        pub.shutdown()

        # Final state
        final_size = target.stat().st_size
        print(f"  final file: {_classify_file(target)} size={final_size}")
        ok, info = _try_replay(target)
        print(f"  replay-check: {info}")
        return ok, info


def main() -> int:
    a_ok, a_info = test_3a_garbage_file()
    b_ok, b_info = test_3b_clean_sequence_file()
    c_ok, c_info = test_3c_torn_tail()

    print("\n=== VERDICT ===")
    print(f"  T3a (garbage file):       {'PASS' if a_ok else 'FAIL'} — {a_info}")
    print(f"  T3b (clean seq tail):     {'PASS' if b_ok else 'FAIL'} — {b_info}")
    print(f"  T3c (torn seq tail):      {'PASS' if c_ok else 'FAIL'} — {c_info}")

    print("\n  → T3a result tells us if rotate refuses garbage or corrupts it silently")
    print("  → T3b is the author's tested case — should PASS")
    print("  → T3c is the load-bearing crash-recovery case — PASS means validateSeq-equivalent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
