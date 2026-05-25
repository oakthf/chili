"""Q2 v4 — re-verify three recovery cases + tick-count preservation on main@cc954d2.

After the author's commits 26b437e + 74acdc6, two things should be different:

  1. prepare_file_writer now calls count_seq_messages + file.set_len(valid_size)
     before returning the writer for ConnType::Sequence files. This means
     the torn tail is TRUNCATED inside prepare_file_writer — applies to BOTH
     open_handle AND rotate_handle.
     Expected: T3c (torn-tail) now PASSES.

  2. open_handle + rollLog set tick[0] to the message count of the existing
     file. After rotating to a file with 10 records, tick[0] == 10.
     Expected: tick count matches existing file content.

Plus a sanity re-run of T3b (clean seq) which should still pass.
T3a (garbage file) is unchanged — no recovery for non-sequence files.
"""
from __future__ import annotations

import tempfile
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


def _publish_n(pub: ChiliEngine, n: int) -> tuple[int, str | None]:
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


def _try_replay(path: Path) -> tuple[bool, str]:
    eng = ChiliEngine(pepper=True)
    try:
        n = eng.fn_call(".broker.validateSeq", [str(path), False])
        eng.shutdown()
        return True, f"validateSeq returned: {n}"
    except Exception as e:
        eng.shutdown()
        return False, f"validateSeq RAISED: {e}"


def _classify_file(path: Path) -> str:
    if not path.exists() or path.stat().st_size == 0:
        return "empty"
    with path.open("rb") as f:
        head = f.read(4)
    if head != bytes([255, 0, 0, 0]):
        return f"no-seq-header (head={head!r})"
    return f"seq-header (size={path.stat().st_size})"


def test_3a_garbage() -> tuple[bool, str]:
    print("\n--- T3a: GARBAGE file (no seq header) — unchanged ---")
    with tempfile.TemporaryDirectory() as log_dir:
        target = Path(log_dir) / "segment_002"
        target.write_bytes(b"partial-content-from-prior-crash")
        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename=str(date.today()))
        _publish_n(pub, 3)
        try:
            pub.roll_tick_log(log_dir + "/", "segment_002")
            print("  rotate: SUCCESS (silent)")
        except Exception as e:
            print(f"  rotate RAISED: {e}")
            pub.shutdown()
            return False, "rotate failed"
        _publish_n(pub, 3)
        pub.shutdown()
        print(f"  result file: {_classify_file(target)}")
        ok, info = _try_replay(target)
        print(f"  replay: {info}")
        return ok, info


def test_3b_clean_seq() -> tuple[bool, str]:
    print("\n--- T3b: CLEAN sequence file — author's case ---")
    with tempfile.TemporaryDirectory() as log_dir:
        builder = ChiliEngine(pepper=True)
        builder.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename="seed")
        _publish_n(builder, 5)
        builder.shutdown()

        target = Path(log_dir) / "seed"
        print(f"  pre-existing: {_classify_file(target)}")
        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename="origin")
        _publish_n(pub, 3)
        try:
            pub.roll_tick_log(log_dir + "/", "seed")
            print("  rotate: SUCCESS")
        except Exception as e:
            print(f"  rotate RAISED: {e}")
            pub.shutdown()
            return False, "rotate failed"
        _publish_n(pub, 3)
        pub.shutdown()
        ok, info = _try_replay(target)
        print(f"  replay: {info}")
        return ok, info


def test_3c_torn_tail() -> tuple[bool, str]:
    print("\n--- T3c: TORN TAIL — the kill-9-mid-write case ---")
    with tempfile.TemporaryDirectory() as log_dir:
        builder = ChiliEngine(pepper=True)
        builder.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename="torn_seed")
        _publish_n(builder, 10)
        builder.shutdown()

        target = Path(log_dir) / "torn_seed"
        full_size = target.stat().st_size
        torn_size = full_size - 17
        print(f"  built: {full_size} bytes; truncating to {torn_size} (torn)")
        with target.open("r+b") as f:
            f.truncate(torn_size)
        print(f"  after truncate: {_classify_file(target)}")

        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename="origin2")
        _publish_n(pub, 3)
        try:
            pub.roll_tick_log(log_dir + "/", "torn_seed")
            print("  rotate INTO torn: SUCCESS")
        except Exception as e:
            print(f"  rotate RAISED: {e}")
            pub.shutdown()
            return False, "rotate failed"

        # check size AFTER rotate (should be truncated to last-valid)
        size_after_rotate = target.stat().st_size
        print(f"  size after rotate: {size_after_rotate} (was {torn_size} pre-rotate)")
        print(f"    → if < {torn_size}: prepare_file_writer truncated torn tail")

        _publish_n(pub, 3)
        pub.shutdown()
        print(f"  final file: {_classify_file(target)}")
        ok, info = _try_replay(target)
        print(f"  replay: {info}")
        return ok, info


def test_tickcount_preservation() -> bool:
    print("\n--- T4: tick-count preservation after rotate to existing file ---")
    with tempfile.TemporaryDirectory() as log_dir:
        builder = ChiliEngine(pepper=True)
        builder.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename="existing")
        _publish_n(builder, 7)
        builder_tick = builder.fn_call("tick", [0, 0])
        print(f"  builder tick[0] after 7 publishes: {builder_tick}")
        builder.shutdown()

        # New engine, rotate INTO the existing file
        pub = ChiliEngine(pepper=True)
        pub.init_tick(schema={"trade": _trade_schema()}, log_dir=log_dir + "/", filename="fresh")
        tick_before = pub.fn_call("tick", [0, 0])
        print(f"  pub tick[0] after init (fresh log): {tick_before}")

        pub.roll_tick_log(log_dir + "/", "existing")
        tick_after_rotate = pub.fn_call("tick", [0, 0])
        print(f"  pub tick[0] after rotating to file with 7 records: {tick_after_rotate}")

        # After 74acdc6: rollLog resets tick[0] then syncs with the handle's msg_count
        # Expected: tick_after_rotate == 7

        pub.shutdown()
        ok = tick_after_rotate == 7
        print(f"  expected 7; got {tick_after_rotate}: {'PASS' if ok else 'FAIL'}")
        return ok


def main() -> int:
    a_ok, a_info = test_3a_garbage()
    b_ok, b_info = test_3b_clean_seq()
    c_ok, c_info = test_3c_torn_tail()
    t4_ok = test_tickcount_preservation()

    print("\n=== VERDICT ===")
    print(f"  T3a (garbage):          {'PASS' if a_ok else 'FAIL'} — {a_info}")
    print(f"  T3b (clean seq):        {'PASS' if b_ok else 'FAIL'} — {b_info}")
    print(f"  T3c (torn tail):        {'PASS' if c_ok else 'FAIL'} — {c_info}")
    print(f"  T4 (tick-count sync):   {'PASS' if t4_ok else 'FAIL'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
