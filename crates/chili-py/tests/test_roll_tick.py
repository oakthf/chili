"""Sprint 18 — chili-py acceptance tests for ``engine.roll_tick``.

Atomic tplog segment rollover (mdata wishlist v2 P0, thread
``mdata-chili-eod-upd-race-2026-05-15``). Tier-2 of the dispatch
brief's matrix: exercises the FFI + the REAL TCP async publish path
(``engine_state.rs:2215-2225`` thread-per-conn → ``handle_chili_conn``
→ ``eval`` → ``sync``) end-to-end, which is strictly more realistic
than the Rust-side `sync()` contention in
``crates/chili-core/tests/roll_tick_test.rs`` (that file owns the
deterministic teeth + per-publisher SEQ-MONO proofs).

Independent oracle: ``_count_frames`` parses the raw on-disk tplog
frame stream in pure Python — 8-byte ``\\xff\\x00\\x00\\x00`` magic
then repeating ``[len:u64 LE | ts:u64 LE | payload]``. It never calls
chili to verify chili. Conservation (Σ frames across every segment ==
rows published) is the durable-layer no-loss invariant mdata's PRD
§5.1 cares about.
"""

import os
import socket
import tempfile
import threading
import time
from datetime import date

import polars as pl
import pytest
from chili import ChiliEngine


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _trade_schema() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sym": pl.Series([], dtype=pl.Categorical),
            "price": pl.Series([], dtype=pl.Float64),
            "size": pl.Series([], dtype=pl.Int64),
        }
    )


def _row(i: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sym": pl.Series(["AAPL"], dtype=pl.Categorical),
            "price": pl.Series([float(i)], dtype=pl.Float64),
            "size": pl.Series([i], dtype=pl.Int64),
        }
    )


def _count_frames(path: str) -> int:
    """Independent raw-frame parser. Returns count of complete tplog
    frames at `path` (8-byte magic, then [len u64 | ts u64 | payload]).
    Torn trailing frame ignored, matching chili's validate_seq."""
    try:
        with open(path, "rb") as fh:
            b = fh.read()
    except FileNotFoundError:
        return 0
    if len(b) < 8:
        return 0
    assert b[0:4] == b"\xff\x00\x00\x00", f"{path}: missing sequence magic"
    pos, n = 8, 0
    while pos + 16 <= len(b):
        ln = int.from_bytes(b[pos : pos + 8], "little")
        pos += 16
        if ln == 0 or pos + ln > len(b):
            break
        pos += ln
        n += 1
    return n


def _total_frames(log_dir: str) -> int:
    """Σ frames across every segment file in the tplog dir."""
    return sum(
        _count_frames(os.path.join(log_dir, f)) for f in os.listdir(log_dir)
    )


class TestRollTick:
    def test_no_loss_via_tcp_under_concurrent_publish(self):
        """THE realistic proof: a TCP publisher streams `.tick.upd`
        through an open handle while the tp `roll_tick`s mid-stream.
        Every published row must be durable in exactly one segment —
        Σ frames across both segments == rows sent, zero loss."""
        port = _free_port()
        with tempfile.TemporaryDirectory() as log_dir:
            ld = log_dir + "/"
            tp = ChiliEngine(pepper=True)
            tp.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=ld,
                date=date.today(),
            )
            # Tickerplant ingest shim: remote `publish_via_handle` sends a
            # bare `upd` symbol; route it into the tplog via `.tick.upd`
            # (a real tp exposes this; mirrors mdata's fh→tp ingest).
            tp.eval("upd: {[t; d] .tick.upd[t; d]}", "ingest.pep")
            tp.start_tcp_listener(port)
            time.sleep(0.1)

            pub = ChiliEngine(pepper=True)
            h = pub.fn_call(".handle.open", [f"chili://127.0.0.1:{port}"])
            assert isinstance(h, int)

            sent = 0
            done = threading.Event()

            def publisher():
                nonlocal sent
                while not done.is_set():
                    pub.publish_via_handle(h, "trade", _row(sent))
                    sent += 1

            t = threading.Thread(target=publisher)
            t.start()
            time.sleep(0.05)
            tp.roll_tick(ld, "seg-0001")  # roll mid-stream
            time.sleep(0.05)
            done.set()
            t.join()

            time.sleep(0.05)  # let the last in-flight upd settle
            total = _total_frames(log_dir)
            assert sent > 0, "publisher must have sent something"
            assert total == sent, (
                f"durable no-loss: Σ tplog frames ({total}) must equal rows "
                f"published ({sent}); a deficit is the silent-drop bug"
            )

            pub.shutdown()
            tp.shutdown()

    def test_uhf_rapid_successive_rolls_lose_nothing(self):
        """UHF / size-triggered rollover: 50 rapid `roll_tick`s in a
        tight loop while a publisher streams. Every row durable across
        all 50 boundaries; no daily/date assumption (opaque labels)."""
        port = _free_port()
        with tempfile.TemporaryDirectory() as log_dir:
            ld = log_dir + "/"
            tp = ChiliEngine(pepper=True)
            tp.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=ld,
                date=date.today(),
            )
            # Tickerplant ingest shim: remote `publish_via_handle` sends a
            # bare `upd` symbol; route it into the tplog via `.tick.upd`
            # (a real tp exposes this; mirrors mdata's fh→tp ingest).
            tp.eval("upd: {[t; d] .tick.upd[t; d]}", "ingest.pep")
            tp.start_tcp_listener(port)
            time.sleep(0.1)

            pub = ChiliEngine(pepper=True)
            h = pub.fn_call(".handle.open", [f"chili://127.0.0.1:{port}"])

            sent = 0
            done = threading.Event()

            def publisher():
                nonlocal sent
                while not done.is_set():
                    pub.publish_via_handle(h, "trade", _row(sent))
                    sent += 1

            t = threading.Thread(target=publisher)
            t.start()
            for i in range(1, 51):
                tp.roll_tick(ld, f"uhf-{i:04d}")
                time.sleep(0.001)
            done.set()
            t.join()
            time.sleep(0.05)

            total = _total_frames(log_dir)
            assert sent > 0
            assert total == sent, (
                f"UHF: Σ frames across 51 segments ({total}) == rows sent "
                f"({sent}); no loss across any of the 50 rapid boundaries"
            )
            pub.shutdown()
            tp.shutdown()

    def test_idempotent_repeat_and_schema_continuity(self):
        """Second roll_tick(same label) is a no-op; `.tick.schema` and
        `.tick.msgLog` stay coherent across the roll."""
        with tempfile.TemporaryDirectory() as log_dir:
            ld = log_dir + "/"
            e = ChiliEngine(pepper=True)
            e.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=ld,
                date=date.today(),
            )
            e.publish("trade", _row(1))
            e.roll_tick(ld, "seg-A")
            e.publish("trade", _row(2))
            e.roll_tick(ld, "seg-A")  # idempotent — no-op
            e.publish("trade", _row(3))

            assert _count_frames(ld + "seg-A") == 2, (
                "seg-A accumulates rows 2 & 3 across the idempotent repeat "
                "(no truncation / reopen)"
            )
            assert e.has_var(".tick.schema"), ".tick.schema preserved"
            msglog = e.get_var(".tick.msgLog")  # chili String → py bytes
            if isinstance(msglog, bytes):
                msglog = msglog.decode()
            assert msglog == ld + "seg-A", (
                ".tick.msgLog reflects the rolled-to segment"
            )
            # Schema still usable: a further publish works.
            e.publish("trade", _row(4))
            assert _count_frames(ld + "seg-A") == 3
            e.shutdown()

    def test_roll_tick_before_init_raises(self):
        """No `.tick.msgHandle` yet → RuntimeError, not a panic/segfault."""
        e = ChiliEngine(pepper=True)
        with pytest.raises(RuntimeError):
            e.roll_tick("/tmp/", "seg-0001")
        e.shutdown()

    def test_empty_segment_label_raises(self):
        with tempfile.TemporaryDirectory() as log_dir:
            ld = log_dir + "/"
            e = ChiliEngine(pepper=True)
            e.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=ld,
                date=date.today(),
            )
            with pytest.raises(RuntimeError):
                e.roll_tick(ld, "")
            e.shutdown()

    def test_bytes_since_flush_reset_after_roll(self):
        """roll_tick zeroes the per-handle write counter (it fsync'd the
        old segment and swapped in a fresh writer): flush_tplog right
        after a roll with no new writes returns 0."""
        with tempfile.TemporaryDirectory() as log_dir:
            ld = log_dir + "/"
            e = ChiliEngine(pepper=True)
            e.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=ld,
                date=date.today(),
            )
            e.publish("trade", _row(1))
            assert e.flush_tplog() > 0, "writes registered before roll"
            e.roll_tick(ld, "seg-Z")
            assert e.flush_tplog() == 0, (
                "roll_tick fsync'd old + reset bytes_since_flush; a flush "
                "with no post-roll writes must return 0"
            )
            e.shutdown()

    def test_eod_then_roll_is_cutover_only(self):
        """Contract lock-in: roll_tick does NOT fire the EOD broadcast.
        A subscriber's pepper `eod` handler fires for `eod(d)` but NOT
        for a bare `roll_tick` — so a future change can't silently make
        roll_tick subsume the broadcast."""
        port = _free_port()
        with tempfile.TemporaryDirectory() as log_dir:
            ld = log_dir + "/"
            pub = ChiliEngine(pepper=True)
            pub.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=ld,
                date=date.today(),
            )
            pub.start_tcp_listener(port)
            time.sleep(0.1)
            pub_port = port

            sub = ChiliEngine(pepper=True)
            sub.eval(".sub.eod.fired: ::", "t.pep")
            sub.eval(
                "eod: {[d] .sub.eod.fired: d}",
                "t.pep",
            )
            sub.subscribe(f"chili://127.0.0.1:{pub_port}", ["trade"])
            time.sleep(0.1)

            # roll_tick alone — must NOT trigger the subscriber eod shim.
            pub.roll_tick(ld, "seg-0001")
            time.sleep(0.2)
            assert sub.get_var(".sub.eod.fired") is None, (
                "roll_tick is cutover-only — it must NOT broadcast eod"
            )

            # eod(d) DOES trigger it (proves the shim works at all).
            pub.eod(date.today())
            deadline = time.time() + 5
            fired = None
            while time.time() < deadline:
                v = sub.get_var(".sub.eod.fired")
                if v is not None:
                    fired = v
                    break
                time.sleep(0.05)
            assert fired is not None, (
                "eod(d) must still broadcast (sanity: the shim works)"
            )

            sub.shutdown()
            pub.shutdown()
