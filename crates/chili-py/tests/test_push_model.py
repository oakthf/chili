"""Sprint 21 / ADR-0006 — committed e2e guards for the mdata push-model
(D-1 GIL-free upd notification, D-3 resumable cursor, D-2 lazy accessor).

Mirrors the two-engine IPC harness from ``test_tick_sub.py``: a tick
(publisher) engine + a subscriber engine over a real ``chili://`` socket,
so the subscriber's pure-Rust ``handle_chili_conn`` receive thread runs
the D-1 enqueue hook for real.
"""

import fcntl
import os
import select
import socket
import tempfile
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


def _batch(syms, prices, sizes) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sym": pl.Series(syms, dtype=pl.Categorical),
            "price": prices,
            "size": sizes,
        }
    )


def _wait_until(predicate, timeout=3.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class TestD1UpdNotify:
    """D-1: arming ``upd_notify_fd`` before ``subscribe`` delivers every
    applied ``upd`` as an ``UpdEvent`` over the self-pipe — no polling of
    ``get_var``."""

    def test_fd_drain_roundtrip(self):
        port = _free_port()
        t = ChiliEngine(pepper=True)
        with tempfile.TemporaryDirectory() as log_dir:
            t.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=log_dir + "/",
                date=date.today(),
            )
            t.start_tcp_listener(port)
            time.sleep(0.1)

            s = ChiliEngine(pepper=True)
            # Arm notification BEFORE subscribe (ADR-0006 §1).
            fd = s.upd_notify_fd()
            assert isinstance(fd, int) and fd >= 0
            # Idempotent: same fd every call.
            assert s.upd_notify_fd() == fd
            # FD_CLOEXEC must be set (Q5 — defensive belt for
            # multiprocessing across exec).
            flags = fcntl.fcntl(fd, fcntl.F_GETFD)
            assert flags & fcntl.FD_CLOEXEC, "self-pipe fd must be FD_CLOEXEC"

            # Quiet before any upd.
            assert select.select([fd], [], [], 0)[0] == []

            s.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
            time.sleep(0.1)

            t.publish("trade", _batch(["AAPL", "GOOG"], [150.0, 2800.0], [100, 200]))

            # The fd becomes readable; drain yields the applied delta —
            # NO get_var poll loop anywhere.
            assert _wait_until(lambda: select.select([fd], [], [], 0)[0] != []), (
                "self-pipe must signal an applied upd"
            )
            events = s.drain_upds()
            # Replay (subscribe) + the live publish may arrive as
            # separate batches; assert at least the live one and that
            # every event is well-formed.
            assert len(events) >= 1
            seen_rows = 0
            last_hi = -1
            for e in events:
                assert e.table == "trade"
                assert e.cursor_hi > e.cursor_lo  # monotone advance
                assert e.cursor_hi > last_hi  # FIFO per handle
                last_hi = e.cursor_hi
                assert isinstance(e.frame, pl.DataFrame)
                seen_rows += e.frame.height
            assert seen_rows >= 2  # the AAPL+GOOG batch

            # After draining the pipe + queue, the fd quiesces (no
            # spurious re-arm).
            assert select.select([fd], [], [], 0)[0] == []

            # The applied state still matches (notification is in
            # addition to, not instead of, the upsert).
            assert s.get_var("trade").height >= 2

            s.shutdown()
        t.shutdown()

    def test_drain_empty_when_unarmed(self):
        e = ChiliEngine(pepper=True)
        # Never armed → drain is a no-op, not an error.
        assert e.drain_upds() == []
        e.shutdown()


class TestD3ResumeCursor:
    """D-3: ``subscribe(resume_from=...)`` threads a persisted cursor
    through ``.sub.init`` → ``resume_cursor[topics]`` → ``replay`` start.
    chili's cursor is a monotonic delivery position; per-row dedup is
    the caller's own ``seq`` (Q1 Path-1). Here we assert the
    chili-coordinate behavior: a resume cursor past every logged message
    replays nothing; cursor 0 replays everything."""

    def _publish_n(self, t, n):
        for i in range(n):
            t.publish("trade", _batch([f"S{i}"], [float(i)], [i]))

    def test_resume_skips_already_seen(self):
        port = _free_port()
        t = ChiliEngine(pepper=True)
        with tempfile.TemporaryDirectory() as log_dir:
            t.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=log_dir + "/",
                date=date.today(),
            )
            t.start_tcp_listener(port)
            time.sleep(0.1)
            self._publish_n(t, 5)  # message-log indices 0..4

            # Fresh subscriber, resume past all logged msgs → 0 replayed.
            s_past = ChiliEngine(pepper=True)
            s_past.subscribe(
                f"chili://127.0.0.1:{port}", ["trade"], resume_from={"trade": 99}
            )
            time.sleep(0.2)
            replayed_past = (
                s_past.get_var("trade").height if s_past.has_var("trade") else 0
            )
            assert replayed_past == 0, (
                f"resume_from past all msgs must replay nothing, got {replayed_past}"
            )
            s_past.shutdown()

            # Fresh subscriber, no resume (cursor 0) → full replay.
            s_all = ChiliEngine(pepper=True)
            s_all.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
            assert _wait_until(
                lambda: s_all.has_var("trade") and s_all.get_var("trade").height == 5
            ), (
                "no resume cursor must replay the full log "
                f"(got {s_all.get_var('trade').height if s_all.has_var('trade') else 0})"
            )
            s_all.shutdown()
        t.shutdown()

    def test_unclean_restart_recovers_via_replay(self):
        """Q4 kill-9 contract at the chili boundary: a subscriber that
        dies *without* shutdown and restarts with the cursor it had
        drained recovers exactly the tail via replay — no in-flight
        queue durability needed (the tplog is the source of truth)."""
        port = _free_port()
        t = ChiliEngine(pepper=True)
        with tempfile.TemporaryDirectory() as log_dir:
            t.init_tick(
                schema={"trade": _trade_schema()},
                log_dir=log_dir + "/",
                date=date.today(),
            )
            t.start_tcp_listener(port)
            time.sleep(0.1)
            self._publish_n(t, 3)  # indices 0,1,2

            # Subscriber drains the first 3, persists cursor=3, then
            # dies UNCLEANLY (no .shutdown() — models kill -9).
            s1 = ChiliEngine(pepper=True)
            s1.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
            assert _wait_until(
                lambda: s1.has_var("trade") and s1.get_var("trade").height == 3
            )
            persisted_cursor = 3
            del s1  # no shutdown — unclean loss

            # Two more msgs land while the subscriber is dead.
            self._publish_n(t, 2)  # indices 3,4

            # Restart with the persisted cursor → replay only the tail
            # (indices >= 3): exactly the 2 missed, zero of the first 3.
            s2 = ChiliEngine(pepper=True)
            s2.subscribe(
                f"chili://127.0.0.1:{port}",
                ["trade"],
                resume_from={"trade": persisted_cursor},
            )
            assert _wait_until(
                lambda: s2.has_var("trade") and s2.get_var("trade").height == 2
            ), (
                "unclean restart must recover exactly the post-cursor tail "
                f"(got {s2.get_var('trade').height if s2.has_var('trade') else 0})"
            )
            s2.shutdown()
        t.shutdown()


class TestD2LazyAccessor:
    """D-2: ``get_var_lazy`` is a snapshot ``LazyFrame`` — pushdown in
    the plan over the in-memory frame; ``.collect()`` byte-identical to
    ``get_var``."""

    def test_lazy_pushdown_and_equivalence(self):
        e = ChiliEngine(pepper=True)
        e.set_var(
            "t",
            pl.DataFrame({"sym": ["AAPL", "GOOG", "MSFT"], "px": [1.0, 2.0, 3.0]}),
        )
        eager = e.get_var("t")

        lf = e.get_var_lazy("t")
        assert isinstance(lf, pl.LazyFrame)

        # .collect() is byte-identical to get_var.
        from polars.testing import assert_frame_equal

        assert_frame_equal(lf.collect(), eager)

        # A predicate pushes down in the lazy plan over the in-memory
        # frame (a DF-rooted plan, not a Parquet scan).
        plan = lf.filter(pl.col("px") > 1.5).explain()
        assert "DF" in plan, f"expected an in-memory DataFrame plan, got:\n{plan}"
        assert "parquet" not in plan.lower(), (
            f"in-memory var must not be a Parquet scan:\n{plan}"
        )
        # Pushed-down filter is present in the plan + correct result.
        filtered = lf.filter(pl.col("px") > 1.5).collect()
        assert filtered.height == 2
        assert set(filtered["sym"].to_list()) == {"GOOG", "MSFT"}

        e.shutdown()

    def test_get_var_lazy_missing_raises(self):
        e = ChiliEngine(pepper=True)
        with pytest.raises(Exception):
            e.get_var_lazy("nope")
        e.shutdown()


class TestQ5CloseOnExecFork:
    """Q5: the notify fd is FD_CLOEXEC so it cannot leak into a
    ``multiprocessing`` child across exec. fork(2) alone does NOT close
    a CLOEXEC fd (that is exec-only) — so the precise, deterministic
    guard is: the FD_CLOEXEC flag is set on the live fd (an exec would
    then close it), and the flag is observably set in a forked child
    too."""

    def test_cloexec_flag_set_and_inherited(self):
        e = ChiliEngine(pepper=True)
        fd = e.upd_notify_fd()
        assert fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC

        pid = os.fork()
        if pid == 0:
            # Child: a real exec here would close the fd; assert the
            # flag that guarantees it is present, then hard-exit so we
            # never run the test machinery twice.
            try:
                ok = bool(fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC)
                os._exit(0 if ok else 1)
            except Exception:
                os._exit(2)
        else:
            _, status = os.waitpid(pid, 0)
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, (
                "forked child must observe FD_CLOEXEC on the notify fd"
            )
        e.shutdown()
