"""Sprint 17 Part A — acceptance test for subscriber-side ``eod`` dispatch.

Ports mdata's ``tests/rdb/test_rdb_subscriber.py::test_subscriber_eod_shim_triggered_by_publisher_eod``
into chili-py pytest as the load-bearing acceptance test for the
chili-side bug fix. mdata flips their xfail strict-pass on receipt of
the 0.8.5 wheel that makes this test green.

Per dispatch brief audit C2: chili-py public API uses:
- ``subscribe(uri, [topics])`` — single socket URI + topics list, NOT
  ``subscribe(host=, port=, topic=)`` as originally drafted.
- ``has_var(id)`` returns ``bool``; ``get_var`` raises
  ``chili.engine_state.NameError`` (a subclass of ``ChiliError →
  RuntimeError``) when the var is unset. Use ``has_var`` to avoid
  the bare-``except NameError`` trap that catches Python builtin
  ``NameError`` instead.
- ``::`` (pepper null literal, Sprint 16 P2) round-trips to Python
  ``None``; use ``is None`` not ``== chili.Null`` (no such symbol exists).
- ``pub_port`` must be captured AFTER ``start_tcp_listener`` by reading
  the OS-assigned port back from the engine.

Per dispatch brief audit C1: the original H2 (function-body assignment
scope) hypothesis is code-disproved — ``eval.rs:125`` routes
dot-namespaced assignments to global ``state.set_var`` regardless of fn
context. So this test is designed to discriminate between H1 (eod var
visibility), H4 (message shape mismatch), and H5 (timing race).
"""

import socket
import tempfile
import time
from datetime import date

import polars as pl
from chili import ChiliEngine


def _free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_subscriber_eod_shim_triggered_by_publisher_eod():
    """End-to-end: publisher fires .tick.eod[date]; subscriber's
    pepper-level ``eod`` handler should write ``.sub.eod.fired`` so
    Python can observe the message arrived."""
    port = _free_port()

    # -- publisher: tp engine with TCP listener --
    pub = ChiliEngine(pepper=True)
    with tempfile.TemporaryDirectory() as log_dir:
        trade_schema = pl.DataFrame(
            {
                "sym": pl.Series([], dtype=pl.Categorical),
                "price": pl.Series([], dtype=pl.Float64),
                "size": pl.Series([], dtype=pl.Int64),
            }
        )
        pub.init_tick(
            schema={"trade": trade_schema},
            log_dir=log_dir + "/",
            date=date.today(),
        )
        pub.start_tcp_listener(port)
        time.sleep(0.1)

        # -- subscriber: define eod shim BEFORE subscribing --
        sub = ChiliEngine(pepper=True)
        # `.sub.eod.fired: ::` initializes the sentinel to pepper null
        # (which round-trips to Python None). `eod: {[msg] .sub.eod.fired: msg}`
        # defines the shim that captures the message into the global var.
        sub.eval(".sub.eod.fired: ::")
        sub.eval("eod: {[msg] .sub.eod.fired: msg}")
        sub.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
        time.sleep(0.1)

        # -- publisher fires .tick.eod[date] --
        # This routes pub → .broker.eod → signal_eod → sync(h, (`eod; date))
        # to each Publishing handle. Subscriber's handle_chili_conn receives
        # the message and calls state.eval → eval_op → should dispatch
        # `eod[date]` invoking the shim.
        pub.eod(date.today())

        # -- wait for the eod shim to fire (extended polling per audit C1 H5
        # timing-race hypothesis) --
        eod_msg = None
        for _ in range(100):  # 100 * 50ms = 5s ceiling
            if sub.has_var(".sub.eod.fired"):
                got = sub.get_var(".sub.eod.fired")
                # Initial value is pepper null = Python None.
                # The shim writes a non-None value when it fires.
                if got is not None:
                    eod_msg = got
                    break
            time.sleep(0.05)

        # The bug: eod_msg stays None forever; .sub.eod.fired never gets
        # written by the eod shim. Either eod was never invoked (H1, H4)
        # or there's a timing race we should reconsider (H5).
        assert eod_msg is not None, (
            "subscriber's eod shim never fired — .sub.eod.fired is still "
            "None / unset after publisher's .tick.eod[date] broadcast. "
            "Expected the shim to have written the (`eod; <date>) message."
        )

        sub.shutdown()
    pub.shutdown()


def test_multi_message_subscriber_observes_upd_then_eod():
    """O1 (audit appendix) — `handle_chili_conn` calls `stack.clear_vars`
    at the top of each loop iteration; verify a subscriber thread can
    process a multi-message sequence (an upd followed by an eod) and
    both side-effects land.

    Sprint 17 bug shape: the original signal_eod failure also took out
    the upd path indirectly (signal_eod's `disconnect_handle(&h)` killed
    the subscriber's incoming-side handle, so subsequent publishes were
    silently dropped). This test guards against the regression in both
    directions.
    """
    port = _free_port()

    pub = ChiliEngine(pepper=True)
    with tempfile.TemporaryDirectory() as log_dir:
        trade_schema = pl.DataFrame(
            {
                "sym": pl.Series([], dtype=pl.Categorical),
                "price": pl.Series([], dtype=pl.Float64),
                "size": pl.Series([], dtype=pl.Int64),
            }
        )
        pub.init_tick(
            schema={"trade": trade_schema},
            log_dir=log_dir + "/",
            date=date.today(),
        )
        pub.start_tcp_listener(port)
        time.sleep(0.1)

        sub = ChiliEngine(pepper=True)
        sub.eval(".sub.eod.fired: ::")
        sub.eval("eod: {[msg] .sub.eod.fired: msg}")
        sub.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
        time.sleep(0.1)

        # Step 1 — publisher fires an upd; subscriber's `upd` (from
        # sub.pep) should upsert into `trade`.
        df = pl.DataFrame(
            {
                "sym": pl.Series(["AAPL"], dtype=pl.Categorical),
                "price": [150.0],
                "size": [100],
            }
        )
        pub.publish("trade", df)
        time.sleep(0.1)

        # Step 2 — publisher fires EOD; subscriber's `eod` shim should
        # write `.sub.eod.fired`.
        pub.eod(date.today())

        eod_msg = None
        for _ in range(100):
            if sub.has_var(".sub.eod.fired"):
                got = sub.get_var(".sub.eod.fired")
                if got is not None:
                    eod_msg = got
                    break
            time.sleep(0.05)

        # Both side-effects must have landed.
        sub_trade = sub.get_var("trade")
        assert sub_trade.shape[0] == 1, (
            f"subscriber's upd shim should have one row after the pub upd; "
            f"got shape {sub_trade.shape}"
        )
        assert eod_msg is not None, (
            "subscriber's eod shim should have fired after the multi-message "
            "sequence; `.sub.eod.fired` is still unset / None"
        )

        sub.shutdown()
    pub.shutdown()
