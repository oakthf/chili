"""Sprint 17 — chili-py acceptance tests for ``engine.publish_via_handle``.

Sprint 16 mdata-wishlist Q3 locked Option B: chili ships a thin
one-shot publish primitive (``publish_via_handle(h, table, df)``);
mdata writes their ``RemoteTpClient`` class on top.

These tests exercise the marshalling + dispatch chain end-to-end via
TCP loopback, which is strictly more realistic than a Rust-side mock
``Handle``. Per dispatch brief audit C5, the Rust integration test
is intentionally omitted; loopback pytest covers the path.
"""

import socket
import time

import polars as pl
import pytest
from chili import ChiliEngine


def _free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestPublishViaHandle:
    """Spin up a receiver engine that defines ``upd`` (via ``load_sub``)
    and a separate client engine that opens an Outgoing handle and
    publishes a DataFrame through it."""

    def test_round_trip_via_outgoing_handle(self):
        """Client opens ``chili://`` handle, publishes via it; receiver
        applies ``upd[table; df]`` and the table reflects the rows."""
        port = _free_port()

        # -- receiver: defines `upd` + an empty `trade` table + TCP listener --
        receiver = ChiliEngine(pepper=True)
        receiver.load_sub()  # defines `upd: {[table; data] table upsert data; ...}`
        # Pre-create the `trade` table with the schema we'll publish.
        empty = pl.DataFrame(
            {
                "sym": pl.Series([], dtype=pl.Categorical),
                "price": pl.Series([], dtype=pl.Float64),
                "size": pl.Series([], dtype=pl.Int64),
            }
        )
        receiver.set_var("trade", empty)
        receiver.start_tcp_listener(port)
        time.sleep(0.1)

        # -- client: opens Outgoing handle, calls publish_via_handle --
        client = ChiliEngine(pepper=True)
        h = client.fn_call(".handle.open", [f"chili://127.0.0.1:{port}"])
        assert isinstance(h, int), f"expected i64 handle id, got {type(h).__name__}"

        df = pl.DataFrame(
            {
                "sym": pl.Series(["AAPL", "GOOG"], dtype=pl.Categorical),
                "price": [150.0, 2800.0],
                "size": [100, 200],
            }
        )
        client.publish_via_handle(h, "trade", df)

        # publish_via_handle is blocking (sync send+receive). The receiver's
        # `upd` should have already executed by the time we get here, but
        # give the receiver thread a tick to flush its state.
        time.sleep(0.05)

        recv_trade = receiver.get_var("trade")
        assert recv_trade.shape[0] == 2, (
            f"expected 2 rows on receiver after publish_via_handle, got:\n{recv_trade}"
        )

        client.shutdown()
        receiver.shutdown()

    def test_invalid_handle_raises(self):
        """Calling publish_via_handle with a bogus handle id raises
        RuntimeError (chili-side InvalidHandleErr)."""
        client = ChiliEngine(pepper=True)
        df = pl.DataFrame({"sym": ["AAPL"], "price": [150.0], "size": [100]})

        with pytest.raises(RuntimeError):
            client.publish_via_handle(99_999, "trade", df)

        client.shutdown()
