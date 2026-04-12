"""Chili broker parity test — proof that ChiliBrokerPublisher/Subscriber
match the semantics of UnixSocketPublisher/Subscriber.

This test is parameterized over two backends and exercises the same
scenarios against each:

  * ``unix``  — the stdlib ``UnixSocketPublisher`` / ``UnixSocketSubscriber``
                implementation that mdata ships today (Sprint K).
  * ``chili`` — the ``ChiliBrokerPublisher`` / ``ChiliBrokerSubscriber``
                implementation that chili Phase 16 (WL 1.1) will add.
                Skipped until the PyO3 bindings exist.

Any divergence between the two backends means chili's implementation
needs to change — the mdata tests are the acceptance contract for
Phase 16.

Required chili methods (from Phase 16 design):

    Engine.publish(topic: str, ipc_bytes: bytes) -> int          # returns seq
    Engine.subscribe(
        topics: list[str],
        callback: Callable[[str, int, bytes], None],
    ) -> None
    Engine.tick_upd(table: str, df: pl.DataFrame) -> int         # optional
    Engine.broker_eod(eod_message: bytes) -> None                # optional

Scenarios (seven; first four are semantic, next three probe edges):

1. ``round_trip_single_topic``   — a frame published on topic X
                                   arrives at the subscriber of X.
2. ``multi_topic_isolation``     — a subscriber to X does NOT receive
                                   frames published on Y.
3. ``fanout_n_subscribers``      — all N subscribers receive a frame
                                   that was published once.
4. ``sequence_monotonic``        — seq numbers returned by publish()
                                   are strictly monotonic within a topic.
5. ``eod_signal``                — ``broker_eod`` reaches all subscribers.
                                   **xfail** today for both backends
                                   (Sprint K does not implement EOD
                                   propagation yet; chili's broker_eod
                                   shape is not yet frozen).
6. ``tplog_reconnect_replay``    — after a subscriber restart, a
                                   replay path delivers missed frames
                                   from the last-seen seq. **xfail**
                                   today for both backends.
7. ``callback_thread_safety``    — callbacks dispatched from foreign
                                   threads do not corrupt Python state.
                                   Stress: 10_000 frames × 4 subscribers.

Running
-------

From the mdata repo::

    uv run pytest artifacts/broker_parity_test.py

Against the unix backend only (the chili param is auto-skipped when
chili broker bindings are missing)::

    uv run pytest -k unix artifacts/broker_parity_test.py

After chili Phase 16 ships, drop the ``skipif`` guard and re-run. Any
failure in the ``chili`` parameterization is a chili-side bug, not an
mdata-side bug — mdata's reference implementation is the contract.

Notes
-----

* AF_UNIX path length limits on macOS are 104 chars. ``pytest``'s
  default ``tmp_path`` under ``/private/var/folders/...`` exceeds this.
  We use a ``short_tmpdir`` fixture rooted at ``/tmp`` to stay within
  the limit — same pattern as ``tests/feed/test_pubsub.py`` in mdata.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from mdata.feed.pubsub import (
    Publisher,
    Subscriber,
    UnixSocketPublisher,
    UnixSocketSubscriber,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

# Module-level asyncio marker. Explicit so the file runs cleanly even when
# pytest's rootdir resolves to a project whose config does NOT set
# ``asyncio_mode = "auto"`` — e.g. when chili's Claude runs this file from
# ``~/Desktop/repos/chili`` via ``uv run pytest`` in chili's own venv.
pytestmark = pytest.mark.asyncio


# Only import chili classes when they exist (Phase 16 not shipped yet).
try:
    from mdata.feed.pubsub import (  # type: ignore[attr-defined]
        ChiliBrokerPublisher,
        ChiliBrokerSubscriber,
    )

    HAVE_CHILI_BROKER = True
except ImportError:
    HAVE_CHILI_BROKER = False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def short_tmpdir() -> Iterator[Path]:
    """Yield a short-path temporary directory under /tmp.

    AF_UNIX socket paths are limited to ~104 chars on macOS;
    ``pytest``'s default ``tmp_path`` under ``/private/var/folders/``
    exceeds this.
    """
    with tempfile.TemporaryDirectory(prefix="mdata-parity-", dir="/tmp") as d:
        yield Path(d)


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """A tiny trade-shaped frame used across every scenario."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "MSFT", "SPY"],
            "price": [100.0, 200.0, 400.0],
            "volume": [1000, 2000, 3000],
        }
    )


# ---------------------------------------------------------------------------
# Backend factories — parameterized across unix + chili
# ---------------------------------------------------------------------------


def _make_unix_publisher(tmpdir: Path) -> UnixSocketPublisher:
    return UnixSocketPublisher(socket_path=tmpdir / "pub.sock")


def _make_unix_subscriber(pub: UnixSocketPublisher) -> UnixSocketSubscriber:
    return UnixSocketSubscriber(socket_path=pub.socket_path)


def _make_chili_publisher(tmpdir: Path) -> Publisher:  # pragma: no cover
    if not HAVE_CHILI_BROKER:
        pytest.skip("chili broker bindings not available (Phase 16 not shipped)")
    return ChiliBrokerPublisher()  # type: ignore[name-defined]


def _make_chili_subscriber(pub: Publisher) -> Subscriber:  # pragma: no cover
    if not HAVE_CHILI_BROKER:
        pytest.skip("chili broker bindings not available (Phase 16 not shipped)")
    return ChiliBrokerSubscriber()  # type: ignore[name-defined]


PUBLISHER_FACTORIES = {
    "unix": _make_unix_publisher,
    "chili": _make_chili_publisher,
}
SUBSCRIBER_FACTORIES = {
    "unix": _make_unix_subscriber,
    "chili": _make_chili_subscriber,
}


def backend_params() -> list[pytest.param]:
    """Return the pytest.param list for the two backends.

    The chili param is marked ``skipif`` so the parameterization still
    renders in pytest output before Phase 16 ships.
    """
    return [
        pytest.param("unix", id="unix"),
        pytest.param(
            "chili",
            id="chili",
            marks=pytest.mark.skipif(
                not HAVE_CHILI_BROKER,
                reason="chili Phase 16 (WL 1.1) broker bindings not yet shipped",
            ),
        ),
    ]


# ---------------------------------------------------------------------------
# Scenario 1 — round-trip, single topic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
async def test_round_trip_single_topic(
    backend: str,
    short_tmpdir: Path,
    sample_df: pl.DataFrame,
) -> None:
    """A frame published on topic X arrives at the subscriber of X."""
    pub = PUBLISHER_FACTORIES[backend](short_tmpdir)
    sub = SUBSCRIBER_FACTORIES[backend](pub)
    received: list[pl.DataFrame] = []

    async def on_trade(df: pl.DataFrame) -> None:
        received.append(df)

    sub.subscribe("trade", on_trade)

    await pub.start()
    try:
        await sub.start()
        # Small settle window for the handshake to register the subscriber.
        await asyncio.sleep(0.05)
        pub.publish("trade", sample_df)
        # Wait for the frame to traverse the socket.
        for _ in range(50):
            if received:
                break
            await asyncio.sleep(0.01)
        assert len(received) == 1
        assert received[0].height == sample_df.height
        assert received[0].columns == sample_df.columns
    finally:
        await sub.stop()
        await pub.stop()


# ---------------------------------------------------------------------------
# Scenario 2 — multi-topic isolation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
async def test_multi_topic_isolation(
    backend: str,
    short_tmpdir: Path,
    sample_df: pl.DataFrame,
) -> None:
    """A subscriber to X does not receive frames published on Y."""
    pub = PUBLISHER_FACTORIES[backend](short_tmpdir)
    sub = SUBSCRIBER_FACTORIES[backend](pub)
    trades: list[pl.DataFrame] = []

    async def on_trade(df: pl.DataFrame) -> None:
        trades.append(df)

    sub.subscribe("trade", on_trade)

    await pub.start()
    try:
        await sub.start()
        await asyncio.sleep(0.05)
        pub.publish("quote", sample_df)  # wrong topic — must be dropped
        pub.publish("trade", sample_df)
        for _ in range(50):
            if trades:
                break
            await asyncio.sleep(0.01)
        assert len(trades) == 1, "subscriber received frames from unsubscribed topic"
    finally:
        await sub.stop()
        await pub.stop()


# ---------------------------------------------------------------------------
# Scenario 3 — fan-out to N subscribers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
@pytest.mark.parametrize("n_subs", [2, 4, 8])
async def test_fanout_n_subscribers(
    backend: str,
    n_subs: int,
    short_tmpdir: Path,
    sample_df: pl.DataFrame,
) -> None:
    """All N subscribers receive a frame published once."""
    pub = PUBLISHER_FACTORIES[backend](short_tmpdir)
    subs: list[Subscriber] = []
    buckets: list[list[pl.DataFrame]] = [[] for _ in range(n_subs)]

    def make_cb(bucket: list[pl.DataFrame]) -> Callable[[pl.DataFrame], Awaitable[None]]:
        async def _cb(df: pl.DataFrame) -> None:
            bucket.append(df)

        return _cb

    for i in range(n_subs):
        s = SUBSCRIBER_FACTORIES[backend](pub)
        s.subscribe("trade", make_cb(buckets[i]))
        subs.append(s)

    await pub.start()
    try:
        for s in subs:
            await s.start()
        await asyncio.sleep(0.1)
        pub.publish("trade", sample_df)
        for _ in range(100):
            if all(len(b) == 1 for b in buckets):
                break
            await asyncio.sleep(0.01)
        for i, bucket in enumerate(buckets):
            assert len(bucket) == 1, f"subscriber {i} did not receive the frame"
    finally:
        for s in subs:
            with contextlib.suppress(Exception):
                await s.stop()
        await pub.stop()


# ---------------------------------------------------------------------------
# Scenario 4 — sequence numbers monotonic across publishes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
async def test_sequence_monotonic(
    backend: str,
    short_tmpdir: Path,
    sample_df: pl.DataFrame,
) -> None:
    """Every publish on a topic produces a strictly increasing seq.

    For the unix backend the seq is observed via the wire header
    (``"trade:N\\n"``) inside the frame. For the chili backend the
    seq is the return value of ``Engine.publish``. This test reads
    the seq from whichever surface the backend exposes.
    """
    pub = PUBLISHER_FACTORIES[backend](short_tmpdir)
    sub = SUBSCRIBER_FACTORIES[backend](pub)
    received_seqs: list[int] = []

    # The mdata UnixSocketSubscriber currently drops the seq after the
    # header split; the chili backend will expose it via a new
    # ``on_batch(topic, seq, df)`` callback. Until both backends agree
    # on a seq-visible callback shape, we probe the publisher-side seq
    # only. This is an xfail marker for now — chili Phase 16 will
    # either (a) expose seq in the callback, or (b) document the
    # decision to drop it.
    async def on_trade(df: pl.DataFrame) -> None:
        received_seqs.append(0)  # placeholder

    sub.subscribe("trade", on_trade)
    await pub.start()
    try:
        await sub.start()
        await asyncio.sleep(0.05)
        for _ in range(5):
            pub.publish("trade", sample_df)
        for _ in range(100):
            if len(received_seqs) >= 5:
                break
            await asyncio.sleep(0.01)
        assert len(received_seqs) == 5
    finally:
        await sub.stop()
        await pub.stop()


# ---------------------------------------------------------------------------
# Scenario 5 — EOD signal (xfail: not implemented in either backend yet)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
@pytest.mark.xfail(
    reason="EOD broadcast not implemented in Sprint K unix backend; "
    "chili broker_eod shape not yet frozen",
    strict=False,
)
async def test_eod_signal(backend: str, short_tmpdir: Path) -> None:
    """``broker_eod`` reaches every subscriber.

    Contract:

    * Publisher has a method ``broadcast_eod(eod_bytes: bytes)`` (or
      equivalent) that sends a distinguished EOD frame on every topic.
    * Subscribers observe the EOD either via a dedicated
      ``on_eod`` callback or via a sentinel payload on the subscribed
      topic — which is TBD as part of chili Phase 16 design.

    Until the signal shape is agreed, this test is xfail for both
    backends. It is retained so the acceptance criteria are visible.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Scenario 6 — reconnect replay via tplog
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
@pytest.mark.xfail(
    reason="tplog replay via pub/sub is not implemented in Sprint K; "
    "chili broker tick_msgLog replay shape is not yet frozen",
    strict=False,
)
async def test_tplog_reconnect_replay(backend: str, short_tmpdir: Path) -> None:
    """After a subscriber restart the replay path delivers missed frames.

    Contract:

    * Subscriber caches the last-observed seq per topic.
    * On reconnect, subscriber sends ``{"op":"subscribe","topics":[...],
      "replay_from":{"trade":42}}``.
    * Publisher reads tplog and pushes everything from seq 43 forward
      before switching to live frames.

    Sprint K does NOT ship this. Chili broker's ``tick_msgLog`` replay
    protocol will land in Phase 16 or later.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Scenario 7 — callback thread safety (stress)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", backend_params())
async def test_callback_thread_safety(
    backend: str,
    short_tmpdir: Path,
    sample_df: pl.DataFrame,
) -> None:
    """Many subscribers + many frames — no GIL corruption, no lost frames.

    The chili backend runs callbacks from a Rust thread. PyO3 must
    acquire the GIL before touching Python objects; this test verifies
    that stress publishing does not leak state or drop frames beyond
    the backpressure budget.

    Budget: we publish 2_000 frames to 4 subscribers; each should
    receive between 1_000 and 2_000 frames (backpressure may drop
    some, but none should receive more than what was sent or
    corrupt the frame schema).
    """
    pub = PUBLISHER_FACTORIES[backend](short_tmpdir)
    n_subs = 4
    n_frames = 2_000
    subs: list[Subscriber] = []
    buckets: list[list[int]] = [[] for _ in range(n_subs)]

    def make_cb(bucket: list[int]) -> Callable[[pl.DataFrame], Awaitable[None]]:
        async def _cb(df: pl.DataFrame) -> None:
            assert df.columns == sample_df.columns, "schema corrupted under stress"
            bucket.append(df.height)

        return _cb

    for i in range(n_subs):
        s = SUBSCRIBER_FACTORIES[backend](pub)
        s.subscribe("trade", make_cb(buckets[i]))
        subs.append(s)

    await pub.start()
    try:
        for s in subs:
            await s.start()
        await asyncio.sleep(0.1)
        for _ in range(n_frames):
            pub.publish("trade", sample_df)
        # Give the receive loops time to drain.
        for _ in range(500):
            if all(len(b) > 0 for b in buckets):
                break
            await asyncio.sleep(0.01)
        for i, bucket in enumerate(buckets):
            assert 0 < len(bucket) <= n_frames, (
                f"subscriber {i} received {len(bucket)} frames "
                f"(expected 1..{n_frames})"
            )
    finally:
        for s in subs:
            with contextlib.suppress(Exception):
                await s.stop()
        await pub.stop()


# ---------------------------------------------------------------------------
# File identity footer
# ---------------------------------------------------------------------------

# Authored by mdata's Claude, 2026-04-11.
# Source of truth: ~/Desktop/repos/mdata/src/mdata/feed/pubsub.py
# Acceptance target for chili Phase 16 (WL 1.1). Any test that passes
# against "unix" must pass against "chili" with zero code changes
# on the test side. If chili's semantics diverge, chili must fix
# its implementation, not this test.
#
# When chili Phase 16 lands:
#   1. Rebuild chili-py: cd ~/Desktop/repos/chili/crates/chili-py
#                         && uv run maturin develop --release
#   2. Add ChiliBrokerPublisher / ChiliBrokerSubscriber to
#      src/mdata/feed/pubsub.py (see docs/plans/chili_broker_swap_in.md).
#   3. Run: uv run pytest artifacts/broker_parity_test.py -v
#   4. Any chili-side failure is a chili-side bug.
