"""Tests for chili Phase 16 — in-process broker pub/sub (WL 1.1).

Mirrors the five non-xfail scenarios from the mdata broker parity test
(docs/bench/mdata-collab/artifacts/broker_parity_test.py) but tests the
raw Engine.publish() / Engine.subscribe() API directly — no mdata ABCs
or asyncio required.

Scenarios:
  1. round_trip          — publish on X → callback on X receives it
  2. multi_topic_isolation — subscriber to X does NOT receive Y
  3. fanout              — all N subscribers receive a single publish
  4. sequence_monotonic  — publish() returns strictly increasing seq
  5. stress_thread_safety — 2000 frames × 4 subs, no corruption
  6. tick_upd            — convenience DataFrame publish
  7. publish_no_subscribers — publish with no subscribers returns seq
"""
from __future__ import annotations

import io
import time

import polars as pl
import pytest

import chili


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ipc_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return buf.getvalue()


def _wait_for(predicate, *, timeout_s: float = 2.0, poll_s: float = 0.01):
    """Spin until predicate() is truthy or timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(poll_s)


SAMPLE_DF = pl.DataFrame(
    {"symbol": ["AAPL", "MSFT", "SPY"], "price": [100.0, 200.0, 400.0], "volume": [1000, 2000, 3000]}
)
SAMPLE_IPC = _make_ipc_bytes(SAMPLE_DF)


# ---------------------------------------------------------------------------
# Scenario 1 — round-trip, single topic
# ---------------------------------------------------------------------------

def test_round_trip_single_topic():
    """A frame published on topic X arrives at the subscriber of X."""
    engine = chili.Engine(pepper=True)
    received: list[tuple[str, int, bytes]] = []

    def on_msg(topic: str, seq: int, ipc_bytes: bytes) -> None:
        received.append((topic, seq, ipc_bytes))

    engine.subscribe(["trade"], on_msg)
    seq = engine.publish("trade", SAMPLE_IPC)
    assert seq == 1

    _wait_for(lambda: len(received) >= 1)

    assert len(received) == 1
    topic, rseq, rbytes = received[0]
    assert topic == "trade"
    assert rseq == 1
    result = pl.read_ipc_stream(io.BytesIO(rbytes))
    assert result.height == SAMPLE_DF.height
    assert result.columns == SAMPLE_DF.columns


# ---------------------------------------------------------------------------
# Scenario 2 — multi-topic isolation
# ---------------------------------------------------------------------------

def test_multi_topic_isolation():
    """A subscriber to X does NOT receive frames published on Y."""
    engine = chili.Engine(pepper=True)
    trades: list[str] = []

    def on_trade(topic: str, seq: int, ipc_bytes: bytes) -> None:
        trades.append(topic)

    engine.subscribe(["trade"], on_trade)

    engine.publish("quote", SAMPLE_IPC)   # wrong topic — must be dropped
    engine.publish("trade", SAMPLE_IPC)   # right topic

    _wait_for(lambda: len(trades) >= 1)
    # Extra wait to ensure no late delivery from "quote"
    time.sleep(0.05)

    assert trades == ["trade"]


# ---------------------------------------------------------------------------
# Scenario 3 — fan-out to N subscribers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n_subs", [2, 4, 8])
def test_fanout_n_subscribers(n_subs: int):
    """All N subscribers receive a frame published once."""
    engine = chili.Engine(pepper=True)
    buckets: list[list[int]] = [[] for _ in range(n_subs)]

    for i in range(n_subs):
        bucket = buckets[i]

        def on_msg(topic: str, seq: int, ipc_bytes: bytes, b=bucket) -> None:
            b.append(seq)

        engine.subscribe(["trade"], on_msg)

    engine.publish("trade", SAMPLE_IPC)

    _wait_for(lambda: all(len(b) >= 1 for b in buckets))

    for i, bucket in enumerate(buckets):
        assert len(bucket) == 1, f"subscriber {i} did not receive the frame"


# ---------------------------------------------------------------------------
# Scenario 4 — sequence numbers monotonic
# ---------------------------------------------------------------------------

def test_sequence_monotonic():
    """publish() returns strictly monotonic seq numbers per topic."""
    engine = chili.Engine(pepper=True)
    received_seqs: list[int] = []

    def on_msg(topic: str, seq: int, ipc_bytes: bytes) -> None:
        received_seqs.append(seq)

    engine.subscribe(["trade"], on_msg)

    pub_seqs = [engine.publish("trade", SAMPLE_IPC) for _ in range(5)]
    assert pub_seqs == [1, 2, 3, 4, 5]

    _wait_for(lambda: len(received_seqs) >= 5)
    assert received_seqs == [1, 2, 3, 4, 5]


def test_sequence_independent_per_topic():
    """Each topic has its own independent sequence counter."""
    engine = chili.Engine(pepper=True)

    s1 = engine.publish("trade", SAMPLE_IPC)
    s2 = engine.publish("trade", SAMPLE_IPC)
    s3 = engine.publish("quote", SAMPLE_IPC)

    assert s1 == 1
    assert s2 == 2
    assert s3 == 1  # independent counter


# ---------------------------------------------------------------------------
# Scenario 5 — callback thread safety (stress)
# ---------------------------------------------------------------------------

def test_stress_thread_safety():
    """2000 frames × 4 subscribers — no GIL corruption, no lost frames
    beyond backpressure budget."""
    engine = chili.Engine(pepper=True)
    n_subs = 4
    n_frames = 2_000
    buckets: list[list[int]] = [[] for _ in range(n_subs)]

    for i in range(n_subs):
        bucket = buckets[i]

        def on_msg(topic: str, seq: int, ipc_bytes: bytes, b=bucket) -> None:
            # Verify schema not corrupted under stress
            df = pl.read_ipc_stream(io.BytesIO(ipc_bytes))
            assert df.columns == SAMPLE_DF.columns, "schema corrupted under stress"
            b.append(seq)

        engine.subscribe(["trade"], on_msg)

    for _ in range(n_frames):
        engine.publish("trade", SAMPLE_IPC)

    _wait_for(lambda: all(len(b) > 0 for b in buckets), timeout_s=5.0)

    for i, bucket in enumerate(buckets):
        assert 0 < len(bucket) <= n_frames, (
            f"subscriber {i} received {len(bucket)} frames "
            f"(expected 1..{n_frames})"
        )


# ---------------------------------------------------------------------------
# Scenario 6 — tick_upd convenience method
# ---------------------------------------------------------------------------

def test_tick_upd():
    """tick_upd serializes a DataFrame and publishes it."""
    engine = chili.Engine(pepper=True)
    received: list[tuple[str, int]] = []

    def on_msg(topic: str, seq: int, ipc_bytes: bytes) -> None:
        df = pl.read_ipc_stream(io.BytesIO(ipc_bytes))
        received.append((topic, df.height))

    engine.subscribe(["ohlcv_1d"], on_msg)

    seq = engine.tick_upd("ohlcv_1d", SAMPLE_DF)
    assert seq == 1

    _wait_for(lambda: len(received) >= 1)

    assert received[0] == ("ohlcv_1d", 3)


# ---------------------------------------------------------------------------
# Scenario 7 — publish with no subscribers
# ---------------------------------------------------------------------------

def test_publish_no_subscribers():
    """publish returns a seq even when there are no subscribers."""
    engine = chili.Engine(pepper=True)
    assert engine.publish("trade", SAMPLE_IPC) == 1
    assert engine.publish("trade", SAMPLE_IPC) == 2
    assert engine.publish("quote", SAMPLE_IPC) == 1


# ---------------------------------------------------------------------------
# Scenario 8 — broker_eod raises NotImplementedError
# ---------------------------------------------------------------------------

def test_broker_eod_not_implemented():
    """broker_eod raises NotImplementedError (shape not yet frozen)."""
    engine = chili.Engine(pepper=True)
    with pytest.raises(NotImplementedError):
        engine.broker_eod(b"eod")
