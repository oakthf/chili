"""Tests for post-Phase-16 extras: overwrite_partition, broker_eod, query_plan."""
from __future__ import annotations

import io
import os
import shutil
import tempfile
import time

import polars as pl
import pytest

import chili


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ipc(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return buf.getvalue()


SAMPLE_DF = pl.DataFrame(
    {"symbol": ["AAPL", "MSFT", "SPY"], "price": [100.0, 200.0, 400.0]}
)


@pytest.fixture
def tmp_hdb():
    """Create a temp HDB with one table and two partitions."""
    tmpdir = tempfile.mkdtemp(prefix="chili-test-")
    engine = chili.Engine(pepper=True)

    df1 = pl.DataFrame(
        {"symbol": ["AAPL", "MSFT"], "close": [150.0, 300.0]}
    )
    df2 = pl.DataFrame(
        {"symbol": ["AAPL", "MSFT", "SPY"], "close": [155.0, 310.0, 450.0]}
    )

    engine.wpar(df1, tmpdir, "test_tbl", "2024.01.02")
    engine.wpar(df2, tmpdir, "test_tbl", "2024.01.03")

    yield tmpdir, engine
    engine.close()
    shutil.rmtree(tmpdir)


# ---------------------------------------------------------------------------
# 1. overwrite_partition
# ---------------------------------------------------------------------------

class TestOverwritePartition:

    def test_overwrite_replaces_data(self, tmp_hdb):
        """overwrite_partition replaces the partition content."""
        tmpdir, engine = tmp_hdb

        # Write initial partition via wpar (creates _0000)
        df_orig = pl.DataFrame({"symbol": ["GOOG"], "close": [100.0]})
        engine.wpar(df_orig, tmpdir, "test_tbl", "2024.01.04")

        # Overwrite with different data
        df_new = pl.DataFrame({"symbol": ["GOOG", "META"], "close": [200.0, 300.0]})
        engine.overwrite_partition(df_new, tmpdir, "test_tbl", "2024.01.04")

        # Verify: read partition directly
        tbl_dir = os.path.join(tmpdir, "test_tbl")
        files = [f for f in os.listdir(tbl_dir) if f.startswith("2024.01.04_")]
        assert len(files) == 1, f"Expected 1 shard, got {files}"
        assert files[0] == "2024.01.04_0000"

        result = pl.read_parquet(os.path.join(tbl_dir, files[0]))
        assert result.height == 2
        assert set(result["symbol"].to_list()) == {"GOOG", "META"}

    def test_overwrite_removes_multiple_shards(self, tmp_hdb):
        """overwrite_partition removes all existing shards."""
        tmpdir, engine = tmp_hdb

        df = pl.DataFrame({"symbol": ["X"], "close": [1.0]})
        # Create 3 shards via wpar
        engine.wpar(df, tmpdir, "test_tbl", "2024.02.01")
        engine.wpar(df, tmpdir, "test_tbl", "2024.02.01")
        engine.wpar(df, tmpdir, "test_tbl", "2024.02.01")

        tbl_dir = os.path.join(tmpdir, "test_tbl")
        shards_before = [f for f in os.listdir(tbl_dir) if f.startswith("2024.02.01_")]
        assert len(shards_before) == 3

        # Overwrite — should delete all 3 and write 1
        df_new = pl.DataFrame({"symbol": ["Y"], "close": [99.0]})
        engine.overwrite_partition(df_new, tmpdir, "test_tbl", "2024.02.01")

        shards_after = [f for f in os.listdir(tbl_dir) if f.startswith("2024.02.01_")]
        assert len(shards_after) == 1
        assert shards_after[0] == "2024.02.01_0000"

    def test_overwrite_nonexistent_creates_fresh(self, tmp_hdb):
        """overwrite_partition on a date with no existing data creates _0000."""
        tmpdir, engine = tmp_hdb
        df = pl.DataFrame({"symbol": ["NEW"], "close": [42.0]})
        engine.overwrite_partition(df, tmpdir, "test_tbl", "2024.06.15")

        tbl_dir = os.path.join(tmpdir, "test_tbl")
        files = [f for f in os.listdir(tbl_dir) if f.startswith("2024.06.15_")]
        assert files == ["2024.06.15_0000"]


# ---------------------------------------------------------------------------
# 2. broker_eod
# ---------------------------------------------------------------------------

class TestBrokerEod:

    def test_eod_reaches_all_subscribers(self):
        """broker_eod sends __eod__ to all subscriber callbacks."""
        engine = chili.Engine(pepper=True)
        received: list[tuple[str, int, bytes]] = []

        def on_msg(topic: str, seq: int, ipc_bytes: bytes) -> None:
            received.append((topic, seq, ipc_bytes))

        engine.subscribe(["trade", "quote"], on_msg)
        engine.broker_eod(b"end-of-day-2024.01.02")

        # Wait for delivery
        deadline = time.monotonic() + 2.0
        while len(received) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)

        # Should receive 2 EOD messages (one per topic subscription)
        assert len(received) == 2
        for topic, seq, payload in received:
            assert topic == "__eod__"
            assert seq == 0
            assert payload == b"end-of-day-2024.01.02"

    def test_eod_does_not_interfere_with_normal_publish(self):
        """Normal publishes continue working after EOD."""
        engine = chili.Engine(pepper=True)
        received: list[str] = []

        def on_msg(topic: str, seq: int, ipc_bytes: bytes) -> None:
            received.append(topic)

        engine.subscribe(["trade"], on_msg)

        engine.broker_eod(b"eod")
        time.sleep(0.05)

        # Publish after EOD should still work
        engine.publish("trade", b"data")
        time.sleep(0.05)

        assert "__eod__" in received
        assert "trade" in received


# ---------------------------------------------------------------------------
# 3. query_plan
# ---------------------------------------------------------------------------

class TestQueryPlan:

    def test_query_plan_returns_string(self, tmp_hdb):
        """query_plan returns a non-empty plan string."""
        tmpdir, engine = tmp_hdb
        engine.load(tmpdir)

        plan = engine.query_plan(
            "select last close by symbol from test_tbl "
            "where date within 2024.01.01 2024.12.31"
        )
        assert isinstance(plan, str)
        assert len(plan) > 0
        # Plan should mention the table or scan operation
        plan_lower = plan.lower()
        assert "parquet" in plan_lower or "scan" in plan_lower or "filter" in plan_lower

    def test_query_plan_does_not_execute(self, tmp_hdb):
        """query_plan should NOT collect/execute the query."""
        tmpdir, engine = tmp_hdb
        engine.load(tmpdir)

        # This should return quickly (no data materialization)
        t0 = time.perf_counter()
        plan = engine.query_plan(
            "select from test_tbl where date=2024.01.02"
        )
        elapsed = (time.perf_counter() - t0) * 1000
        assert isinstance(plan, str)
        # Should be very fast (< 100ms) since no data is read
        # (allowing generous margin for load_par_df)
        assert elapsed < 500, f"query_plan took {elapsed:.0f}ms — too slow, may be executing"

    def test_query_plan_requires_hdb(self):
        """query_plan raises if no HDB is loaded."""
        engine = chili.Engine(pepper=True)
        with pytest.raises(RuntimeError, match="No HDB"):
            engine.query_plan("select from t")
