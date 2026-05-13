"""Sprint 16 — `engine.flush_tplog()` tests.

Backs mdata's PRD §5.1 part-2 kill-9 durability requirement. After `publish`,
calling `flush_tplog` advances the kernel-level fsync so a hard process kill
cannot lose the row.

Three invariants:
  1. `flush_tplog` after publishes returns the payload-bytes-since-last-flush
     count (replacing mdata's `stat().st_size` proxy).
  2. Counter resets to 0 after each successful flush.
  3. Calling `flush_tplog` BEFORE `init_tick` raises RuntimeError (since
     `.tick.msgHandle` is undefined).
"""

import tempfile
from datetime import date

import polars as pl
import pytest
from chili import ChiliEngine


class TestTplogFlush:
    """`engine.flush_tplog()` durability hook."""

    def _trade_schema(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "sym": pl.Series([], dtype=pl.Categorical),
                "price": pl.Series([], dtype=pl.Float64),
                "size": pl.Series([], dtype=pl.Int64),
            }
        )

    def test_flush_before_init_tick_raises(self):
        """Before init_tick, `.tick.msgHandle` is unset — flush raises."""
        e = ChiliEngine(pepper=True)
        with pytest.raises(RuntimeError, match=r"\.tick\.msgHandle"):
            e.flush_tplog()

    def test_flush_returns_bytes_then_resets(self):
        """Publish 2 rows → flush returns >0 byte count. Flush again with no
        new publishes → returns 0. Publish more → counter advances from 0."""
        e = ChiliEngine(pepper=True)
        with tempfile.TemporaryDirectory() as log_dir:
            e.init_tick(
                schema={"trade": self._trade_schema()},
                log_dir=log_dir + "/",
                date=date.today(),
            )

            # First publish — write some rows.
            data1 = pl.DataFrame(
                {
                    "sym": pl.Series(["AAPL", "GOOG"], dtype=pl.Categorical),
                    "price": [150.0, 2800.0],
                    "size": [100, 200],
                }
            )
            e.publish("trade", data1)

            # Flush — expect a positive byte count.
            bytes_flushed = e.flush_tplog()
            assert bytes_flushed > 0, (
                f"flush_tplog after publish should return positive byte "
                f"count, got {bytes_flushed}"
            )
            first_flush = bytes_flushed

            # Flush again — no new publishes, expect 0.
            bytes_flushed_2 = e.flush_tplog()
            assert bytes_flushed_2 == 0, (
                f"flush_tplog with no writes between should return 0, "
                f"got {bytes_flushed_2}"
            )

            # Publish more — counter advances from 0.
            data2 = pl.DataFrame(
                {
                    "sym": pl.Series(["TSLA"], dtype=pl.Categorical),
                    "price": [800.0],
                    "size": [50],
                }
            )
            e.publish("trade", data2)
            bytes_flushed_3 = e.flush_tplog()
            assert 0 < bytes_flushed_3 < first_flush, (
                f"third flush should be smaller than first (fewer rows), got "
                f"{bytes_flushed_3} vs first {first_flush}"
            )

    def test_flush_is_idempotent_under_repeated_calls(self):
        """Calling flush_tplog twice in succession is safe — second returns 0."""
        e = ChiliEngine(pepper=True)
        with tempfile.TemporaryDirectory() as log_dir:
            e.init_tick(
                schema={"trade": self._trade_schema()},
                log_dir=log_dir + "/",
                date=date.today(),
            )
            e.publish(
                "trade",
                pl.DataFrame(
                    {
                        "sym": pl.Series(["AAPL"], dtype=pl.Categorical),
                        "price": [100.0],
                        "size": [10],
                    }
                ),
            )
            n1 = e.flush_tplog()
            n2 = e.flush_tplog()
            n3 = e.flush_tplog()
            assert n1 > 0
            assert n2 == 0
            assert n3 == 0
