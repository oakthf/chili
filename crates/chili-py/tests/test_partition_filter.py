"""End-to-end pytest for HDB partition filtering through the chili-py bindings.

This test owns the full "customer" path: build an HDB via `engine.wpar`,
reload it via `engine.load`, and query through `engine.eval`. Each query
shape that the mdata gateway relies on is covered:

    * `where date=X`            — exact equality
    * `where date>=X, date<=Y`  — two-sided narrow range
    * `where date>=X`           — half-open lower bound
    * `where date<=Y`           — half-open upper bound
    * `where date within X Y`   — inclusive k/q range
    * `where symbol=X, date=Y`  — non-partition clause before partition clause

All of these used to fail (R1: "partition date filter returns wrong rows",
discovered 2026-04-10) because `load_par_df` inserted `par_vec` in raw
filesystem order and `slice::binary_search` then returned wrong indices.

Run with: `pytest crates/chili-py/tests/test_partition_filter.py`
"""
from __future__ import annotations

import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

import chili


DATES = [
    date(2024, 1, 2),
    date(2024, 1, 3),
    date(2024, 1, 4),
    date(2024, 1, 5),
    date(2024, 1, 8),
]

SYMBOLS = ["AAPL", "MSFT", "SPY"]


def _make_df(symbols: list[str], day: date) -> pl.DataFrame:
    """3 rows per partition, one per symbol."""
    ts = datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=timezone.utc)
    n = len(symbols)
    return pl.DataFrame(
        {
            "symbol": symbols,
            "timestamp": [ts] * n,
            "open": [float(i + 1) for i in range(n)],
            "high": [float(i + 1) + 0.5 for i in range(n)],
            "low": [float(i + 1) - 0.5 for i in range(n)],
            "close": [float(i + 1) for i in range(n)],
            "volume": [float((i + 1) * 100) for i in range(n)],
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("us", "UTC")))


@pytest.fixture(scope="module")
def loaded_engine() -> chili.Engine:
    """Write 5 date partitions in non-sorted creation order, then load."""
    tmp = tempfile.mkdtemp(prefix="chili_r1_py_")
    hdb = Path(tmp) / "hdb"
    hdb.mkdir()

    writer = chili.Engine(pepper=True)
    # Intentionally write out of order — the R1 bug only surfaces when
    # fs iteration order differs from lexical/numeric order.
    for d in [DATES[2], DATES[0], DATES[4], DATES[1], DATES[3]]:
        writer.wpar(_make_df(SYMBOLS, d), str(hdb), "ohlcv_1d", d.strftime("%Y.%m.%d"))

    engine = chili.Engine(pepper=True)
    engine.load(str(hdb))
    return engine


def _dates_in(df: pl.DataFrame) -> list[date]:
    if df.height == 0:
        return []
    if "date" in df.columns:
        return sorted({d for d in df["date"].to_list()})
    if "timestamp" in df.columns:
        return sorted({ts.date() for ts in df["timestamp"].to_list()})
    return []


def test_wide_range_returns_everything(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval(
        "select from ohlcv_1d where date>=1900.01.01, date<=2099.12.31"
    )
    assert df.height == len(DATES) * len(SYMBOLS) == 15
    assert _dates_in(df) == DATES


@pytest.mark.parametrize("target", DATES)
def test_equality_returns_exact_partition(
    loaded_engine: chili.Engine, target: date
) -> None:
    """R1 sub-bug 1: `where date=X` used to return 0 rows for most X."""
    q = f"select from ohlcv_1d where date={target.strftime('%Y.%m.%d')}"
    df = loaded_engine.eval(q)
    assert df.height == len(SYMBOLS) == 3, f"expected 3 rows for {target}, got {df.height}"
    assert _dates_in(df) == [target]


def test_equality_missing_partition_returns_empty(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval("select from ohlcv_1d where date=2024.01.06")
    assert df.height == 0


def test_narrow_two_sided_range(loaded_engine: chili.Engine) -> None:
    """R1 sub-bug 2: `where date>=X, date<=Y` used to drop the middle date."""
    df = loaded_engine.eval(
        "select from ohlcv_1d where date>=2024.01.02, date<=2024.01.04"
    )
    assert df.height == 9
    assert _dates_in(df) == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


def test_lower_bound_only(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval("select from ohlcv_1d where date>=2024.01.05")
    assert df.height == 6
    assert _dates_in(df) == [date(2024, 1, 5), date(2024, 1, 8)]


def test_upper_bound_only(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval("select from ohlcv_1d where date<=2024.01.04")
    assert df.height == 9
    assert _dates_in(df) == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


def test_strict_bounds(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval(
        "select from ohlcv_1d where date>2024.01.03, date<2024.01.05"
    )
    assert df.height == 3
    assert _dates_in(df) == [date(2024, 1, 4)]


def test_within_operator(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval(
        "select from ohlcv_1d where date within 2024.01.03 2024.01.05"
    )
    assert df.height == 9
    assert _dates_in(df) == [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]


def test_partition_then_symbol_filter(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval(
        "select from ohlcv_1d where date=2024.01.03, symbol=`AAPL"
    )
    assert df.height == 1
    assert df["symbol"][0] == "AAPL"


def test_symbol_filter_before_partition_clause(loaded_engine: chili.Engine) -> None:
    """R1 sub-bug (not in original report): partition extractor used to
    silently skip where_exp[0] even when it wasn't a partition predicate."""
    df = loaded_engine.eval(
        "select from ohlcv_1d where symbol=`AAPL, date=2024.01.03"
    )
    assert df.height == 1
    assert df["symbol"][0] == "AAPL"
    assert _dates_in(df) == [date(2024, 1, 3)]


def test_symbol_filter_with_range(loaded_engine: chili.Engine) -> None:
    df = loaded_engine.eval(
        "select from ohlcv_1d where date>=2024.01.02, date<=2024.01.04, symbol=`MSFT"
    )
    assert df.height == 3
    assert df["symbol"].to_list() == ["MSFT", "MSFT", "MSFT"]
    assert _dates_in(df) == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


def test_bare_select_requires_partition_condition(loaded_engine: chili.Engine) -> None:
    """Intentional behavior: partitioned tables require a date predicate."""
    with pytest.raises(RuntimeError, match="ByDate"):
        loaded_engine.eval("select from ohlcv_1d")
