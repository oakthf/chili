"""Regression tests for the full set of chili pepper patterns mdata relies on.

These tests pin the partition-pruning semantics that the chili author fixed
upstream on 2026-04-11 (Risk R1 root cause: unsorted ``par_vec`` →
``binary_search`` undefined behaviour; secondary fixes around multi-clause
predicates and tight range combining).

The test matrix mirrors the "Chili semantics the new code can rely on" table
in ``project_chili_r1_unblocked.md``. If any pattern ever regresses upstream,
these tests will surface it before the benchmark does — which is our main
insurance policy now that the gateway pushes narrow date ranges directly
through to the Rust engine.

Each test is a self-contained arrange/act/assert and uses a tiny temp HDB
(3 symbols × 5 days, written via ``chili.Engine.wpar``). None touch
``data/hdb``. The whole file is skipped when chili is not importable.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl
import pytest

from mdata.server.chili_gateway import ChiliGateway, _ensure_chili_importable

if TYPE_CHECKING:
    from pathlib import Path

try:
    _ensure_chili_importable()
except ImportError as _exc:  # pragma: no cover - visible to pytest
    pytest.skip(
        f"chili package not available: {_exc}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Fixtures (duplicated from test_chili_gateway.py to keep this file
# self-contained — the pepper-patterns matrix is a long-lived regression
# surface and we do not want it coupled to helper changes in the other file).
# ---------------------------------------------------------------------------


OhlcvFactory = Callable[[int, int], pl.DataFrame]

_BASE_DATE = date(2024, 1, 2)
_N_SYMBOLS = 3
_N_DAYS = 5  # yields 3 × 5 = 15 rows across 5 partitions
_ALL_SYMBOLS = ["AAPL", "MSFT", "SPY"]


def _make_frame(n_symbols: int, n_days: int) -> pl.DataFrame:
    """Return a deterministic OHLCV frame with one row per (symbol, day)."""
    symbols = _ALL_SYMBOLS[:n_symbols]
    start = datetime(
        _BASE_DATE.year, _BASE_DATE.month, _BASE_DATE.day, 14, 30, 0, tzinfo=UTC
    )
    rows: list[dict[str, object]] = []
    row_idx = 0
    for day_offset in range(n_days):
        ts = start + timedelta(days=day_offset)
        for sym in symbols:
            row_idx += 1
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": sym,
                    "open": 100.0 + row_idx,
                    "high": 101.0 + row_idx,
                    "low": 99.0 + row_idx,
                    "close": 100.5 + row_idx,
                    "volume": 1_000_000.0 + row_idx * 1000.0,
                    "vwap": 100.25 + row_idx * 0.1,
                    "trades": 500 + row_idx,
                }
            )
    return pl.DataFrame(rows).with_columns(
        pl.col("timestamp").cast(pl.Datetime("ns", "UTC")),  # pyright: ignore[reportUnknownMemberType]
        pl.col("trades").cast(pl.UInt32),  # pyright: ignore[reportUnknownMemberType]
    )


def _expected_days() -> list[date]:
    """Return the calendar dates written by ``populated_hdb``."""
    return [_BASE_DATE + timedelta(days=i) for i in range(_N_DAYS)]


@pytest.fixture
def populated_hdb(tmp_path: Path) -> Path:
    """Build a 3-symbol × 5-day temp HDB via ``ChiliGateway.write_partition``.

    The fixture does NOT return the source frame because these tests only
    assert on row counts, partition membership, and symbol membership —
    exact value round-trips are covered in ``test_chili_gateway.py``.
    """
    hdb = tmp_path / "hdb"
    hdb.mkdir(parents=True, exist_ok=True)
    source = _make_frame(_N_SYMBOLS, _N_DAYS)
    day_col: list[date] = [
        ts.date()
        for ts in source["timestamp"].to_list()  # pyright: ignore[reportUnknownMemberType]
    ]
    source_with_day = source.with_columns(  # pyright: ignore[reportUnknownMemberType]
        pl.Series("_day", day_col)
    )
    gw = ChiliGateway(hdb)
    for day in sorted(set(day_col)):
        part = source_with_day.filter(  # pyright: ignore[reportUnknownMemberType]
            pl.col("_day") == day
        ).drop("_day")
        gw.write_partition(part, "ohlcv_1d", day)
    gw.close()
    return hdb


def _query(hdb: Path, pepper: str) -> pl.DataFrame:
    """Issue a pepper query against the temp HDB via a fresh gateway.

    Each test uses a fresh gateway so state from a previous test cannot
    pollute the next. The gateway's internal lazy-load caches the
    ``chili.Engine`` per instance, so constructing a new one per call is
    a few ms of overhead — acceptable for a regression suite.
    """
    gw = ChiliGateway(hdb)
    try:
        return gw.query(pepper)
    finally:
        gw.close()


def _unique_dates(df: pl.DataFrame) -> list[date]:
    """Extract the sorted set of distinct dates from the ``date`` column.

    Chili surfaces the partition key as a ``date`` column when reading
    partitioned tables — all the pepper patterns below should return
    a frame that carries it.
    """
    if df.height == 0:
        return []
    return sorted(set(df["date"].to_list()))


# ---------------------------------------------------------------------------
# Pattern 1: exact-date partition
# ---------------------------------------------------------------------------


def test_pattern_date_equals_single_partition(populated_hdb: Path) -> None:
    """``where date=YYYY.MM.DD`` returns rows from exactly one partition."""
    # Arrange
    target = date(2024, 1, 3)
    # Act
    df = _query(populated_hdb, f"select from ohlcv_1d where date={target:%Y.%m.%d}")
    # Assert
    assert df.height == _N_SYMBOLS, (
        f"expected {_N_SYMBOLS} rows for single partition, got {df.height}"
    )
    assert _unique_dates(df) == [target]
    assert sorted(set(df["symbol"].to_list())) == _ALL_SYMBOLS


def test_pattern_date_equals_first_partition(populated_hdb: Path) -> None:
    """Edge case: querying the FIRST loaded partition (2024-01-02).

    The original R1 bug manifested as "always returns the first
    partition" — querying the LAST partition was the clearest way to
    see it. This test pins the symmetric case: asking for the first
    partition must ALSO return exactly that partition.
    """
    # Arrange
    target = _BASE_DATE
    # Act
    df = _query(populated_hdb, f"select from ohlcv_1d where date={target:%Y.%m.%d}")
    # Assert
    assert df.height == _N_SYMBOLS
    assert _unique_dates(df) == [target]


def test_pattern_date_equals_last_partition(populated_hdb: Path) -> None:
    """Edge case: the LAST loaded partition — the R1 bug's failure mode."""
    # Arrange
    target = _BASE_DATE + timedelta(days=_N_DAYS - 1)
    # Act
    df = _query(populated_hdb, f"select from ohlcv_1d where date={target:%Y.%m.%d}")
    # Assert
    assert df.height == _N_SYMBOLS
    assert _unique_dates(df) == [target]


# ---------------------------------------------------------------------------
# Pattern 2: tight inclusive range (both bounds prune)
# ---------------------------------------------------------------------------


def test_pattern_date_tight_range_three_partitions(populated_hdb: Path) -> None:
    """``where date>=X, date<=Y`` returns every partition in [X, Y] inclusive.

    R1's secondary bug dropped the middle partition of a 3-date range.
    This test pins the fix by asserting all three intermediate dates
    appear in the result.
    """
    # Arrange
    start = _BASE_DATE + timedelta(days=1)  # 2024-01-03
    end = _BASE_DATE + timedelta(days=3)  # 2024-01-05
    # Act
    df = _query(
        populated_hdb,
        (f"select from ohlcv_1d where date>={start:%Y.%m.%d}, date<={end:%Y.%m.%d}"),
    )
    # Assert — 3 dates × 3 symbols = 9 rows
    assert df.height == _N_SYMBOLS * 3
    assert _unique_dates(df) == [
        start,
        start + timedelta(days=1),
        end,
    ]


def test_pattern_date_range_single_day_equivalent(populated_hdb: Path) -> None:
    """``where date>=X, date<=X`` is equivalent to ``where date=X``."""
    # Arrange
    target = date(2024, 1, 4)
    # Act
    df = _query(
        populated_hdb,
        (
            f"select from ohlcv_1d "
            f"where date>={target:%Y.%m.%d}, date<={target:%Y.%m.%d}"
        ),
    )
    # Assert
    assert df.height == _N_SYMBOLS
    assert _unique_dates(df) == [target]


# ---------------------------------------------------------------------------
# Pattern 3: half-open ranges (only one bound)
# ---------------------------------------------------------------------------


def test_pattern_date_half_open_lower(populated_hdb: Path) -> None:
    """``where date>=X`` returns all partitions from X forward."""
    # Arrange — 2024-01-04 onwards → 01-04, 01-05, 01-06 = 3 days
    start = _BASE_DATE + timedelta(days=2)
    expected_days = _expected_days()[2:]
    # Act
    df = _query(
        populated_hdb,
        f"select from ohlcv_1d where date>={start:%Y.%m.%d}",
    )
    # Assert
    assert df.height == _N_SYMBOLS * len(expected_days)
    assert _unique_dates(df) == expected_days


def test_pattern_date_half_open_upper(populated_hdb: Path) -> None:
    """``where date<=Y`` returns all partitions up to and including Y."""
    # Arrange — ...<=2024-01-04 → 01-02, 01-03, 01-04 = 3 days
    end = _BASE_DATE + timedelta(days=2)
    expected_days = _expected_days()[:3]
    # Act
    df = _query(
        populated_hdb,
        f"select from ohlcv_1d where date<={end:%Y.%m.%d}",
    )
    # Assert
    assert df.height == _N_SYMBOLS * len(expected_days)
    assert _unique_dates(df) == expected_days


# ---------------------------------------------------------------------------
# Pattern 4: strict bounds (open interval)
# ---------------------------------------------------------------------------


def test_pattern_date_strict_bounds_exclusive(populated_hdb: Path) -> None:
    """``where date>X, date<Y`` excludes the endpoints.

    Strict bounds are the kdb+/pepper way to ask "everything between
    two dates, not including them." Verifying this shape here protects
    any future code that wants to use it.
    """
    # Arrange — >01-02, <01-06 → 01-03, 01-04, 01-05 = 3 days
    lower = _BASE_DATE
    upper = _BASE_DATE + timedelta(days=_N_DAYS - 1)
    expected_days = _expected_days()[1:-1]
    # Act
    df = _query(
        populated_hdb,
        (f"select from ohlcv_1d where date>{lower:%Y.%m.%d}, date<{upper:%Y.%m.%d}"),
    )
    # Assert
    assert df.height == _N_SYMBOLS * len(expected_days)
    assert _unique_dates(df) == expected_days


# ---------------------------------------------------------------------------
# Pattern 5: non-partition clause before/after the partition predicate
# ---------------------------------------------------------------------------


def test_pattern_symbol_before_date(populated_hdb: Path) -> None:
    """``where symbol=`X, date=Y`` — symbol clause comes FIRST.

    Chili's original R1 secondary bug silently dropped ``where_exp[0]``
    whenever it was any binary expression, causing the query planner
    to miss the partition predicate entirely. The fix allows either
    ordering. This test pins the "non-partition first" ordering.
    """
    # Arrange
    target = date(2024, 1, 4)
    # Act
    df = _query(
        populated_hdb,
        (f"select from ohlcv_1d where symbol=`AAPL, date={target:%Y.%m.%d}"),
    )
    # Assert
    assert df.height == 1
    assert _unique_dates(df) == [target]
    assert df["symbol"].to_list() == ["AAPL"]


def test_pattern_date_before_symbol(populated_hdb: Path) -> None:
    """``where date=Y, symbol=`X`` — partition clause comes FIRST.

    The other ordering. Both must work.
    """
    # Arrange
    target = date(2024, 1, 4)
    # Act
    df = _query(
        populated_hdb,
        (f"select from ohlcv_1d where date={target:%Y.%m.%d}, symbol=`MSFT"),
    )
    # Assert
    assert df.height == 1
    assert _unique_dates(df) == [target]
    assert df["symbol"].to_list() == ["MSFT"]


# ---------------------------------------------------------------------------
# Pattern 6: symbol list (``in``) with narrow date
# ---------------------------------------------------------------------------


def test_pattern_symbol_in_list_narrow_date(populated_hdb: Path) -> None:
    """``where date=Y, symbol in `X`Y`` combines both predicate shapes."""
    # Arrange
    target = date(2024, 1, 3)
    # Act
    df = _query(
        populated_hdb,
        (f"select from ohlcv_1d where date={target:%Y.%m.%d}, symbol in `AAPL`SPY"),
    )
    # Assert
    assert df.height == 2
    assert _unique_dates(df) == [target]
    assert sorted(set(df["symbol"].to_list())) == ["AAPL", "SPY"]


def test_pattern_symbol_in_list_date_range(populated_hdb: Path) -> None:
    """``where date>=X, date<=Y, symbol in `A`B`` — range + list."""
    # Arrange
    start = _BASE_DATE + timedelta(days=1)
    end = _BASE_DATE + timedelta(days=2)
    # Act
    df = _query(
        populated_hdb,
        (
            f"select from ohlcv_1d "
            f"where date>={start:%Y.%m.%d}, date<={end:%Y.%m.%d}, "
            f"symbol in `AAPL`MSFT"
        ),
    )
    # Assert — 2 dates × 2 symbols = 4 rows
    assert df.height == 4
    assert _unique_dates(df) == [start, end]
    assert sorted(set(df["symbol"].to_list())) == ["AAPL", "MSFT"]


# ---------------------------------------------------------------------------
# Pattern 7: narrow range returns empty when no partitions match
# ---------------------------------------------------------------------------


def test_pattern_narrow_range_with_no_matching_partition(
    populated_hdb: Path,
) -> None:
    """Querying a date range that falls entirely outside loaded partitions
    must return zero rows (not error, not scan everything).
    """
    # Arrange — range is before the earliest loaded partition
    start = date(2023, 6, 1)
    end = date(2023, 6, 5)
    # Act
    df = _query(
        populated_hdb,
        (f"select from ohlcv_1d where date>={start:%Y.%m.%d}, date<={end:%Y.%m.%d}"),
    )
    # Assert
    assert df.height == 0


def test_pattern_exact_date_with_no_matching_partition(
    populated_hdb: Path,
) -> None:
    """``where date=Y`` where Y is not loaded must return zero rows."""
    # Arrange
    target = date(2025, 12, 31)
    # Act
    df = _query(populated_hdb, f"select from ohlcv_1d where date={target:%Y.%m.%d}")
    # Assert
    assert df.height == 0


# ---------------------------------------------------------------------------
# Pattern 8: gateway.ohlcv() end-to-end parity
# ---------------------------------------------------------------------------


def test_gateway_ohlcv_single_date_single_symbol(populated_hdb: Path) -> None:
    """``gateway.ohlcv`` with ``start == end`` uses the single-partition path."""
    from mdata.types import Timeframe

    # Arrange
    target = date(2024, 1, 4)
    gw = ChiliGateway(populated_hdb)
    try:
        # Act
        df = gw.ohlcv(
            Timeframe.DAY_1,
            symbols=["AAPL"],
            start=target,
            end=target,
        )
    finally:
        gw.close()
    # Assert
    assert df.height == 1
    assert df["symbol"].to_list() == ["AAPL"]
    assert _unique_dates(df) == [target]


def test_gateway_ohlcv_date_range_multiple_symbols(populated_hdb: Path) -> None:
    """``gateway.ohlcv`` with a range uses the ``date>=X, date<=Y`` path."""
    from mdata.types import Timeframe

    # Arrange
    start = _BASE_DATE + timedelta(days=1)
    end = _BASE_DATE + timedelta(days=3)
    gw = ChiliGateway(populated_hdb)
    try:
        # Act
        df = gw.ohlcv(
            Timeframe.DAY_1,
            symbols=["AAPL", "MSFT"],
            start=start,
            end=end,
        )
    finally:
        gw.close()
    # Assert — 3 dates × 2 symbols = 6 rows
    assert df.height == 6
    assert sorted(set(df["symbol"].to_list())) == ["AAPL", "MSFT"]
    assert _unique_dates(df) == [
        start,
        start + timedelta(days=1),
        end,
    ]


def test_gateway_ohlcv_only_start_defaults_end(populated_hdb: Path) -> None:
    """``start`` only → ``end`` defaults to ``start``, single-day path."""
    from mdata.types import Timeframe

    # Arrange
    target = date(2024, 1, 4)
    gw = ChiliGateway(populated_hdb)
    try:
        # Act
        df = gw.ohlcv(Timeframe.DAY_1, start=target)
    finally:
        gw.close()
    # Assert
    assert df.height == _N_SYMBOLS
    assert _unique_dates(df) == [target]
