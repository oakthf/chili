"""Tests for :class:`chili.engine.ChiliEngine`."""

import pytest
import polars as pl

from chili import ChiliEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """Create a fresh engine for each test and shut it down afterwards."""
    e = ChiliEngine()
    yield e
    e.shutdown()


@pytest.fixture()
def lazy_engine():
    """Engine with lazy evaluation enabled."""
    e = ChiliEngine(lazy=True)
    yield e
    e.shutdown()


@pytest.fixture()
def pepper_engine():
    """Engine using Pepper syntax."""
    e = ChiliEngine(pepper=True)
    yield e
    e.shutdown()


# ---------------------------------------------------------------------------
# Construction & mode flags
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_default_modes(self, engine: ChiliEngine):
        assert engine.is_lazy_mode() is False
        assert engine.is_repl_use_chili_syntax() is True

    def test_lazy_mode(self, lazy_engine: ChiliEngine):
        assert lazy_engine.is_lazy_mode() is True

    def test_pepper_mode(self, pepper_engine: ChiliEngine):
        assert pepper_engine.is_repl_use_chili_syntax() is False


# ---------------------------------------------------------------------------
# eval – basic type round-trips
# ---------------------------------------------------------------------------


class TestEval:
    def test_eval_int(self, engine: ChiliEngine):
        assert engine.eval("1 + 2") == 3

    def test_eval_float(self, engine: ChiliEngine):
        assert engine.eval("3.14") == pytest.approx(3.14)

    def test_eval_bool_true(self, engine: ChiliEngine):
        assert engine.eval("true") is True

    def test_eval_bool_false(self, engine: ChiliEngine):
        assert engine.eval("false") is False

    def test_eval_string(self, engine: ChiliEngine):
        """Chili strings map to Python bytes."""
        result = engine.eval('"hello"')
        assert result == b"hello"

    def test_eval_arithmetic(self, engine: ChiliEngine):
        assert engine.eval("10 * 5 - 8") == 42

    def test_eval_error_raises(self, engine: ChiliEngine):
        with pytest.raises(Exception):
            engine.eval("undefined_var_xyz")


# ---------------------------------------------------------------------------
# Variable management
# ---------------------------------------------------------------------------


class TestVariables:
    def test_set_get_int(self, engine: ChiliEngine):
        engine.set_var("x", 42)
        assert engine.get_var("x") == 42

    def test_set_get_float(self, engine: ChiliEngine):
        engine.set_var("pi", 3.14)
        assert engine.get_var("pi") == pytest.approx(3.14)

    def test_set_get_str(self, engine: ChiliEngine):
        engine.set_var("name", "chili")
        assert engine.get_var("name") == "chili"

    def test_set_get_bool(self, engine: ChiliEngine):
        engine.set_var("flag", True)
        assert engine.get_var("flag") is True

    def test_set_get_none(self, engine: ChiliEngine):
        engine.set_var("empty", None)
        assert engine.get_var("empty") is None

    def test_set_get_list(self, engine: ChiliEngine):
        engine.set_var("items", [1, 2, 3])
        result = engine.get_var("items")
        assert result == [1, 2, 3]

    def test_set_get_dict(self, engine: ChiliEngine):
        engine.set_var("d", {"a": 1, "b": 2})
        result = engine.get_var("d")
        assert result["a"] == 1
        assert result["b"] == 2

    def test_set_get_dataframe(self, engine: ChiliEngine):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        engine.set_var("df", df)
        result = engine.get_var("df")
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (2, 2)

    def test_has_var_true(self, engine: ChiliEngine):
        engine.set_var("exists", 1)
        assert engine.has_var("exists") is True

    def test_has_var_false(self, engine: ChiliEngine):
        assert engine.has_var("nope_not_here") is False

    def test_del_var(self, engine: ChiliEngine):
        engine.set_var("to_delete", 99)
        assert engine.has_var("to_delete") is True
        result = engine.del_var("to_delete")
        assert result == 99
        assert engine.has_var("to_delete") is False

    def test_get_var_missing_raises(self, engine: ChiliEngine):
        with pytest.raises(Exception):
            engine.get_var("no_such_var")

    def test_overwrite_var(self, engine: ChiliEngine):
        engine.set_var("v", 1)
        assert engine.get_var("v") == 1
        engine.set_var("v", 2)
        assert engine.get_var("v") == 2


# ---------------------------------------------------------------------------
# Source management
# ---------------------------------------------------------------------------


class TestSource:
    def test_set_and_get_source(self, engine: ChiliEngine):
        idx = engine.set_source("test.chi", "1 + 1")
        path, src = engine.get_source(idx)
        assert path == "test.chi"
        assert src == "1 + 1"

    def test_multiple_sources(self, engine: ChiliEngine):
        idx1 = engine.set_source("a.chi", "10")
        idx2 = engine.set_source("b.chi", "20")
        assert idx1 != idx2
        assert engine.get_source(idx1) == ("a.chi", "10")
        assert engine.get_source(idx2) == ("b.chi", "20")


# ---------------------------------------------------------------------------
# Import source path
# ---------------------------------------------------------------------------


class TestImportSource:
    def test_import_missing_file_raises(self, engine: ChiliEngine):
        with pytest.raises(Exception):
            engine.import_source_path("", "/tmp/nonexistent_chili_file.chi")


# ---------------------------------------------------------------------------
# Tick counter
# ---------------------------------------------------------------------------


class TestTick:
    def test_initial_tick(self, engine: ChiliEngine):
        assert engine.get_tick_count(0) == 0

    def test_tick_increment(self, engine: ChiliEngine):
        engine.tick(0, 5)
        assert engine.get_tick_count(0) == 5

    def test_tick_multiple(self, engine: ChiliEngine):
        engine.tick(0, 3)
        engine.tick(0, 7)
        assert engine.get_tick_count(0) == 10


# ---------------------------------------------------------------------------
# Parse cache
# ---------------------------------------------------------------------------


class TestParseCache:
    def test_initial_cache_empty(self, engine: ChiliEngine):
        assert engine.parse_cache_len() == 0

    def test_cache_grows_after_eval(self, engine: ChiliEngine):
        engine.eval("1 + 2")
        assert engine.parse_cache_len() >= 1


# ---------------------------------------------------------------------------
# Displayed variables & list_vars
# ---------------------------------------------------------------------------


class TestVarListing:
    def test_get_displayed_vars_type(self, engine: ChiliEngine):
        result = engine.get_displayed_vars()
        assert isinstance(result, dict)

    def test_displayed_vars_contains_user_var(self, engine: ChiliEngine):
        engine.set_var("mytest", 123)
        dv = engine.get_displayed_vars()
        assert "mytest" in dv

    def test_list_vars_returns_dataframe(self, engine: ChiliEngine):
        result = engine.list_vars("")
        assert isinstance(result, pl.DataFrame)

    def test_list_vars_has_expected_columns(self, engine: ChiliEngine):
        result = engine.list_vars("")
        expected_cols = {"name", "display", "type", "columns", "is_built_in"}
        assert expected_cols.issubset(set(result.columns))

    def test_list_vars_pattern_filter(self, engine: ChiliEngine):
        engine.set_var("abc_test", 1)
        engine.set_var("xyz_test", 2)
        result = engine.list_vars("abc")
        names = result["name"].to_list()
        assert "abc_test" in names
        assert "xyz_test" not in names


# ---------------------------------------------------------------------------
# fn_call
# ---------------------------------------------------------------------------


class TestFnCall:
    def test_fn_call_type(self, engine: ChiliEngine):
        """Call the built-in 'type' function to check a value's type name."""
        result = engine.fn_call("type", [42])
        assert isinstance(result, str)

    def test_fn_call_count(self, engine: ChiliEngine):
        """count([1,2,3]) should return 3."""
        result = engine.fn_call("count", [[1, 2, 3]])
        assert result == 3


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


class TestShutdown:
    def test_shutdown_is_idempotent(self, engine: ChiliEngine):
        """Calling shutdown multiple times should not raise."""
        engine.shutdown()
        engine.shutdown()


# ---------------------------------------------------------------------------
# stats & start_tcp_listener
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_returns_dict(self, engine: ChiliEngine):
        s = engine.stats()
        assert isinstance(s, dict)

    def test_stats_contains_expected_keys(self, engine: ChiliEngine):
        s = engine.stats()
        assert "lazy_mode" in s
        assert "repl_lang" in s
        assert "parse_cache_len" in s


class TestTcpListener:
    def test_start_tcp_listener_binds_port(self):
        import socket
        import time

        e = ChiliEngine()
        # Find an available port
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        e.start_tcp_listener(port)
        # Give the background thread a moment to bind
        time.sleep(0.2)
        # Verify something is listening on that port
        with socket.socket() as s:
            result = s.connect_ex(("127.0.0.1", port))
            assert result == 0, f"Nothing listening on port {port}"
        e.shutdown()


class TestLogBuiltins:
    """`.log.{info,warn,debug,error}` are registered via chili_op::LOG_FN."""

    def test_log_info_returns_null(self, engine: ChiliEngine):
        assert engine.eval('.log.info("hello")') is None

    def test_log_warn_returns_null(self, engine: ChiliEngine):
        assert engine.eval('.log.warn("warn-msg")') is None

    def test_log_debug_returns_null(self, engine: ChiliEngine):
        assert engine.eval('.log.debug("debug-msg")') is None

    def test_log_error_returns_null(self, engine: ChiliEngine):
        assert engine.eval('.log.error("error-msg")') is None


class TestTableCount:
    def test_table_count_zero_on_fresh_engine(self, engine: ChiliEngine):
        assert engine.table_count() == 0


class TestColumnScale:
    """Read-side dequantization helper (golden rule 4)."""

    def test_set_clear_round_trip(self, engine: ChiliEngine):
        engine.set_column_scale("ohlcv_1d", "close", 1_000_000)
        engine.set_column_scale("ohlcv_1d", "open", 1_000_000)
        assert engine._column_scales == {
            "ohlcv_1d": {"close": 1_000_000, "open": 1_000_000}
        }
        engine.clear_column_scales()
        assert engine._column_scales == {}

    def test_apply_dequantizes_int64_column(self, engine: ChiliEngine):
        engine.set_column_scale("ohlcv_1d", "close", 100)
        df = pl.DataFrame({"sym": ["A", "B"], "close": [12345, 67890]})
        out = engine._apply_column_scales(df, "select close from ohlcv_1d")
        assert out["close"].dtype == pl.Float64
        assert out["close"].to_list() == [123.45, 678.90]

    def test_apply_skips_non_referenced_table(self, engine: ChiliEngine):
        engine.set_column_scale("ohlcv_1d", "close", 100)
        df = pl.DataFrame({"sym": ["A"], "close": [12345]})
        out = engine._apply_column_scales(df, "select close from ohlcv_1m")
        assert out["close"].dtype == pl.Int64

    def test_apply_no_op_when_no_scales(self, engine: ChiliEngine):
        df = pl.DataFrame({"close": [12345]})
        out = engine._apply_column_scales(df, "select close from ohlcv_1d")
        assert out.equals(df)

    def test_apply_does_not_false_match_substring_table(self, engine: ChiliEngine):
        # Word-boundary regex must not match `from trades` against `from
        # all_trades`. Pre-fix code (`f"from {table}" not in query`) would
        # have rescaled the `all_trades` query against the `trades` scale.
        engine.set_column_scale("trades", "px", 100)
        df = pl.DataFrame({"px": [12345]})
        out = engine._apply_column_scales(df, "select px from all_trades")
        assert out["px"].dtype == pl.Int64
        assert out["px"].to_list() == [12345]

    def test_apply_dequantizes_two_tables_in_join(self, engine: ChiliEngine):
        # Both tables register a scale; result df has columns matching
        # both. Pre-fix code stopped after the first match (single-table
        # break) so only one of the two columns was rescaled.
        engine.set_column_scale("ohlcv_1d", "close", 100)
        engine.set_column_scale("trades", "px", 1000)
        df = pl.DataFrame({"close": [12345], "px": [678900]})
        out = engine._apply_column_scales(
            df, "select close, px from ohlcv_1d join trades on sym"
        )
        assert out["close"].dtype == pl.Float64
        assert out["px"].dtype == pl.Float64
        assert out["close"].to_list() == [123.45]
        assert out["px"].to_list() == [678.90]


@pytest.fixture()
def tmp_hdb(tmp_path):
    """Build a small HDB on disk: ohlcv_1d / 2024.01.01 with two rows."""
    hdb_dir = tmp_path / "hdb"
    hdb_dir.mkdir()
    df = pl.DataFrame(
        {
            "sym": ["AAPL", "MSFT"],
            "close": [19000, 38000],
        }
    )
    e = ChiliEngine()
    hdb = str(hdb_dir)
    e.write_partitioned_df(df, hdb, "ohlcv_1d", "2024.01.01")
    e.shutdown()
    return hdb


class TestOverwritePartition:
    def test_overwrite_returns_positive(self, engine: ChiliEngine, tmp_hdb):
        new_df = pl.DataFrame({"sym": ["AAPL"], "close": [20000]})
        result = engine.overwrite_partition(new_df, tmp_hdb, "ohlcv_1d", "2024.01.01")
        # wpar returns bytes-written or row-count depending on shard layout;
        # the contract here is "did not error" + "returned a non-zero int."
        assert isinstance(result, int)
        assert result > 0


class TestQueryPlan:
    def test_query_plan_returns_string(self, tmp_hdb):
        # query_plan internally creates a temp pepper-syntax engine; the
        # caller-visible engine just needs an HDB path passed.
        e = ChiliEngine(pepper=True)
        try:
            plan = e.query_plan(
                "select last close by sym from ohlcv_1d where date=2024.01.01", tmp_hdb
            )
            assert isinstance(plan, str)
            assert len(plan) > 0
        finally:
            e.shutdown()

    def test_query_plan_uses_cached_hdb_path(self, tmp_hdb):
        e = ChiliEngine(pepper=True)
        try:
            e.load_partitioned_df(tmp_hdb)
            plan = e.query_plan("select last close by sym from ohlcv_1d where date=2024.01.01")
            assert isinstance(plan, str)
            assert len(plan) > 0
        finally:
            e.shutdown()

    def test_query_plan_raises_without_hdb(self, engine: ChiliEngine):
        with pytest.raises(RuntimeError, match="No HDB path provided"):
            engine.query_plan("select * from ohlcv_1d")
