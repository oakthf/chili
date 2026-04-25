"""Regression tests for the direct-PyDataFrame boundary (post-bytes-removal).

These tests pin the contract introduced when chili-py 0.7.5 dropped the
Arrow IPC round-trip in eval/wpar/overwrite_partition/tick_upd. A regression
to the old bytes path would change return / argument types and these tests
would fail.

Background: the original chili-py (Phase-2 era) returned Arrow IPC bytes
from `eval()` and accepted IPC bytes for writes. Upstream commit bf9fa14
showed the canonical pattern via `pyo3_polars::PyDataFrame`. Adopting it
saved ~5 ms encode + ~5 ms decode on a 3 MB result and removed two full
buffer copies. See `feedback_chili_py_optimization_lessons.md`.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

import chili


# ---------------------------------------------------------------------------
# eval()
# ---------------------------------------------------------------------------


def test_eval_returns_polars_dataframe_directly():
    """eval() must return a polars.DataFrame (not bytes, not pyarrow.Table)."""
    e = chili.Engine()
    out = e.eval("([] x:1 2 3; y:`a`b`c)")
    assert isinstance(out, pl.DataFrame), f"eval() returned {type(out)!r}, expected pl.DataFrame"
    assert out.shape == (3, 2)
    assert out.columns == ["x", "y"]


def test_eval_dataframe_dtype_preserved():
    """Arrow IPC bytes round-trip used to lose category dtype on small frames.
    Direct PyDataFrame must preserve the engine-side dtype exactly."""
    e = chili.Engine()
    out = e.eval("([] i:1 2 3i; j:1.5 2.5 3.5)")
    assert out.schema["i"] == pl.Int32
    assert out.schema["j"] == pl.Float64


def test_eval_does_not_return_bytes():
    """Hard guard against accidental return of the old IPC path."""
    e = chili.Engine()
    out = e.eval("([] x:1 2 3)")
    assert not isinstance(out, bytes), "regression: eval() returned bytes (old IPC path)"
    assert not isinstance(out, bytearray)


# ---------------------------------------------------------------------------
# wpar() / overwrite_partition() — accept DataFrame, not bytes
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_hdb():
    tmpdir = tempfile.mkdtemp(prefix="chili-direct-")
    yield Path(tmpdir)
    shutil.rmtree(tmpdir, ignore_errors=True)


def test_wpar_accepts_polars_dataframe_no_ipc_serialization(tmp_hdb):
    """wpar() must accept a polars.DataFrame directly."""
    e = chili.Engine()
    df = pl.DataFrame({"symbol": ["AAPL", "MSFT"], "close": [150.0, 300.0]})
    n = e.wpar(df, str(tmp_hdb), "tbl", "2024.01.02")
    assert n > 0
    # Round-trip: load and read back
    e.load(str(tmp_hdb))
    out = e.eval("select from tbl where date=2024.01.02")
    assert isinstance(out, pl.DataFrame)
    assert out["symbol"].to_list() == ["AAPL", "MSFT"]


def test_wpar_rejects_bytes_input(tmp_hdb):
    """The new contract takes DataFrames, not IPC bytes. Passing bytes must fail."""
    e = chili.Engine()
    df = pl.DataFrame({"x": [1, 2]})
    import io
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    with pytest.raises((TypeError, AttributeError, RuntimeError)):
        e.wpar(buf.getvalue(), str(tmp_hdb), "tbl", "2024.01.02")


def test_overwrite_partition_accepts_polars_dataframe(tmp_hdb):
    e = chili.Engine()
    df1 = pl.DataFrame({"x": [1, 2, 3]})
    df2 = pl.DataFrame({"x": [10, 20]})
    e.wpar(df1, str(tmp_hdb), "tbl", "2024.01.02")
    e.overwrite_partition(df2, str(tmp_hdb), "tbl", "2024.01.02")
    e.load(str(tmp_hdb))
    out = e.eval("select from tbl where date=2024.01.02")
    assert sorted(out["x"].to_list()) == [10, 20]


# ---------------------------------------------------------------------------
# tick_upd() — Rust-side serialization (no df.write_ipc_stream call in Python)
# ---------------------------------------------------------------------------


def test_tick_upd_publishes_dataframe_directly():
    """tick_upd() takes a DataFrame and serializes inside Rust with GIL released.
    Subscriber still receives IPC bytes (the wire format is unchanged)."""
    import threading

    e = chili.Engine()
    received: list[tuple[str, int, bytes]] = []
    done = threading.Event()

    def cb(topic, seq, ipc_bytes):
        received.append((topic, seq, ipc_bytes))
        if len(received) >= 1:
            done.set()

    e.subscribe(["t1"], cb)
    df = pl.DataFrame({"x": [1, 2, 3]})
    seq = e.tick_upd("t1", df)
    assert seq == 1
    assert done.wait(timeout=2.0), "subscriber did not receive frame"
    topic, recv_seq, ipc_bytes = received[0]
    assert topic == "t1"
    assert recv_seq == 1
    assert isinstance(ipc_bytes, bytes)  # subscriber API contract unchanged
    # Subscriber can decode the IPC bytes back to a DataFrame
    import io
    import pyarrow as pa
    table = pa.ipc.open_stream(io.BytesIO(ipc_bytes)).read_all()
    decoded = pl.from_arrow(table)
    assert decoded["x"].to_list() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Structured exception module surface — pyo3 0.27 migration check
# ---------------------------------------------------------------------------


def test_exception_classes_exported():
    """All Phase-13 structured exception classes must still be importable
    after the pyo3 0.22 → 0.27 upgrade."""
    assert issubclass(chili.ChiliError, RuntimeError)
    assert issubclass(chili.PepperParseError, chili.ChiliError)
    assert issubclass(chili.PepperEvalError, chili.ChiliError)
    assert issubclass(chili.PartitionError, chili.ChiliError)
    assert issubclass(chili.TypeMismatchError, chili.ChiliError)
    assert issubclass(chili.NameError, chili.ChiliError)
    assert issubclass(chili.SerializationError, chili.ChiliError)


def test_partition_error_raised_for_missing_date_filter(tmp_hdb):
    e = chili.Engine()
    e.wpar(pl.DataFrame({"x": [1]}), str(tmp_hdb), "tbl", "2024.01.02")
    e.load(str(tmp_hdb))
    with pytest.raises(chili.PartitionError):
        e.eval("select from tbl")  # no date predicate
