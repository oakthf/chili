"""
Chili Python bindings — high-performance kdb+/q-compatible analytical engine.

The Rust extension (compiled via maturin) is imported as the `chili` native
module placed alongside this package by maturin's `python-source = "python"`
layout.  This wrapper handles the Arrow IPC bridge: DataFrames cross the
Rust/Python boundary as Arrow IPC bytes and are deserialized into polars
DataFrames here in Python.
"""
from __future__ import annotations

import io
from typing import Optional

import pyarrow as pa
import polars as pl

from .chili import Engine as _Engine  # the PyO3 Rust class


class Engine:
    """
    Chili query engine — wraps the Rust EngineState.

    Parameters
    ----------
    debug : bool
        Enable debug logging (default False).
    lazy : bool
        Enable lazy evaluation mode (default False).
    pepper : bool
        Use pepper/kdb+ q-like syntax (default True).
        Set to False for Chili native syntax.
    """

    def __init__(self, debug: bool = False, lazy: bool = False, pepper: bool = True) -> None:
        self._inner = _Engine(debug=debug, lazy=lazy, pepper=pepper)

    def load(self, path: str) -> None:
        """Load a partitioned HDB directory into the engine."""
        self._inner.load(path)

    def eval(self, query: str) -> pl.DataFrame:
        """
        Evaluate a Chili/pepper query and return the result as a polars DataFrame.

        Parameters
        ----------
        query : str
            A Chili or pepper (kdb+/q) query string, e.g.
            ``"select from ohlcv_1d where date=2024.01.02d"``

        Returns
        -------
        polars.DataFrame
        """
        ipc_bytes: bytes = self._inner.eval(query)
        return pl.from_arrow(pa.ipc.open_stream(io.BytesIO(ipc_bytes)).read_all())

    def wpar(
        self,
        df: pl.DataFrame,
        hdb_path: str,
        table: str,
        date: str,
    ) -> int:
        """
        Write a DataFrame as a partition to an HDB directory.

        Parameters
        ----------
        df : polars.DataFrame
            Data to write.
        hdb_path : str
            Root HDB directory path (must already exist).
        table : str
            Table name (subdirectory under hdb_path).
        date : str
            Partition date in ``YYYY.MM.DD`` format, e.g. ``"2024.01.02"``.

        Returns
        -------
        int
            Bytes written.
        """
        buf = io.BytesIO()
        # write_ipc_stream produces Arrow IPC stream format, which IpcStreamReader
        # on the Rust side expects. Do NOT use write_ipc() — that writes the IPC
        # file format (with footer) which IpcStreamReader cannot parse.
        df.write_ipc_stream(buf)
        ipc_bytes = buf.getvalue()
        return self._inner.wpar(ipc_bytes, hdb_path, table, date)
