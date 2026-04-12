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
        self._hdb_path: Optional[str] = None

    def load(self, path: str) -> None:
        """Load a partitioned HDB directory into the engine."""
        self._inner.load(path)
        self._hdb_path = path

    # -----------------------------------------------------------------------
    # Phase 12 — Engine lifecycle API (WL 3.1)
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the Rust engine state immediately.

        After calling ``close()``, any subsequent call to ``eval()``,
        ``wpar()``, ``load()``, or ``reload()`` will raise ``AttributeError``.
        Use this instead of relying on Python's garbage collector to reclaim
        the Rust state — especially in migration scripts and tests where
        deterministic cleanup matters.
        """
        self._inner = None  # type: ignore[assignment]
        self._hdb_path = None

    def unload(self) -> None:
        """Drop all loaded partitions but keep the engine alive.

        Subsequent queries on partitioned tables will error with "table not
        found" until ``load()`` or ``reload()`` is called again. The HDB
        path is preserved so ``reload()`` still works after ``unload()``.
        Non-partitioned variables, registered functions, and IPC connections
        are unaffected.
        """
        self._inner.unload()

    def reload(self) -> None:
        """Re-scan the most recently loaded HDB directory for new partitions.

        Equivalent to ``engine.unload(); engine.load(original_path)`` but
        preserves the engine's other state (variables, functions, connections).

        Raises ``RuntimeError`` if no HDB directory has been loaded yet.
        """
        if self._hdb_path is None:
            raise RuntimeError(
                "No HDB directory has been loaded yet. Call engine.load(path) first."
            )
        self._inner.unload()
        self._inner.load(self._hdb_path)

    def is_loaded(self) -> bool:
        """Return True if at least one partitioned table is loaded."""
        return self._inner.table_count() > 0

    def table_count(self) -> int:
        """Return the number of loaded partitioned tables."""
        return self._inner.table_count()

    # -----------------------------------------------------------------------
    # Phase 14 — Observability primitives (WL 3.2)
    # -----------------------------------------------------------------------

    def stats(self) -> dict:
        """Return engine-internal metrics as a Python dict.

        Keys:
          - ``partitions_loaded``: number of loaded partitioned tables
          - ``parse_cache_len``: entries in the LRU parse cache
          - ``hdb_path``: last-loaded HDB path (or None)

        Useful for Prometheus metric export or health checks.
        """
        return {
            "partitions_loaded": self._inner.table_count(),
            "parse_cache_len": self._inner.parse_cache_len(),
            "hdb_path": self._hdb_path,
        }

    # -----------------------------------------------------------------------
    # Phase 15 — Quantized column helper (WL 3.4)
    # -----------------------------------------------------------------------

    _column_scales: dict[str, dict[str, int]] = {}

    def set_column_scale(self, table: str, column: str, factor: int) -> None:
        """Register a dequantization scale factor for a column.

        After calling ``set_column_scale("ohlcv_1d", "close", 1_000_000)``,
        any ``engine.eval()`` result from ``ohlcv_1d`` that contains a
        ``close`` column of type ``Int64`` will be automatically cast to
        ``Float64`` and divided by ``factor`` before returning the DataFrame.

        This is the mdata "Option B" approach: set the scale once at engine
        construction time and forget about it. No query-string changes needed.

        Parameters
        ----------
        table : str
            Table name (e.g. ``"ohlcv_1d"``).
        column : str
            Column name (e.g. ``"close"``).
        factor : int
            Scale factor (e.g. ``1_000_000``). The stored Int64 value is
            divided by this factor to produce Float64.
        """
        self._column_scales.setdefault(table, {})[column] = factor

    def clear_column_scales(self) -> None:
        """Remove all registered column scale factors."""
        self._column_scales.clear()

    def query_plan(self, query: str) -> str:
        """Return the polars query plan for a pepper query WITHOUT executing it.

        Equivalent to DuckDB's ``EXPLAIN``. Shows the optimized plan that
        polars' lazy engine would execute, including predicate pushdown,
        projection pushdown, and partition pruning.

        Parameters
        ----------
        query : str
            A pepper query string.

        Returns
        -------
        str
            The human-readable optimized plan.
        """
        return self._inner.query_plan(query)

    def eval(self, query: str) -> pl.DataFrame:
        """
        Evaluate a Chili/pepper query and return the result as a polars DataFrame.

        If ``set_column_scale()`` has been called for any columns in the
        result table, those columns are automatically dequantized from
        Int64 to Float64 before returning (Phase 15 / WL 3.4).

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
        df = pl.from_arrow(pa.ipc.open_stream(io.BytesIO(ipc_bytes)).read_all())
        return self._apply_column_scales(df, query)

    def _apply_column_scales(self, df: pl.DataFrame, query: str) -> pl.DataFrame:
        """Phase 15 — auto-dequantize columns registered via set_column_scale.

        For each (table, column, factor) that matches a column in `df`,
        cast the column from Int64 to Float64 and divide by factor.
        Only applies if the column dtype is Int64 (Float64 columns are
        left untouched, which handles the case where the HDB hasn't been
        quantized yet).
        """
        if not self._column_scales:
            return df
        # Try to detect the table name from the query (best-effort: look
        # for "from <table>" pattern). If the query doesn't match any
        # registered table, no scaling is applied.
        for table, scales in self._column_scales.items():
            if f"from {table}" not in query:
                continue
            cast_exprs = []
            for col_name, factor in scales.items():
                if col_name in df.columns and df[col_name].dtype == pl.Int64:
                    cast_exprs.append(
                        pl.col(col_name).cast(pl.Float64) / factor  # pyright: ignore[reportUnknownMemberType]
                    )
            if cast_exprs:
                df = df.with_columns(cast_exprs)  # pyright: ignore[reportUnknownMemberType]
            break  # Only match the first table
        return df

    def wpar(
        self,
        df: pl.DataFrame,
        hdb_path: str,
        table: str,
        date: str,
        sort_columns: Optional[list[str]] = None,
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
        sort_columns : list[str], optional
            Column names to sort the partition by before writing. When set,
            chili also forces a small parquet ``row_group_size`` (16384) so
            polars can later prune row groups via column min/max statistics.

            Required for ``where symbol=X`` queries to skip parquet row
            groups (Phase 10 / mdata wishlist 2.2). Without this option,
            chili writes a single row group per partition and ``where``
            predicates are applied at the row level after the full partition
            is read into memory.

            Recommended setting for symbol-filtered analytical workloads:
            ``sort_columns=["symbol"]``.

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
        return self._inner.wpar(
            ipc_bytes, hdb_path, table, date, sort_columns or []
        )
