"""
Chili Python bindings — high-performance kdb+/q-compatible analytical engine.

The Rust extension (compiled via maturin) is imported as the `chili.chili`
native module placed alongside this package by maturin's
``python-source = "python"`` layout. As of 0.7.5 the engine returns polars
DataFrames directly through ``pyo3_polars::PyDataFrame`` — there is no
Arrow IPC round-trip on either eval results or partition writes.
"""
from __future__ import annotations

from typing import Optional

import polars as pl

from .chili import (  # the PyO3 Rust extension
    Engine as _Engine,
    ChiliError,
    PepperParseError,
    PepperEvalError,
    PartitionError,
    TypeMismatchError,
    NameError,
    SerializationError,
)

__all__ = [
    "Engine",
    "ChiliError",
    "PepperParseError",
    "PepperEvalError",
    "PartitionError",
    "TypeMismatchError",
    "NameError",
    "SerializationError",
]


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
        self._column_scales: dict[str, dict[str, int]] = {}

    def load(self, path: str) -> None:
        """Load a partitioned HDB directory into the engine."""
        self._inner.load(path)
        self._hdb_path = path

    # -----------------------------------------------------------------------
    # Phase 12 — Engine lifecycle API (WL 3.1)
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the Rust engine state immediately."""
        self._inner = None  # type: ignore[assignment]
        self._hdb_path = None

    def unload(self) -> None:
        """Drop all loaded partitions but keep the engine alive."""
        self._inner.unload()

    def reload(self) -> None:
        """Re-scan the most recently loaded HDB directory for new partitions."""
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
        """Return engine-internal metrics as a Python dict."""
        return {
            "partitions_loaded": self._inner.table_count(),
            "parse_cache_len": self._inner.parse_cache_len(),
            "hdb_path": self._hdb_path,
        }

    # -----------------------------------------------------------------------
    # Phase 15 — Quantized column helper (WL 3.4)
    # -----------------------------------------------------------------------

    def set_column_scale(self, table: str, column: str, factor: int) -> None:
        """Register a dequantization scale factor for a column.

        After ``set_column_scale("ohlcv_1d", "close", 1_000_000)``, any
        ``eval()`` result from ``ohlcv_1d`` containing an ``Int64`` ``close``
        column is auto-cast to ``Float64`` and divided by ``factor`` before
        being returned.
        """
        self._column_scales.setdefault(table, {})[column] = factor

    def clear_column_scales(self) -> None:
        """Remove all registered column scale factors."""
        self._column_scales.clear()

    def query_plan(self, query: str) -> str:
        """Return the polars query plan for a pepper query WITHOUT executing it."""
        if self._hdb_path is None:
            raise RuntimeError(
                "No HDB directory has been loaded yet. Call engine.load(path) first."
            )
        return self._inner.query_plan(query, self._hdb_path)

    def eval(self, query: str) -> pl.DataFrame:
        """Evaluate a Chili/pepper query.

        Returns the result directly as a ``polars.DataFrame`` via
        ``pyo3_polars::PyDataFrame`` — zero-copy, no Arrow IPC round-trip.

        If ``set_column_scale()`` has been called for any columns in the
        result, those columns are auto-dequantized from Int64 to Float64
        before returning.
        """
        result = self._inner.eval(query)
        if isinstance(result, pl.DataFrame):
            return self._apply_column_scales(result, query)
        return result  # type: ignore[return-value]

    def _apply_column_scales(self, df: pl.DataFrame, query: str) -> pl.DataFrame:
        if not self._column_scales:
            return df
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
            break
        return df

    # -----------------------------------------------------------------------
    # Phase 16 — Broker bindings (WL 1.1)
    # -----------------------------------------------------------------------

    def publish(self, topic: str, ipc_bytes: bytes) -> int:
        """Publish raw IPC bytes to all subscribers of a topic.

        Returns the publisher-side per-topic monotonic sequence number.
        Use this when you already have IPC-encoded bytes (e.g. forwarded
        from a network broker). For a polars DataFrame, prefer
        ``tick_upd()`` — it serializes Rust-side with the GIL released.
        """
        return self._inner.publish(topic, ipc_bytes)

    def subscribe(self, topics: list[str], callback) -> None:
        """Register a callback invoked for each published batch.

        ``callback(topic: str, seq: int, ipc_bytes: bytes)`` is invoked
        from a background Rust thread. Must not block — dispatch to an
        event loop or queue.
        """
        self._inner.subscribe(topics, callback)

    def tick_upd(self, table: str, df: pl.DataFrame) -> int:
        """Serialize a DataFrame and publish to subscribers.

        Serialization runs Rust-side with the GIL released — faster than
        calling ``df.write_ipc_stream()`` + ``publish()`` from Python.
        """
        return self._inner.tick_upd(table, df)

    def broker_eod(self, eod_message: bytes) -> None:
        """Broadcast end-of-day signal to all subscribers."""
        self._inner.broker_eod(eod_message)

    def overwrite_partition(
        self,
        df: pl.DataFrame,
        hdb_path: str,
        table: str,
        date: str,
        sort_columns: Optional[list[str]] = None,
    ) -> int:
        """Replace an existing partition with new data.

        Unlike ``wpar()`` (which appends a new shard ``_0001``, ``_0002``,
        etc.), this deletes all existing shard files for the given date
        and writes a single fresh ``_0000`` file. Use for in-place HDB
        rewrites (dtype migrations, re-sorting, etc.).
        """
        return self._inner.overwrite_partition(
            df, hdb_path, table, date, sort_columns or []
        )

    def wpar(
        self,
        df: pl.DataFrame,
        hdb_path: str,
        table: str,
        date: str,
        sort_columns: Optional[list[str]] = None,
    ) -> int:
        """Append a polars DataFrame as a new partition shard.

        ``sort_columns`` (recommended ``["symbol"]`` for analytical
        workloads): when set, chili sorts the partition and forces a
        small parquet ``row_group_size`` (16384) so later queries can
        prune row groups via column min/max statistics — required for
        ``where symbol=X`` to skip row groups (Phase 10 / WL 2.2).
        """
        return self._inner.wpar(
            df, hdb_path, table, date, sort_columns or []
        )
