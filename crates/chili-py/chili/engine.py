"""Python bindings for Chili's ``EngineState`` (Rust ``chili-core``)."""

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import polars as pl

from .engine_state import EngineState  # type: ignore


class ChiliEngine:
    """High-level Python interface to the Chili evaluation engine.

    Wraps the Rust ``EngineState`` exposed via PyO3 and provides a
    Pythonic API for evaluating Chili/Pepper expressions, managing
    variables, partitioned DataFrames, and IPC connections.

    Args:
        debug: Enable debug-level logging inside the engine.
        lazy: Enable lazy evaluation mode.
        pepper: Use Pepper syntax instead of the default Chili syntax.
        job_interval: Job scheduler polling interval in milliseconds (0 = disabled).
        memory_limit: Memory limit in MB (0 = unlimited, minimum 1024 MB).
    """

    def __init__(
        self,
        debug: bool = False,
        lazy: bool = False,
        pepper: bool = False,
        job_interval: int = 0,
        memory_limit: float = 0,
    ):
        self.is_tick_loaded = False
        self.is_sub_loaded = False
        self.engine = EngineState(debug, lazy, pepper, job_interval, memory_limit)
        self._hdb_path: Optional[str] = None
        self._column_scales: Dict[str, Dict[str, int]] = {}

    def eval(
        self,
        source: str,
        src_path: Optional[str] = None,
        lazy: bool = False,
    ) -> Any:
        """Evaluate a Chili or Pepper expression string.

        Args:
            source: The expression to evaluate (same syntax as the REPL).
            src_path: Optional logical source path for error messages.
                      Defaults to ``"repl.pep"`` or ``"repl.chi"``
                      depending on the engine's syntax mode.
            lazy: ADR 0002 — when False (default), DataFrame-shaped
                  results are returned eagerly as ``polars.DataFrame``;
                  when True, results are returned as ``polars.LazyFrame``
                  for further chained ops + ``.collect()`` (true
                  cross-FFI lazy with predicate pushdown — ADR 0003,
                  preserved through the Sprint-20 main merge). Both
                  paths release the GIL around the heavy work
                  (golden rule 5).

        Returns:
            The result of the evaluation, converted to a Python type.
            Sprint 20 / M-1: results are **no longer auto-dequantized** —
            callers apply column scales themselves (see
            :meth:`set_column_scale` / :meth:`_apply_column_scales`),
            unifying the eager path with the long-standing lazy-path
            contract. The on-disk + FFI schema stays Int64-quantized
            (golden rule 4); M-1 only removes the read-time convenience.
        """
        if src_path is None:
            src_path = "repl.chi" if self.is_repl_use_chili_syntax() else "repl.pep"
        return self.engine.eval(source, src_path, lazy)

    def get_var(self, id: str) -> Any:
        """Retrieve the value of a variable by name.

        Args:
            id: Variable name.

        Returns:
            The variable's value, converted to a Python type.

        Raises:
            NameError: If the variable does not exist.
        """
        return self.engine.get_var(id)

    def get_var_lazy(self, id: str) -> Any:
        """Retrieve a variable as a ``polars.LazyFrame`` snapshot.

        D-2 (Sprint 21 / ADR-0006 §5). A snapshot-clone of the
        in-memory accumulated frame then ``.lazy()``, so further
        ``.filter()`` / ``.select()`` push down in the lazy plan and
        ``.collect()`` is byte-identical to :meth:`get_var`. Sound vs
        the IPC receive thread (it mutates only under the write-lock —
        the snapshot is stable, not a live view).

        Args:
            id: Variable name (must be a DataFrame-valued variable).

        Returns:
            A ``polars.LazyFrame``.

        Raises:
            NameError: If the variable does not exist.
        """
        return self.engine.get_var_lazy(id)

    def set_var(self, id: str, value: Any):
        """Set or overwrite a variable in the engine.

        Args:
            id: Variable name.
            value: The value to assign (automatically converted from Python).
        """
        return self.engine.set_var(id, value)

    def has_var(self, id: str) -> bool:
        """Check whether a variable exists.

        Args:
            id: Variable name.

        Returns:
            ``True`` if the variable exists, ``False`` otherwise.
        """
        return self.engine.has_var(id)

    def del_var(self, id: str) -> Any:
        """Delete a variable and return its last value.

        Args:
            id: Variable name.

        Returns:
            The deleted variable's value, or ``None`` if it did not exist.
        """
        return self.engine.del_var(id)

    def import_source_path(self, relative: str, path: str) -> Any:
        """Import and evaluate a Chili/Pepper source file.

        Args:
            relative: Base path used to resolve relative imports inside
                      the source file.  Pass ``""`` when importing a
                      top-level file.
            path: File system path to the source file.

        Returns:
            The result of evaluating the file.
        """
        return self.engine.import_source_path(relative, path)

    def set_source(self, path: str, src: str) -> Any:
        """Register an in-memory source string under *path*.

        Args:
            path: Logical path associated with the source.
            src: The source code string.

        Returns:
            The index of the registered source entry.
        """
        return self.engine.set_source(path, src)

    def get_source(self, index: int) -> Tuple[str, str]:
        """Retrieve a previously registered source by its index.

        Args:
            index: Zero-based source index.

        Returns:
            A ``(path, source)`` tuple.
        """
        return self.engine.get_source(index)

    def shutdown(self):
        """Shut down the engine and release all IPC handles."""
        self.engine.shutdown()

    def get_displayed_vars(self) -> dict[str, Any]:
        """Return a mapping of variable names to their display strings.

        Functions are shown with their call signatures; other values are
        shown in short-string form.
        """
        return self.engine.get_displayed_vars()

    def list_vars(self, pattern: str) -> list[str]:
        """List engine variables as a Polars DataFrame.

        Args:
            pattern: Prefix filter.  Pass ``""`` to list all variables.

        Returns:
            A ``polars.DataFrame`` with columns ``name``, ``display``,
            ``type``, ``columns``, and ``is_built_in``.
        """
        return self.engine.list_vars(pattern)

    def parse_cache_len(self) -> int:
        """Return the current number of entries in the LRU parse cache."""
        return self.engine.parse_cache_len()

    def get_tick_count(self, index: int = 0) -> int:
        """Return the current tick counter value at *index* (default 0)."""
        return self.engine.get_tick_count(index)

    def tick(self, index: int = 0, inc: int = 1) -> Any:
        """Increment the tick counter at *index* by *inc*.

        Args:
            index: Tick stream index (default 0).
            inc: Amount to add to the counter (default 1).

        Returns:
            The updated tick count.
        """
        return self.engine.tick(index, inc)

    def is_lazy_mode(self) -> bool:
        """Return ``True`` if lazy evaluation mode is enabled."""
        return self.engine.is_lazy_mode()

    def is_repl_use_chili_syntax(self) -> bool:
        """Return ``True`` if the engine uses Chili syntax (not Pepper)."""
        return self.engine.is_repl_use_chili_syntax()

    def fn_call(self, func: str, args: list[Any]) -> Any:
        """Call a registered engine function by name.

        Args:
            func: Function name as registered in the engine.
            args: Positional arguments (converted from Python automatically).

        Returns:
            The function's return value, converted to a Python type.
        """
        return self.engine.fn_call(func, args)

    def write_partitioned_df(
        self,
        df: pl.DataFrame,
        hdb_path: str,
        table: str,
        date: Any,
        sort_columns: Optional[list[str]] = None,
        rechunk: bool = False,
        overwrite: bool = False,
    ) -> int:
        """Write a DataFrame as a date-partitioned Parquet table.

        Args:
            df: The data to write.
            hdb_path: Root directory of the partitioned database.
            table: Table name (sub-directory under each date partition).
            date: Partition date — accepts ``datetime.date`` directly or
                  a ``"YYYY.MM.DD"`` string.
            sort_columns: Optional columns to sort by before writing.
            rechunk: Re-chunk the data into a single contiguous allocation.
            overwrite: If ``True``, overwrite an existing partition.

        Returns:
            The number of rows written.
        """
        from datetime import date as _date_t, datetime as _dt_t

        if isinstance(date, str):
            partition = _dt_t.strptime(date, "%Y.%m.%d").date()
        elif isinstance(date, (_date_t, _dt_t)):
            partition = date
        else:
            partition = date  # let the engine validate
        sort_cols_arg: Any = pl.Series(
            "sort_columns", sort_columns or [], dtype=pl.Categorical
        )
        return self.fn_call(
            "wpar",
            [
                hdb_path,
                partition,
                table,
                df,
                sort_cols_arg,
                rechunk,
                overwrite,
            ],
        )

    def load_partitioned_df(self, hdb_path: str) -> None:
        """Load a partitioned database from disk.

        After loading, partitions can be queried via the engine.

        Args:
            hdb_path: Root directory of the partitioned database.
        """
        self.fn_call("load", [hdb_path])
        self._hdb_path = hdb_path

    def clear_partitioned_df(self) -> None:
        """Remove all loaded partitioned DataFrames from memory."""
        return self.engine.clear_par_df()

    def table_count(self) -> int:
        """Return the number of partitioned tables currently loaded."""
        return self.engine.table_count()

    # -----------------------------------------------------------------------
    # Column scale dequantization (golden rule 4 read-side helper)
    # -----------------------------------------------------------------------

    def set_column_scale(self, table: str, column: str, factor: int) -> None:
        """Register a dequantization scale factor for a column.

        After ``set_column_scale("ohlcv_1d", "close", 1_000_000)``, any
        ``eval()`` result whose query references ``ohlcv_1d`` and contains
        an ``Int64`` ``close`` column is auto-cast to ``Float64`` and
        divided by ``factor`` before being returned.

        The on-disk schema stays Int64-quantized (golden rule 4); this is
        a read-time helper for callers that want Float64 ergonomics.
        """
        self._column_scales.setdefault(table, {})[column] = factor

    def clear_column_scales(self) -> None:
        """Remove all registered column scale factors."""
        self._column_scales.clear()

    def _apply_column_scales(
        self, df: "pl.DataFrame", query: str
    ) -> "pl.DataFrame":
        if not self._column_scales:
            return df
        for table, scales in self._column_scales.items():
            # Word-boundary match preceded by `from` or `join` so e.g.
            # `from trades` does not false-match `from all_trades`, AND so
            # tables introduced by a join also dequantize. This is still
            # a best-effort textual scan; a future sprint may move table
            # detection into the engine eval result for full robustness.
            pattern = r"\b(?:from|join)\s+" + re.escape(table) + r"\b"
            if not re.search(pattern, query):
                continue
            cast_exprs = []
            for col_name, factor in scales.items():
                if col_name in df.columns and df[col_name].dtype == pl.Int64:
                    cast_exprs.append(
                        pl.col(col_name).cast(pl.Float64) / factor
                    )
            if cast_exprs:
                df = df.with_columns(cast_exprs)
        return df

    def query_plan(self, query: str, hdb_path: Optional[str] = None) -> str:
        """Return the polars query plan for *query* without executing it.

        Internally spins up a temporary **pepper-syntax** lazy-mode engine,
        loads the HDB, evaluates *query* to obtain a ``LazyFrame``, and
        returns its ``describe_plan()`` string. The current engine state
        is unaffected. **Pepper syntax only** — chili-syntax queries will
        fail to parse here even if the calling engine is in chili mode;
        this matches parked-claude's behavior.

        Args:
            query: A pepper-syntax query string (e.g.,
                ``"select last close by sym from ohlcv_1d where date=..."``).
            hdb_path: HDB root directory. Defaults to the most recently
                loaded path on this engine (via ``load_partitioned_df``).
        """
        path = hdb_path if hdb_path is not None else self._hdb_path
        if path is None:
            raise RuntimeError(
                "No HDB path provided and no prior load_partitioned_df() call. "
                "Pass hdb_path= explicitly or call load_partitioned_df() first."
            )
        return self.engine.query_plan(query, path)

    def start_tcp_listener(
        self,
        port: int,
        remote: bool = False,
        users: Optional[list[str]] = None,
    ) -> None:
        """Start a TCP listener on *port* in a background thread.

        The listener accepts incoming IPC connections (Q or Chili
        protocol), performs authentication, and dispatches each
        connection to its own handler thread.

        Args:
            port: TCP port number to listen on.
            remote: If ``True``, bind to ``0.0.0.0`` (accept remote connections).
                    Otherwise bind to ``127.0.0.1`` (localhost only).
            users: Optional list of usernames allowed to authenticate.
                   An empty list (the default) allows any user.
        """
        self.engine.start_tcp_listener(port, remote, users or [])

    def list_handle(self) -> pl.DataFrame:
        """Return a DataFrame listing all active handles."""
        return self.engine.list_handle()

    def stats(self) -> dict[str, Any]:
        """Return engine statistics as a dictionary.

        Includes lazy mode status, REPL language, partitioned DataFrame
        count, parse cache size, and partition paths.
        """
        return self.engine.stats()

    def load_tick(self) -> None:
        """Load the built-in tick plant source (``src/tick.pep``).

        Evaluates the bundled Pepper script that defines ``.tick.*``
        functions (``createLog``, ``upd``, ``subscribe``, ``unsubscribe``,
        ``eod``).
        """
        if not self.is_tick_loaded:
            tick_path = Path(__file__).parent / "src" / "tick.pep"
            source = tick_path.read_text()
            self.engine.eval(source, "tick.pep")
            self.is_tick_loaded = True

    # Tick functions
    # Feed handler should call .tick.upd
    # Subscriber should call .tick.subscribe and .tick.unsubscribe on tick process
    def init_tick(
        self, schema: Dict[str, pl.DataFrame], log_dir: str, date: date
    ) -> None:
        self.load_tick()
        self.set_var(".tick.schema", schema)
        self.fn_call(".tick.createLog", [log_dir, date])

    def publish(self, table: str, data: Any) -> None:
        self.fn_call(".tick.upd", [table, data])

    def eod(self, date: date) -> None:
        self.fn_call(".tick.eod", [date])

    def add_at_time(
        self,
        fn_name: str,
        start_time: datetime,
        description: str = "",
    ) -> int:
        """Schedule a pepper function to fire once at ``start_time``.

        Thin wrapper over chili's ``.job.addAtTime`` registered builtin.
        Backs mdata's PRD §3.2 Option A EOD timer path — replaces their
        Python asyncio timer with a chili-scheduler-owned timer.

        Parameters
        ----------
        fn_name : str
            Name of a **nullary** pepper function in the engine's global
            namespace (e.g., ``my_handler: {[] ...}``). The scheduler
            invokes it as ``fn_name[]`` — passing args via the job spec
            is not supported; use engine variables (``today[]``, ``now[]``,
            or a pre-set global) inside the handler for time context.
        start_time : datetime.datetime
            When to fire. Must be timezone-aware; naive datetimes raise
            ``TypeError``. Attach ``timezone.utc`` explicitly for UTC.
        description : str, optional
            Free-text label, returned by the job-list helpers.

        Returns
        -------
        int
            Job ID. Pass to :meth:`cancel_job` to revoke.

        Notes
        -----
        The chili job scheduler must be running for the timer to fire.
        Construct :class:`ChiliEngine` with ``job_interval > 0`` (in
        milliseconds) to start the scheduler thread.
        """
        return self.engine.add_at_time(fn_name, start_time, description)

    def flush_tplog(self) -> int:
        """Flush + ``fsync`` the active tplog handle (``.tick.msgHandle``).

        Closes the durability gap for mdata's PRD §5.1 part-2 ``kill -9``
        cold-restart guarantee. After this call returns, any row previously
        accepted via :meth:`publish` is on disk — a hard process kill cannot
        lose it.

        Returns
        -------
        int
            Payload bytes-since-last-flush. Replaces the
            ``log_path.stat().st_size`` proxy with a precise monitor probe.

        Raises
        ------
        RuntimeError
            If :meth:`init_tick` hasn't been called yet (``.tick.msgHandle``
            is undefined).
        """
        return self.engine.flush_tplog()

    # Subscriber functions
    def load_sub(self) -> None:
        if not self.is_sub_loaded:
            sub_path = Path(__file__).parent / "src" / "sub.pep"
            source = sub_path.read_text()
            self.engine.eval(source, "sub.pep")
            self.is_sub_loaded = True

    # The socket should start with chili://hostname:port
    def subscribe(
        self,
        tick_socket: str,
        topics: Optional[list[str]] = None,
        resume_from: Optional[dict[str, int]] = None,
    ) -> None:
        """Subscribe to a tp, optionally resuming from a persisted cursor.

        Args:
            tick_socket: ``chili://host:port`` of the tickerplant.
            topics: Tables to subscribe to (``None``/``[]`` = all).
            resume_from: D-3 (Sprint 21 / ADR-0006 §4) — ``{table:
                cursor}`` last-delivered positions the caller persisted.
                When given, replay starts from the conservative **min**
                across subscribed topics instead of the start of the
                tplog. chili's cursor is only a monotonic delivery
                position; per-table gap-free / zero-dup contiguity is
                the caller's own ``seq`` column (Q1 Path-1) — a bounded
                over-replay is expected and deduped caller-side.
                ``.sub.recover`` reuses the same persisted cursors on
                reconnect (replacing the old latent ``tick[0]``).
        """
        self.load_sub()
        if resume_from:
            self.engine.set_resume_cursors(resume_from)
        self.fn_call(".sub.init", [tick_socket, topics or []])

    # Push-model D-1 (Sprint 21 / ADR-0006)
    def upd_notify_fd(self) -> int:
        """Arm GIL-free ``upd`` delivery notification; return the
        self-pipe **read** fd.

        Register it with ``loop.add_reader(fd, cb)`` (or ``kqueue``);
        when readable, call :meth:`drain_upds`. The fd is
        ``O_NONBLOCK`` + ``FD_CLOEXEC``; the call is idempotent (same
        fd every time). Arm this **before** :meth:`subscribe` so no
        applied ``upd`` goes unsignalled. Lets an mdata-style rdb/wdb
        subscriber delete its ~10 ms poll-loop + ``_last_seen_seq``
        dedup + parallel buffer.

        The fd must not be used across ``os.fork`` without re-creation.
        """
        return self.engine.upd_notify_fd()

    def drain_upds(self) -> list:
        """Drain all applied-``upd`` notifications since the last call.

        Non-blocking; returns ``[]`` when the queue is empty or
        notification was never armed. Each element is an ``UpdEvent``
        with ``table``, ``cursor_lo``/``cursor_hi`` (chili's per-handle
        monotonic delivery ordinal — **not** mdata's per-row ``seq``;
        per-table contiguity is the caller's own ``seq``, Q1 Path-1)
        and ``frame`` (the raw delta as sent by the tp, Q3).

        Back-pressure (ADR-0006 §3): the bounded queue blocks the
        receive thread at capacity (never drops — the tplog is the
        source of truth); a slow drainer back-pressures the upstream
        tp, kdb+-like.
        """
        return self.engine.drain_upds()

    # Publisher functions (Sprint 17)
    def publish_via_handle(self, h: int, table: str, df: pl.DataFrame) -> None:
        """Publish a DataFrame to a remote tp via an open chili-IPC handle.

        Thin one-shot wrapper — open the handle via
        ``engine.open_handle("chili://host:port")``, cache it, call
        ``publish_via_handle`` repeatedly, then close via
        ``engine.close_handle``. Per Sprint 16 mdata-wishlist Q3
        lock-in (Option B): chili owns the marshalling primitive;
        callers (e.g. mdata's RemoteTpClient) own connection-manager
        semantics on top.

        Args:
            h: Handle id from ``engine.open_handle("chili://...")``;
                must still be ``Outgoing`` (not promoted to Subscribing).
            table: Table name the remote tp will dispatch via ``.tick.upd``.
            df: Rows to publish.

        Raises:
            RuntimeError: if ``df`` is not a DataFrame, ``h`` has no
                live connection, or the handle is not ``Outgoing``.

        Note:
            ``sync()`` is a blocking send-and-receive on chili IPC;
            this method does not return until the remote tp has
            answered. The GIL is released around the network round-trip
            so concurrent Python publishers don't serialize on it.
        """
        self.engine.publish_via_handle(h, table, df)

    def roll_tick(self, log_dir: str, segment_label: str) -> None:
        """Atomically roll the tplog to the next segment.

        Holds chili's internal handle write-lock across open-next →
        swap-writer (same handle id) → fsync+close-old so a concurrent
        inbound ``.tick.upd`` is serviced by exactly one valid handle
        and lands wholly in the old segment or wholly in the new one —
        never dropped, never misplaced. Replaces the racy
        ``engine.eod(d)`` + ``init_tick(.., d+1)`` pair at the segment
        boundary; no Python-side drain barrier required.

        ``roll_tick`` is the safe replacement for the create-next-log
        step only. It is **cutover-only**: it does NOT fire the EOD
        broadcast. If a ``(eod;d)`` broadcast is wanted, call
        ``eod(d)`` first, then ``roll_tick(log_dir, next_label)``.

        Rollover is not date-bound: ``segment_label`` is an opaque
        caller-owned path component appended to ``log_dir`` exactly as
        ``init_tick`` does (``.tick.msgLog = log_dir + label``). Pass a
        date for daily rolls, or a zero-padded counter for size/count-
        triggered UHF rolls — the caller owns the monotonic increment
        and naming convention. The logical tick sequence is cumulative
        across segments (carry-over, matching ``init_tick``).

        Args:
            log_dir: Same directory string passed to ``init_tick``.
            segment_label: Opaque next-segment path component
                (non-empty); caller-owned and monotonically increasing.

        Raises:
            RuntimeError: if ``segment_label`` is empty, the live
                ``.tick.msgHandle`` is unset/invalid, the next segment
                cannot be opened (in which case the current segment is
                left intact and writable), or the durability fsync of
                the current segment fails.

        Note:
            The GIL is released around the cutover so concurrent Python
            publishers don't serialize on it.
        """
        self.engine.roll_tick(log_dir, segment_label)

    # IPC remote queries (upstream 606d1cc, merged Sprint 19).
    def open_handle(self, socket: str) -> int:
        return self.fn_call(".handle.open", [socket])

    def sync(self, handle_num: int, query: Any) -> Any:
        # Adapted for claude-2 (Sprint 19): upstream's `sync` called
        # `self.eval("pyHandle", [query])`, but claude-2's `eval()` 2nd
        # positional is `src_path` (ADR 0002 lazy/src_path divergence),
        # not apply-args. Route via `fn_call` instead — 606d1cc's own
        # `fn_call` I64 arm (engine_state.rs) → `eval_call` I64 arm
        # (eval.rs) → `state.sync(h, query)` is the claude-2 path.
        self.fn_call("set", ["pyHandle", handle_num])
        return self.fn_call("pyHandle", [query])
