# Changelog

All notable changes to this project will be documented in this file.

## [0.8.9] - 2026-05-24

### Added

- **W3 Python-callable bridge** (`engine.register_fn(name, callable, arity)` /
  `engine.unregister_fn(name)`) — register Python callables as
  pepper-invokable functions, dispatched via a new `ExternalFnDispatcher`
  trait. Tuple-form `sync(h, (name, *args))` over chili-IPC dispatches into
  the registered Python handler; Python exceptions propagate as
  `ChiliError` with the traceback embedded. See
  `docs/decisions/0007-w3-python-callable-bridge.md` for the full contract.
- `Func::new_external(name, arity)` constructor + `Func::is_external_fn()`
  helper + `Func::external_name: Option<String>` field.
- `EngineState::set_external_dispatcher` / `clear_external_dispatcher` API
  for installing an `ExternalFnDispatcher` trait object.
- 5 Rust unit tests (`crates/chili-core/tests/external_fn_test.rs`) using
  a Rust-side stub dispatcher; covers happy path, missing-dispatcher
  error, arity-mismatch projection, dispatcher replacement, and
  concurrent dispatch + register/unregister (no deadlock).
- 8 chili-py pytest tests (`crates/chili-py/tests/test_register_fn.py`)
  for the Python end: local invoke, callback re-entry into the engine,
  exception propagation, arity-mismatch projection, unregister, dangling-
  dispatcher warn-on-inconsistency, remote-via-chili-IPC closure gate,
  and concurrent register + dispatch.

### Changed

- chili-core lib.rs re-exports `ExternalFnDispatcher`.
- `serde9.rs` SpicyObj::Fn serializer carries an inline note that external
  Funcs serialize as their fn_body only — deserialized form on the remote
  side is non-callable. Clients invoking external Funcs MUST use call-form
  sync, not variable-lookup sync.

### Notes

- GR5 (GIL released around `Engine::eval`) is preserved by design:
  callback dispatch acquires the GIL only for the callback duration
  (~300ns per round-trip + Python body time). Non-W3 users see the
  `external_dispatcher` slot as `None` and never reach the W3 branch in
  `eval_fn_call` — zero overhead.
- No on-disk format change. No parse-cache impact.

## [0.8.3] - 2026-05-17

### Added

- `rotate_handle(handle_num, uri)` built-in (`.handle.rotate`) — swap a file handle's writer to a new `file://` path and reset its tick counter, enabling log rotation without closing/reopening handles
- `query_plan(query, hdb_path)` in Python bindings — returns the Polars logical plan string for a query without executing it, useful for query-tuning workflows
- `add_at_time(fn_name, start_time, description)` in Python bindings — schedule a pepper function to fire once at a given time on the chili job scheduler thread
- `table_count()` in Python bindings — return the number of partitioned tables currently loaded
- `lazy` parameter on `ChiliEngine.eval()` — when `True`, DataFrame results are returned as `polars.LazyFrame`
- `write_partitioned_df` now accepts `datetime.date` directly for the `date` parameter
- Default arguments for `tick(index=0, inc=1)` and `get_tick_count(index=0)`
- `test-py` task in Taskfile for running pytest on chili-py
- Parse cache unit tests and log-rotation integration tests
- Criterion benchmark for categorical eval in `chili-op`
- Register `LOG_FN` in Python engine for logging support

### Changed

- Moved `prepare_file_writer` helper to `utils.rs`, shared between `open_handle` and `rotate_handle`
- `LazyCell` → `LazyLock` for the regex style constant (thread-safe)
- GIL released around `load_par_df`, `clear_par_df`, `add_at_time`, and `query_plan` in Python bindings
- `eval()` in Rust binding simplified — lazy/eager normalization moved to the Python layer
- Replaced `disconnect_handle` with `ConnType::Disconnected` flag on send errors to avoid killing subscriber handles
- `signal_eod` refactored for robustness
- Moved `logger.rs` from `chili-bin` to `chili-op`

### Fixed

- `upsert` / `insert` clippy lint fixes (unnecessary references)

## [0.8.2] - 2026-05-10

### Added

- Package import support in `import` — paths starting with `@` or alphabetic characters are resolved as chiz package imports (e.g. `import "@scope/dep-name/util"` resolves to `$CHIZPATH/@scope/dep-name/<version>/src/util.chi`)
- Version is resolved from local `chiz_index.json` or global `$CHIZPATH/.index`; import fails if the package is not found in either index
- File extension order follows the current language setting (`.chi` first in Chili mode, `.pep` first in Pepper mode)
- Supports deeper module paths (e.g. `import "@scope/pkg/sub/module"` → `src/sub/module.chi`)

## [0.8.0] - 2026-05-03

### Added

- Python bindings package "chili-sauce" for the Chili engine via PyO3
- `load_par_df` now recursively traverses subdirectories to discover tables, producing dot-separated qualified names (e.g. `load("/path/sub")` loads `sub.trade`, `sub.order`)
- `HandleOutOfRangeErr` error variant for handle numbers outside 0..1024
- Tick/Sub publish-subscribe system for Python bindings:
  - `init_tick(schema, log_dir, date)` — initialize the tick engine with table schemas
  - `publish(table, data)` — publish data to subscribers
  - `subscribe(tick_socket, topics)` — subscribe to a tick engine and replay log
  - Bundled `tick.pep` and `sub.pep` scripts for tick and subscriber logic
- `list_handle()` — return a DataFrame listing all active handles (num, socket, conn_type, ipc_type, is_local, on_disconnected)
- `.handle.exists` built-in function to check if a handle exists
- `job_interval` and `memory_limit` parameters to `EngineState` constructor (Python and Rust)
- `start_job_scheduler()` and `start_memory_monitor()` methods on `EngineState`
- `src_path` parameter to `eval()` in the Python binding for source location in error traces
- `tick(index, inc)` and `get_tick_count(index)` now accept an index parameter for multiple tick counters

### Changed

- Moved `check_memory_usage`, job scheduler, and memory monitor logic from `chili-bin` into `chili-core::EngineState` methods
- `sysinfo` dependency moved from `chili-bin` to `chili-core`
- `upsert` now supports `MixedList` in addition to `Series` for the collection argument

### Fixed

- **serde9 nested mixed list deserialization** — fixed a critical bug where nested `MixedList` items were deserialized from a hardcoded buffer offset (byte 16) instead of the current position, causing inner list elements to be read as the parent list's data. This was the root cause of broker topic misregistration in IPC pub/sub.
- Added bounds guard on `tick_count` access — handle numbers outside 0..1024 now return an error instead of panicking

## [0.7.5] - 2026-04-15

### Added

- Criterion benchmarks for `chili-op` (`load_par_df`, `scan`, `eval`, `write_partition`)
- Integration tests covering HDB partition loading + selection (R1 regression guard)
- `write_partition_native` helper for writing partitions programmatically

### Changed

- `load_par_df` now builds table metadata in parallel and shrinks the write-lock window
- `write_partition` optimizes parquet output for partition pruning by tuning row group size when sorting

### Fixed

- Partition discovery is now deterministic (partition vectors are sorted before binary-search-based lookups)
- Partition predicate handling now scans all where-clauses and preserves non-partition filters (fixes missing rows for `where date=...` / ranges)

## [0.7.4] - 2026-03-21

### Added

- Cache source code from REPL and IPC connections for printing error messages

## [0.7.2] - 2026-02-24

### Removed

- Removed the `%` operator
- Vim syntax highlighting support, chiz will support it soon

### Fixed

- Fixed the handling of Windows paths
- Fixed the symbol token for Windows paths
- Fixed `upsert` and `insert` to support DataFrame as the first argument

## [0.7.1] - 2026-02-22

### Added

- Supported Windows

## [0.7.0] - 2026-02-21

### Added

- Refactored to use chumsky as the parser
- Unified the binaries into one chili binary
- Added `-P` or `--pepper` flag to enable REPL using pepper syntax
- Supported macOS

## [0.6.4] - 2026-02-08

### Added

- Improved the handling of MixedList in the deserialization process to return an empty list when appropriate.
- Modified ListExp, SelectExp, ByExp, Table, Matrix, and Dict to support optional trailing commas.
- Enhanced BracketExp and ColNames to maintain consistency with the new syntax rules.

## [0.6.3] - 2026-01-25

### Added

- Lazy evaluation mode for the runtime
- New built-in function `collect` to collect a lazy DataFrame
- `os.pid` to retrieve the process ID
- `os.version` to retrieve the operating system version
- `os.syntax` to retrieve the syntax type (chili or pepper)

## [0.6.2] - 2025-12-14

### Added

- New built-in function `insert` to insert data from DataFrame or list and keep the last record for each group
- New built-in function `.os.mem` to retrieve memory statistics
- Memory limit command line option for the runtime

### Changed

- Enhanced upsert function to enforce DataFrame type for the first argument
- Updated parser to support new syntax for column definitions

## [0.6.1] - 2025-12-10

### Added

- Support for new syntax highlighting and validation in Chili language
- Short-circuit evaluation for logical operators (`||`, `&&`, `??`) in AST and evaluation logic
- Support for `if` statements with optional `else` blocks for `chili` language
- New binary operators and control keywords in grammar definitions

### Changed

- Enhanced startup banner with vintage feature support and updated graphics
- Updated job function name formatting in EngineState evaluation
- Updated parser to support new control flow structures
- Adjusted tests to reflect changes in parsing and evaluation structure

### Fixed

- Corrected comparison operator in 'lt' function

## [0.6.0] - 2025-12-07

### Added

- Initial release of Chili
- Support for two language syntaxes:
  - chili: a modern programming language similar to JavaScript
  - pepper: a vintage programming language similar to q
- Integration with Polars for data manipulation
- Support for Arrow/Parquet data storage
- Vim syntax highlighting support
