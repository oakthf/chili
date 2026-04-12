// Use mimalloc as the global allocator for the chili-py cdylib. Same
// rationale as chili-bin: polars' allocation pattern fits mimalloc much
// better than the system allocator. Safe because the cdylib has a single
// linked allocator boundary and pyo3 does not mandate any particular
// allocator on the Rust side.
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, LazyLock, Mutex};

use chili_core::{EngineState, Func, SpicyError, SpicyObj, SpicyResult, Stack};
use chili_op::{write_partition_py, BUILT_IN_FN};
use polars::io::{SerReader, SerWriter};
use polars::io::ipc::{IpcStreamReader, IpcStreamWriter};
use polars::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::PyTypeInfo;

// ---------------------------------------------------------------------------
// Phase 13 — Structured exception hierarchy (WL 3.3)
//
// ChiliError is the base class, extending RuntimeError for backwards
// compatibility (existing `except RuntimeError` catches still work).
// Subclasses let callers catch specific failure modes:
//
//   try:
//       engine.eval("select from ohlcv_1d")
//   except chili.PartitionError:
//       print("missing date predicate")
//   except chili.PepperParseError:
//       print("syntax error")
//   except chili.ChiliError:
//       print("some other chili error")
//   except RuntimeError:
//       print("still caught by RuntimeError too")
// ---------------------------------------------------------------------------
pyo3::create_exception!(chili, ChiliError, pyo3::exceptions::PyRuntimeError);
pyo3::create_exception!(chili, PepperParseError, ChiliError);
pyo3::create_exception!(chili, PepperEvalError, ChiliError);
pyo3::create_exception!(chili, PartitionError, ChiliError);
pyo3::create_exception!(chili, TypeMismatchError, ChiliError);
pyo3::create_exception!(chili, NameError, ChiliError);
pyo3::create_exception!(chili, SerializationError, ChiliError);

/// Map a SpicyError to the most specific Python exception class.
fn spicy_err_to_py(err: &SpicyError) -> PyErr {
    match err {
        SpicyError::ParserErr(_) => PepperParseError::new_err(err.to_string()),
        SpicyError::EvalErr(msg) => {
            // Detect the specific "requires 'ByDate' condition" pattern so
            // callers can catch PartitionError separately.
            if msg.contains("condition for this partitioned") {
                PartitionError::new_err(err.to_string())
            } else {
                PepperEvalError::new_err(err.to_string())
            }
        }
        SpicyError::NameErr(_) => NameError::new_err(err.to_string()),
        SpicyError::MismatchedTypeErr(..)
        | SpicyError::MismatchedArgTypeErr(..)
        | SpicyError::MismatchedArgNumErr(..)
        | SpicyError::MismatchedArgNumFnErr(..) => TypeMismatchError::new_err(err.to_string()),
        SpicyError::NotAbleToSerializeErr(_)
        | SpicyError::DeserializationErr(_)
        | SpicyError::NotAbleToDeserializeErr(_) => SerializationError::new_err(err.to_string()),
        _ => ChiliError::new_err(err.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Log built-in functions (mirrors chili-bin/src/logger.rs)
// These register `.log.info` / `.log.warn` / `.log.debug` / `.log.error`
// in the engine so Chili scripts can call them.
// ---------------------------------------------------------------------------

fn log_str(args: &[&SpicyObj]) -> SpicyResult<String> {
    match args[0] {
        SpicyObj::String(s) => Ok(s.to_owned()),
        SpicyObj::MixedList(v) => Ok(v
            .iter()
            .map(|a| {
                if let SpicyObj::String(s) = a {
                    s.as_str().to_owned()
                } else {
                    a.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ")),
        other => Ok(other.to_string()),
    }
}

fn log_debug_fn(args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
    log::debug!("{}", log_str(args)?);
    Ok(SpicyObj::Null)
}
fn log_info_fn(args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
    log::info!("{}", log_str(args)?);
    Ok(SpicyObj::Null)
}
fn log_warn_fn(args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
    log::warn!("{}", log_str(args)?);
    Ok(SpicyObj::Null)
}
fn log_error_fn(args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
    log::error!("{}", log_str(args)?);
    Ok(SpicyObj::Null)
}

static LOG_FN: LazyLock<HashMap<String, Func>> = LazyLock::new(|| {
    [
        (
            ".log.debug".to_owned(),
            Func::new_built_in_fn(Some(Box::new(log_debug_fn)), 1, ".log.debug", &["message"]),
        ),
        (
            ".log.info".to_owned(),
            Func::new_built_in_fn(Some(Box::new(log_info_fn)), 1, ".log.info", &["message"]),
        ),
        (
            ".log.warn".to_owned(),
            Func::new_built_in_fn(Some(Box::new(log_warn_fn)), 1, ".log.warn", &["message"]),
        ),
        (
            ".log.error".to_owned(),
            Func::new_built_in_fn(Some(Box::new(log_error_fn)), 1, ".log.error", &["message"]),
        ),
    ]
    .into_iter()
    .collect()
});

// ---------------------------------------------------------------------------
// Arrow IPC bridge helpers
// ---------------------------------------------------------------------------

fn df_to_ipc_bytes(df: &mut DataFrame) -> PyResult<Vec<u8>> {
    let mut buf = Vec::new();
    IpcStreamWriter::new(&mut buf)
        .finish(df)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(buf)
}

fn ipc_bytes_to_df(bytes: &[u8]) -> PyResult<DataFrame> {
    IpcStreamReader::new(Cursor::new(bytes))
        .finish()
        .map_err(|e: polars::error::PolarsError| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
}

// ---------------------------------------------------------------------------
// Parse a date string "YYYY.MM.DD" into days since Unix epoch (1970-01-01).
// SpicyObj::Date(i32) stores days since 1970-01-01.
// ---------------------------------------------------------------------------

fn parse_date_str(date_str: &str) -> PyResult<i32> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(date_str, "%Y.%m.%d").map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "invalid date '{}': {} (expected YYYY.MM.DD)",
            date_str, e
        ))
    })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    i32::try_from((date - epoch).num_days()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOverflowError, _>(format!(
            "date '{}' is out of i32 range: {}",
            date_str, e
        ))
    })
}

// ---------------------------------------------------------------------------
// Engine Python class
// ---------------------------------------------------------------------------

#[pyclass]
struct Engine {
    state: Arc<EngineState>,
    /// Phase 11 — PID recorded at construction time. If a subsequent
    /// method call sees a different PID, we're in a forked child and
    /// the Rust state is unsalvageable (Rust threads + RwLock + Arc
    /// don't survive fork). Raise a clear error instead of deadlocking.
    init_pid: u32,
    // Phase 16 — in-process broker pub/sub state.
    // py_subscribers: topic → list of bounded channel senders (capacity 1024).
    // Each subscribe() call spawns a background thread per topic that reads
    // from the receiver and invokes the Python callback with GIL.
    py_subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::SyncSender<(String, i64, Vec<u8>)>>>>>,
    // topic_seq: per-topic monotonic sequence counter, starting at 0 (first
    // publish returns 1).
    topic_seq: Arc<Mutex<HashMap<String, i64>>>,
    // broker_shutdown: signals subscriber threads to stop processing
    // messages. Set to true in Drop so threads don't drain large queues
    // after the engine is deallocated.
    broker_shutdown: Arc<AtomicBool>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Signal subscriber threads to exit immediately instead of
        // draining their entire message queue — avoids multi-second
        // GIL contention when large queues are buffered.
        self.broker_shutdown.store(true, Ordering::Relaxed);
    }
}

#[pymethods]
impl Engine {
    /// Create a new Chili engine.
    ///
    /// debug  – enable debug-level logging
    /// lazy   – enable lazy evaluation
    /// pepper – use pepper/kdb+ q-like syntax (default True)
    #[new]
    #[pyo3(signature = (debug=false, lazy=false, pepper=true))]
    fn new(debug: bool, lazy: bool, pepper: bool) -> Self {
        let state = EngineState::new(debug, lazy, pepper);
        state.register_fn(&LOG_FN);
        state.register_fn(&BUILT_IN_FN);
        let arc_state = Arc::new(state);
        arc_state.set_arc_self(Arc::clone(&arc_state)).unwrap();
        Engine {
            state: arc_state,
            init_pid: std::process::id(),
            py_subscribers: Arc::new(Mutex::new(HashMap::new())),
            topic_seq: Arc::new(Mutex::new(HashMap::new())),
            broker_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Phase 11 — check that we're still in the same process that created
    /// the engine. If not, we were forked, and the Rust state (RwLock,
    /// Arc, thread-local allocator pools) is in an undefined state.
    /// Raise a clear error instead of deadlocking (which is the symptom
    /// mdata's Sprint D hit with ProcessPoolExecutor on macOS).
    fn check_fork(&self) -> PyResult<()> {
        if std::process::id() != self.init_pid {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "chili.Engine is not fork-safe after construction. \
                 This engine was created in PID {} but is now running in PID {}. \
                 Use multiprocessing.get_context('spawn') instead of 'fork', \
                 or create a new Engine in each child process.",
                self.init_pid,
                std::process::id(),
            )));
        }
        Ok(())
    }

    /// Phase 12 — clear all loaded partitioned DataFrames from the engine.
    /// The engine stays alive; use `load()` to re-load afterwards.
    fn unload(&self) -> PyResult<()> {
        self.check_fork()?;
        self.state
            .clear_par_df()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Phase 12 — return the number of loaded partitioned tables.
    fn table_count(&self) -> usize {
        self.state.par_df_count()
    }

    /// Phase 14 — return the parse cache size.
    fn parse_cache_len(&self) -> usize {
        self.state.parse_cache_len()
    }

    /// Phase 14 — placeholder for query plan.
    /// TODO: requires a lazy-mode eval path that chili doesn't currently
    /// expose through the Python bindings. Would need either a per-query
    /// lazy mode flag or a dedicated "compile to plan" API in EngineState.
    /// For now returns a stub; real implementation deferred to Phase 14b
    /// when chili gains a proper lazy-mode Python API.
    fn query_plan(&self, _query: &str) -> PyResult<String> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "query_plan is not yet implemented. Chili currently collects \
             results eagerly; a lazy-mode eval path is needed to expose \
             the polars query plan before execution."
        ))
    }

    /// Load a partitioned HDB directory.
    ///
    /// Releases the GIL for the duration of the load (which can take up to
    /// hundreds of milliseconds for large HDBs and includes filesystem
    /// traversal + parquet schema reads). Other Python threads can run
    /// during this time.
    fn load(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.check_fork()?;
        let state = Arc::clone(&self.state);
        let path = path.to_owned();
        py.allow_threads(move || {
            state
                .load_par_df(&path)
                .map_err(|e| spicy_err_to_py(&e))
        })
    }

    /// Evaluate a Chili/pepper query; returns Arrow IPC bytes.
    /// The Python wrapper in __init__.py converts these to a polars DataFrame.
    ///
    /// **Proposal A — GIL release.**
    ///
    /// The entire query body (parse → eval → polars collect → Arrow IPC
    /// serialization) is wrapped in `py.allow_threads`, so other Python
    /// threads can make progress while this query runs. `EngineState` is
    /// already `Send + Sync` (it's used across `thread::spawn` boundaries
    /// in `chili-bin/src/main.rs`'s IPC server), and nothing inside the
    /// closure touches Python objects, so the closure body is fully
    /// `Send`.
    ///
    /// Result of releasing the GIL: an N-thread Python client driving the
    /// same engine in parallel scales close to N× throughput, bounded by
    /// the polars rayon pool size, instead of being serialized at 0.92×
    /// (worse than serial — measured pre-Phase-6).
    fn eval<'py>(&self, py: Python<'py>, query: &str) -> PyResult<Bound<'py, PyBytes>> {
        self.check_fork()?;
        let state = Arc::clone(&self.state);
        let query = query.to_owned();
        // Run the entire query OFF the GIL, carrying the full SpicyError
        // out of the closure so we can map it to a structured Python
        // exception on the GIL side (Phase 13).
        let bytes = py.allow_threads(move || -> Result<Vec<u8>, SpicyError> {
            let query_obj = SpicyObj::String(query);
            let mut stack = Stack::new(None, 0, 0, "");
            let obj = state
                .eval(&mut stack, &query_obj, "py.chi")?;
            let mut df = match obj {
                SpicyObj::DataFrame(df) => df,
                SpicyObj::LazyFrame(lf) => lf
                    .collect()
                    .map_err(|e| SpicyError::EvalErr(e.to_string()))?,
                other => {
                    return Err(SpicyError::EvalErr(format!(
                        "eval returned {}, expected DataFrame",
                        other.get_type_name()
                    )));
                }
            };
            df_to_ipc_bytes_unchecked(&mut df)
                .map_err(|e| SpicyError::Err(e.to_string()))
        })
        .map_err(|e| spicy_err_to_py(&e))?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Write a polars DataFrame (passed as Arrow IPC bytes) to an HDB partition.
    ///
    /// ipc_bytes : Arrow IPC stream bytes produced by polars DataFrame.write_ipc()
    /// hdb_path  : root HDB directory (must exist and be canonicalisable)
    /// table     : table name
    /// date      : partition date string "YYYY.MM.DD"
    /// sort_columns : optional list of column names to sort the partition
    ///                by before writing. When non-empty, also forces a small
    ///                row_group_size (16384) so polars can later prune row
    ///                groups via parquet column statistics on the sort key
    ///                — required for `where symbol=X` queries to skip
    ///                row groups (Phase 10 / WL 2.2).
    ///
    /// Proposal A — releases the GIL for the IPC bytes → DataFrame parse,
    /// the date parse, and the partition write. mdata's batch ingest paths
    /// can issue many `wpar` calls concurrently from a Python thread pool
    /// without GIL contention.
    #[pyo3(signature = (ipc_bytes, hdb_path, table, date, sort_columns=Vec::new()))]
    fn wpar(
        &self,
        py: Python<'_>,
        ipc_bytes: &[u8],
        hdb_path: &str,
        table: &str,
        date: &str,
        sort_columns: Vec<String>,
    ) -> PyResult<i64> {
        self.check_fork()?;
        // Copy bytes off the &[u8] reference because the closure must own
        // its captured data (the Python buffer is released as soon as the
        // GIL is released).
        let ipc_bytes = ipc_bytes.to_vec();
        let hdb_path = hdb_path.to_owned();
        let table = table.to_owned();
        let date = date.to_owned();
        // Mirror Engine::eval: return String from the closure and re-wrap as
        // PyErr after re-acquiring the GIL. Avoids constructing PyErr inside
        // allow_threads — PyO3 docs warn that this is technically unsound for
        // exception types whose construction calls back into Python.
        let n = py.allow_threads(move || -> Result<i64, String> {
            let df = polars::io::ipc::IpcStreamReader::new(std::io::Cursor::new(&ipc_bytes))
                .finish()
                .map_err(|e: polars::error::PolarsError| e.to_string())?;
            // Inline parse_date_str without the PyResult wrapping.
            let days = {
                use chrono::NaiveDate;
                let parsed = NaiveDate::parse_from_str(&date, "%Y.%m.%d")
                    .map_err(|e| format!("invalid date '{}': {} (expected YYYY.MM.DD)", date, e))?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                i32::try_from((parsed - epoch).num_days())
                    .map_err(|e| format!("date '{}' is out of i32 range: {}", date, e))?
            };
            let date_obj = SpicyObj::Date(days);
            let sort_refs: Vec<&str> = sort_columns.iter().map(|s| s.as_str()).collect();
            let obj = write_partition_py(&hdb_path, &date_obj, &table, &df, &sort_refs, false)
                .map_err(|e| e.to_string())?;
            Ok(match obj {
                SpicyObj::I64(n) => n,
                _ => 0,
            })
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        Ok(n)
    }

    // -----------------------------------------------------------------------
    // Phase 16 — Broker bindings (WL 1.1)
    //
    // In-process pub/sub: publish() fans out Arrow IPC bytes to Python
    // callbacks registered via subscribe(). Each subscriber gets a bounded
    // mpsc channel (capacity 1024); a background thread per subscription
    // reads from the channel and invokes the callback with GIL acquired.
    //
    // This is independent of the Rust broker's IPC dispatch path (which
    // writes to TCP socket handles for inter-process communication).
    // -----------------------------------------------------------------------

    /// Publish Arrow IPC bytes to all in-process subscribers of a topic.
    ///
    /// Returns the publisher-side monotonic sequence number (per-topic,
    /// starting at 1). Backpressure: if a subscriber's channel is full
    /// (1024 pending messages), the message is silently dropped for that
    /// subscriber.
    fn publish(&self, topic: &str, ipc_bytes: &[u8]) -> PyResult<i64> {
        self.check_fork()?;
        let seq = {
            let mut seqs = self.topic_seq.lock().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "topic_seq lock poisoned: {}", e
                ))
            })?;
            let entry = seqs.entry(topic.to_owned()).or_insert(0);
            *entry += 1;
            *entry
        };
        let subs = self.py_subscribers.lock().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "py_subscribers lock poisoned: {}", e
            ))
        })?;
        if let Some(senders) = subs.get(topic) {
            let bytes = ipc_bytes.to_vec();
            for sender in senders {
                // Backpressure: try_send drops the message if the channel
                // is full. The parity test allows receiving fewer than
                // published frames under pressure.
                if let Err(mpsc::TrySendError::Full(_)) =
                    sender.try_send((topic.to_owned(), seq, bytes.clone()))
                {
                    log::warn!(
                        "chili broker: subscriber backpressure on topic={}, dropping seq={}",
                        topic, seq
                    );
                }
            }
        }
        Ok(seq)
    }

    /// Register a Python callback for published frames on the given topics.
    ///
    /// `callback(topic: str, seq: int, ipc_bytes: bytes)` is invoked from
    /// a background Rust thread for each published frame. The callback
    /// must not block — dispatch to an asyncio event loop or queue.
    ///
    /// Each call spawns one background thread per topic that acquires the
    /// GIL only when invoking the callback. This avoids deadlocks with
    /// concurrent `eval()` calls that release the GIL (Phase 6).
    fn subscribe(
        &self,
        py: Python<'_>,
        topics: Vec<String>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        self.check_fork()?;
        let mut subs = self.py_subscribers.lock().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "py_subscribers lock poisoned: {}", e
            ))
        })?;
        for topic in topics {
            let (sender, receiver) =
                mpsc::sync_channel::<(String, i64, Vec<u8>)>(1024);
            let cb = callback.clone_ref(py);
            let shutdown = Arc::clone(&self.broker_shutdown);
            std::thread::Builder::new()
                .name(format!("chili-broker-{}", topic))
                .spawn(move || {
                    while let Ok((topic, seq, bytes)) = receiver.recv() {
                        // Exit immediately if the engine has been dropped —
                        // don't drain the queue and thrash the GIL.
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        Python::with_gil(|py| {
                            let py_bytes = PyBytes::new_bound(py, &bytes);
                            if let Err(e) =
                                cb.call1(py, (topic.as_str(), seq, py_bytes))
                            {
                                log::warn!(
                                    "chili broker callback error: {}", e
                                );
                            }
                        });
                    }
                    // Channel closed — drop callback with GIL for safe
                    // Py_DECREF.
                    Python::with_gil(|_py| drop(cb));
                })
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "failed to spawn broker thread: {}", e
                    ))
                })?;
            subs.entry(topic).or_default().push(sender);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper that mirrors df_to_ipc_bytes but returns PolarsError so it can be
// called outside the GIL. The caller maps the error to a String and re-wraps
// it as a PyErr after re-acquiring the GIL.
// ---------------------------------------------------------------------------
fn df_to_ipc_bytes_unchecked(df: &mut DataFrame) -> Result<Vec<u8>, polars::error::PolarsError> {
    let mut buf = Vec::new();
    IpcStreamWriter::new(&mut buf).finish(df)?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

#[pymodule]
fn chili(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    // Phase 13 — register structured exception types so Python callers
    // can `from chili import ChiliError, PepperParseError, ...`
    m.add("ChiliError", ChiliError::type_object_bound(py))?;
    m.add("PepperParseError", PepperParseError::type_object_bound(py))?;
    m.add("PepperEvalError", PepperEvalError::type_object_bound(py))?;
    m.add("PartitionError", PartitionError::type_object_bound(py))?;
    m.add("TypeMismatchError", TypeMismatchError::type_object_bound(py))?;
    m.add("NameError", NameError::type_object_bound(py))?;
    m.add("SerializationError", SerializationError::type_object_bound(py))?;
    Ok(())
}
