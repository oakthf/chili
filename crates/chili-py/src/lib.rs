// Use mimalloc as the global allocator for the chili-py cdylib. Same
// rationale as chili-bin: polars' allocation pattern fits mimalloc much
// better than the system allocator.
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, LazyLock, Mutex};

use chili_core::constant::{NS_IN_DAY, UNIX_EPOCH_DAY};
use chili_core::{EngineState, Func, SpicyError, SpicyObj, SpicyResult, Stack};
use chili_op::{write_partition_py, BUILT_IN_FN};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use indexmap::IndexMap;
use polars::frame::DataFrame;
use polars::io::ipc::IpcStreamWriter;
use polars::io::SerWriter;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList, PyString, PyTime,
    PyTuple, PyTzInfo,
};
use pyo3::{create_exception, intern};
use pyo3_polars::{PyDataFrame, PySeries};

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
// ---------------------------------------------------------------------------
create_exception!(chili, ChiliError, PyRuntimeError);
create_exception!(chili, PepperParseError, ChiliError);
create_exception!(chili, PepperEvalError, ChiliError);
create_exception!(chili, PartitionError, ChiliError);
create_exception!(chili, TypeMismatchError, ChiliError);
create_exception!(chili, NameError, ChiliError);
create_exception!(chili, SerializationError, ChiliError);

/// Map a SpicyError to the most specific Python exception class.
fn spicy_err_to_py(err: &SpicyError) -> PyErr {
    match err {
        SpicyError::ParserErr(_) => PepperParseError::new_err(err.to_string()),
        SpicyError::EvalErr(msg) => {
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
// Register `.log.info` / `.log.warn` / `.log.debug` / `.log.error`.
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
// SpicyObj <-> Python conversion helpers (ported from upstream bf9fa14).
// These replace the previous Arrow-IPC bytes round-trip path entirely.
// `pyo3_polars::PyDataFrame` is a zero-copy wrapper over polars::DataFrame —
// no serialization, no buffer copy.
// ---------------------------------------------------------------------------

fn unwrap_return(mut o: SpicyObj) -> SpicyObj {
    while let SpicyObj::Return(inner) = o {
        o = *inner;
    }
    o
}

#[allow(dead_code)] // reserved for future set_var / fn_call surface
fn spicy_from_py_bound(any: &Bound<'_, PyAny>) -> PyResult<SpicyObj> {
    if any.is_instance_of::<PyBool>() {
        Ok(SpicyObj::Boolean(any.extract::<bool>()?))
    } else if any.is_instance_of::<PyInt>() {
        Ok(SpicyObj::I64(any.extract::<i64>()?))
    } else if any.is_instance_of::<PyFloat>() {
        Ok(SpicyObj::F64(any.extract::<f64>()?))
    } else if any.is_instance_of::<PyString>() {
        Ok(SpicyObj::Symbol(any.extract::<String>()?))
    } else if any.is_instance_of::<PyBytes>() {
        let bytes = any.cast::<PyBytes>()?;
        Ok(SpicyObj::String(String::from_utf8(bytes.as_bytes().to_vec())?))
    } else if any.hasattr(intern!(any.py(), "_s"))? {
        let series = any.extract::<PySeries>()?.into();
        Ok(SpicyObj::Series(series))
    } else if any.hasattr(intern!(any.py(), "_df"))? {
        let df = any.extract::<PyDataFrame>()?.into();
        Ok(SpicyObj::DataFrame(df))
    } else if any.is_none() {
        Ok(SpicyObj::Null)
    } else if any.is_instance_of::<PyDateTime>() {
        let datetime: DateTime<Utc> = any.extract()?;
        Ok(SpicyObj::Timestamp(datetime.timestamp_nanos_opt().unwrap_or(0)))
    } else if any.is_instance_of::<PyDate>() {
        let dt: NaiveDate = any.cast::<PyDate>()?.extract()?;
        Ok(SpicyObj::Date(dt.num_days_from_ce() - UNIX_EPOCH_DAY))
    } else if any.is_instance_of::<PyTime>() {
        let dt: NaiveTime = any.cast::<PyTime>()?.extract()?;
        Ok(SpicyObj::Time(
            dt.nanosecond() as i64 + dt.num_seconds_from_midnight() as i64 * 1_000_000_000,
        ))
    } else if any.is_instance_of::<PyDelta>() {
        let delta: Duration = any.extract()?;
        Ok(SpicyObj::Duration(delta.num_nanoseconds().unwrap_or(0)))
    } else if any.is_instance_of::<PyDict>() {
        let py_dict = any.cast::<PyDict>()?;
        let mut dict = IndexMap::with_capacity(py_dict.len());
        for (k, v) in py_dict.into_iter() {
            let k = k.extract::<String>().map_err(|_| {
                ChiliError::new_err(format!("Requires str as key, got {:?}", k.get_type()))
            })?;
            dict.insert(k, spicy_from_py_bound(&v)?);
        }
        Ok(SpicyObj::Dict(dict))
    } else if any.is_instance_of::<PyList>() {
        let py_list = any.cast::<PyList>()?;
        let mut k_list = Vec::with_capacity(py_list.len());
        for py_any in py_list {
            k_list.push(spicy_from_py_bound(&py_any)?);
        }
        Ok(SpicyObj::MixedList(k_list))
    } else if any.is_instance_of::<PyTuple>() {
        let py_tuple = any.cast::<PyTuple>()?;
        let mut k_tuple = Vec::with_capacity(py_tuple.len());
        for py_any in py_tuple {
            k_tuple.push(spicy_from_py_bound(&py_any)?);
        }
        Ok(SpicyObj::MixedList(k_tuple))
    } else {
        Err(ChiliError::new_err(format!(
            "Unsupported Python type for chili conversion: {}",
            any.get_type().name()?
        )))
    }
}

fn spicy_to_py(py: Python<'_>, obj: SpicyObj) -> PyResult<Py<PyAny>> {
    let obj = unwrap_return(obj);
    match obj {
        SpicyObj::Null => Ok(py.None()),
        SpicyObj::Boolean(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
        SpicyObj::U8(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SpicyObj::I16(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SpicyObj::I32(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SpicyObj::I64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SpicyObj::F32(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SpicyObj::F64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        // String → Python bytes (matches q-style char vector semantics).
        SpicyObj::String(v) => Ok(v.as_bytes().into_pyobject(py)?.into_any().unbind()),
        SpicyObj::Symbol(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        SpicyObj::Date(v) => Ok(PyDate::from_timestamp(py, v as i64 * 86400)?
            .into_any()
            .unbind()),
        SpicyObj::Time(v) => {
            let seconds = v / 1_000_000_000;
            let microseconds = v % 1_000_000_000 / 1_000;
            let hour = seconds / 3600 % 24;
            let minute = seconds / 60 % 60;
            let second = seconds % 60;
            let utc = PyTzInfo::utc(py)?.to_owned();
            Ok(PyTime::new(
                py,
                hour as u8,
                minute as u8,
                second as u8,
                microseconds as u32,
                Some(&utc),
            )?
            .into_any()
            .unbind())
        }
        SpicyObj::Datetime(v) => {
            let utc = PyTzInfo::utc(py)?.to_owned();
            Ok(
                PyDateTime::from_timestamp(py, v as f64 / 1000.0, Some(&utc))?
                    .into_any()
                    .unbind(),
            )
        }
        SpicyObj::Timestamp(v) => {
            let utc = PyTzInfo::utc(py)?.to_owned();
            Ok(
                PyDateTime::from_timestamp(py, v as f64 / 1_000_000_000.0, Some(&utc))?
                    .into_any()
                    .unbind(),
            )
        }
        SpicyObj::Duration(v) => {
            let abs_v = v.abs();
            let sign = if v < 0 { -1 } else { 1 };
            let days = abs_v / NS_IN_DAY;
            let seconds = abs_v / 1_000_000_000 % 86400;
            let microseconds = v % 1_000_000_000 / 1_000;
            Ok(PyDelta::new(
                py,
                days as i32 * sign,
                seconds as i32,
                microseconds as i32,
                false,
            )?
            .into_any()
            .unbind())
        }
        SpicyObj::MixedList(items) => {
            let mut list = Vec::with_capacity(items.len());
            for it in items {
                list.push(spicy_to_py(py, it)?);
            }
            Ok(PyList::new(py, &list)?.into_any().unbind())
        }
        SpicyObj::Dict(map) => {
            let d = PyDict::new(py);
            for (k, v) in map {
                d.set_item(k, spicy_to_py(py, v)?)?;
            }
            Ok(d.into_any().unbind())
        }
        // Zero-copy: pyo3_polars::PyDataFrame wraps the polars DataFrame
        // directly as a Python object — no IPC round-trip.
        SpicyObj::DataFrame(df) => Ok(PyDataFrame(df).into_pyobject(py)?.into_any().unbind()),
        SpicyObj::Series(s) => Ok(PySeries(s).into_pyobject(py)?.into_any().unbind()),
        SpicyObj::Err(msg) => Err(ChiliError::new_err(msg)),
        other => Ok(other.to_string().into_pyobject(py)?.into_any().unbind()),
    }
}

// ---------------------------------------------------------------------------
// Date helper: parse "YYYY.MM.DD" → days since Unix epoch.
// ---------------------------------------------------------------------------

fn parse_date_to_days(date_str: &str) -> Result<i32, String> {
    let parsed = NaiveDate::parse_from_str(date_str, "%Y.%m.%d")
        .map_err(|e| format!("invalid date '{}': {} (expected YYYY.MM.DD)", date_str, e))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    i32::try_from((parsed - epoch).num_days())
        .map_err(|e| format!("date '{}' is out of i32 range: {}", date_str, e))
}

// ---------------------------------------------------------------------------
// Engine Python class — high-level wrapper around chili_core::EngineState.
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
    py_subscribers: Arc<Mutex<HashMap<String, Vec<mpsc::SyncSender<(String, i64, Vec<u8>)>>>>>,
    topic_seq: Arc<Mutex<HashMap<String, i64>>>,
    broker_shutdown: Arc<AtomicBool>,
}

impl Drop for Engine {
    fn drop(&mut self) {
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

    fn check_fork(&self) -> PyResult<()> {
        if std::process::id() != self.init_pid {
            return Err(PyRuntimeError::new_err(format!(
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

    fn unload(&self) -> PyResult<()> {
        self.check_fork()?;
        self.state.clear_par_df().map_err(|e| spicy_err_to_py(&e))
    }

    fn table_count(&self) -> usize {
        self.state.par_df_count()
    }

    fn parse_cache_len(&self) -> usize {
        self.state.parse_cache_len()
    }

    fn query_plan(&self, py: Python<'_>, query: &str, hdb_path: &str) -> PyResult<String> {
        self.check_fork()?;
        let query = query.to_owned();
        let hdb_path = hdb_path.to_owned();
        py.detach(move || -> Result<String, String> {
            let plan_state = EngineState::new(false, true, true);
            plan_state.register_fn(&LOG_FN);
            plan_state.register_fn(&BUILT_IN_FN);
            plan_state.load_par_df(&hdb_path).map_err(|e| e.to_string())?;
            let query_obj = SpicyObj::String(query);
            let mut stack = Stack::new(None, 0, 0, "");
            let obj = plan_state
                .eval(&mut stack, &query_obj, "plan.chi")
                .map_err(|e| e.to_string())?;
            match obj {
                SpicyObj::LazyFrame(lf) => lf.describe_plan().map_err(|e| e.to_string()),
                SpicyObj::DataFrame(_) => Err(
                    "query collected eagerly — lazy plan not available for this query shape"
                        .into(),
                ),
                other => Err(format!(
                    "query returned {}, expected LazyFrame",
                    other.get_type_name()
                )),
            }
        })
        .map_err(PyRuntimeError::new_err)
    }

    fn load(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.check_fork()?;
        let state = Arc::clone(&self.state);
        let path = path.to_owned();
        py.detach(move || state.load_par_df(&path).map_err(|e| spicy_err_to_py(&e)))
    }

    /// Evaluate a Chili/pepper query and return the result as a Python
    /// object. polars DataFrames are returned via `pyo3_polars::PyDataFrame`
    /// (zero-copy — no Arrow IPC round-trip).
    ///
    /// **GIL release.** The entire query body (parse → eval → collect)
    /// runs inside `py.allow_threads`. Other Python threads can drive
    /// independent queries on the same engine in parallel; concurrent
    /// throughput scales linearly with thread count up to the polars
    /// rayon pool size. Pre-bytes-removal: 8-thread = 6.10× single-thread.
    fn eval(&self, py: Python<'_>, query: &str) -> PyResult<Py<PyAny>> {
        self.check_fork()?;
        let state = Arc::clone(&self.state);
        let query = query.to_owned();
        // Run parse+eval+collect off the GIL. Materialize LazyFrames into
        // DataFrames inside the closure so the GIL-free phase covers the
        // expensive part. Carry the SpicyObj out and convert to Python on
        // the GIL side (pyo3 conversions need the GIL).
        let obj = py
            .detach(move || -> Result<SpicyObj, SpicyError> {
                let query_obj = SpicyObj::String(query);
                let mut stack = Stack::new(None, 0, 0, "");
                let obj = state.eval(&mut stack, &query_obj, "py.chi")?;
                // Eagerly collect LazyFrames inside the closure so the
                // collect cost stays off the GIL.
                let obj = match obj {
                    SpicyObj::LazyFrame(lf) => SpicyObj::DataFrame(
                        lf.collect().map_err(|e| SpicyError::EvalErr(e.to_string()))?,
                    ),
                    other => other,
                };
                Ok(obj)
            })
            .map_err(|e| spicy_err_to_py(&e))?;
        spicy_to_py(py, obj)
    }

    /// Append a polars DataFrame as a new partition shard.
    ///
    /// hdb_path : root HDB directory (must exist)
    /// table    : table name
    /// date     : partition date "YYYY.MM.DD"
    /// sort_columns : optional sort keys for the partition. When non-empty,
    ///                forces a small parquet row_group_size (16384) so
    ///                later queries can prune row groups via column stats
    ///                — required for `where symbol=X` to skip row groups
    ///                (Phase 10 / WL 2.2).
    ///
    /// GIL is released for the entire write.
    #[pyo3(signature = (df, hdb_path, table, date, sort_columns=Vec::new()))]
    fn wpar(
        &self,
        py: Python<'_>,
        df: PyDataFrame,
        hdb_path: &str,
        table: &str,
        date: &str,
        sort_columns: Vec<String>,
    ) -> PyResult<i64> {
        self.check_fork()?;
        let frame: DataFrame = df.into();
        let hdb_path = hdb_path.to_owned();
        let table = table.to_owned();
        let date = date.to_owned();
        py.detach(move || -> Result<i64, String> {
            let days = parse_date_to_days(&date)?;
            let date_obj = SpicyObj::Date(days);
            let sort_refs: Vec<&str> = sort_columns.iter().map(|s| s.as_str()).collect();
            let obj = write_partition_py(&hdb_path, &date_obj, &table, &frame, &sort_refs, false)
                .map_err(|e| e.to_string())?;
            Ok(match obj {
                SpicyObj::I64(n) => n,
                _ => 0,
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    /// Replace an existing partition with the given DataFrame.
    /// Deletes all `{date}_*` shard files first, then writes a single
    /// fresh `_0000` file. Use for in-place HDB rewrites (dtype
    /// migrations, re-sorting, etc.). Schema validation is enforced.
    #[pyo3(signature = (df, hdb_path, table, date, sort_columns=Vec::new()))]
    fn overwrite_partition(
        &self,
        py: Python<'_>,
        df: PyDataFrame,
        hdb_path: &str,
        table: &str,
        date: &str,
        sort_columns: Vec<String>,
    ) -> PyResult<i64> {
        self.check_fork()?;
        let frame: DataFrame = df.into();
        let hdb_path = hdb_path.to_owned();
        let table = table.to_owned();
        let date = date.to_owned();
        py.detach(move || -> Result<i64, String> {
            let parsed = NaiveDate::parse_from_str(&date, "%Y.%m.%d")
                .map_err(|e| format!("invalid date '{}': {}", date, e))?;
            let par_str = format!(
                "{}.{:02}.{:02}",
                parsed.year(),
                parsed.month(),
                parsed.day()
            );
            let canon_hdb = std::fs::canonicalize(&hdb_path)
                .map_err(|e| format!("cannot resolve '{}': {}", hdb_path, e))?;
            let table_dir = canon_hdb.join(&table);
            if table_dir.exists() {
                let prefix = format!("{}_", par_str);
                for entry in std::fs::read_dir(&table_dir)
                    .map_err(|e| format!("read_dir: {}", e))?
                {
                    let entry = entry.map_err(|e| format!("dir entry: {}", e))?;
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with(&prefix) {
                            std::fs::remove_file(entry.path()).map_err(|e| {
                                format!("remove {}: {}", entry.path().display(), e)
                            })?;
                        }
                    }
                }
            }
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = i32::try_from((parsed - epoch).num_days())
                .map_err(|e| format!("date out of range: {}", e))?;
            let date_obj = SpicyObj::Date(days);
            let sort_refs: Vec<&str> = sort_columns.iter().map(|s| s.as_str()).collect();
            let obj = write_partition_py(
                &canon_hdb.to_string_lossy(),
                &date_obj,
                &table,
                &frame,
                &sort_refs,
                false,
            )
            .map_err(|e| e.to_string())?;
            Ok(match obj {
                SpicyObj::I64(n) => n,
                _ => 0,
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    // -----------------------------------------------------------------------
    // Phase 16 — Broker bindings (WL 1.1)
    //
    // In-process pub/sub: publish() fans out IPC bytes to Python callbacks
    // registered via subscribe(). Each subscriber gets a bounded mpsc
    // channel (capacity 1024); a background thread per subscription reads
    // from the channel and invokes the callback with the GIL held.
    // -----------------------------------------------------------------------

    /// Publish raw IPC bytes to all subscribers of a topic.
    /// Returns the publisher-side per-topic monotonic sequence number
    /// (starts at 1). Backpressure: full channels drop the message.
    fn publish(&self, topic: &str, ipc_bytes: &[u8]) -> PyResult<i64> {
        self.check_fork()?;
        let seq = {
            let mut seqs = self.topic_seq.lock().map_err(|e| {
                PyRuntimeError::new_err(format!("topic_seq lock poisoned: {}", e))
            })?;
            let entry = seqs.entry(topic.to_owned()).or_insert(0);
            *entry += 1;
            *entry
        };
        let subs = self.py_subscribers.lock().map_err(|e| {
            PyRuntimeError::new_err(format!("py_subscribers lock poisoned: {}", e))
        })?;
        if let Some(senders) = subs.get(topic) {
            let bytes = ipc_bytes.to_vec();
            for sender in senders {
                if let Err(mpsc::TrySendError::Full(_)) =
                    sender.try_send((topic.to_owned(), seq, bytes.clone()))
                {
                    log::warn!(
                        "chili broker: subscriber backpressure on topic={}, dropping seq={}",
                        topic,
                        seq
                    );
                }
            }
        }
        Ok(seq)
    }

    /// Serialize a polars DataFrame and publish to subscribers.
    /// Faster than the Python wrapper's `df.write_ipc_stream()` path —
    /// serialization happens Rust-side with the GIL released.
    fn tick_upd(&self, py: Python<'_>, table: &str, df: PyDataFrame) -> PyResult<i64> {
        self.check_fork()?;
        let mut frame: DataFrame = df.into();
        let bytes = py
            .detach(move || -> Result<Vec<u8>, polars::error::PolarsError> {
                let mut buf = Vec::new();
                IpcStreamWriter::new(&mut buf).finish(&mut frame)?;
                Ok(buf)
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        self.publish(table, &bytes)
    }

    /// Register a Python callback for published frames on the given topics.
    /// Callback signature: `callback(topic: str, seq: int, ipc_bytes: bytes)`.
    /// Each call spawns one background thread per topic that acquires the
    /// GIL only when invoking the callback.
    fn subscribe(
        &self,
        py: Python<'_>,
        topics: Vec<String>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        self.check_fork()?;
        let mut subs = self.py_subscribers.lock().map_err(|e| {
            PyRuntimeError::new_err(format!("py_subscribers lock poisoned: {}", e))
        })?;
        for topic in topics {
            let (sender, receiver) = mpsc::sync_channel::<(String, i64, Vec<u8>)>(1024);
            let cb = callback.clone_ref(py);
            let shutdown = Arc::clone(&self.broker_shutdown);
            std::thread::Builder::new()
                .name(format!("chili-broker-{}", topic))
                .spawn(move || {
                    while let Ok((topic, seq, bytes)) = receiver.recv() {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        Python::attach(|py| {
                            let py_bytes = PyBytes::new(py, &bytes);
                            if let Err(e) = cb.call1(py, (topic.as_str(), seq, py_bytes)) {
                                log::warn!("chili broker callback error: {}", e);
                            }
                        });
                    }
                    Python::attach(|_py| drop(cb));
                })
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("failed to spawn broker thread: {}", e))
                })?;
            subs.entry(topic).or_default().push(sender);
        }
        Ok(())
    }

    /// Broadcast end-of-day signal to ALL subscribers regardless of topic.
    /// Subscribers see `topic == "__eod__"` in their callback.
    fn broker_eod(&self, eod_message: &[u8]) -> PyResult<()> {
        self.check_fork()?;
        let subs = self.py_subscribers.lock().map_err(|e| {
            PyRuntimeError::new_err(format!("py_subscribers lock poisoned: {}", e))
        })?;
        let bytes = eod_message.to_vec();
        for senders in subs.values() {
            for sender in senders {
                let _ = sender.try_send(("__eod__".to_owned(), 0, bytes.clone()));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

#[pymodule]
fn chili(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add_class::<Engine>()?;
    // Phase 13 — register structured exception types so Python callers
    // can `from chili import ChiliError, PepperParseError, ...`
    m.add("ChiliError", py.get_type::<ChiliError>())?;
    m.add("PepperParseError", py.get_type::<PepperParseError>())?;
    m.add("PepperEvalError", py.get_type::<PepperEvalError>())?;
    m.add("PartitionError", py.get_type::<PartitionError>())?;
    m.add("TypeMismatchError", py.get_type::<TypeMismatchError>())?;
    m.add("NameError", py.get_type::<NameError>())?;
    m.add("SerializationError", py.get_type::<SerializationError>())?;
    Ok(())
}
