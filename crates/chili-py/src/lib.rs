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
use std::sync::{Arc, LazyLock};

use chili_core::{EngineState, Func, SpicyObj, SpicyResult, Stack};
use chili_op::{write_partition_py, BUILT_IN_FN};
use polars::io::{SerReader, SerWriter};
use polars::io::ipc::{IpcStreamReader, IpcStreamWriter};
use polars::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

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
        Engine { state: arc_state }
    }

    /// Load a partitioned HDB directory.
    ///
    /// Releases the GIL for the duration of the load (which can take up to
    /// hundreds of milliseconds for large HDBs and includes filesystem
    /// traversal + parquet schema reads). Other Python threads can run
    /// during this time.
    fn load(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let state = Arc::clone(&self.state);
        let path = path.to_owned();
        py.allow_threads(move || {
            state
                .load_par_df(&path)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
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
        let state = Arc::clone(&self.state);
        let query = query.to_owned();
        // Run the entire query OFF the GIL.
        let bytes = py.allow_threads(move || -> Result<Vec<u8>, String> {
            let query_obj = SpicyObj::String(query);
            let mut stack = Stack::new(None, 0, 0, "");
            let obj = state
                .eval(&mut stack, &query_obj, "py.chi")
                .map_err(|e| e.to_string())?;
            let mut df = match obj {
                SpicyObj::DataFrame(df) => df,
                SpicyObj::LazyFrame(lf) => lf.collect().map_err(|e| e.to_string())?,
                other => {
                    return Err(format!(
                        "eval returned {}, expected DataFrame",
                        other.get_type_name()
                    ));
                }
            };
            // df_to_ipc_bytes returns PyResult; map the PyErr to a String
            // here so we don't try to construct a PyErr while the GIL is
            // released. Re-wrap on the GIL side below.
            df_to_ipc_bytes_unchecked(&mut df).map_err(|e| e.to_string())
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Write a polars DataFrame (passed as Arrow IPC bytes) to an HDB partition.
    ///
    /// ipc_bytes : Arrow IPC stream bytes produced by polars DataFrame.write_ipc()
    /// hdb_path  : root HDB directory (must exist and be canonicalisable)
    /// table     : table name
    /// date      : partition date string "YYYY.MM.DD"
    ///
    /// Proposal A — releases the GIL for the IPC bytes → DataFrame parse,
    /// the date parse, and the partition write. mdata's batch ingest paths
    /// can issue many `wpar` calls concurrently from a Python thread pool
    /// without GIL contention.
    fn wpar(&self, py: Python<'_>, ipc_bytes: &[u8], hdb_path: &str, table: &str, date: &str) -> PyResult<i64> {
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
            let obj = write_partition_py(&hdb_path, &date_obj, &table, &df, &[], false)
                .map_err(|e| e.to_string())?;
            Ok(match obj {
                SpicyObj::I64(n) => n,
                _ => 0,
            })
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        Ok(n)
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
fn chili(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    Ok(())
}
