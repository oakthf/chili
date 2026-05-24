//! ADR-0007 (Sprint 23) — chili-py implementation of `ExternalFnDispatcher`.
//!
//! Holds a name → Py<PyAny> registry; dispatches pepper-side calls into
//! the registered Python callables.
//!
//! ## Lock discipline (load-bearing — audit MC-3)
//!
//! `callables: RwLock<HashMap<String, Py<PyAny>>>`. Write-lock is taken
//! ONLY in `register` / `unregister`, NEVER held while calling Python.
//! The `dispatch()` path clones `Py<PyAny>` out under a brief read-lock
//! (with the GIL held — `clone_ref` requires it) and then invokes the
//! callable outside the lock. Concurrent register + dispatch is
//! exercised in both `crates/chili-core/tests/external_fn_test.rs::
//! concurrent_dispatch_no_deadlock` (chili-core side, against the
//! `external_dispatcher` RwLock on EngineState) and
//! `crates/chili-py/tests/test_register_fn.py` (chili-py side, against
//! the `callables` RwLock here).
//!
//! ## GIL semantics
//!
//! `dispatch()` is called from pepper eval — chili-core's eval path has
//! already released the GIL (chili-py's `eval` does `py.detach`). We
//! re-acquire the GIL via `Python::attach(...)` ONLY for the duration
//! of the callback invocation. The callable may freely call back into
//! engine methods (`engine.fn_call`, `engine.set_var`, ...); each of
//! those pymethods does its own `py.detach` to release the GIL before
//! re-entering chili-core. No held lock crosses the GIL release/acquire
//! boundary — verified by audit + ADR-0007 §3.

use std::collections::HashMap;
use std::sync::RwLock;

use chili_core::{ExternalFnDispatcher, SpicyError, SpicyObj, SpicyResult};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::{spicy_from_py_bound, spicy_to_py};

pub struct PyExternalDispatcher {
    callables: RwLock<HashMap<String, Py<PyAny>>>,
}

impl PyExternalDispatcher {
    pub fn new() -> Self {
        Self {
            callables: RwLock::new(HashMap::new()),
        }
    }

    /// Register (or replace) a Python callable under `name`. Takes the
    /// write-lock briefly; never holds it while calling Python.
    pub fn register(&self, name: &str, callable: Py<PyAny>) {
        self.callables
            .write()
            .expect("callables RwLock poisoned")
            .insert(name.to_owned(), callable);
    }

    /// Remove a registered callable. Returns true if a callable was
    /// removed, false if not registered.
    pub fn unregister(&self, name: &str) -> bool {
        self.callables
            .write()
            .expect("callables RwLock poisoned")
            .remove(name)
            .is_some()
    }
}

impl ExternalFnDispatcher for PyExternalDispatcher {
    fn dispatch(&self, name: &str, args: &[&SpicyObj]) -> SpicyResult<SpicyObj> {
        // Step 1 — clone the Py<PyAny> out from under a brief read lock,
        // with the GIL acquired (clone_ref requires Python<'_>). Drop the
        // lock before invoking.
        let callable: Option<Py<PyAny>> = Python::attach(|py| {
            self.callables
                .read()
                .ok()
                .and_then(|guard| guard.get(name).map(|c| c.clone_ref(py)))
        });
        let callable = callable.ok_or_else(|| {
            SpicyError::EvalErr(format!(
                "no Python callable registered for external fn '{}'",
                name
            ))
        })?;

        // Step 2 — invoke. The lock is no longer held; the callable may
        // freely re-enter the engine.
        Python::attach(|py| invoke_python(py, &callable, name, args))
    }
}

fn invoke_python(
    py: Python<'_>,
    callable: &Py<PyAny>,
    name: &str,
    args: &[&SpicyObj],
) -> SpicyResult<SpicyObj> {
    // Convert chili args → Python objects.
    let mut py_args: Vec<Py<PyAny>> = Vec::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        let v = spicy_to_py(py, (*a).clone()).map_err(|e| {
            SpicyError::EvalErr(format!(
                "external fn '{}' arg {} conversion failed: {}",
                name, i, e
            ))
        })?;
        py_args.push(v);
    }
    let bound_args: Vec<Bound<'_, PyAny>> =
        py_args.iter().map(|p| p.bind(py).clone()).collect();
    let py_tuple = PyTuple::new(py, bound_args)
        .map_err(|e| SpicyError::EvalErr(format!("PyTuple::new: {}", e)))?;

    // Invoke. Python exceptions become stringified ChiliError with
    // traceback embedded (ADR-0007 §5).
    let result = callable.bind(py).call1(py_tuple).map_err(|e| {
        let msg = format_pyerr_with_traceback(py, &e, name);
        SpicyError::EvalErr(msg)
    })?;

    // Convert Python return → SpicyObj.
    spicy_from_py_bound(&result).map_err(|e| {
        SpicyError::EvalErr(format!(
            "external fn '{}' return conversion failed: {}",
            name, e
        ))
    })
}

/// Format a PyErr as a chili-side error message with the Python traceback
/// embedded. Best-effort: if traceback formatting fails for any reason
/// (e.g., the `traceback` module can't be imported, the traceback is
/// missing), falls back to a plain `<ExcType>: <repr>` shape.
fn format_pyerr_with_traceback(py: Python<'_>, e: &PyErr, name: &str) -> String {
    let type_name = e.get_type(py).name().map(|s| s.to_string()).unwrap_or_else(
        |_| "PyErr".to_string(),
    );
    let value_repr = e.value(py).repr().map(|r| r.to_string()).unwrap_or_else(
        |_| "<unrepresentable>".to_string(),
    );

    // Try to format the traceback via the `traceback` module.
    let tb_text = match py.import("traceback") {
        Ok(traceback_mod) => match e.traceback(py) {
            Some(tb) => match traceback_mod.call_method1("format_tb", (tb,)) {
                Ok(frames_obj) => match frames_obj.extract::<Vec<String>>() {
                    Ok(frames) => frames.join("").trim_end().to_string(),
                    Err(_) => String::new(),
                },
                Err(_) => String::new(),
            },
            None => String::new(),
        },
        Err(_) => String::new(),
    };

    if tb_text.is_empty() {
        format!(
            "external fn '{}' raised: {}: {}",
            name, type_name, value_repr
        )
    } else {
        format!(
            "external fn '{}' raised: {}: {}\n{}",
            name, type_name, value_repr, tb_text
        )
    }
}
