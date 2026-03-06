//! PyO3 bindings exposing the check engine to Python.
//!
//! The Python module is importable as:
//! ```python
//! from ironlayer_check_engine import CheckEngine, CheckConfig, CheckResult, quick_check
//! ```
//!
//! All public types (CheckEngine, CheckConfig, CheckResult, CheckDiagnostic,
//! Severity, CheckCategory, Dialect) are registered as PyO3 classes.

use std::path::Path;

use pyo3::prelude::*;

use crate::config::CheckConfig;
use crate::engine::CheckEngine as RustCheckEngine;
use crate::types::{CheckCategory, CheckDiagnostic, CheckResult, Dialect, Severity};

/// Python-facing CheckEngine wrapper.
///
/// Usage from Python:
/// ```python
/// from ironlayer_check_engine import CheckEngine, CheckConfig
/// config = CheckConfig()
/// engine = CheckEngine(config)
/// result = engine.check("/path/to/project")
/// ```
#[pyclass(name = "CheckEngine")]
pub struct PyCheckEngine {
    inner: RustCheckEngine,
}

#[pymethods]
impl PyCheckEngine {
    /// Create a new CheckEngine with the given configuration.
    #[new]
    fn new(config: CheckConfig) -> Self {
        Self {
            inner: RustCheckEngine::new(config),
        }
    }

    /// Run all checks on the project at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` — Path to the project root directory.
    ///
    /// # Returns
    ///
    /// A `CheckResult` containing all diagnostics and summary counts.
    fn check(&self, path: &str) -> PyResult<CheckResult> {
        let root = Path::new(path);
        if !root.is_dir() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Path '{}' is not a directory",
                path
            )));
        }
        Ok(self.inner.check(root))
    }

    /// Return a human-readable string representation.
    fn __repr__(&self) -> String {
        "CheckEngine(...)".to_owned()
    }
}

/// One-shot check function (no config required).
///
/// Equivalent to `CheckEngine(CheckConfig()).check(path)`.
///
/// # Arguments
///
/// * `path` — Path to the project root directory.
///
/// # Returns
///
/// A `CheckResult` with all diagnostics.
///
/// # Errors
///
/// Returns a `PyErr` if the path is not a valid directory.
#[pyfunction]
pub fn quick_check(path: &str) -> PyResult<CheckResult> {
    let root = Path::new(path);
    if !root.is_dir() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Path '{}' is not a directory",
            path
        )));
    }
    let config = CheckConfig::default();
    let engine = RustCheckEngine::new(config);
    Ok(engine.check(root))
}

/// Register all PyO3 classes and functions into the Python module.
///
/// This is called from `lib.rs` to populate the module.
pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCheckEngine>()?;
    m.add_class::<CheckConfig>()?;
    m.add_class::<CheckResult>()?;
    m.add_class::<CheckDiagnostic>()?;
    m.add_class::<Severity>()?;
    m.add_class::<CheckCategory>()?;
    m.add_class::<Dialect>()?;
    m.add_function(wrap_pyfunction!(quick_check, m)?)?;
    Ok(())
}
