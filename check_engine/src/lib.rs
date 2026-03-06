//! IronLayer Check Engine — Rust-powered validation for SQL models.
//!
//! This crate provides the `ironlayer_check_engine` Python module via PyO3,
//! implementing fast parallel validation of SQL models, YAML schemas, naming
//! conventions, ref integrity, and project structure.
//!
//! The module is bundled inside the `ironlayer-core` wheel and imported as:
//! ```python
//! from ironlayer_check_engine import CheckEngine, CheckConfig, CheckResult
//! ```

// PyO3-generated code triggers this lint on PyResult return types — suppress globally.
#![allow(clippy::useless_conversion)]

pub mod cache;
pub mod checkers;
pub mod config;
pub mod discovery;
pub mod engine;
pub mod pyo3_bindings;
pub mod reporter;
pub mod sql_lexer;
pub mod types;

use pyo3::prelude::*;

/// The Python module exposed by this crate.
///
/// Importable as: `from ironlayer_check_engine import CheckEngine`
#[pymodule]
fn ironlayer_check_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize Rust logging → Python logging bridge
    pyo3_log::init();

    pyo3_bindings::register_module(m)?;

    Ok(())
}
