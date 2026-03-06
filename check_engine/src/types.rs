//! Core type definitions for the IronLayer Check Engine.
//!
//! These types form the contract between Rust and Python via PyO3. They mirror
//! existing IronLayer Pydantic models exactly where applicable:
//!
//! - [`Dialect`] mirrors `core_engine.sql_toolkit._types.Dialect`
//! - [`Severity`] unifies `sql_guard.Severity` and `schema_validator.ViolationSeverity`
//! - [`CheckCategory`] groups diagnostics by check type
//! - [`CheckDiagnostic`] is the atomic unit of check output
//! - [`CheckResult`] is the aggregate result of running all checks
//! - [`DiscoveredModel`] is lightweight model metadata (no full AST)

use std::collections::HashMap;

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// Mirrors `core_engine.sql_toolkit._types.Dialect`.
///
/// Python values are lowercase strings: `"databricks"`, `"duckdb"`, `"redshift"`.
/// The PyO3 enum exposes PascalCase variants but serializes to lowercase via serde.
#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    /// Databricks SQL dialect (default).
    #[default]
    Databricks,
    /// DuckDB SQL dialect.
    DuckDB,
    /// Amazon Redshift SQL dialect.
    Redshift,
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Databricks => write!(f, "databricks"),
            Self::DuckDB => write!(f, "duckdb"),
            Self::Redshift => write!(f, "redshift"),
        }
    }
}

/// Check engine unified severity level.
///
/// Unifies two existing IronLayer severity systems:
///
/// | Source System | Source Level | Maps To |
/// |---|---|---|
/// | `sql_guard.Severity` | `CRITICAL` | `Error` |
/// | `sql_guard.Severity` | `HIGH` | `Warning` |
/// | `sql_guard.Severity` | `MEDIUM` | `Warning` |
/// | `schema_validator.ViolationSeverity` | `BREAKING` | `Error` |
/// | `schema_validator.ViolationSeverity` | `WARNING` | `Warning` |
/// | `schema_validator.ViolationSeverity` | `INFO` | `Info` |
#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Critical issue that must be fixed before proceeding.
    Error,
    /// Issue that should be addressed but does not block.
    Warning,
    /// Informational notice, no action required.
    Info,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error => write!(f, "error"),
            Self::Warning => write!(f, "warning"),
            Self::Info => write!(f, "info"),
        }
    }
}

/// What kind of check produced a diagnostic.
#[pyclass(eq, eq_int)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CheckCategory {
    /// SQL lexing/parsing issues (SQL001-SQL009).
    SqlSyntax,
    /// Dangerous SQL operations mirroring `sql_guard.py` (SAF001-SAF010).
    SqlSafety,
    /// Model header validation mirroring `model_loader.py` (HDR001-HDR013).
    SqlHeader,
    /// `{{ ref('...') }}` issues mirroring `ref_resolver.py` (REF001-REF006).
    RefResolution,
    /// Column contract violations mirroring `schema_validator.py`.
    SchemaContract,
    /// YAML structure issues (YML001-YML009).
    YamlSchema,
    /// Naming pattern violations (NAME001-NAME008).
    NamingConvention,
    /// dbt-specific project structure issues (DBT001-DBT006).
    DbtProject,
    /// Cross-model consistency issues (CON001-CON005).
    ModelConsistency,
    /// File/directory organization issues.
    FileStructure,
    /// Databricks-specific SQL validation (DBK001-DBK007).
    DatabricksSql,
    /// Incremental model logic validation (INC001-INC005).
    IncrementalLogic,
    /// Performance anti-pattern detection (PERF001-PERF007).
    Performance,
    /// Test adequacy validation (TST001-TST005).
    TestAdequacy,
}

impl std::fmt::Display for CheckCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SqlSyntax => write!(f, "SqlSyntax"),
            Self::SqlSafety => write!(f, "SqlSafety"),
            Self::SqlHeader => write!(f, "SqlHeader"),
            Self::RefResolution => write!(f, "RefResolution"),
            Self::SchemaContract => write!(f, "SchemaContract"),
            Self::YamlSchema => write!(f, "YamlSchema"),
            Self::NamingConvention => write!(f, "NamingConvention"),
            Self::DbtProject => write!(f, "DbtProject"),
            Self::ModelConsistency => write!(f, "ModelConsistency"),
            Self::FileStructure => write!(f, "FileStructure"),
            Self::DatabricksSql => write!(f, "DatabricksSql"),
            Self::IncrementalLogic => write!(f, "IncrementalLogic"),
            Self::Performance => write!(f, "Performance"),
            Self::TestAdequacy => write!(f, "TestAdequacy"),
        }
    }
}

/// A single diagnostic (error, warning, or info) produced by a check rule.
///
/// This is the atomic unit of check output. Every diagnostic must have:
/// - A specific `rule_id` (e.g., `"HDR001"`, `"REF002"`)
/// - An actionable `message` explaining the issue
/// - The correct `severity` level
/// - A `suggestion` when a fix is feasible
#[pyclass(get_all)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDiagnostic {
    /// Rule identifier (e.g., `"SQL001"`, `"REF002"`, `"NAME003"`).
    pub rule_id: String,

    /// Human-readable, actionable message describing the issue.
    pub message: String,

    /// Severity level for this diagnostic.
    pub severity: Severity,

    /// Check category that produced this diagnostic.
    pub category: CheckCategory,

    /// File path relative to project root (always uses forward slashes).
    pub file_path: String,

    /// 1-based line number where the issue occurs (0 if not applicable).
    pub line: u32,

    /// 1-based column number where the issue occurs (0 if not applicable).
    pub column: u32,

    /// The offending text snippet (max 120 chars), if available.
    pub snippet: Option<String>,

    /// Suggested fix, if one exists.
    pub suggestion: Option<String>,

    /// URL to documentation for this rule.
    pub doc_url: Option<String>,
}

#[pymethods]
impl CheckDiagnostic {
    /// Return a human-readable string representation.
    fn __repr__(&self) -> String {
        format!(
            "CheckDiagnostic(rule_id={:?}, severity={}, file={:?}, line={})",
            self.rule_id, self.severity, self.file_path, self.line,
        )
    }
}

/// Aggregate result of running all checks on a project.
///
/// Contains all diagnostics sorted by `(file_path, line, column)`, summary
/// counts, timing information, and the auto-detected project type.
#[pyclass(get_all)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// All diagnostics, sorted by `(file_path, line, column)`.
    pub diagnostics: Vec<CheckDiagnostic>,

    /// Number of files that were checked (not cached).
    pub total_files_checked: u32,

    /// Number of files skipped due to cache hit.
    pub total_files_skipped_cache: u32,

    /// Count of diagnostics with severity `Error`.
    pub total_errors: u32,

    /// Count of diagnostics with severity `Warning`.
    pub total_warnings: u32,

    /// Count of diagnostics with severity `Info`.
    pub total_infos: u32,

    /// Wall-clock milliseconds elapsed for the entire check run.
    pub elapsed_ms: u64,

    /// Auto-detected project type: `"ironlayer"`, `"dbt"`, or `"raw_sql"`.
    pub project_type: String,

    /// Whether the check passed (zero errors).
    pub passed: bool,
}

#[pymethods]
impl CheckResult {
    /// Serialize the result to JSON using the spec §8.2 format.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if serialization fails (should not happen for valid data).
    fn to_json(&self) -> PyResult<String> {
        crate::reporter::to_json(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("JSON serialization failed: {e}"))
        })
    }

    /// Serialize the result to SARIF v2.1.0 JSON for GitHub Code Scanning.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if serialization fails.
    fn to_sarif_json(&self) -> PyResult<String> {
        crate::reporter::to_sarif(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("SARIF serialization failed: {e}"))
        })
    }

    /// Return a human-readable string representation.
    fn __repr__(&self) -> String {
        format!(
            "CheckResult(passed={}, errors={}, warnings={}, infos={}, files={}, elapsed_ms={})",
            self.passed,
            self.total_errors,
            self.total_warnings,
            self.total_infos,
            self.total_files_checked,
            self.elapsed_ms,
        )
    }
}

/// Lightweight metadata about a discovered SQL model file.
///
/// Contains only the information extracted by the Rust engine's header parser
/// and ref extractor — no full AST analysis. This is used for cross-file checks
/// (ref resolution, naming, consistency).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredModel {
    /// Model name from `-- name:` header, or derived from filename.
    pub name: String,

    /// Relative file path (forward slashes, relative to project root).
    pub file_path: String,

    /// SHA-256 hex digest of the entire file content.
    pub content_hash: String,

    /// Model names referenced via `{{ ref('...') }}` in the SQL body.
    pub ref_names: Vec<String>,

    /// Raw header fields parsed from `-- key: value` comment lines.
    pub header: HashMap<String, String>,

    /// The full file content (used by checkers that need the raw text).
    pub content: String,
}

/// Auto-detected project type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectType {
    /// IronLayer native project (has `ironlayer.yaml` or `-- name:` headers).
    IronLayer,
    /// dbt project (has `dbt_project.yml`).
    Dbt,
    /// Plain SQL files with no framework detected.
    RawSql,
}

impl std::fmt::Display for ProjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IronLayer => write!(f, "ironlayer"),
            Self::Dbt => write!(f, "dbt"),
            Self::RawSql => write!(f, "raw_sql"),
        }
    }
}

/// Lightweight file metadata collected by a stat-only directory walk.
///
/// Used for the fast-path cache check: if the file's mtime and size match
/// the cached entry, the file content is never read. This makes warm cache
/// runs O(stat) instead of O(read + SHA-256).
#[derive(Debug, Clone)]
pub struct DiscoveredFileMeta {
    /// Relative path from project root (forward slashes).
    pub rel_path: String,

    /// File size in bytes from `fs::metadata`.
    pub size: u64,

    /// File modification time as seconds since the Unix epoch.
    pub mtime_secs: i64,
}

/// A discovered file with its content and metadata, ready for checking.
#[derive(Debug, Clone)]
pub struct DiscoveredFile {
    /// Relative path from project root (forward slashes).
    pub rel_path: String,

    /// Full file content as a UTF-8 string.
    pub content: String,

    /// SHA-256 hex digest of the content.
    pub content_hash: String,
}
