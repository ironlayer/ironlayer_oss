//! Configuration loading and resolution for the IronLayer Check Engine.
//!
//! Supports a 4-level configuration resolution order:
//! 1. `ironlayer.check.toml` (project root)
//! 2. `[tool.ironlayer.check]` section in `pyproject.toml`
//! 3. `[check]` section in `ironlayer.yaml` (existing IronLayer config)
//! 4. Built-in defaults
//!
//! Per-path overrides allow different rule configurations for different
//! directory subtrees (e.g., stricter rules for `marts/` than `staging/`).

use std::collections::HashMap;
use std::path::Path;

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::Dialect;

// ---------------------------------------------------------------------------
// Rule severity override
// ---------------------------------------------------------------------------

/// Per-rule severity override, or `Off` to disable a rule entirely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleSeverityOverride {
    /// Override severity to Error.
    Error,
    /// Override severity to Warning.
    Warning,
    /// Override severity to Info.
    Info,
    /// Disable the rule entirely.
    Off,
}

// ---------------------------------------------------------------------------
// Cache config
// ---------------------------------------------------------------------------

/// Configuration for the content-addressable check cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Cache file path relative to project root.
    pub path: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: ".ironlayer/check_cache.json".to_owned(),
        }
    }
}

// ---------------------------------------------------------------------------
// Naming config
// ---------------------------------------------------------------------------

/// Configuration for naming convention checks (NAME001-NAME008).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamingConfig {
    /// Regex pattern that model names must match (NAME005 default: `^[a-z][a-z0-9_]*$`).
    pub model_pattern: String,
    /// Directory name → regex pattern for layer-based naming (NAME001-NAME004, NAME006).
    pub layers: HashMap<String, String>,
}

impl Default for NamingConfig {
    fn default() -> Self {
        let mut layers = HashMap::new();
        layers.insert("staging".to_owned(), "^(stg|staging)_".to_owned());
        layers.insert("stg".to_owned(), "^(stg|staging)_".to_owned());
        layers.insert("intermediate".to_owned(), "^(int|intermediate)_".to_owned());
        layers.insert("int".to_owned(), "^(int|intermediate)_".to_owned());
        layers.insert("marts".to_owned(), "^(fct|fact|dim|dimension)_".to_owned());
        layers.insert("mart".to_owned(), "^(fct|fact|dim|dimension)_".to_owned());
        Self {
            model_pattern: "^[a-z][a-z0-9_]*$".to_owned(),
            layers,
        }
    }
}

// ---------------------------------------------------------------------------
// dbt config
// ---------------------------------------------------------------------------

/// Configuration for dbt-specific checks (DBT001-DBT006).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbtConfig {
    /// Path to `dbt_project.yml` (auto-detected if `None`).
    pub project_file: Option<String>,
    /// Whether to require documentation for every model.
    pub require_model_docs: bool,
    /// Minimum number of tests required per model.
    pub min_tests_per_model: u32,
}

// ---------------------------------------------------------------------------
// Per-path override
// ---------------------------------------------------------------------------

/// Per-path rule overrides using glob patterns.
///
/// Most specific path wins when multiple overrides match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerPathOverride {
    /// Glob pattern to match file paths against.
    pub path: String,
    /// Rule ID → severity override for files matching this glob.
    pub rules: HashMap<String, RuleSeverityOverride>,
}

// ---------------------------------------------------------------------------
// Main config
// ---------------------------------------------------------------------------

/// Complete check engine configuration.
///
/// Loaded via the 4-level resolution order, then optionally overridden
/// by CLI flags. Passed to every checker via reference.
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConfig {
    /// Whether warnings should cause a non-zero exit code.
    pub fail_on_warnings: bool,

    /// Maximum number of diagnostics to report (0 = unlimited).
    pub max_diagnostics: usize,

    /// SQL dialect for dialect-aware checks.
    pub dialect: Dialect,

    /// Additional file/directory exclusion patterns (beyond `.gitignore`).
    pub exclude: Vec<String>,

    /// Additional file extensions to check (beyond `.sql`, `.yml`, `.yaml`).
    pub extra_extensions: Vec<String>,

    /// Cache configuration.
    pub cache: CacheConfig,

    /// Per-rule severity overrides (rule ID → override).
    pub rules: HashMap<String, RuleSeverityOverride>,

    /// Naming convention configuration.
    pub naming: NamingConfig,

    /// dbt-specific configuration.
    pub dbt: DbtConfig,

    /// Per-path rule overrides (most specific path wins).
    pub per_path: Vec<PerPathOverride>,

    /// Whether to use `--changed-only` mode (check only git-modified files).
    pub changed_only: bool,

    /// Whether to auto-fix fixable rules (`--fix` mode).
    pub fix: bool,

    /// Whether to disable the cache entirely (`--no-cache`).
    pub no_cache: bool,

    /// Comma-separated rule IDs or categories to select.
    pub select: Option<String>,

    /// Comma-separated rule IDs or categories to exclude.
    pub exclude_rules: Option<String>,
}

impl Default for CheckConfig {
    fn default() -> Self {
        Self {
            fail_on_warnings: false,
            max_diagnostics: 200,
            dialect: Dialect::default(),
            exclude: vec![
                "target/".to_owned(),
                "dbt_packages/".to_owned(),
                "logs/".to_owned(),
                "macros/".to_owned(),
                ".venv/".to_owned(),
            ],
            extra_extensions: Vec::new(),
            cache: CacheConfig::default(),
            rules: HashMap::new(),
            naming: NamingConfig::default(),
            dbt: DbtConfig::default(),
            per_path: Vec::new(),
            changed_only: false,
            fix: false,
            no_cache: false,
            select: None,
            exclude_rules: None,
        }
    }
}

#[pymethods]
impl CheckConfig {
    /// Create a new `CheckConfig` with built-in defaults.
    #[new]
    fn py_new() -> Self {
        Self::default()
    }

    /// Whether warnings should cause a non-zero exit code.
    #[getter]
    fn get_fail_on_warnings(&self) -> bool {
        self.fail_on_warnings
    }

    /// Set whether warnings should cause a non-zero exit code.
    #[setter]
    fn set_fail_on_warnings(&mut self, value: bool) {
        self.fail_on_warnings = value;
    }

    /// Maximum number of diagnostics to report.
    #[getter]
    fn get_max_diagnostics(&self) -> usize {
        self.max_diagnostics
    }

    /// Set maximum number of diagnostics.
    #[setter]
    fn set_max_diagnostics(&mut self, value: usize) {
        self.max_diagnostics = value;
    }

    /// Whether to use `--changed-only` mode.
    #[getter]
    fn get_changed_only(&self) -> bool {
        self.changed_only
    }

    /// Set `--changed-only` mode.
    #[setter]
    fn set_changed_only(&mut self, value: bool) {
        self.changed_only = value;
    }

    /// Whether to auto-fix fixable rules.
    #[getter]
    fn get_fix(&self) -> bool {
        self.fix
    }

    /// Set `--fix` mode.
    #[setter]
    fn set_fix(&mut self, value: bool) {
        self.fix = value;
    }

    /// Whether to disable the cache.
    #[getter]
    fn get_no_cache(&self) -> bool {
        self.no_cache
    }

    /// Set `--no-cache` mode.
    #[setter]
    fn set_no_cache(&mut self, value: bool) {
        self.no_cache = value;
    }

    /// Comma-separated rule IDs or categories to select.
    #[getter]
    fn get_select(&self) -> Option<String> {
        self.select.clone()
    }

    /// Set rule/category selection.
    #[setter]
    fn set_select(&mut self, value: Option<String>) {
        self.select = value;
    }

    /// Comma-separated rule IDs or categories to exclude.
    #[getter]
    fn get_exclude_rules(&self) -> Option<String> {
        self.exclude_rules.clone()
    }

    /// Set rule/category exclusion.
    #[setter]
    fn set_exclude_rules(&mut self, value: Option<String>) {
        self.exclude_rules = value;
    }

    /// SQL dialect.
    #[getter]
    fn get_dialect(&self) -> Dialect {
        self.dialect
    }

    /// Set SQL dialect.
    #[setter]
    fn set_dialect(&mut self, value: Dialect) {
        self.dialect = value;
    }

    /// Return a human-readable string representation.
    fn __repr__(&self) -> String {
        format!(
            "CheckConfig(dialect={}, fail_on_warnings={}, max_diagnostics={})",
            self.dialect, self.fail_on_warnings, self.max_diagnostics,
        )
    }
}

// ---------------------------------------------------------------------------
// TOML deserialization helpers
// ---------------------------------------------------------------------------

/// Raw TOML structure for `ironlayer.check.toml`.
#[derive(Debug, Deserialize)]
struct TomlCheckFile {
    check: Option<TomlCheckSection>,
}

/// The `[check]` section inside the TOML file.
#[derive(Debug, Deserialize)]
struct TomlCheckSection {
    fail_on_warnings: Option<bool>,
    max_diagnostics: Option<usize>,
    dialect: Option<String>,
    exclude: Option<Vec<String>>,
    extra_extensions: Option<Vec<String>>,
    cache: Option<TomlCacheSection>,
    rules: Option<HashMap<String, String>>,
    naming: Option<TomlNamingSection>,
    dbt: Option<TomlDbtSection>,
    per_path: Option<Vec<TomlPerPathSection>>,
}

#[derive(Debug, Deserialize)]
struct TomlCacheSection {
    enabled: Option<bool>,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TomlNamingSection {
    model_pattern: Option<String>,
    layers: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct TomlDbtSection {
    project_file: Option<String>,
    require_model_docs: Option<bool>,
    min_tests_per_model: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TomlPerPathSection {
    path: String,
    rules: Option<HashMap<String, String>>,
}

/// Raw TOML structure for `pyproject.toml` with `[tool.ironlayer.check]`.
#[derive(Debug, Deserialize)]
struct PyprojectToml {
    tool: Option<PyprojectTool>,
}

#[derive(Debug, Deserialize)]
struct PyprojectTool {
    ironlayer: Option<PyprojectIronlayer>,
}

#[derive(Debug, Deserialize)]
struct PyprojectIronlayer {
    check: Option<TomlCheckSection>,
}

/// Raw YAML structure for `ironlayer.yaml` with `[check]` section.
#[derive(Debug, Deserialize)]
struct IronlayerYaml {
    check: Option<TomlCheckSection>,
}

/// Parse a string severity value into a [`RuleSeverityOverride`].
fn parse_rule_severity(s: &str) -> Option<RuleSeverityOverride> {
    match s.to_lowercase().as_str() {
        "error" => Some(RuleSeverityOverride::Error),
        "warning" | "warn" => Some(RuleSeverityOverride::Warning),
        "info" => Some(RuleSeverityOverride::Info),
        "off" | "disabled" | "false" => Some(RuleSeverityOverride::Off),
        _ => None,
    }
}

/// Parse a dialect string into a [`Dialect`].
fn parse_dialect(s: &str) -> Option<Dialect> {
    match s.to_lowercase().as_str() {
        "databricks" => Some(Dialect::Databricks),
        "duckdb" => Some(Dialect::DuckDB),
        "redshift" => Some(Dialect::Redshift),
        _ => None,
    }
}

/// Apply a [`TomlCheckSection`] onto a [`CheckConfig`], overriding any set values.
fn apply_toml_section(config: &mut CheckConfig, section: &TomlCheckSection) {
    if let Some(v) = section.fail_on_warnings {
        config.fail_on_warnings = v;
    }
    if let Some(v) = section.max_diagnostics {
        config.max_diagnostics = v;
    }
    if let Some(ref v) = section.dialect {
        if let Some(d) = parse_dialect(v) {
            config.dialect = d;
        }
    }
    if let Some(ref v) = section.exclude {
        config.exclude = v.clone();
    }
    if let Some(ref v) = section.extra_extensions {
        config.extra_extensions = v.clone();
    }
    if let Some(ref cache) = section.cache {
        if let Some(v) = cache.enabled {
            config.cache.enabled = v;
        }
        if let Some(ref v) = cache.path {
            config.cache.path = v.clone();
        }
    }
    if let Some(ref rules) = section.rules {
        for (rule_id, severity_str) in rules {
            if let Some(sev) = parse_rule_severity(severity_str) {
                config.rules.insert(rule_id.clone(), sev);
            }
        }
    }
    if let Some(ref naming) = section.naming {
        if let Some(ref v) = naming.model_pattern {
            config.naming.model_pattern = v.clone();
        }
        if let Some(ref v) = naming.layers {
            for (dir, pattern) in v {
                config.naming.layers.insert(dir.clone(), pattern.clone());
            }
        }
    }
    if let Some(ref dbt) = section.dbt {
        if let Some(ref v) = dbt.project_file {
            config.dbt.project_file = Some(v.clone());
        }
        if let Some(v) = dbt.require_model_docs {
            config.dbt.require_model_docs = v;
        }
        if let Some(v) = dbt.min_tests_per_model {
            config.dbt.min_tests_per_model = v;
        }
    }
    if let Some(ref per_path) = section.per_path {
        for pp in per_path {
            let rules: HashMap<String, RuleSeverityOverride> = pp
                .rules
                .as_ref()
                .map(|r| {
                    r.iter()
                        .filter_map(|(k, v)| parse_rule_severity(v).map(|sev| (k.clone(), sev)))
                        .collect()
                })
                .unwrap_or_default();
            config.per_path.push(PerPathOverride {
                path: pp.path.clone(),
                rules,
            });
        }
    }
}

impl CheckConfig {
    /// Load configuration from the project root using the 4-level resolution order.
    ///
    /// 1. `ironlayer.check.toml` (project root)
    /// 2. `[tool.ironlayer.check]` in `pyproject.toml`
    /// 3. `[check]` in `ironlayer.yaml`
    /// 4. Built-in defaults
    ///
    /// First file found wins — later files are not consulted.
    ///
    /// # Errors
    ///
    /// Returns an error if a found config file cannot be read or parsed.
    pub fn load_from_project(root: &Path) -> Result<Self, ConfigError> {
        let mut config = Self::default();

        // 1. ironlayer.check.toml
        let check_toml = root.join("ironlayer.check.toml");
        if check_toml.is_file() {
            let content = std::fs::read_to_string(&check_toml).map_err(|e| {
                ConfigError::ReadError(check_toml.display().to_string(), e.to_string())
            })?;
            let parsed: TomlCheckFile = toml::from_str(&content).map_err(|e| {
                ConfigError::ParseError(check_toml.display().to_string(), e.to_string())
            })?;
            if let Some(ref section) = parsed.check {
                apply_toml_section(&mut config, section);
            }
            return Ok(config);
        }

        // 2. pyproject.toml [tool.ironlayer.check]
        let pyproject = root.join("pyproject.toml");
        if pyproject.is_file() {
            let content = std::fs::read_to_string(&pyproject).map_err(|e| {
                ConfigError::ReadError(pyproject.display().to_string(), e.to_string())
            })?;
            if let Ok(parsed) = toml::from_str::<PyprojectToml>(&content) {
                if let Some(tool) = parsed.tool {
                    if let Some(il) = tool.ironlayer {
                        if let Some(ref section) = il.check {
                            apply_toml_section(&mut config, section);
                            return Ok(config);
                        }
                    }
                }
            }
        }

        // 3. ironlayer.yaml [check] or ironlayer.yml [check]
        for name in &["ironlayer.yaml", "ironlayer.yml"] {
            let yaml_path = root.join(name);
            if yaml_path.is_file() {
                let content = std::fs::read_to_string(&yaml_path).map_err(|e| {
                    ConfigError::ReadError(yaml_path.display().to_string(), e.to_string())
                })?;
                if let Ok(parsed) = serde_yaml::from_str::<IronlayerYaml>(&content) {
                    if let Some(ref section) = parsed.check {
                        apply_toml_section(&mut config, section);
                        return Ok(config);
                    }
                }
            }
        }

        // 4. Built-in defaults (already set)
        Ok(config)
    }

    /// Check whether a specific rule is enabled, considering the config overrides
    /// and the rule's default enabled state.
    ///
    /// Returns `true` if the rule should run, `false` if disabled.
    #[must_use]
    pub fn is_rule_enabled(&self, rule_id: &str, default_enabled: bool) -> bool {
        // Check select/exclude filters first
        if let Some(ref select) = self.select {
            let selected = select
                .split(',')
                .map(|s| s.trim())
                .any(|s| rule_id == s || rule_id.starts_with(s));
            if !selected {
                return false;
            }
        }
        if let Some(ref exclude) = self.exclude_rules {
            let excluded = exclude
                .split(',')
                .map(|s| s.trim())
                .any(|s| rule_id == s || rule_id.starts_with(s));
            if excluded {
                return false;
            }
        }

        // Check per-rule override
        if let Some(override_val) = self.rules.get(rule_id) {
            return *override_val != RuleSeverityOverride::Off;
        }

        default_enabled
    }

    /// Get the effective severity for a rule, considering config overrides.
    ///
    /// Returns the overridden severity if configured, otherwise the default.
    #[must_use]
    pub fn effective_severity(
        &self,
        rule_id: &str,
        default: crate::types::Severity,
    ) -> crate::types::Severity {
        if let Some(override_val) = self.rules.get(rule_id) {
            match override_val {
                RuleSeverityOverride::Error => return crate::types::Severity::Error,
                RuleSeverityOverride::Warning => return crate::types::Severity::Warning,
                RuleSeverityOverride::Info => return crate::types::Severity::Info,
                RuleSeverityOverride::Off => return default,
            }
        }
        default
    }

    /// Get the effective severity for a rule considering per-path overrides.
    ///
    /// Checks per-path overrides first (most specific match wins), then falls
    /// back to the global rule override, then the default.
    #[must_use]
    pub fn effective_severity_for_path(
        &self,
        rule_id: &str,
        file_path: &str,
        default: crate::types::Severity,
    ) -> crate::types::Severity {
        // Check per-path overrides (last matching entry wins, as it's the most specific)
        for pp in self.per_path.iter().rev() {
            if let Ok(glob) = globset::Glob::new(&pp.path) {
                let matcher = glob.compile_matcher();
                if matcher.is_match(file_path) {
                    if let Some(override_val) = pp.rules.get(rule_id) {
                        return match override_val {
                            RuleSeverityOverride::Error => crate::types::Severity::Error,
                            RuleSeverityOverride::Warning => crate::types::Severity::Warning,
                            RuleSeverityOverride::Info => crate::types::Severity::Info,
                            RuleSeverityOverride::Off => default,
                        };
                    }
                }
            }
        }

        self.effective_severity(rule_id, default)
    }

    /// Check if a rule is enabled for a specific file path, considering per-path overrides.
    #[must_use]
    pub fn is_rule_enabled_for_path(
        &self,
        rule_id: &str,
        file_path: &str,
        default_enabled: bool,
    ) -> bool {
        // Check per-path overrides first
        for pp in self.per_path.iter().rev() {
            if let Ok(glob) = globset::Glob::new(&pp.path) {
                let matcher = glob.compile_matcher();
                if matcher.is_match(file_path) {
                    if let Some(override_val) = pp.rules.get(rule_id) {
                        return *override_val != RuleSeverityOverride::Off;
                    }
                }
            }
        }

        self.is_rule_enabled(rule_id, default_enabled)
    }

    /// Compute a SHA-256 hash of the configuration for cache invalidation.
    ///
    /// If any config value changes, the hash changes, invalidating the entire cache.
    /// Uses canonical JSON (sorted keys) to ensure deterministic hashing regardless
    /// of HashMap iteration order.
    #[must_use]
    pub fn config_hash(&self) -> String {
        use sha2::{Digest, Sha256};

        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        let canonical = canonical_json(&value);
        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        hex::encode(hasher.finalize())
    }
}

// ---------------------------------------------------------------------------
// Canonical JSON for deterministic hashing
// ---------------------------------------------------------------------------

/// Produce a canonical JSON string with sorted object keys.
///
/// HashMap iteration order is non-deterministic in Rust, so `serde_json::to_string`
/// may produce different JSON for the same logical config on different runs. This
/// function ensures the output is deterministic by sorting all object keys recursively.
fn canonical_json(value: &serde_json::Value) -> String {
    let mut buf = String::new();
    write_canonical(value, &mut buf);
    buf
}

/// Recursively write a JSON value with sorted object keys.
fn write_canonical(value: &serde_json::Value, buf: &mut String) {
    use std::fmt::Write;

    match value {
        serde_json::Value::Null => buf.push_str("null"),
        serde_json::Value::Bool(b) => {
            let _ = write!(buf, "{b}");
        }
        serde_json::Value::Number(n) => {
            let _ = write!(buf, "{n}");
        }
        serde_json::Value::String(s) => {
            // Use serde_json for proper escaping
            let _ = write!(buf, "{}", serde_json::to_string(s).unwrap_or_default());
        }
        serde_json::Value::Array(arr) => {
            buf.push('[');
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_canonical(v, buf);
            }
            buf.push(']');
        }
        serde_json::Value::Object(map) => {
            buf.push('{');
            // Sort keys for deterministic output
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                let _ = write!(buf, "{}", serde_json::to_string(*k).unwrap_or_default());
                buf.push(':');
                write_canonical(&map[*k], buf);
            }
            buf.push('}');
        }
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to read a configuration file.
    #[error("Failed to read config file '{0}': {1}")]
    ReadError(String, String),

    /// Failed to parse a configuration file.
    #[error("Failed to parse config file '{0}': {1}")]
    ParseError(String, String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CheckConfig::default();
        assert!(!config.fail_on_warnings);
        assert_eq!(config.max_diagnostics, 200);
        assert_eq!(config.dialect, Dialect::Databricks);
        assert!(config.cache.enabled);
        assert!(config.rules.is_empty());
    }

    #[test]
    fn test_rule_enabled_default() {
        let config = CheckConfig::default();
        assert!(config.is_rule_enabled("HDR001", true));
        assert!(!config.is_rule_enabled("HDR007", false));
    }

    #[test]
    fn test_rule_enabled_override_off() {
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("SQL004".to_owned(), RuleSeverityOverride::Off);
        assert!(!config.is_rule_enabled("SQL004", true));
    }

    #[test]
    fn test_rule_enabled_override_on() {
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("HDR007".to_owned(), RuleSeverityOverride::Warning);
        assert!(config.is_rule_enabled("HDR007", false));
    }

    #[test]
    fn test_effective_severity_default() {
        let config = CheckConfig::default();
        assert_eq!(
            config.effective_severity("HDR001", crate::types::Severity::Error),
            crate::types::Severity::Error
        );
    }

    #[test]
    fn test_effective_severity_override() {
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME005".to_owned(), RuleSeverityOverride::Error);
        assert_eq!(
            config.effective_severity("NAME005", crate::types::Severity::Warning),
            crate::types::Severity::Error
        );
    }

    #[test]
    fn test_select_filter() {
        let mut config = CheckConfig::default();
        config.select = Some("HDR,SQL001".to_owned());
        assert!(config.is_rule_enabled("HDR001", true));
        assert!(config.is_rule_enabled("HDR013", true));
        assert!(config.is_rule_enabled("SQL001", true));
        assert!(!config.is_rule_enabled("SQL002", true));
        assert!(!config.is_rule_enabled("REF001", true));
    }

    #[test]
    fn test_exclude_filter() {
        let mut config = CheckConfig::default();
        config.exclude_rules = Some("SQL004,NAME".to_owned());
        assert!(!config.is_rule_enabled("SQL004", true));
        assert!(!config.is_rule_enabled("NAME001", true));
        assert!(config.is_rule_enabled("HDR001", true));
    }

    #[test]
    fn test_config_hash_changes_with_rules() {
        let config1 = CheckConfig::default();
        let mut config2 = CheckConfig::default();
        config2
            .rules
            .insert("SQL004".to_owned(), RuleSeverityOverride::Off);
        assert_ne!(config1.config_hash(), config2.config_hash());
    }

    #[test]
    fn test_config_hash_deterministic() {
        let config = CheckConfig::default();
        assert_eq!(config.config_hash(), config.config_hash());
    }

    #[test]
    fn test_load_nonexistent_dir() {
        let result = CheckConfig::load_from_project(Path::new("/nonexistent/path"));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.dialect, Dialect::Databricks);
    }

    #[test]
    fn test_parse_rule_severity() {
        assert_eq!(
            parse_rule_severity("error"),
            Some(RuleSeverityOverride::Error)
        );
        assert_eq!(
            parse_rule_severity("warning"),
            Some(RuleSeverityOverride::Warning)
        );
        assert_eq!(
            parse_rule_severity("warn"),
            Some(RuleSeverityOverride::Warning)
        );
        assert_eq!(
            parse_rule_severity("info"),
            Some(RuleSeverityOverride::Info)
        );
        assert_eq!(parse_rule_severity("off"), Some(RuleSeverityOverride::Off));
        assert_eq!(
            parse_rule_severity("disabled"),
            Some(RuleSeverityOverride::Off)
        );
        assert_eq!(parse_rule_severity("unknown"), None);
    }

    #[test]
    fn test_parse_dialect() {
        assert_eq!(parse_dialect("databricks"), Some(Dialect::Databricks));
        assert_eq!(parse_dialect("DATABRICKS"), Some(Dialect::Databricks));
        assert_eq!(parse_dialect("duckdb"), Some(Dialect::DuckDB));
        assert_eq!(parse_dialect("redshift"), Some(Dialect::Redshift));
        assert_eq!(parse_dialect("postgres"), None);
    }

    #[test]
    fn test_load_from_toml_file() {
        let dir = tempfile::tempdir().unwrap();
        let toml_content = r#"
[check]
fail_on_warnings = true
max_diagnostics = 100
dialect = "duckdb"
exclude = ["custom/"]

[check.cache]
enabled = false

[check.rules]
SQL004 = "off"
NAME005 = "error"

[check.naming]
model_pattern = "^[a-z_]+$"

[check.naming.layers]
staging = "^stg_"
"#;
        std::fs::write(dir.path().join("ironlayer.check.toml"), toml_content).unwrap();
        let config = CheckConfig::load_from_project(dir.path()).unwrap();
        assert!(config.fail_on_warnings);
        assert_eq!(config.max_diagnostics, 100);
        assert_eq!(config.dialect, Dialect::DuckDB);
        assert_eq!(config.exclude, vec!["custom/"]);
        assert!(!config.cache.enabled);
        assert!(!config.is_rule_enabled("SQL004", true));
        assert_eq!(
            config.effective_severity("NAME005", crate::types::Severity::Warning),
            crate::types::Severity::Error
        );
        assert_eq!(config.naming.model_pattern, "^[a-z_]+$");
    }

    #[test]
    fn test_load_from_pyproject_toml() {
        let dir = tempfile::tempdir().unwrap();
        let content = r#"
[project]
name = "my-project"

[tool.ironlayer.check]
fail_on_warnings = true
dialect = "redshift"
"#;
        std::fs::write(dir.path().join("pyproject.toml"), content).unwrap();
        let config = CheckConfig::load_from_project(dir.path()).unwrap();
        assert!(config.fail_on_warnings);
        assert_eq!(config.dialect, Dialect::Redshift);
    }

    #[test]
    fn test_per_path_override() {
        let mut config = CheckConfig::default();
        config.per_path.push(PerPathOverride {
            path: "models/staging/**".to_owned(),
            rules: {
                let mut r = HashMap::new();
                r.insert("SQL004".to_owned(), RuleSeverityOverride::Off);
                r
            },
        });

        // Per-path override sets SQL004 = Off for staging, so it should be disabled
        assert!(!config.is_rule_enabled_for_path("SQL004", "models/staging/stg_orders.sql", true));
        assert!(config.is_rule_enabled_for_path("SQL004", "models/marts/fct_revenue.sql", true));
    }
}
