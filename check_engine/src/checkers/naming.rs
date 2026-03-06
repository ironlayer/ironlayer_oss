//! Naming convention checker — rules NAME001 through NAME008.
//!
//! Validates model naming conventions based on configurable regex patterns
//! and directory-based layer detection. Layer directories (staging, intermediate,
//! marts) impose prefix requirements on model names within them.
//!
//! Rules:
//! - **NAME001** (warning, enabled): Staging models must start with `stg_` or `staging_`
//! - **NAME002** (warning, enabled): Intermediate models must start with `int_` or `intermediate_`
//! - **NAME003** (warning, enabled): Fact models in marts must start with `fct_` or `fact_`
//! - **NAME004** (warning, enabled): Dimension models in marts must start with `dim_` or `dimension_`
//! - **NAME005** (warning, enabled): Model names must be lowercase snake_case
//! - **NAME006** (warning, enabled): Model file location must match its layer prefix
//! - **NAME007** (warning, disabled): Column names must be lowercase snake_case
//! - **NAME008** (info, disabled): Model `-- name:` should not include file extension

use regex::Regex;

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Naming convention checker implementing NAME001 through NAME008.
///
/// Validates model names against configurable regex patterns, enforces
/// layer-based prefix requirements, and checks file location consistency.
pub struct NamingChecker;

/// Generate the doc URL for a given rule ID.
fn doc_url(rule_id: &str) -> Option<String> {
    Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}"))
}

/// Detect the semantic layer from a file path by examining directory components.
///
/// Checks each path segment (case-insensitive) for known layer directory names.
/// Returns the canonical layer name: `"staging"`, `"intermediate"`, or `"marts"`.
/// Returns `None` if no known layer directory is found.
fn detect_layer(file_path: &str) -> Option<&'static str> {
    for part in file_path.split('/') {
        match part.to_lowercase().as_str() {
            "staging" | "stg" => return Some("staging"),
            "intermediate" | "int" => return Some("intermediate"),
            "marts" | "mart" => return Some("marts"),
            _ => {}
        }
    }
    None
}

/// Detect which layer a model name's prefix implies.
///
/// Returns the canonical layer name based on the model name prefix:
/// - `stg_` or `staging_` implies `"staging"`
/// - `int_` or `intermediate_` implies `"intermediate"`
/// - `fct_`, `fact_`, `dim_`, or `dimension_` implies `"marts"`
/// - No recognized prefix returns `None`
fn detect_layer_from_prefix(model_name: &str) -> Option<&'static str> {
    let lower = model_name.to_lowercase();
    if lower.starts_with("stg_") || lower.starts_with("staging_") {
        Some("staging")
    } else if lower.starts_with("int_") || lower.starts_with("intermediate_") {
        Some("intermediate")
    } else if lower.starts_with("fct_")
        || lower.starts_with("fact_")
        || lower.starts_with("dim_")
        || lower.starts_with("dimension_")
    {
        Some("marts")
    } else {
        None
    }
}

/// Get the model name, preferring the header `name` field, then filename stem.
///
/// Returns the model's `name` field if non-empty. Otherwise extracts the
/// filename stem (without `.sql` extension) from the file path.
fn get_model_name(model: &DiscoveredModel) -> &str {
    if model.name.is_empty() {
        model
            .file_path
            .rsplit('/')
            .next()
            .and_then(|f| f.strip_suffix(".sql"))
            .unwrap_or(&model.name)
    } else {
        &model.name
    }
}

/// Find the 1-based line number of the `-- name:` header field in file content.
///
/// Scans comment lines at the top of the file for a `-- name:` declaration.
/// Returns 1 if no such line is found (default for diagnostics).
fn find_name_line(content: &str) -> u32 {
    for (i, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("--") {
            let after_dash = trimmed.trim_start_matches('-').trim();
            if let Some((key, _)) = after_dash.split_once(':') {
                if key.trim().eq_ignore_ascii_case("name") {
                    return (i + 1) as u32;
                }
            }
        }
    }
    1
}

/// Find the 1-based line number of a specific header field in file content.
///
/// Scans comment lines for `-- <field_name>:` and returns its line number.
/// Returns 1 if not found.
fn find_header_field_line(content: &str, field_name: &str) -> u32 {
    for (i, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("--") {
            let after_dash = trimmed.trim_start_matches('-').trim();
            if let Some((key, _)) = after_dash.split_once(':') {
                if key.trim() == field_name {
                    return (i + 1) as u32;
                }
            }
        }
    }
    1
}

// ---------------------------------------------------------------------------
// NAME001: Staging models must start with stg_ or staging_
// ---------------------------------------------------------------------------

/// Check NAME001: staging layer models must match the staging pattern.
///
/// Only fires when the model resides in a staging directory and its name
/// does not match the configured staging pattern (default: `^(stg|staging)_`).
fn check_name001(
    file_path: &str,
    content: &str,
    model_name: &str,
    layer: Option<&str>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if layer != Some("staging") {
        return;
    }
    if !config.is_rule_enabled_for_path("NAME001", file_path, true) {
        return;
    }

    let pattern = config
        .naming
        .layers
        .get("staging")
        .or_else(|| config.naming.layers.get("stg"))
        .map(String::as_str)
        .unwrap_or("^(stg|staging)_");

    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return,
    };

    if re.is_match(model_name) {
        return;
    }

    let line = find_name_line(content);
    diags.push(CheckDiagnostic {
        rule_id: "NAME001".to_owned(),
        message: format!(
            "Staging model '{}' does not match naming pattern '{}'. \
             Staging models should start with 'stg_' or 'staging_'.",
            model_name, pattern,
        ),
        severity: config.effective_severity_for_path("NAME001", file_path, Severity::Warning),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!(
            "Rename to 'stg_{}' or move to a non-staging directory.",
            model_name,
        )),
        doc_url: doc_url("NAME001"),
    });
}

// ---------------------------------------------------------------------------
// NAME002: Intermediate models must start with int_ or intermediate_
// ---------------------------------------------------------------------------

/// Check NAME002: intermediate layer models must match the intermediate pattern.
///
/// Only fires when the model resides in an intermediate directory and its name
/// does not match the configured intermediate pattern (default: `^(int|intermediate)_`).
fn check_name002(
    file_path: &str,
    content: &str,
    model_name: &str,
    layer: Option<&str>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if layer != Some("intermediate") {
        return;
    }
    if !config.is_rule_enabled_for_path("NAME002", file_path, true) {
        return;
    }

    let pattern = config
        .naming
        .layers
        .get("intermediate")
        .or_else(|| config.naming.layers.get("int"))
        .map(String::as_str)
        .unwrap_or("^(int|intermediate)_");

    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return,
    };

    if re.is_match(model_name) {
        return;
    }

    let line = find_name_line(content);
    diags.push(CheckDiagnostic {
        rule_id: "NAME002".to_owned(),
        message: format!(
            "Intermediate model '{}' does not match naming pattern '{}'. \
             Intermediate models should start with 'int_' or 'intermediate_'.",
            model_name, pattern,
        ),
        severity: config.effective_severity_for_path("NAME002", file_path, Severity::Warning),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!(
            "Rename to 'int_{}' or move to a non-intermediate directory.",
            model_name,
        )),
        doc_url: doc_url("NAME002"),
    });
}

// ---------------------------------------------------------------------------
// NAME003: Fact models must start with fct_ or fact_
// ---------------------------------------------------------------------------

/// Check NAME003: models in marts must match the marts naming pattern.
///
/// For models in a marts directory, validates the name matches the combined
/// marts pattern (default: `^(fct|fact|dim|dimension)_`). Models with a
/// `dim_` or `dimension_` prefix are skipped (handled by NAME004).
///
/// If the model has neither a fact nor dimension prefix, this rule fires
/// to indicate the model should use a recognized marts prefix.
fn check_name003(
    file_path: &str,
    content: &str,
    model_name: &str,
    layer: Option<&str>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if layer != Some("marts") {
        return;
    }
    if !config.is_rule_enabled_for_path("NAME003", file_path, true) {
        return;
    }

    // If the model already has a dim/dimension prefix, skip — NAME004 handles those
    if model_name.starts_with("dim_") || model_name.starts_with("dimension_") {
        return;
    }

    let pattern = config
        .naming
        .layers
        .get("marts")
        .or_else(|| config.naming.layers.get("mart"))
        .map(String::as_str)
        .unwrap_or("^(fct|fact|dim|dimension)_");

    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return,
    };

    if re.is_match(model_name) {
        return;
    }

    let line = find_name_line(content);
    diags.push(CheckDiagnostic {
        rule_id: "NAME003".to_owned(),
        message: format!(
            "Mart model '{}' does not match naming pattern '{}'. \
             Fact models should start with 'fct_' or 'fact_', \
             dimension models with 'dim_' or 'dimension_'.",
            model_name, pattern,
        ),
        severity: config.effective_severity_for_path("NAME003", file_path, Severity::Warning),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!(
            "Rename to 'fct_{}' or 'dim_{}' depending on model type.",
            model_name, model_name,
        )),
        doc_url: doc_url("NAME003"),
    });
}

// ---------------------------------------------------------------------------
// NAME004: Dimension models must start with dim_ or dimension_
// ---------------------------------------------------------------------------

/// Check NAME004: dimension models in marts must match the dimension pattern.
///
/// Only fires for models in a marts directory whose name starts with `dim_`
/// or `dimension_` but does not match the configured dimension pattern.
/// Since the default dimension pattern is `^(dim|dimension)_`, this rule
/// primarily catches edge cases with custom patterns that require more
/// specific naming.
///
/// Models without a dim prefix are handled by NAME003.
fn check_name004(
    file_path: &str,
    content: &str,
    model_name: &str,
    layer: Option<&str>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if layer != Some("marts") {
        return;
    }
    if !config.is_rule_enabled_for_path("NAME004", file_path, true) {
        return;
    }

    // NAME004 only applies to models that already have a dim/dimension prefix
    // Models without a dim prefix are handled by NAME003
    let has_dim_prefix = model_name.starts_with("dim_") || model_name.starts_with("dimension_");
    if !has_dim_prefix {
        return;
    }

    // Check against the dimension-specific pattern
    let pattern = "^(dim|dimension)_";
    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return,
    };

    if re.is_match(model_name) {
        return;
    }

    let line = find_name_line(content);
    diags.push(CheckDiagnostic {
        rule_id: "NAME004".to_owned(),
        message: format!(
            "Dimension model '{}' does not match naming pattern '{}'. \
             Dimension models should start with 'dim_' or 'dimension_'.",
            model_name, pattern,
        ),
        severity: config.effective_severity_for_path("NAME004", file_path, Severity::Warning),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!("Rename to 'dim_{}'.", model_name)),
        doc_url: doc_url("NAME004"),
    });
}

// ---------------------------------------------------------------------------
// NAME005: Model names must be lowercase snake_case
// ---------------------------------------------------------------------------

/// Check NAME005: model names must be lowercase snake_case.
///
/// Validates the model name against the configured model pattern
/// (default: `^[a-z][a-z0-9_]*$`). Fires if the name contains uppercase
/// letters, starts with a digit, or uses non-snake_case characters.
fn check_name005(
    file_path: &str,
    content: &str,
    model_name: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("NAME005", file_path, true) {
        return;
    }

    let pattern = &config.naming.model_pattern;
    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return,
    };

    if re.is_match(model_name) {
        return;
    }

    let line = find_name_line(content);
    let suggested = model_name
        .chars()
        .map(|c| {
            if c.is_ascii_uppercase() {
                c.to_ascii_lowercase()
            } else if c == '-' || c == ' ' {
                '_'
            } else {
                c
            }
        })
        .collect::<String>();

    diags.push(CheckDiagnostic {
        rule_id: "NAME005".to_owned(),
        message: format!(
            "Model name '{}' does not match pattern '{}'. \
             Model names must be lowercase snake_case.",
            model_name, pattern,
        ),
        severity: config.effective_severity_for_path("NAME005", file_path, Severity::Warning),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!("Rename to '{}'.", suggested)),
        doc_url: doc_url("NAME005"),
    });
}

// ---------------------------------------------------------------------------
// NAME006: Model file location must match its layer prefix
// ---------------------------------------------------------------------------

/// Check NAME006: model file location must match its layer prefix.
///
/// Detects the layer implied by the model name prefix and compares it with
/// the layer implied by the file's directory path. Fires if they disagree
/// — for example, a model named `stg_orders` living in `marts/`.
///
/// Only fires when both a directory layer and a name prefix layer can be
/// detected and they differ.
fn check_name006(
    file_path: &str,
    content: &str,
    model_name: &str,
    layer: Option<&str>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("NAME006", file_path, true) {
        return;
    }

    let dir_layer = match layer {
        Some(l) => l,
        None => return,
    };

    let prefix_layer = match detect_layer_from_prefix(model_name) {
        Some(l) => l,
        None => return,
    };

    if dir_layer == prefix_layer {
        return;
    }

    let expected_dir = match prefix_layer {
        "staging" => "staging/ or stg/",
        "intermediate" => "intermediate/ or int/",
        "marts" => "marts/ or mart/",
        _ => return,
    };

    let line = find_name_line(content);
    let name_prefix = model_name.split('_').next().unwrap_or("");

    diags.push(CheckDiagnostic {
        rule_id: "NAME006".to_owned(),
        message: format!(
            "Model '{}' has a '{}' layer prefix but is located in the '{}' directory. \
             Move it to {}.",
            model_name, name_prefix, dir_layer, expected_dir,
        ),
        severity: config.effective_severity_for_path("NAME006", file_path, Severity::Warning),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!("Move this file to the {} directory.", expected_dir)),
        doc_url: doc_url("NAME006"),
    });
}

// ---------------------------------------------------------------------------
// NAME007: Column names must be lowercase snake_case (disabled by default)
// ---------------------------------------------------------------------------

/// Check NAME007: column names must be lowercase snake_case (disabled by default).
///
/// Inspects the `contract_columns` header field if present, parsing the
/// comma-separated list of column names and checking each against the configured
/// model pattern. Only fires when explicitly enabled in configuration.
fn check_name007(
    file_path: &str,
    model: &DiscoveredModel,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("NAME007", file_path, false) {
        return;
    }

    let columns_str = match model.header.get("contract_columns") {
        Some(s) => s,
        None => return,
    };

    let pattern = &config.naming.model_pattern;
    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return,
    };

    let severity = config.effective_severity_for_path("NAME007", file_path, Severity::Warning);
    let columns_line = find_header_field_line(&model.content, "contract_columns");

    for col_raw in columns_str.split(',') {
        let col = col_raw.trim();
        if col.is_empty() {
            continue;
        }
        if re.is_match(col) {
            continue;
        }

        let suggested = col
            .chars()
            .map(|c| {
                if c.is_ascii_uppercase() {
                    c.to_ascii_lowercase()
                } else if c == '-' || c == ' ' {
                    '_'
                } else {
                    c
                }
            })
            .collect::<String>();

        diags.push(CheckDiagnostic {
            rule_id: "NAME007".to_owned(),
            message: format!(
                "Column name '{}' does not match the required naming pattern '{}'. \
                 Column names must be lowercase snake_case.",
                col, pattern,
            ),
            severity,
            category: CheckCategory::NamingConvention,
            file_path: file_path.to_owned(),
            line: columns_line,
            column: 0,
            snippet: Some(format!("-- contract_columns: {}", columns_str)),
            suggestion: Some(format!("Rename column to '{}'.", suggested)),
            doc_url: doc_url("NAME007"),
        });
    }
}

// ---------------------------------------------------------------------------
// NAME008: Model name should not include file extension (disabled by default)
// ---------------------------------------------------------------------------

/// Check NAME008: model name should not include file extension (disabled by default).
///
/// Fires if the `-- name:` header value ends with `.sql`, `.SQL`, `.py`,
/// `.yml`, or `.yaml`, since the engine automatically handles extensions.
fn check_name008(
    file_path: &str,
    content: &str,
    model_name: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("NAME008", file_path, false) {
        return;
    }

    let has_extension = model_name.ends_with(".sql")
        || model_name.ends_with(".SQL")
        || model_name.ends_with(".py")
        || model_name.ends_with(".yml")
        || model_name.ends_with(".yaml");

    if !has_extension {
        return;
    }

    let line = find_name_line(content);
    let stem = model_name
        .rsplit_once('.')
        .map(|(base, _)| base)
        .unwrap_or(model_name);

    diags.push(CheckDiagnostic {
        rule_id: "NAME008".to_owned(),
        message: format!(
            "Model name '{}' includes the file extension. \
             The '-- name:' field should contain just the model name without the extension.",
            model_name,
        ),
        severity: config.effective_severity_for_path("NAME008", file_path, Severity::Info),
        category: CheckCategory::NamingConvention,
        file_path: file_path.to_owned(),
        line,
        column: 0,
        snippet: Some(format!("-- name: {}", model_name)),
        suggestion: Some(format!("Change to '-- name: {}'.", stem)),
        doc_url: doc_url("NAME008"),
    });
}

impl Checker for NamingChecker {
    fn name(&self) -> &'static str {
        "naming"
    }

    fn check_file(
        &self,
        file_path: &str,
        content: &str,
        model: Option<&DiscoveredModel>,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        // Only check .sql files
        if !file_path.ends_with(".sql") {
            return Vec::new();
        }

        let model = match model {
            Some(m) => m,
            None => return Vec::new(),
        };

        let model_name = get_model_name(model);
        if model_name.is_empty() {
            return Vec::new();
        }

        let layer = detect_layer(file_path);
        let mut diags = Vec::new();

        // NAME001: staging prefix check
        check_name001(file_path, content, model_name, layer, config, &mut diags);

        // NAME002: intermediate prefix check
        check_name002(file_path, content, model_name, layer, config, &mut diags);

        // NAME003: marts fact prefix check
        check_name003(file_path, content, model_name, layer, config, &mut diags);

        // NAME004: marts dimension prefix check
        check_name004(file_path, content, model_name, layer, config, &mut diags);

        // NAME005: lowercase snake_case
        check_name005(file_path, content, model_name, config, &mut diags);

        // NAME006: file location matches layer prefix
        check_name006(file_path, content, model_name, layer, config, &mut diags);

        // NAME007: column naming (disabled by default)
        check_name007(file_path, model, config, &mut diags);

        // NAME008: no file extension in name (disabled by default)
        check_name008(file_path, content, model_name, config, &mut diags);

        diags
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{CheckConfig, RuleSeverityOverride};

    /// Helper to create a `DiscoveredModel` for testing.
    fn make_model(name: &str, file_path: &str) -> DiscoveredModel {
        DiscoveredModel {
            name: name.to_owned(),
            file_path: file_path.to_owned(),
            content_hash: "sha256:test".to_owned(),
            ref_names: Vec::new(),
            header: HashMap::new(),
            content: format!("-- name: {}\n-- kind: FULL_REFRESH\nSELECT 1", name),
        }
    }

    /// Helper to create a model with specific header fields.
    fn make_model_with_header(
        name: &str,
        file_path: &str,
        content: &str,
        header: HashMap<String, String>,
    ) -> DiscoveredModel {
        DiscoveredModel {
            name: name.to_owned(),
            file_path: file_path.to_owned(),
            content_hash: "sha256:test".to_owned(),
            ref_names: Vec::new(),
            header,
            content: content.to_owned(),
        }
    }

    /// Helper to run the checker with default config.
    fn check_with_model(name: &str, file_path: &str) -> Vec<CheckDiagnostic> {
        let model = make_model(name, file_path);
        let checker = NamingChecker;
        let config = CheckConfig::default();
        checker.check_file(file_path, &model.content, Some(&model), &config)
    }

    /// Check if a specific rule fired in the diagnostics.
    fn has_rule(diags: &[CheckDiagnostic], rule_id: &str) -> bool {
        diags.iter().any(|d| d.rule_id == rule_id)
    }

    /// Count diagnostics with a specific rule ID.
    fn count_rule(diags: &[CheckDiagnostic], rule_id: &str) -> usize {
        diags.iter().filter(|d| d.rule_id == rule_id).count()
    }

    // ── Layer detection tests ────────────────────────────────────────────

    #[test]
    fn test_detect_layer_staging() {
        assert_eq!(
            detect_layer("models/staging/stg_orders.sql"),
            Some("staging")
        );
        assert_eq!(detect_layer("models/stg/stg_orders.sql"), Some("staging"));
    }

    #[test]
    fn test_detect_layer_intermediate() {
        assert_eq!(
            detect_layer("models/intermediate/int_calc.sql"),
            Some("intermediate")
        );
        assert_eq!(
            detect_layer("models/int/int_calc.sql"),
            Some("intermediate")
        );
    }

    #[test]
    fn test_detect_layer_marts() {
        assert_eq!(detect_layer("models/marts/fct_revenue.sql"), Some("marts"));
        assert_eq!(detect_layer("models/mart/dim_user.sql"), Some("marts"));
    }

    #[test]
    fn test_detect_layer_none() {
        assert_eq!(detect_layer("models/other/query.sql"), None);
        assert_eq!(detect_layer("query.sql"), None);
    }

    #[test]
    fn test_detect_layer_case_insensitive() {
        assert_eq!(
            detect_layer("models/Staging/stg_orders.sql"),
            Some("staging")
        );
        assert_eq!(detect_layer("models/MARTS/fct_revenue.sql"), Some("marts"));
    }

    #[test]
    fn test_detect_layer_nested_path() {
        assert_eq!(
            detect_layer("project/models/staging/customers/stg_orders.sql"),
            Some("staging")
        );
    }

    #[test]
    fn test_detect_layer_from_prefix_stg() {
        assert_eq!(detect_layer_from_prefix("stg_orders"), Some("staging"));
        assert_eq!(detect_layer_from_prefix("staging_orders"), Some("staging"));
    }

    #[test]
    fn test_detect_layer_from_prefix_int() {
        assert_eq!(detect_layer_from_prefix("int_calc"), Some("intermediate"));
        assert_eq!(
            detect_layer_from_prefix("intermediate_calc"),
            Some("intermediate")
        );
    }

    #[test]
    fn test_detect_layer_from_prefix_marts() {
        assert_eq!(detect_layer_from_prefix("fct_revenue"), Some("marts"));
        assert_eq!(detect_layer_from_prefix("fact_revenue"), Some("marts"));
        assert_eq!(detect_layer_from_prefix("dim_user"), Some("marts"));
        assert_eq!(detect_layer_from_prefix("dimension_user"), Some("marts"));
    }

    #[test]
    fn test_detect_layer_from_prefix_none() {
        assert_eq!(detect_layer_from_prefix("orders"), None);
        assert_eq!(detect_layer_from_prefix("my_model"), None);
    }

    // ── NAME001 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name001_stg_prefix_passes_in_staging() {
        let diags = check_with_model("stg_orders", "models/staging/stg_orders.sql");
        assert!(!has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_name001_staging_prefix_passes_in_staging() {
        let diags = check_with_model("staging_orders", "models/staging/staging_orders.sql");
        assert!(!has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_name001_fires_for_missing_prefix_in_staging() {
        let diags = check_with_model("orders", "models/staging/orders.sql");
        assert!(has_rule(&diags, "NAME001"));
        let d = diags.iter().find(|d| d.rule_id == "NAME001").unwrap();
        assert_eq!(d.severity, Severity::Warning);
        assert_eq!(d.category, CheckCategory::NamingConvention);
        assert!(d.doc_url.is_some());
        assert!(d.message.contains("orders"));
        assert!(d.suggestion.is_some());
    }

    #[test]
    fn test_name001_does_not_fire_in_marts() {
        let diags = check_with_model("stg_orders", "models/marts/stg_orders.sql");
        assert!(!has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_name001_stg_dir_variant() {
        let diags = check_with_model("raw_orders", "models/stg/raw_orders.sql");
        assert!(has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_name001_severity_is_warning() {
        let diags = check_with_model("orders", "models/staging/orders.sql");
        let d = diags.iter().find(|d| d.rule_id == "NAME001").unwrap();
        assert_eq!(d.severity, Severity::Warning);
    }

    // ── NAME002 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name002_int_prefix_passes() {
        let diags = check_with_model(
            "int_orders_pivoted",
            "models/intermediate/int_orders_pivoted.sql",
        );
        assert!(!has_rule(&diags, "NAME002"));
    }

    #[test]
    fn test_name002_intermediate_prefix_passes() {
        let diags = check_with_model(
            "intermediate_calc",
            "models/intermediate/intermediate_calc.sql",
        );
        assert!(!has_rule(&diags, "NAME002"));
    }

    #[test]
    fn test_name002_fires_for_missing_prefix() {
        let diags = check_with_model("calc", "models/intermediate/calc.sql");
        assert!(has_rule(&diags, "NAME002"));
        let d = diags.iter().find(|d| d.rule_id == "NAME002").unwrap();
        assert_eq!(d.severity, Severity::Warning);
        assert!(d.doc_url.is_some());
    }

    #[test]
    fn test_name002_int_dir_variant() {
        let diags = check_with_model("calc", "models/int/calc.sql");
        assert!(has_rule(&diags, "NAME002"));
    }

    #[test]
    fn test_name002_does_not_fire_outside_intermediate() {
        let diags = check_with_model("orders", "models/staging/orders.sql");
        assert!(!has_rule(&diags, "NAME002"));
    }

    // ── NAME003 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name003_fct_prefix_passes_in_marts() {
        let diags = check_with_model("fct_revenue", "models/marts/fct_revenue.sql");
        assert!(!has_rule(&diags, "NAME003"));
    }

    #[test]
    fn test_name003_fact_prefix_passes_in_marts() {
        let diags = check_with_model("fact_revenue", "models/marts/fact_revenue.sql");
        assert!(!has_rule(&diags, "NAME003"));
    }

    #[test]
    fn test_name003_fires_for_unprefixed_in_marts() {
        let diags = check_with_model("revenue", "models/marts/revenue.sql");
        assert!(has_rule(&diags, "NAME003"));
        let d = diags.iter().find(|d| d.rule_id == "NAME003").unwrap();
        assert_eq!(d.severity, Severity::Warning);
        assert!(d.doc_url.is_some());
    }

    #[test]
    fn test_name003_dim_prefix_does_not_fire() {
        let diags = check_with_model("dim_customer", "models/marts/dim_customer.sql");
        assert!(!has_rule(&diags, "NAME003"));
    }

    #[test]
    fn test_name003_mart_dir_variant() {
        let diags = check_with_model("revenue", "models/mart/revenue.sql");
        assert!(has_rule(&diags, "NAME003"));
    }

    // ── NAME004 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name004_dim_prefix_passes_in_marts() {
        let diags = check_with_model("dim_user", "models/marts/dim_user.sql");
        assert!(!has_rule(&diags, "NAME004"));
    }

    #[test]
    fn test_name004_dimension_prefix_passes_in_marts() {
        let diags = check_with_model("dimension_user", "models/marts/dimension_user.sql");
        assert!(!has_rule(&diags, "NAME004"));
    }

    #[test]
    fn test_name004_fct_prefix_does_not_fire() {
        let diags = check_with_model("fct_orders", "models/marts/fct_orders.sql");
        assert!(!has_rule(&diags, "NAME004"));
    }

    #[test]
    fn test_name004_does_not_fire_outside_marts() {
        let diags = check_with_model("dim_customers", "models/staging/dim_customers.sql");
        assert!(!has_rule(&diags, "NAME004"));
    }

    // ── NAME005 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name005_snake_case_passes() {
        let diags = check_with_model("my_model_v2", "models/my_model_v2.sql");
        assert!(!has_rule(&diags, "NAME005"));
    }

    #[test]
    fn test_name005_fires_for_camel_case() {
        let diags = check_with_model("MyModel", "models/MyModel.sql");
        assert!(has_rule(&diags, "NAME005"));
        let d = diags.iter().find(|d| d.rule_id == "NAME005").unwrap();
        assert_eq!(d.severity, Severity::Warning);
        assert!(d.message.contains("MyModel"));
        assert!(d.doc_url.is_some());
    }

    #[test]
    fn test_name005_fires_for_hyphenated() {
        let diags = check_with_model("my-model", "models/my-model.sql");
        assert!(has_rule(&diags, "NAME005"));
    }

    #[test]
    fn test_name005_fires_for_leading_digit() {
        let diags = check_with_model("3_model", "models/3_model.sql");
        assert!(has_rule(&diags, "NAME005"));
    }

    #[test]
    fn test_name005_single_char_passes() {
        let diags = check_with_model("a", "models/a.sql");
        assert!(!has_rule(&diags, "NAME005"));
    }

    #[test]
    fn test_name005_all_uppercase_fires() {
        let diags = check_with_model("STG_ORDERS", "models/STG_ORDERS.sql");
        assert!(has_rule(&diags, "NAME005"));
    }

    #[test]
    fn test_name005_with_numbers_passes() {
        let diags = check_with_model("stg_orders_v2", "models/stg_orders_v2.sql");
        assert!(!has_rule(&diags, "NAME005"));
    }

    // ── NAME006 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name006_stg_in_staging_passes() {
        let diags = check_with_model("stg_orders", "models/staging/stg_orders.sql");
        assert!(!has_rule(&diags, "NAME006"));
    }

    #[test]
    fn test_name006_fires_stg_in_marts() {
        let diags = check_with_model("stg_orders", "models/marts/stg_orders.sql");
        assert!(has_rule(&diags, "NAME006"));
        let d = diags.iter().find(|d| d.rule_id == "NAME006").unwrap();
        assert!(d.message.contains("staging"));
        assert!(d.message.contains("marts"));
        assert!(d.doc_url.is_some());
    }

    #[test]
    fn test_name006_fires_fct_in_staging() {
        let diags = check_with_model("fct_revenue", "models/staging/fct_revenue.sql");
        assert!(has_rule(&diags, "NAME006"));
    }

    #[test]
    fn test_name006_no_layer_no_fire() {
        let diags = check_with_model("stg_orders", "models/other/stg_orders.sql");
        assert!(!has_rule(&diags, "NAME006"));
    }

    #[test]
    fn test_name006_no_prefix_no_fire() {
        let diags = check_with_model("orders", "models/staging/orders.sql");
        assert!(!has_rule(&diags, "NAME006"));
    }

    #[test]
    fn test_name006_int_in_marts_fires() {
        let diags = check_with_model("int_calc", "models/marts/int_calc.sql");
        assert!(has_rule(&diags, "NAME006"));
    }

    // ── NAME007 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name007_disabled_by_default() {
        let checker = NamingChecker;
        let config = CheckConfig::default();
        let content = "-- name: orders\n-- contract_columns: OrderID, Amount\nSELECT 1";
        let mut header = HashMap::new();
        header.insert("contract_columns".to_owned(), "OrderID, Amount".to_owned());
        let model = make_model_with_header("orders", "models/orders.sql", content, header);
        let diags = checker.check_file("models/orders.sql", content, Some(&model), &config);
        assert!(!has_rule(&diags, "NAME007"));
    }

    #[test]
    fn test_name007_fires_when_enabled() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME007".to_owned(), RuleSeverityOverride::Warning);
        let content = "-- name: orders\n-- contract_columns: OrderID, Amount\nSELECT 1";
        let mut header = HashMap::new();
        header.insert("contract_columns".to_owned(), "OrderID, Amount".to_owned());
        let model = make_model_with_header("orders", "models/orders.sql", content, header);
        let diags = checker.check_file("models/orders.sql", content, Some(&model), &config);
        assert!(has_rule(&diags, "NAME007"));
        assert_eq!(count_rule(&diags, "NAME007"), 2);
    }

    #[test]
    fn test_name007_passes_for_valid_columns() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME007".to_owned(), RuleSeverityOverride::Warning);
        let content = "-- name: orders\n-- contract_columns: order_id, amount\nSELECT 1";
        let mut header = HashMap::new();
        header.insert("contract_columns".to_owned(), "order_id, amount".to_owned());
        let model = make_model_with_header("orders", "models/orders.sql", content, header);
        let diags = checker.check_file("models/orders.sql", content, Some(&model), &config);
        assert!(!has_rule(&diags, "NAME007"));
    }

    #[test]
    fn test_name007_no_columns_header_no_fire() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME007".to_owned(), RuleSeverityOverride::Warning);
        let model = make_model("orders", "models/orders.sql");
        let diags = checker.check_file("models/orders.sql", &model.content, Some(&model), &config);
        assert!(!has_rule(&diags, "NAME007"));
    }

    #[test]
    fn test_name007_mixed_valid_invalid_columns() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME007".to_owned(), RuleSeverityOverride::Warning);
        let content =
            "-- name: orders\n-- contract_columns: order_id, Amount, created_at\nSELECT 1";
        let mut header = HashMap::new();
        header.insert(
            "contract_columns".to_owned(),
            "order_id, Amount, created_at".to_owned(),
        );
        let model = make_model_with_header("orders", "models/orders.sql", content, header);
        let diags = checker.check_file("models/orders.sql", content, Some(&model), &config);
        assert_eq!(count_rule(&diags, "NAME007"), 1); // Only Amount fails
    }

    // ── NAME008 tests ────────────────────────────────────────────────────

    #[test]
    fn test_name008_disabled_by_default() {
        let diags = check_with_model("model.sql", "models/model.sql");
        assert!(!has_rule(&diags, "NAME008"));
    }

    #[test]
    fn test_name008_fires_when_enabled() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME008".to_owned(), RuleSeverityOverride::Info);
        let content = "-- name: model.sql\nSELECT 1";
        let model = make_model("model.sql", "models/model.sql");
        let diags = checker.check_file("models/model.sql", content, Some(&model), &config);
        assert!(has_rule(&diags, "NAME008"));
        let d = diags.iter().find(|d| d.rule_id == "NAME008").unwrap();
        assert_eq!(d.severity, Severity::Info);
        assert!(d.suggestion.as_ref().unwrap().contains("model"));
        assert!(d.doc_url.is_some());
    }

    #[test]
    fn test_name008_passes_without_extension() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME008".to_owned(), RuleSeverityOverride::Info);
        let model = make_model("model", "models/model.sql");
        let diags = checker.check_file("models/model.sql", &model.content, Some(&model), &config);
        assert!(!has_rule(&diags, "NAME008"));
    }

    #[test]
    fn test_name008_yml_extension_fires() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME008".to_owned(), RuleSeverityOverride::Info);
        let content = "-- name: model.yml\nSELECT 1";
        let model = make_model("model.yml", "models/model.sql");
        let diags = checker.check_file("models/model.sql", content, Some(&model), &config);
        assert!(has_rule(&diags, "NAME008"));
    }

    #[test]
    fn test_name008_yaml_extension_fires() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME008".to_owned(), RuleSeverityOverride::Info);
        let content = "-- name: model.yaml\nSELECT 1";
        let model = make_model("model.yaml", "models/model.sql");
        let diags = checker.check_file("models/model.sql", content, Some(&model), &config);
        assert!(has_rule(&diags, "NAME008"));
    }

    // ── Edge cases and integration ───────────────────────────────────────

    #[test]
    fn test_non_sql_file_returns_empty() {
        let checker = NamingChecker;
        let config = CheckConfig::default();
        let diags = checker.check_file("models/schema.yml", "version: 2", None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_no_model_returns_empty() {
        let checker = NamingChecker;
        let config = CheckConfig::default();
        let diags = checker.check_file("models/raw.sql", "SELECT 1", None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_empty_model_name_falls_back_to_filename() {
        let checker = NamingChecker;
        let config = CheckConfig::default();
        let content = "SELECT 1";
        let model = DiscoveredModel {
            name: String::new(),
            file_path: "models/staging/stg_orders.sql".to_owned(),
            content_hash: String::new(),
            ref_names: Vec::new(),
            header: HashMap::new(),
            content: content.to_owned(),
        };
        let diags = checker.check_file(
            "models/staging/stg_orders.sql",
            content,
            Some(&model),
            &config,
        );
        // Empty name falls back to filename stem "stg_orders" which passes NAME001
        assert!(!has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_truly_empty_model_name_returns_empty() {
        let checker = NamingChecker;
        let config = CheckConfig::default();
        let content = "SELECT 1";
        let model = DiscoveredModel {
            name: String::new(),
            file_path: "models/staging/".to_owned(),
            content_hash: String::new(),
            ref_names: Vec::new(),
            header: HashMap::new(),
            content: content.to_owned(),
        };
        // File path doesn't end in .sql, so checker exits early
        let diags = checker.check_file("models/staging/", content, Some(&model), &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_checker_name_is_naming() {
        let checker = NamingChecker;
        assert_eq!(checker.name(), "naming");
    }

    #[test]
    fn test_all_diags_have_doc_url() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME007".to_owned(), RuleSeverityOverride::Warning);
        config
            .rules
            .insert("NAME008".to_owned(), RuleSeverityOverride::Info);

        let content = "-- name: BadName.sql\n-- contract_columns: BadCol\nSELECT 1";
        let mut header = HashMap::new();
        header.insert("contract_columns".to_owned(), "BadCol".to_owned());
        let model =
            make_model_with_header("BadName.sql", "models/staging/BadName.sql", content, header);
        let diags =
            checker.check_file("models/staging/BadName.sql", content, Some(&model), &config);
        assert!(!diags.is_empty());
        for d in &diags {
            assert!(
                d.doc_url.is_some(),
                "Diagnostic {} missing doc_url",
                d.rule_id
            );
        }
    }

    #[test]
    fn test_severity_override_via_config() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME001".to_owned(), RuleSeverityOverride::Error);
        let model = make_model("orders", "models/staging/orders.sql");
        let diags = checker.check_file(
            "models/staging/orders.sql",
            &model.content,
            Some(&model),
            &config,
        );
        let d = diags.iter().find(|d| d.rule_id == "NAME001").unwrap();
        assert_eq!(d.severity, Severity::Error);
    }

    #[test]
    fn test_rule_disabled_via_config() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .rules
            .insert("NAME001".to_owned(), RuleSeverityOverride::Off);
        let model = make_model("orders", "models/staging/orders.sql");
        let diags = checker.check_file(
            "models/staging/orders.sql",
            &model.content,
            Some(&model),
            &config,
        );
        assert!(!has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_custom_layer_pattern() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config
            .naming
            .layers
            .insert("staging".to_owned(), "^src_".to_owned());
        let model = make_model("src_orders", "models/staging/src_orders.sql");
        let diags = checker.check_file(
            "models/staging/src_orders.sql",
            &model.content,
            Some(&model),
            &config,
        );
        assert!(!has_rule(&diags, "NAME001"));
    }

    #[test]
    fn test_custom_model_pattern() {
        let checker = NamingChecker;
        let mut config = CheckConfig::default();
        config.naming.model_pattern = "^[A-Z][A-Z0-9_]*$".to_owned();
        let model = make_model("ORDERS", "models/ORDERS.sql");
        let diags = checker.check_file("models/ORDERS.sql", &model.content, Some(&model), &config);
        assert!(!has_rule(&diags, "NAME005"));
    }

    #[test]
    fn test_find_name_line_returns_correct_line() {
        let content = "-- kind: FULL_REFRESH\n-- name: stg_orders\nSELECT 1";
        assert_eq!(find_name_line(content), 2);
    }

    #[test]
    fn test_find_name_line_defaults_to_1() {
        let content = "SELECT 1";
        assert_eq!(find_name_line(content), 1);
    }

    #[test]
    fn test_find_header_field_line() {
        let content = "-- name: orders\n-- kind: FULL_REFRESH\n-- contract_columns: a, b\nSELECT 1";
        assert_eq!(find_header_field_line(content, "contract_columns"), 3);
        assert_eq!(find_header_field_line(content, "missing"), 1);
    }

    #[test]
    fn test_get_model_name_from_header() {
        let model = make_model("stg_orders", "models/staging/stg_orders.sql");
        assert_eq!(get_model_name(&model), "stg_orders");
    }

    #[test]
    fn test_get_model_name_from_filename() {
        let model = DiscoveredModel {
            name: String::new(),
            file_path: "models/staging/stg_orders.sql".to_owned(),
            content_hash: String::new(),
            ref_names: Vec::new(),
            header: HashMap::new(),
            content: "SELECT 1".to_owned(),
        };
        assert_eq!(get_model_name(&model), "stg_orders");
    }

    #[test]
    fn test_multiple_rules_can_fire_simultaneously() {
        // A model with camelCase name in staging should fire NAME001 + NAME005
        let diags = check_with_model("BadOrders", "models/staging/BadOrders.sql");
        assert!(has_rule(&diags, "NAME001")); // not stg_ prefix
        assert!(has_rule(&diags, "NAME005")); // not snake_case
    }
}
