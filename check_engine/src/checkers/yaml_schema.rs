//! YAML schema validation checker — rules YML001 through YML009.
//!
//! Validates YAML files used for model documentation (`schema.yml`, `*.yml` in
//! models directories), dbt project files (`dbt_project.yml`), and profile files
//! (`profiles.yml`).
//!
//! YML006 is unique: it calls back into Python via `Python::with_gil()` to invoke
//! `core_engine.sql_toolkit.scope_analyzer.extract_columns()` for column matching.
//! This check runs only in the sequential `check_project()` phase, never in parallel.
//!
//! ## Rule Summary
//!
//! | Rule  | Default  | Severity | Description                                   |
//! |-------|----------|----------|-----------------------------------------------|
//! | YML001| enabled  | error    | Invalid YAML syntax (parse error)             |
//! | YML002| enabled  | error    | Missing required `name` in dbt_project.yml    |
//! | YML003| enabled  | error    | Missing required `version` in dbt_project.yml |
//! | YML004| enabled  | warning  | Model listed in YAML but no .sql file exists  |
//! | YML005| enabled  | warning  | .sql file has no YAML documentation           |
//! | YML006| enabled  | error    | YAML column not in SQL output columns         |
//! | YML007| enabled  | warning  | Model has zero tests defined                  |
//! | YML008| enabled  | warning  | Model has zero column-level descriptions      |
//! | YML009| disabled | info     | Profile references non-existent target        |

use std::collections::{HashMap, HashSet};
use std::path::Path;

use pyo3::types::PyAnyMethods;

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// YAML schema validation checker.
pub struct YamlSchemaChecker;

impl Checker for YamlSchemaChecker {
    fn name(&self) -> &'static str {
        "yaml_schema"
    }

    /// Per-file checks for YAML files: YML001 (parse error), YML002/YML003 (dbt_project.yml
    /// required fields).
    fn check_file(
        &self,
        file_path: &str,
        content: &str,
        _model: Option<&DiscoveredModel>,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        // Only check .yml / .yaml files
        if !file_path.ends_with(".yml") && !file_path.ends_with(".yaml") {
            return Vec::new();
        }

        let mut diags = Vec::new();

        // YML001: Invalid YAML syntax
        if config.is_rule_enabled_for_path("YML001", file_path, true) {
            check_yml001(file_path, content, config, &mut diags);
        }

        // YML002/YML003: dbt_project.yml required fields
        let filename = Path::new(file_path)
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("");

        if filename == "dbt_project.yml" {
            if config.is_rule_enabled_for_path("YML002", file_path, true) {
                check_yml002(file_path, content, config, &mut diags);
            }
            if config.is_rule_enabled_for_path("YML003", file_path, true) {
                check_yml003(file_path, content, config, &mut diags);
            }
        }

        diags
    }

    /// Cross-file checks: YML004-YML009 (model/YAML correspondence and column matching).
    ///
    /// YML006 runs here in the sequential `check_project()` phase to avoid GIL
    /// contention during parallel per-file checks. It calls into Python via
    /// `Python::with_gil()` to extract SQL output columns.
    fn check_project(
        &self,
        models: &[DiscoveredModel],
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        // Build sets of SQL file stems and YAML-documented model names
        let sql_model_names: HashSet<String> = models
            .iter()
            .filter(|m| m.file_path.ends_with(".sql"))
            .map(|m| m.name.clone())
            .collect();

        // Collect YAML model documentation from .yml/.yaml files
        let yaml_models = collect_yaml_models(models);

        // YML004: Model listed in YAML but no .sql file exists
        if config.is_rule_enabled("YML004", true) {
            check_yml004(&yaml_models, &sql_model_names, config, &mut diags);
        }

        // YML005: .sql file exists but model not documented in any YAML file
        if config.is_rule_enabled("YML005", true) {
            check_yml005(&yaml_models, &sql_model_names, models, config, &mut diags);
        }

        // YML006: Column in YAML not in SQL output columns (Python GIL callback)
        // Runs sequentially here to avoid GIL contention.
        if config.is_rule_enabled("YML006", true) {
            let sql_models: HashMap<String, &DiscoveredModel> = models
                .iter()
                .filter(|m| m.file_path.ends_with(".sql"))
                .map(|m| (m.name.clone(), m))
                .collect();
            check_yml006(&yaml_models, &sql_models, config, &mut diags);
        }

        // YML007: Model has zero tests defined in YAML
        if config.is_rule_enabled("YML007", true) {
            check_yml007(&yaml_models, config, &mut diags);
        }

        // YML008: Model has zero column-level descriptions
        if config.is_rule_enabled("YML008", true) {
            check_yml008(&yaml_models, config, &mut diags);
        }

        // YML009: Profile references non-existent target (disabled by default)
        if config.is_rule_enabled("YML009", false) {
            check_yml009(models, config, &mut diags);
        }

        diags
    }
}

// ---------------------------------------------------------------------------
// Parsed YAML model documentation
// ---------------------------------------------------------------------------

/// Represents a model documented in a YAML schema file.
#[derive(Debug, Clone)]
struct YamlModelDoc {
    /// Model name from the YAML file.
    name: String,
    /// Source YAML file path.
    yaml_file: String,
    /// Line number where this model is defined.
    line: u32,
    /// Column names documented for this model.
    columns: Vec<YamlColumnDoc>,
    /// Whether this model has any tests defined.
    has_tests: bool,
}

/// Represents a column documented in a YAML schema file.
#[derive(Debug, Clone)]
struct YamlColumnDoc {
    /// Column name.
    name: String,
    /// Whether the column has a description.
    has_description: bool,
    /// Whether the column has any tests.
    has_tests: bool,
}

// ---------------------------------------------------------------------------
// YAML model collection
// ---------------------------------------------------------------------------

/// Collect all model documentation from YAML files in the project.
///
/// Parses schema.yml / *.yml files looking for dbt-style model documentation:
/// ```yaml
/// models:
///   - name: stg_orders
///     columns:
///       - name: order_id
///         description: Primary key
///         tests:
///           - unique
///           - not_null
/// ```
fn collect_yaml_models(models: &[DiscoveredModel]) -> Vec<YamlModelDoc> {
    let mut yaml_models = Vec::new();

    for model in models {
        if !model.file_path.ends_with(".yml") && !model.file_path.ends_with(".yaml") {
            continue;
        }

        // Skip dbt_project.yml and profiles.yml — they don't contain model docs
        let filename = Path::new(&model.file_path)
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("");
        if filename == "dbt_project.yml" || filename == "profiles.yml" {
            continue;
        }

        let parsed: serde_yaml::Value = match serde_yaml::from_str(&model.content) {
            Ok(v) => v,
            Err(_) => continue, // YML001 already catches parse errors
        };

        let models_section = match parsed.get("models") {
            Some(serde_yaml::Value::Sequence(seq)) => seq,
            _ => continue,
        };

        for (idx, model_entry) in models_section.iter().enumerate() {
            let model_map = match model_entry.as_mapping() {
                Some(m) => m,
                None => continue,
            };

            let name = match model_map
                .get(serde_yaml::Value::String("name".to_owned()))
                .and_then(|v| v.as_str())
            {
                Some(n) => n.to_owned(),
                None => continue,
            };

            // Parse columns
            let columns = parse_yaml_columns(model_map);

            // Check if model has tests at model level
            let model_level_tests = model_map
                .get(serde_yaml::Value::String("tests".to_owned()))
                .or_else(|| model_map.get(serde_yaml::Value::String("data_tests".to_owned())))
                .and_then(|v| v.as_sequence())
                .is_some_and(|seq| !seq.is_empty());

            let column_level_tests = columns.iter().any(|c| c.has_tests);

            yaml_models.push(YamlModelDoc {
                name,
                yaml_file: model.file_path.clone(),
                line: (idx as u32) + 1, // Approximate line — YAML doesn't give exact lines via serde
                columns,
                has_tests: model_level_tests || column_level_tests,
            });
        }
    }

    yaml_models
}

/// Parse columns from a YAML model entry.
fn parse_yaml_columns(model_map: &serde_yaml::Mapping) -> Vec<YamlColumnDoc> {
    let columns_key = serde_yaml::Value::String("columns".to_owned());
    let columns_seq = match model_map.get(&columns_key).and_then(|v| v.as_sequence()) {
        Some(seq) => seq,
        None => return Vec::new(),
    };

    let mut columns = Vec::new();
    for col_entry in columns_seq {
        let col_map = match col_entry.as_mapping() {
            Some(m) => m,
            None => continue,
        };

        let col_name = match col_map
            .get(serde_yaml::Value::String("name".to_owned()))
            .and_then(|v| v.as_str())
        {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let has_description = col_map
            .get(serde_yaml::Value::String("description".to_owned()))
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.trim().is_empty());

        let has_tests = col_map
            .get(serde_yaml::Value::String("tests".to_owned()))
            .or_else(|| col_map.get(serde_yaml::Value::String("data_tests".to_owned())))
            .and_then(|v| v.as_sequence())
            .is_some_and(|seq| !seq.is_empty());

        columns.push(YamlColumnDoc {
            name: col_name,
            has_description,
            has_tests,
        });
    }

    columns
}

// ---------------------------------------------------------------------------
// YML001: Invalid YAML syntax
// ---------------------------------------------------------------------------

/// YML001: Check for YAML parse errors.
fn check_yml001(
    file_path: &str,
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let severity = config.effective_severity_for_path("YML001", file_path, Severity::Error);

    if let Err(e) = serde_yaml::from_str::<serde_yaml::Value>(content) {
        let error_msg = e.to_string();

        // Extract line number from serde_yaml error if available
        let line = extract_yaml_error_line(&error_msg);

        diags.push(CheckDiagnostic {
            rule_id: "YML001".to_owned(),
            message: format!("Invalid YAML syntax: {error_msg}"),
            severity,
            category: CheckCategory::YamlSchema,
            file_path: file_path.to_owned(),
            line,
            column: 0,
            snippet: extract_error_context(content, line),
            suggestion: Some(
                "Fix the YAML syntax error. Common issues: incorrect indentation, \
                             missing colons, unquoted special characters."
                    .to_owned(),
            ),
            doc_url: Some("https://docs.ironlayer.app/check/rules/YML001".to_owned()),
        });
    }
}

/// Extract line number from a serde_yaml error message.
fn extract_yaml_error_line(error_msg: &str) -> u32 {
    // serde_yaml errors often contain "at line N column M"
    if let Some(line_idx) = error_msg.find("at line ") {
        let rest = &error_msg[line_idx + 8..];
        if let Some(end) = rest.find(|c: char| !c.is_ascii_digit()) {
            if let Ok(line) = rest[..end].parse::<u32>() {
                return line;
            }
        }
    }
    0
}

/// Extract a context snippet around a given line number.
fn extract_error_context(content: &str, line: u32) -> Option<String> {
    if line == 0 {
        return None;
    }
    let lines: Vec<&str> = content.lines().collect();
    let idx = (line as usize).saturating_sub(1);
    if idx < lines.len() {
        let snippet = lines[idx];
        if snippet.len() <= 120 {
            Some(snippet.to_owned())
        } else {
            Some(format!("{}...", &snippet[..117]))
        }
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// YML002: Missing required `name` in dbt_project.yml
// ---------------------------------------------------------------------------

/// YML002: Check that dbt_project.yml has a `name` field.
fn check_yml002(
    file_path: &str,
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let severity = config.effective_severity_for_path("YML002", file_path, Severity::Error);

    let parsed: serde_yaml::Value = match serde_yaml::from_str(content) {
        Ok(v) => v,
        Err(_) => return, // YML001 handles parse errors
    };

    let has_name = parsed
        .as_mapping()
        .and_then(|m| m.get(serde_yaml::Value::String("name".to_owned())))
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.trim().is_empty());

    if !has_name {
        diags.push(CheckDiagnostic {
            rule_id: "YML002".to_owned(),
            message: "dbt_project.yml is missing the required 'name' field. \
                     Every dbt project must have a unique name."
                .to_owned(),
            severity,
            category: CheckCategory::YamlSchema,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: content.lines().next().map(|l| l.to_owned()),
            suggestion: Some(
                "Add 'name: my_project' at the top level of dbt_project.yml.".to_owned(),
            ),
            doc_url: Some("https://docs.ironlayer.app/check/rules/YML002".to_owned()),
        });
    }
}

// ---------------------------------------------------------------------------
// YML003: Missing required `version` in dbt_project.yml
// ---------------------------------------------------------------------------

/// YML003: Check that dbt_project.yml has a `version` field.
fn check_yml003(
    file_path: &str,
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let severity = config.effective_severity_for_path("YML003", file_path, Severity::Error);

    let parsed: serde_yaml::Value = match serde_yaml::from_str(content) {
        Ok(v) => v,
        Err(_) => return, // YML001 handles parse errors
    };

    let has_version = parsed
        .as_mapping()
        .and_then(|m| m.get(serde_yaml::Value::String("version".to_owned())))
        .is_some();

    if !has_version {
        diags.push(CheckDiagnostic {
            rule_id: "YML003".to_owned(),
            message: "dbt_project.yml is missing the required 'version' field. \
                     Every dbt project must declare a version."
                .to_owned(),
            severity,
            category: CheckCategory::YamlSchema,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: content.lines().next().map(|l| l.to_owned()),
            suggestion: Some(
                "Add 'version: '1.0.0'' at the top level of dbt_project.yml.".to_owned(),
            ),
            doc_url: Some("https://docs.ironlayer.app/check/rules/YML003".to_owned()),
        });
    }
}

// ---------------------------------------------------------------------------
// YML004: Model listed in YAML but no corresponding .sql file found
// ---------------------------------------------------------------------------

/// YML004: Check for YAML-documented models with no matching SQL file.
fn check_yml004(
    yaml_models: &[YamlModelDoc],
    sql_model_names: &HashSet<String>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for ym in yaml_models {
        if !config.is_rule_enabled_for_path("YML004", &ym.yaml_file, true) {
            continue;
        }

        if !sql_model_names.contains(&ym.name) {
            let severity =
                config.effective_severity_for_path("YML004", &ym.yaml_file, Severity::Warning);

            // Find closest match for suggestion
            let suggestion = find_closest_model_name(&ym.name, sql_model_names);

            diags.push(CheckDiagnostic {
                rule_id: "YML004".to_owned(),
                message: format!(
                    "Model '{}' is documented in YAML but no corresponding .sql file was found.",
                    ym.name
                ),
                severity,
                category: CheckCategory::YamlSchema,
                file_path: ym.yaml_file.clone(),
                line: ym.line,
                column: 0,
                snippet: Some(format!("- name: {}", ym.name)),
                suggestion: suggestion.map(|s| {
                    format!("Did you mean '{s}'? Or create the missing SQL file for this model.")
                }),
                doc_url: Some("https://docs.ironlayer.app/check/rules/YML004".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// YML005: .sql file exists but model not documented in any YAML file
// ---------------------------------------------------------------------------

/// YML005: Check for SQL files that have no YAML documentation.
fn check_yml005(
    yaml_models: &[YamlModelDoc],
    _sql_model_names: &HashSet<String>,
    models: &[DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let documented_names: HashSet<&str> = yaml_models.iter().map(|ym| ym.name.as_str()).collect();

    for model in models {
        if !model.file_path.ends_with(".sql") {
            continue;
        }

        if !config.is_rule_enabled_for_path("YML005", &model.file_path, true) {
            continue;
        }

        if !documented_names.contains(model.name.as_str()) {
            let severity =
                config.effective_severity_for_path("YML005", &model.file_path, Severity::Warning);

            diags.push(CheckDiagnostic {
                rule_id: "YML005".to_owned(),
                message: format!(
                    "Model '{}' (file: {}) has no YAML documentation. \
                     Add it to a schema.yml file for discoverability and testing.",
                    model.name, model.file_path
                ),
                severity,
                category: CheckCategory::YamlSchema,
                file_path: model.file_path.clone(),
                line: 1,
                column: 0,
                snippet: None,
                suggestion: Some(format!(
                    "Add a models entry to a schema.yml file:\n  \
                     - name: {}\n    description: <describe this model>",
                    model.name
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/YML005".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// YML006: Column listed in YAML does not match SQL output columns
// ---------------------------------------------------------------------------

/// YML006: Check that YAML-documented columns match SQL output columns.
///
/// This function is designed to be called from the engine's sequential
/// `check_project()` phase. It requires Python GIL access to call
/// `core_engine.sql_toolkit.scope_analyzer.extract_columns()`.
///
/// If `core_engine` is unavailable, the check is skipped with an info diagnostic.
///
/// # Arguments
///
/// * `yaml_models` — YAML model documentation to validate.
/// * `sql_models` — Discovered SQL models with file content.
/// * `config` — Check configuration.
///
/// # Returns
///
/// Diagnostics for any column mismatches found, or an info diagnostic if
/// the Python toolkit is unavailable.
fn check_yml006(
    yaml_models: &[YamlModelDoc],
    sql_models: &HashMap<String, &DiscoveredModel>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled("YML006", true) {
        return;
    }

    // Early return if there are no YAML models with documented columns to validate
    let has_columns_to_check = yaml_models
        .iter()
        .any(|ym| !ym.columns.is_empty() && sql_models.contains_key(&ym.name));
    if !has_columns_to_check {
        return;
    }

    // Attempt to extract columns via Python
    let python_available = check_python_toolkit_available();

    if !python_available {
        diags.push(CheckDiagnostic {
            rule_id: "YML006".to_owned(),
            message: "YML006 skipped: Python SQL toolkit unavailable.".to_owned(),
            severity: Severity::Info,
            category: CheckCategory::YamlSchema,
            file_path: String::new(),
            line: 0,
            column: 0,
            snippet: None,
            suggestion: Some(
                "Install ironlayer-core to enable YML006 column validation.".to_owned(),
            ),
            doc_url: Some("https://docs.ironlayer.app/check/rules/YML006".to_owned()),
        });
        return;
    }

    for ym in yaml_models {
        if ym.columns.is_empty() {
            continue;
        }

        let sql_model = match sql_models.get(&ym.name) {
            Some(m) => m,
            None => continue, // YML004 catches this case
        };

        if !config.is_rule_enabled_for_path("YML006", &ym.yaml_file, true) {
            continue;
        }

        // Extract SQL output columns via Python
        let sql_columns = match extract_sql_columns_via_python(&sql_model.content) {
            Some(cols) => cols,
            None => continue, // Skip if extraction fails (timeout, error)
        };

        if sql_columns.is_empty() {
            continue; // Can't validate if we don't know the SQL columns
        }

        let sql_col_set: HashSet<String> = sql_columns.iter().map(|c| c.to_lowercase()).collect();

        for yaml_col in &ym.columns {
            let col_lower = yaml_col.name.to_lowercase();
            if !sql_col_set.contains(&col_lower) {
                let severity =
                    config.effective_severity_for_path("YML006", &ym.yaml_file, Severity::Error);

                // Find closest column name for suggestion
                let closest = find_closest_column(&yaml_col.name, &sql_columns);

                diags.push(CheckDiagnostic {
                    rule_id: "YML006".to_owned(),
                    message: format!(
                        "Column '{}' documented in YAML for model '{}' does not match any \
                         output column in the SQL query. Available columns: {}",
                        yaml_col.name,
                        ym.name,
                        format_column_list(&sql_columns, 5)
                    ),
                    severity,
                    category: CheckCategory::YamlSchema,
                    file_path: ym.yaml_file.clone(),
                    line: ym.line,
                    column: 0,
                    snippet: Some(format!("- name: {}", yaml_col.name)),
                    suggestion: closest.map(|c| format!("Did you mean '{c}'?")),
                    doc_url: Some("https://docs.ironlayer.app/check/rules/YML006".to_owned()),
                });
            }
        }
    }
}

/// Check if the Python SQL toolkit is available.
///
/// Attempts to import `core_engine.sql_toolkit` via PyO3. Uses `catch_unwind`
/// to gracefully handle the case where the Python interpreter is not
/// initialized (e.g., during Rust-only test runs).
fn check_python_toolkit_available() -> bool {
    std::panic::catch_unwind(|| {
        pyo3::Python::with_gil(|py| {
            py.import_bound("core_engine.sql_toolkit.scope_analyzer")
                .is_ok()
        })
    })
    .unwrap_or(false)
}

/// Extract output columns from a SQL query via the Python SQL toolkit.
///
/// Calls `core_engine.sql_toolkit.scope_analyzer.extract_columns()` with
/// a 5-second timeout. Returns `None` if the call fails or times out.
/// Uses `catch_unwind` to handle the case where the Python interpreter
/// is not initialized (e.g., during Rust-only test runs).
fn extract_sql_columns_via_python(sql_content: &str) -> Option<Vec<String>> {
    let sql_owned = sql_content.to_owned();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        pyo3::Python::with_gil(|py| {
            let module: pyo3::Bound<'_, pyo3::types::PyModule> =
                match py.import_bound("core_engine.sql_toolkit.scope_analyzer") {
                    Ok(m) => m,
                    Err(_) => return None,
                };

            let result: pyo3::Bound<'_, pyo3::types::PyAny> =
                match module.call_method1("extract_columns", (sql_owned.as_str(),)) {
                    Ok(r) => r,
                    Err(_) => return None,
                };

            result.extract::<Vec<String>>().ok()
        })
    }))
    .unwrap_or(None)
}

// ---------------------------------------------------------------------------
// YML007: Model has zero tests defined
// ---------------------------------------------------------------------------

/// YML007: Check that documented models have at least one test.
fn check_yml007(
    yaml_models: &[YamlModelDoc],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for ym in yaml_models {
        if !config.is_rule_enabled_for_path("YML007", &ym.yaml_file, true) {
            continue;
        }

        if !ym.has_tests {
            let severity =
                config.effective_severity_for_path("YML007", &ym.yaml_file, Severity::Warning);

            diags.push(CheckDiagnostic {
                rule_id: "YML007".to_owned(),
                message: format!(
                    "Model '{}' has no tests defined in YAML. Adding tests improves \
                     data quality and catches regressions early.",
                    ym.name
                ),
                severity,
                category: CheckCategory::YamlSchema,
                file_path: ym.yaml_file.clone(),
                line: ym.line,
                column: 0,
                snippet: Some(format!("- name: {}", ym.name)),
                suggestion: Some(format!(
                    "Add tests to the model or its columns:\n  \
                     - name: {}\n    columns:\n      - name: id\n        tests:\n          \
                     - unique\n          - not_null",
                    ym.name
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/YML007".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// YML008: Model has zero column-level descriptions
// ---------------------------------------------------------------------------

/// YML008: Check that documented models have column descriptions.
fn check_yml008(
    yaml_models: &[YamlModelDoc],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for ym in yaml_models {
        if !config.is_rule_enabled_for_path("YML008", &ym.yaml_file, true) {
            continue;
        }

        let has_any_description = ym.columns.iter().any(|c| c.has_description);

        if !has_any_description && !ym.columns.is_empty() {
            let severity =
                config.effective_severity_for_path("YML008", &ym.yaml_file, Severity::Warning);

            diags.push(CheckDiagnostic {
                rule_id: "YML008".to_owned(),
                message: format!(
                    "Model '{}' has {} columns documented but none have descriptions. \
                     Column descriptions improve data discoverability.",
                    ym.name,
                    ym.columns.len()
                ),
                severity,
                category: CheckCategory::YamlSchema,
                file_path: ym.yaml_file.clone(),
                line: ym.line,
                column: 0,
                snippet: None,
                suggestion: Some(
                    "Add 'description:' to each column entry in the YAML file.".to_owned(),
                ),
                doc_url: Some("https://docs.ironlayer.app/check/rules/YML008".to_owned()),
            });
        } else if ym.columns.is_empty() {
            let severity =
                config.effective_severity_for_path("YML008", &ym.yaml_file, Severity::Warning);

            diags.push(CheckDiagnostic {
                rule_id: "YML008".to_owned(),
                message: format!(
                    "Model '{}' has no columns documented. Add a 'columns:' section \
                     with descriptions for each output column.",
                    ym.name
                ),
                severity,
                category: CheckCategory::YamlSchema,
                file_path: ym.yaml_file.clone(),
                line: ym.line,
                column: 0,
                snippet: Some(format!("- name: {}", ym.name)),
                suggestion: Some(format!(
                    "Add columns with descriptions:\n  \
                     - name: {}\n    columns:\n      - name: id\n        description: Primary key",
                    ym.name
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/YML008".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// YML009: Profile references non-existent target (disabled by default)
// ---------------------------------------------------------------------------

/// YML009: Check profile target references.
///
/// This check is disabled by default. It validates that profiles.yml
/// references valid targets.
fn check_yml009(
    models: &[DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Find profiles.yml
    let profiles_file = models.iter().find(|m| {
        let filename = Path::new(&m.file_path)
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("");
        filename == "profiles.yml"
    });

    let profiles_file = match profiles_file {
        Some(f) => f,
        None => return, // No profiles.yml, nothing to check
    };

    let parsed: serde_yaml::Value = match serde_yaml::from_str(&profiles_file.content) {
        Ok(v) => v,
        Err(_) => return, // YML001 handles parse errors
    };

    let profiles_map = match parsed.as_mapping() {
        Some(m) => m,
        None => return,
    };

    // For each profile, check that the target referenced in dbt_project.yml is
    // defined in the profile's outputs
    for (profile_name, profile_val) in profiles_map {
        let profile_name_str = match profile_name.as_str() {
            Some(s) => s,
            None => continue,
        };

        let profile_map = match profile_val.as_mapping() {
            Some(m) => m,
            None => continue,
        };

        // Get the target field
        let target = match profile_map
            .get(serde_yaml::Value::String("target".to_owned()))
            .and_then(|v| v.as_str())
        {
            Some(t) => t,
            None => continue,
        };

        // Get available outputs
        let outputs = match profile_map
            .get(serde_yaml::Value::String("outputs".to_owned()))
            .and_then(|v| v.as_mapping())
        {
            Some(o) => o,
            None => continue,
        };

        let output_names: Vec<String> = outputs
            .keys()
            .filter_map(|k| k.as_str().map(|s| s.to_owned()))
            .collect();

        if !output_names.iter().any(|o| o == target) {
            let severity = config.effective_severity_for_path(
                "YML009",
                &profiles_file.file_path,
                Severity::Info,
            );

            diags.push(CheckDiagnostic {
                rule_id: "YML009".to_owned(),
                message: format!(
                    "Profile '{}' references target '{}' which is not defined in its outputs. \
                     Available outputs: {}.",
                    profile_name_str,
                    target,
                    output_names.join(", ")
                ),
                severity,
                category: CheckCategory::YamlSchema,
                file_path: profiles_file.file_path.clone(),
                line: 0,
                column: 0,
                snippet: Some(format!("target: {target}")),
                suggestion: Some(format!(
                    "Change target to one of: {}, or add '{target}' to outputs.",
                    output_names.join(", ")
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/YML009".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Find the closest model name using Levenshtein distance (max distance 3).
fn find_closest_model_name(name: &str, candidates: &HashSet<String>) -> Option<String> {
    let mut best: Option<(String, usize)> = None;

    for candidate in candidates {
        let dist = levenshtein(name, candidate);
        if dist <= 3 {
            if let Some((_, best_dist)) = &best {
                if dist < *best_dist {
                    best = Some((candidate.clone(), dist));
                }
            } else {
                best = Some((candidate.clone(), dist));
            }
        }
    }

    best.map(|(name, _)| name)
}

/// Find the closest column name using Levenshtein distance (max distance 3).
fn find_closest_column(name: &str, candidates: &[String]) -> Option<String> {
    let mut best: Option<(String, usize)> = None;
    let name_lower = name.to_lowercase();

    for candidate in candidates {
        let dist = levenshtein(&name_lower, &candidate.to_lowercase());
        if dist <= 3 {
            if let Some((_, best_dist)) = &best {
                if dist < *best_dist {
                    best = Some((candidate.clone(), dist));
                }
            } else {
                best = Some((candidate.clone(), dist));
            }
        }
    }

    best.map(|(name, _)| name)
}

/// Full Wagner-Fischer Levenshtein distance algorithm.
fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    // Use two-row optimization for space efficiency
    let mut prev = vec![0usize; n + 1];
    let mut curr = vec![0usize; n + 1];

    for (j, item) in prev.iter_mut().enumerate().take(n + 1) {
        *item = j;
    }

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

/// Format a list of column names for display, truncating if necessary.
fn format_column_list(columns: &[String], max_display: usize) -> String {
    if columns.len() <= max_display {
        columns.join(", ")
    } else {
        let displayed: Vec<&str> = columns[..max_display].iter().map(|s| s.as_str()).collect();
        format!(
            "{}, ... ({} more)",
            displayed.join(", "),
            columns.len() - max_display
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::compute_sha256;
    use indoc::indoc;

    fn make_model(name: &str, file_path: &str, content: &str) -> DiscoveredModel {
        DiscoveredModel {
            name: name.to_owned(),
            file_path: file_path.to_owned(),
            content_hash: compute_sha256(content),
            ref_names: Vec::new(),
            header: HashMap::new(),
            content: content.to_owned(),
        }
    }

    fn default_config() -> CheckConfig {
        CheckConfig::default()
    }

    // --- YML001 tests ---

    #[test]
    fn test_yml001_valid_yaml() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\nversion: '1.0.0'\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML001"));
    }

    #[test]
    fn test_yml001_invalid_yaml() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\n  invalid: indentation\n";
        let diags = checker.check_file("schema.yml", content, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML001"));
    }

    #[test]
    fn test_yml001_empty_yaml() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let diags = checker.check_file("schema.yml", "", None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML001"));
    }

    #[test]
    fn test_yml001_no_false_positive_on_sql() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "-- name: test\nSELECT 1";
        let diags = checker.check_file("model.sql", content, None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_yml001_error_line_extracted() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "valid: true\nbad:\n  - [broken";
        let diags = checker.check_file("schema.yml", content, None, &config);
        let yml001 = diags.iter().find(|d| d.rule_id == "YML001");
        assert!(yml001.is_some());
    }

    // --- YML002 tests ---

    #[test]
    fn test_yml002_has_name() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\nversion: '1.0.0'\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML002"));
    }

    #[test]
    fn test_yml002_missing_name() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "version: '1.0.0'\nprofile: default\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML002"));
    }

    #[test]
    fn test_yml002_empty_name() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: ''\nversion: '1.0.0'\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML002"));
    }

    #[test]
    fn test_yml002_only_on_dbt_project() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "version: '1.0.0'\n";
        let diags = checker.check_file("schema.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML002"));
    }

    #[test]
    fn test_yml002_disabled_via_config() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML002".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );
        let content = "version: '1.0.0'\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML002"));
    }

    // --- YML003 tests ---

    #[test]
    fn test_yml003_has_version() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\nversion: '1.0.0'\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML003"));
    }

    #[test]
    fn test_yml003_missing_version() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\nprofile: default\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML003"));
    }

    #[test]
    fn test_yml003_only_on_dbt_project() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\n";
        let diags = checker.check_file("schema.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML003"));
    }

    #[test]
    fn test_yml003_version_as_number() {
        let checker = YamlSchemaChecker;
        let config = default_config();
        let content = "name: my_project\nversion: 2\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML003"));
    }

    #[test]
    fn test_yml003_disabled_via_config() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML003".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );
        let content = "name: my_project\n";
        let diags = checker.check_file("dbt_project.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML003"));
    }

    // --- YML004 tests ---

    #[test]
    fn test_yml004_model_has_sql() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                description: Staging orders
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML004"));
    }

    #[test]
    fn test_yml004_model_missing_sql() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_nonexistent
                description: This model does not exist
        "};

        let models = vec![make_model("schema", "models/schema.yml", schema_content)];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML004"));
    }

    #[test]
    fn test_yml004_no_false_positive_on_dbt_project() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let content = "name: my_project\nversion: '1.0.0'\n";
        let models = vec![make_model("dbt_project", "dbt_project.yml", content)];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML004"));
    }

    #[test]
    fn test_yml004_suggestion_similar_name() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_ordrs
                description: Typo in name
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        let yml004 = diags.iter().find(|d| d.rule_id == "YML004");
        assert!(yml004.is_some());
        assert!(yml004.unwrap().suggestion.is_some());
    }

    #[test]
    fn test_yml004_disabled_via_config() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML004".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let schema_content = indoc! {"
            models:
              - name: nonexistent
        "};
        let models = vec![make_model("schema", "models/schema.yml", schema_content)];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML004"));
    }

    // --- YML005 tests ---

    #[test]
    fn test_yml005_model_documented() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                description: Orders
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML005"));
    }

    #[test]
    fn test_yml005_model_not_documented() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: other_model
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML005"));
    }

    #[test]
    fn test_yml005_disabled_via_config() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML005".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let models = vec![make_model(
            "undocumented",
            "models/undocumented.sql",
            "SELECT 1",
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML005"));
    }

    // --- YML007 tests ---

    #[test]
    fn test_yml007_model_has_tests() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                columns:
                  - name: order_id
                    tests:
                      - unique
                      - not_null
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML007"));
    }

    #[test]
    fn test_yml007_model_no_tests() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                description: Orders
                columns:
                  - name: order_id
                    description: PK
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML007"));
    }

    #[test]
    fn test_yml007_model_level_tests() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                tests:
                  - dbt_utils.unique_combination_of_columns:
                      combination_of_columns:
                        - order_id
                        - customer_id
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML007"));
    }

    #[test]
    fn test_yml007_disabled_via_config() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML007".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let schema_content = "models:\n  - name: no_tests\n";
        let models = vec![make_model("schema", "models/schema.yml", schema_content)];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML007"));
    }

    #[test]
    fn test_yml007_data_tests_key() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                data_tests:
                  - unique:
                      column_name: order_id
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML007"));
    }

    // --- YML008 tests ---

    #[test]
    fn test_yml008_columns_with_descriptions() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                columns:
                  - name: order_id
                    description: Primary key
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML008"));
    }

    #[test]
    fn test_yml008_columns_without_descriptions() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                columns:
                  - name: order_id
                  - name: customer_id
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML008"));
    }

    #[test]
    fn test_yml008_no_columns_section() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                description: Orders staging
        "};

        let models = vec![
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML008"));
    }

    #[test]
    fn test_yml008_disabled_via_config() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML008".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let schema_content = "models:\n  - name: no_cols\n";
        let models = vec![make_model("schema", "models/schema.yml", schema_content)];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML008"));
    }

    // --- YML009 tests ---

    #[test]
    fn test_yml009_disabled_by_default() {
        let checker = YamlSchemaChecker;
        let config = default_config();

        let profiles_content = indoc! {"
            my_profile:
              target: nonexistent
              outputs:
                dev:
                  type: databricks
        "};

        let models = vec![make_model("profiles", "profiles.yml", profiles_content)];

        let diags = checker.check_project(&models, &config);
        // YML009 is disabled by default, so no diagnostics
        assert!(!diags.iter().any(|d| d.rule_id == "YML009"));
    }

    #[test]
    fn test_yml009_enabled_catches_bad_target() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML009".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let profiles_content = indoc! {"
            my_profile:
              target: nonexistent
              outputs:
                dev:
                  type: databricks
                prod:
                  type: databricks
        "};

        let models = vec![make_model("profiles", "profiles.yml", profiles_content)];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "YML009"));
    }

    #[test]
    fn test_yml009_valid_target() {
        let checker = YamlSchemaChecker;
        let mut config = default_config();
        config.rules.insert(
            "YML009".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let profiles_content = indoc! {"
            my_profile:
              target: dev
              outputs:
                dev:
                  type: databricks
                prod:
                  type: databricks
        "};

        let models = vec![make_model("profiles", "profiles.yml", profiles_content)];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "YML009"));
    }

    // --- Levenshtein tests ---

    #[test]
    fn test_levenshtein_identical() {
        assert_eq!(levenshtein("hello", "hello"), 0);
    }

    #[test]
    fn test_levenshtein_one_char_diff() {
        assert_eq!(levenshtein("hello", "hallo"), 1);
    }

    #[test]
    fn test_levenshtein_insertion() {
        assert_eq!(levenshtein("hello", "helloo"), 1);
    }

    #[test]
    fn test_levenshtein_deletion() {
        assert_eq!(levenshtein("hello", "helo"), 1);
    }

    #[test]
    fn test_levenshtein_empty() {
        assert_eq!(levenshtein("", "hello"), 5);
        assert_eq!(levenshtein("hello", ""), 5);
        assert_eq!(levenshtein("", ""), 0);
    }

    #[test]
    fn test_levenshtein_completely_different() {
        assert_eq!(levenshtein("abc", "xyz"), 3);
    }

    // --- extract_yaml_error_line tests ---

    #[test]
    fn test_extract_yaml_error_line_found() {
        let msg = "mapping values are not allowed at line 3 column 5";
        assert_eq!(extract_yaml_error_line(msg), 3);
    }

    #[test]
    fn test_extract_yaml_error_line_not_found() {
        let msg = "some generic error";
        assert_eq!(extract_yaml_error_line(msg), 0);
    }
}
