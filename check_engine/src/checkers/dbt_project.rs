//! dbt project validation checker — rules DBT001 through DBT006.
//!
//! Validates dbt project structure, including `dbt_project.yml` presence,
//! model path alignment, source definitions, materialization configuration,
//! and testing requirements.
//!
//! These rules only fire when `project_type == "dbt"`.
//!
//! ## Rule Summary
//!
//! | Rule   | Default  | Severity | Description                                       |
//! |--------|----------|----------|---------------------------------------------------|
//! | DBT001 | enabled  | error    | `dbt_project.yml` not found in project root        |
//! | DBT002 | enabled  | warning  | Model file not in a configured model path          |
//! | DBT003 | enabled  | warning  | Source referenced but not defined in sources.yml   |
//! | DBT004 | enabled  | warning  | Model uses unrecognized materialization in config  |
//! | DBT005 | disabled | info     | Model lacks a unique test on any column            |
//! | DBT006 | disabled | info     | Model lacks a not_null test on primary key         |

use std::collections::{HashMap, HashSet};
use std::path::Path;

use regex::Regex;

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Known dbt materialization values.
const KNOWN_MATERIALIZATIONS: &[&str] = &[
    "table",
    "view",
    "incremental",
    "ephemeral",
    "seed",
    "snapshot",
    "materialized_view",
];

/// dbt project structure validation checker.
pub struct DbtProjectChecker;

impl Checker for DbtProjectChecker {
    fn name(&self) -> &'static str {
        "dbt_project"
    }

    /// Per-file checks: DBT004 (unrecognized materialization in `{{ config() }}`).
    fn check_file(
        &self,
        file_path: &str,
        content: &str,
        _model: Option<&DiscoveredModel>,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        // Only check .sql files
        if !file_path.ends_with(".sql") {
            return Vec::new();
        }

        let mut diags = Vec::new();

        // DBT004: Unrecognized materialization in {{ config(...) }}
        if config.is_rule_enabled_for_path("DBT004", file_path, true) {
            check_dbt004(file_path, content, config, &mut diags);
        }

        diags
    }

    /// Cross-file checks: DBT001, DBT002, DBT003, DBT005, DBT006.
    fn check_project(
        &self,
        models: &[DiscoveredModel],
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        // We need to find the dbt_project.yml content from the models list
        let dbt_project = models.iter().find(|m| {
            let filename = Path::new(&m.file_path)
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("");
            filename == "dbt_project.yml"
        });

        // DBT001: dbt_project.yml not found
        if config.is_rule_enabled("DBT001", true) {
            check_dbt001(dbt_project, config, &mut diags);
        }

        // Parse dbt_project.yml for model-paths
        let model_paths = match dbt_project {
            Some(proj) => parse_model_paths(&proj.content),
            None => vec!["models".to_owned()],
        };

        // DBT002: Model file not in configured model path
        if config.is_rule_enabled("DBT002", true) {
            check_dbt002(models, &model_paths, config, &mut diags);
        }

        // Collect source definitions from sources.yml / schema.yml files
        let defined_sources = collect_defined_sources(models);

        // DBT003: Source referenced but not defined
        if config.is_rule_enabled("DBT003", true) {
            check_dbt003(models, &defined_sources, config, &mut diags);
        }

        // Collect YAML test definitions for DBT005/DBT006
        let yaml_tests = collect_yaml_tests(models);

        // DBT005: Model lacks a unique test (disabled by default)
        if config.is_rule_enabled("DBT005", false) {
            check_dbt005(models, &yaml_tests, config, &mut diags);
        }

        // DBT006: Model lacks a not_null test on primary key (disabled by default)
        if config.is_rule_enabled("DBT006", false) {
            check_dbt006(models, &yaml_tests, config, &mut diags);
        }

        diags
    }
}

// ---------------------------------------------------------------------------
// DBT001: dbt_project.yml not found
// ---------------------------------------------------------------------------

/// DBT001: Check that dbt_project.yml exists in the project.
fn check_dbt001(
    dbt_project: Option<&DiscoveredModel>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if dbt_project.is_some() {
        return;
    }

    let severity = config.effective_severity("DBT001", Severity::Error);

    diags.push(CheckDiagnostic {
        rule_id: "DBT001".to_owned(),
        message: "dbt_project.yml not found in the project root. Every dbt project \
                 must have a dbt_project.yml configuration file."
            .to_owned(),
        severity,
        category: CheckCategory::DbtProject,
        file_path: "dbt_project.yml".to_owned(),
        line: 0,
        column: 0,
        snippet: None,
        suggestion: Some(
            "Create a dbt_project.yml file with at least 'name' and 'version' fields.".to_owned(),
        ),
        doc_url: Some("https://docs.ironlayer.app/check/rules/DBT001".to_owned()),
    });
}

// ---------------------------------------------------------------------------
// DBT002: Model file not in configured model path
// ---------------------------------------------------------------------------

/// DBT002: Check that SQL model files are within configured model paths.
fn check_dbt002(
    models: &[DiscoveredModel],
    model_paths: &[String],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !model.file_path.ends_with(".sql") {
            continue;
        }

        if !config.is_rule_enabled_for_path("DBT002", &model.file_path, true) {
            continue;
        }

        let in_model_path = model_paths.iter().any(|mp| {
            let normalized = mp.trim_end_matches('/');
            model.file_path.starts_with(normalized)
                || model.file_path.starts_with(&format!("{normalized}/"))
        });

        if !in_model_path {
            let severity =
                config.effective_severity_for_path("DBT002", &model.file_path, Severity::Warning);

            diags.push(CheckDiagnostic {
                rule_id: "DBT002".to_owned(),
                message: format!(
                    "Model file '{}' is not within any configured model path. \
                     Expected paths: {}.",
                    model.file_path,
                    model_paths.join(", ")
                ),
                severity,
                category: CheckCategory::DbtProject,
                file_path: model.file_path.clone(),
                line: 0,
                column: 0,
                snippet: None,
                suggestion: Some(format!(
                    "Move this file to one of the configured model paths ({}), \
                     or add its directory to 'model-paths' in dbt_project.yml.",
                    model_paths.join(", ")
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/DBT002".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// DBT003: Source referenced but not defined
// ---------------------------------------------------------------------------

/// DBT003: Check that all `{{ source('...', '...') }}` references have definitions.
fn check_dbt003(
    models: &[DiscoveredModel],
    defined_sources: &HashSet<(String, String)>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let source_re = Regex::new(
        r#"\{\{\s*source\s*\(\s*(?:'([^']+)'|"([^"]+)")\s*,\s*(?:'([^']+)'|"([^"]+)")\s*\)\s*\}\}"#,
    )
    .expect("source pattern regex is valid");

    for model in models {
        if !model.file_path.ends_with(".sql") {
            continue;
        }

        if !config.is_rule_enabled_for_path("DBT003", &model.file_path, true) {
            continue;
        }

        for cap in source_re.captures_iter(&model.content) {
            let source_name = cap
                .get(1)
                .or_else(|| cap.get(2))
                .map(|m| m.as_str().to_owned())
                .unwrap_or_default();
            let table_name = cap
                .get(3)
                .or_else(|| cap.get(4))
                .map(|m| m.as_str().to_owned())
                .unwrap_or_default();

            if source_name.is_empty() || table_name.is_empty() {
                continue;
            }

            if !defined_sources.contains(&(source_name.clone(), table_name.clone())) {
                let severity = config.effective_severity_for_path(
                    "DBT003",
                    &model.file_path,
                    Severity::Warning,
                );

                // Find the line number of this source reference
                let line = find_source_line(&model.content, &source_name, &table_name);

                diags.push(CheckDiagnostic {
                    rule_id: "DBT003".to_owned(),
                    message: format!(
                        "Source '{}' table '{}' is referenced in SQL but not defined in \
                         any sources.yml or schema.yml file.",
                        source_name, table_name
                    ),
                    severity,
                    category: CheckCategory::DbtProject,
                    file_path: model.file_path.clone(),
                    line,
                    column: 0,
                    snippet: Some(format!("{{{{ source('{source_name}', '{table_name}') }}}}")),
                    suggestion: Some(format!(
                        "Add a source definition to a .yml file:\n  \
                         sources:\n    - name: {source_name}\n      tables:\n        \
                         - name: {table_name}"
                    )),
                    doc_url: Some("https://docs.ironlayer.app/check/rules/DBT003".to_owned()),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DBT004: Unrecognized materialization in {{ config() }}
// ---------------------------------------------------------------------------

/// DBT004: Check for unrecognized materialization values in `{{ config() }}`.
fn check_dbt004(
    file_path: &str,
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let config_re = Regex::new(
        r#"\{\{\s*config\s*\([^)]*materialized\s*=\s*(?:'([^']+)'|"([^"]+)")[^)]*\)\s*\}\}"#,
    )
    .expect("config materialized pattern regex is valid");

    for cap in config_re.captures_iter(content) {
        let materialization = cap
            .get(1)
            .or_else(|| cap.get(2))
            .map(|m| m.as_str())
            .unwrap_or("");

        if materialization.is_empty() {
            continue;
        }

        let mat_lower = materialization.to_lowercase();
        if !KNOWN_MATERIALIZATIONS.contains(&mat_lower.as_str()) {
            let severity =
                config.effective_severity_for_path("DBT004", file_path, Severity::Warning);

            // Find line number
            let line = find_materialization_line(content, materialization);

            diags.push(CheckDiagnostic {
                rule_id: "DBT004".to_owned(),
                message: format!(
                    "Unrecognized materialization '{}' in config block. Known values: {}.",
                    materialization,
                    KNOWN_MATERIALIZATIONS.join(", ")
                ),
                severity,
                category: CheckCategory::DbtProject,
                file_path: file_path.to_owned(),
                line,
                column: 0,
                snippet: Some(format!("materialized='{materialization}'")),
                suggestion: Some(format!(
                    "Use one of the recognized materializations: {}.",
                    KNOWN_MATERIALIZATIONS.join(", ")
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/DBT004".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// DBT005: Model lacks a unique test (disabled by default)
// ---------------------------------------------------------------------------

/// DBT005: Check that models have at least one unique test.
fn check_dbt005(
    models: &[DiscoveredModel],
    yaml_tests: &HashMap<String, ModelTestInfo>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !model.file_path.ends_with(".sql") {
            continue;
        }

        if !config.is_rule_enabled_for_path("DBT005", &model.file_path, false) {
            continue;
        }

        let has_unique = yaml_tests
            .get(&model.name)
            .is_some_and(|info| info.has_unique_test);

        if !has_unique {
            let severity =
                config.effective_severity_for_path("DBT005", &model.file_path, Severity::Info);

            diags.push(CheckDiagnostic {
                rule_id: "DBT005".to_owned(),
                message: format!(
                    "Model '{}' does not have a 'unique' test defined on any column. \
                     Consider adding a uniqueness test to ensure data quality.",
                    model.name
                ),
                severity,
                category: CheckCategory::DbtProject,
                file_path: model.file_path.clone(),
                line: 0,
                column: 0,
                snippet: None,
                suggestion: Some(format!(
                    "Add a unique test to a column in your schema.yml:\n  \
                     - name: {}\n    columns:\n      - name: id\n        tests:\n          \
                     - unique",
                    model.name
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/DBT005".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// DBT006: Model lacks a not_null test on primary key (disabled by default)
// ---------------------------------------------------------------------------

/// DBT006: Check that models have a not_null test on at least one column.
fn check_dbt006(
    models: &[DiscoveredModel],
    yaml_tests: &HashMap<String, ModelTestInfo>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !model.file_path.ends_with(".sql") {
            continue;
        }

        if !config.is_rule_enabled_for_path("DBT006", &model.file_path, false) {
            continue;
        }

        let has_not_null = yaml_tests
            .get(&model.name)
            .is_some_and(|info| info.has_not_null_test);

        if !has_not_null {
            let severity =
                config.effective_severity_for_path("DBT006", &model.file_path, Severity::Info);

            diags.push(CheckDiagnostic {
                rule_id: "DBT006".to_owned(),
                message: format!(
                    "Model '{}' does not have a 'not_null' test on its primary key. \
                     Consider adding a not_null test to ensure data integrity.",
                    model.name
                ),
                severity,
                category: CheckCategory::DbtProject,
                file_path: model.file_path.clone(),
                line: 0,
                column: 0,
                snippet: None,
                suggestion: Some(format!(
                    "Add a not_null test to the primary key column:\n  \
                     - name: {}\n    columns:\n      - name: id\n        tests:\n          \
                     - not_null",
                    model.name
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/DBT006".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Helper types and functions
// ---------------------------------------------------------------------------

/// Summary of test coverage for a model.
#[derive(Debug, Default)]
struct ModelTestInfo {
    /// Whether the model has a unique test on any column.
    has_unique_test: bool,
    /// Whether the model has a not_null test on any column.
    has_not_null_test: bool,
}

/// Parse model-paths from dbt_project.yml content.
///
/// Returns the list of configured model directories. Falls back to `["models"]`
/// if not specified.
fn parse_model_paths(content: &str) -> Vec<String> {
    let parsed: serde_yaml::Value = match serde_yaml::from_str(content) {
        Ok(v) => v,
        Err(_) => return vec!["models".to_owned()],
    };

    // Try "model-paths" first (dbt >= 1.0), then "source-paths" (legacy)
    let paths_key = parsed
        .get("model-paths")
        .or_else(|| parsed.get("source-paths"));

    match paths_key.and_then(|v| v.as_sequence()) {
        Some(seq) => {
            let paths: Vec<String> = seq
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_owned()))
                .collect();
            if paths.is_empty() {
                vec!["models".to_owned()]
            } else {
                paths
            }
        }
        None => vec!["models".to_owned()],
    }
}

/// Collect source definitions from YAML files.
///
/// Returns a set of `(source_name, table_name)` tuples representing defined sources.
fn collect_defined_sources(models: &[DiscoveredModel]) -> HashSet<(String, String)> {
    let mut sources = HashSet::new();

    for model in models {
        if !model.file_path.ends_with(".yml") && !model.file_path.ends_with(".yaml") {
            continue;
        }

        let parsed: serde_yaml::Value = match serde_yaml::from_str(&model.content) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let sources_section = match parsed.get("sources").and_then(|v| v.as_sequence()) {
            Some(seq) => seq,
            None => continue,
        };

        for source_entry in sources_section {
            let source_map = match source_entry.as_mapping() {
                Some(m) => m,
                None => continue,
            };

            let source_name = match source_map
                .get(serde_yaml::Value::String("name".to_owned()))
                .and_then(|v| v.as_str())
            {
                Some(n) => n.to_owned(),
                None => continue,
            };

            let tables = match source_map
                .get(serde_yaml::Value::String("tables".to_owned()))
                .and_then(|v| v.as_sequence())
            {
                Some(seq) => seq,
                None => continue,
            };

            for table_entry in tables {
                let table_map = match table_entry.as_mapping() {
                    Some(m) => m,
                    None => continue,
                };

                if let Some(table_name) = table_map
                    .get(serde_yaml::Value::String("name".to_owned()))
                    .and_then(|v| v.as_str())
                {
                    sources.insert((source_name.clone(), table_name.to_owned()));
                }
            }
        }
    }

    sources
}

/// Collect test information from YAML model documentation.
fn collect_yaml_tests(models: &[DiscoveredModel]) -> HashMap<String, ModelTestInfo> {
    let mut test_info: HashMap<String, ModelTestInfo> = HashMap::new();

    for model in models {
        if !model.file_path.ends_with(".yml") && !model.file_path.ends_with(".yaml") {
            continue;
        }

        let parsed: serde_yaml::Value = match serde_yaml::from_str(&model.content) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let models_section = match parsed.get("models").and_then(|v| v.as_sequence()) {
            Some(seq) => seq,
            None => continue,
        };

        for model_entry in models_section {
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

            let info = test_info.entry(name).or_default();

            // Check columns for tests
            if let Some(columns) = model_map
                .get(serde_yaml::Value::String("columns".to_owned()))
                .and_then(|v| v.as_sequence())
            {
                for col in columns {
                    let col_map = match col.as_mapping() {
                        Some(m) => m,
                        None => continue,
                    };

                    let tests = col_map
                        .get(serde_yaml::Value::String("tests".to_owned()))
                        .or_else(|| col_map.get(serde_yaml::Value::String("data_tests".to_owned())))
                        .and_then(|v| v.as_sequence());

                    if let Some(test_list) = tests {
                        for test in test_list {
                            let test_name = match test {
                                serde_yaml::Value::String(s) => s.to_lowercase(),
                                serde_yaml::Value::Mapping(m) => m
                                    .keys()
                                    .next()
                                    .and_then(|k| k.as_str())
                                    .unwrap_or("")
                                    .to_lowercase(),
                                _ => continue,
                            };

                            if test_name == "unique" || test_name.contains("unique_combination") {
                                info.has_unique_test = true;
                            }
                            if test_name == "not_null" {
                                info.has_not_null_test = true;
                            }
                        }
                    }
                }
            }
        }
    }

    test_info
}

/// Find the line number where a source reference appears.
fn find_source_line(content: &str, source_name: &str, table_name: &str) -> u32 {
    for (idx, line) in content.lines().enumerate() {
        if line.contains("source") && line.contains(source_name) && line.contains(table_name) {
            return (idx + 1) as u32;
        }
    }
    0
}

/// Find the line number where a materialization value appears.
fn find_materialization_line(content: &str, materialization: &str) -> u32 {
    for (idx, line) in content.lines().enumerate() {
        if line.contains("materialized") && line.contains(materialization) {
            return (idx + 1) as u32;
        }
    }
    0
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

    // --- DBT001 tests ---

    #[test]
    fn test_dbt001_project_file_exists() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "name: my_project\nversion: '1.0.0'\n";
        let models = vec![make_model("dbt_project", "dbt_project.yml", content)];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT001"));
    }

    #[test]
    fn test_dbt001_project_file_missing() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "SELECT 1",
        )];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "DBT001"));
    }

    #[test]
    fn test_dbt001_disabled_via_config() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT001".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "SELECT 1",
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT001"));
    }

    #[test]
    fn test_dbt001_severity_correct() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let models: Vec<DiscoveredModel> = Vec::new();

        let diags = checker.check_project(&models, &config);
        let dbt001 = diags.iter().find(|d| d.rule_id == "DBT001");
        assert!(dbt001.is_some());
        assert_eq!(dbt001.unwrap().severity, Severity::Error);
    }

    #[test]
    fn test_dbt001_has_suggestion() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let models: Vec<DiscoveredModel> = Vec::new();

        let diags = checker.check_project(&models, &config);
        let dbt001 = diags.iter().find(|d| d.rule_id == "DBT001");
        assert!(dbt001.unwrap().suggestion.is_some());
    }

    // --- DBT002 tests ---

    #[test]
    fn test_dbt002_model_in_configured_path() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let dbt_content = "name: my_project\nversion: '1.0.0'\nmodel-paths:\n  - models\n";
        let models = vec![
            make_model("dbt_project", "dbt_project.yml", dbt_content),
            make_model("stg_orders", "models/staging/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT002"));
    }

    #[test]
    fn test_dbt002_model_outside_configured_path() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let dbt_content = "name: my_project\nversion: '1.0.0'\nmodel-paths:\n  - models\n";
        let models = vec![
            make_model("dbt_project", "dbt_project.yml", dbt_content),
            make_model("stg_orders", "other_dir/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "DBT002"));
    }

    #[test]
    fn test_dbt002_default_model_path() {
        let checker = DbtProjectChecker;
        let config = default_config();

        // No model-paths specified → defaults to "models"
        let dbt_content = "name: my_project\nversion: '1.0.0'\n";
        let models = vec![
            make_model("dbt_project", "dbt_project.yml", dbt_content),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT002"));
    }

    #[test]
    fn test_dbt002_legacy_source_paths() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let dbt_content = "name: my_project\nversion: '1.0.0'\nsource-paths:\n  - src\n";
        let models = vec![
            make_model("dbt_project", "dbt_project.yml", dbt_content),
            make_model("stg_orders", "src/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT002"));
    }

    #[test]
    fn test_dbt002_disabled_via_config() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT002".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let dbt_content = "name: my_project\nversion: '1.0.0'\n";
        let models = vec![
            make_model("dbt_project", "dbt_project.yml", dbt_content),
            make_model("stg_orders", "outside/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT002"));
    }

    // --- DBT003 tests ---

    #[test]
    fn test_dbt003_source_defined() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let sources_content = indoc! {"
            sources:
              - name: raw
                tables:
                  - name: orders
        "};

        let sql_content = "SELECT * FROM {{ source('raw', 'orders') }}";

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("sources", "models/sources.yml", sources_content),
            make_model("stg_orders", "models/stg_orders.sql", sql_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT003"));
    }

    #[test]
    fn test_dbt003_source_undefined() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let sql_content = "SELECT * FROM {{ source('raw', 'orders') }}";

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", sql_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "DBT003"));
    }

    #[test]
    fn test_dbt003_source_double_quotes() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let sources_content = indoc! {"
            sources:
              - name: raw
                tables:
                  - name: orders
        "};

        let sql_content = r#"SELECT * FROM {{ source("raw", "orders") }}"#;

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("sources", "models/sources.yml", sources_content),
            make_model("stg_orders", "models/stg_orders.sql", sql_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT003"));
    }

    #[test]
    fn test_dbt003_no_sources_in_sql() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT003"));
    }

    #[test]
    fn test_dbt003_disabled_via_config() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT003".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let sql_content = "SELECT * FROM {{ source('raw', 'orders') }}";
        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", sql_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT003"));
    }

    // --- DBT004 tests ---

    #[test]
    fn test_dbt004_known_materialization() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "{{ config(materialized='table') }}\nSELECT 1";
        let diags = checker.check_file("models/stg_orders.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    #[test]
    fn test_dbt004_unknown_materialization() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "{{ config(materialized='custom_mat') }}\nSELECT 1";
        let diags = checker.check_file("models/stg_orders.sql", content, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    #[test]
    fn test_dbt004_incremental_is_valid() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "{{ config(materialized='incremental') }}\nSELECT 1";
        let diags = checker.check_file("models/stg_orders.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    #[test]
    fn test_dbt004_case_insensitive() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "{{ config(materialized='TABLE') }}\nSELECT 1";
        let diags = checker.check_file("models/stg_orders.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    #[test]
    fn test_dbt004_no_config_block() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "SELECT 1";
        let diags = checker.check_file("models/stg_orders.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    #[test]
    fn test_dbt004_disabled_via_config() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT004".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let content = "{{ config(materialized='invalid_type') }}\nSELECT 1";
        let diags = checker.check_file("models/stg_orders.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    #[test]
    fn test_dbt004_only_on_sql_files() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let content = "materialized: 'bogus'\n";
        let diags = checker.check_file("schema.yml", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT004"));
    }

    // --- DBT005 tests ---

    #[test]
    fn test_dbt005_disabled_by_default() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                columns:
                  - name: order_id
        "};

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT005"));
    }

    #[test]
    fn test_dbt005_enabled_no_unique_test() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT005".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                columns:
                  - name: order_id
                    tests:
                      - not_null
        "};

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "DBT005"));
    }

    #[test]
    fn test_dbt005_enabled_has_unique_test() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT005".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

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
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT005"));
    }

    // --- DBT006 tests ---

    #[test]
    fn test_dbt006_disabled_by_default() {
        let checker = DbtProjectChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT006"));
    }

    #[test]
    fn test_dbt006_enabled_no_not_null() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT006".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let schema_content = indoc! {"
            models:
              - name: stg_orders
                columns:
                  - name: order_id
                    tests:
                      - unique
        "};

        let models = vec![
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "DBT006"));
    }

    #[test]
    fn test_dbt006_enabled_has_not_null() {
        let checker = DbtProjectChecker;
        let mut config = default_config();
        config.rules.insert(
            "DBT006".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

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
            make_model(
                "dbt_project",
                "dbt_project.yml",
                "name: test\nversion: '1.0'\n",
            ),
            make_model("stg_orders", "models/stg_orders.sql", "SELECT 1"),
            make_model("schema", "models/schema.yml", schema_content),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "DBT006"));
    }

    // --- Helper function tests ---

    #[test]
    fn test_parse_model_paths_default() {
        let content = "name: my_project\nversion: '1.0.0'\n";
        let paths = parse_model_paths(content);
        assert_eq!(paths, vec!["models"]);
    }

    #[test]
    fn test_parse_model_paths_custom() {
        let content = "name: my_project\nmodel-paths:\n  - src/models\n  - src/staging\n";
        let paths = parse_model_paths(content);
        assert_eq!(paths, vec!["src/models", "src/staging"]);
    }

    #[test]
    fn test_parse_model_paths_legacy() {
        let content = "name: my_project\nsource-paths:\n  - src\n";
        let paths = parse_model_paths(content);
        assert_eq!(paths, vec!["src"]);
    }

    #[test]
    fn test_collect_defined_sources() {
        let sources_content = indoc! {"
            sources:
              - name: raw
                tables:
                  - name: orders
                  - name: customers
              - name: external
                tables:
                  - name: exchange_rates
        "};

        let models = vec![make_model("sources", "models/sources.yml", sources_content)];
        let sources = collect_defined_sources(&models);

        assert!(sources.contains(&("raw".to_owned(), "orders".to_owned())));
        assert!(sources.contains(&("raw".to_owned(), "customers".to_owned())));
        assert!(sources.contains(&("external".to_owned(), "exchange_rates".to_owned())));
        assert_eq!(sources.len(), 3);
    }

    #[test]
    fn test_collect_defined_sources_empty() {
        let models = vec![make_model("model", "models/model.sql", "SELECT 1")];
        let sources = collect_defined_sources(&models);
        assert!(sources.is_empty());
    }
}
