//! Model consistency checker — rules CON001 through CON005.
//!
//! Performs cross-file analysis to detect inconsistencies between models:
//! duplicate names, dependency drift between headers and `{{ ref() }}` calls,
//! orphan models, and missing ownership.
//!
//! All CON rules run in the `check_project()` phase because they require
//! the full model registry for comparison.
//!
//! ## Rule Summary
//!
//! | Rule   | Default  | Severity | Description                                    |
//! |--------|----------|----------|------------------------------------------------|
//! | CON001 | enabled  | error    | Duplicate model name                           |
//! | CON002 | enabled  | warning  | Declared dependency not in ref() calls         |
//! | CON003 | enabled  | warning  | ref() call not in declared dependencies        |
//! | CON004 | disabled | info     | Orphan model: no other model references it     |
//! | CON005 | disabled | info     | Model has no declared owner                    |

use std::collections::{HashMap, HashSet};

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Model consistency checker.
pub struct ModelConsistencyChecker;

impl Checker for ModelConsistencyChecker {
    fn name(&self) -> &'static str {
        "model_consistency"
    }

    /// No per-file checks — all CON rules are cross-file.
    fn check_file(
        &self,
        _file_path: &str,
        _content: &str,
        _model: Option<&DiscoveredModel>,
        _config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        Vec::new()
    }

    /// Cross-file checks: CON001-CON005.
    fn check_project(
        &self,
        models: &[DiscoveredModel],
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        // Only check SQL models
        let sql_models: Vec<&DiscoveredModel> = models
            .iter()
            .filter(|m| m.file_path.ends_with(".sql"))
            .collect();

        // CON001: Duplicate model names
        if config.is_rule_enabled("CON001", true) {
            check_con001(&sql_models, config, &mut diags);
        }

        // CON002: Declared dependency not in ref() calls
        if config.is_rule_enabled("CON002", true) {
            check_con002(&sql_models, config, &mut diags);
        }

        // CON003: ref() call not in declared dependencies
        if config.is_rule_enabled("CON003", true) {
            check_con003(&sql_models, config, &mut diags);
        }

        // CON004: Orphan model (disabled by default)
        if config.is_rule_enabled("CON004", false) {
            check_con004(&sql_models, config, &mut diags);
        }

        // CON005: Model has no declared owner (disabled by default)
        if config.is_rule_enabled("CON005", false) {
            check_con005(&sql_models, config, &mut diags);
        }

        diags
    }
}

// ---------------------------------------------------------------------------
// CON001: Duplicate model name
// ---------------------------------------------------------------------------

/// CON001: Two or more models share the same `-- name:` value.
fn check_con001(
    models: &[&DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Build a map of name → list of file paths
    let mut name_to_files: HashMap<&str, Vec<&str>> = HashMap::new();

    for model in models {
        name_to_files
            .entry(model.name.as_str())
            .or_default()
            .push(model.file_path.as_str());
    }

    for (name, files) in &name_to_files {
        if files.len() < 2 {
            continue;
        }

        // Emit a diagnostic for every file that has a duplicate
        for file_path in files {
            if !config.is_rule_enabled_for_path("CON001", file_path, true) {
                continue;
            }

            let severity = config.effective_severity_for_path("CON001", file_path, Severity::Error);

            let other_files: Vec<&str> =
                files.iter().filter(|f| *f != file_path).copied().collect();

            diags.push(CheckDiagnostic {
                rule_id: "CON001".to_owned(),
                message: format!(
                    "Duplicate model name '{}'. Also defined in: {}. \
                     Model names must be unique across the project.",
                    name,
                    other_files.join(", ")
                ),
                severity,
                category: CheckCategory::ModelConsistency,
                file_path: file_path.to_string(),
                line: find_name_line_in_model(models.iter().find(|m| m.file_path == *file_path)),
                column: 0,
                snippet: Some(format!("-- name: {name}")),
                suggestion: Some(
                    "Rename the model in one of the files to ensure uniqueness. \
                     Each model must have a distinct '-- name:' value."
                        .to_string(),
                ),
                doc_url: Some("https://docs.ironlayer.app/check/rules/CON001".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// CON002: Declared dependency not in ref() calls
// ---------------------------------------------------------------------------

/// CON002: A model declares a dependency in its `-- dependencies:` header field
/// that is not present in any `{{ ref() }}` call in the SQL body.
fn check_con002(
    models: &[&DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !config.is_rule_enabled_for_path("CON002", &model.file_path, true) {
            continue;
        }

        let declared_deps = parse_dependencies_field(model);
        if declared_deps.is_empty() {
            continue;
        }

        let ref_set: HashSet<&str> = model.ref_names.iter().map(|s| s.as_str()).collect();

        for dep in &declared_deps {
            if !ref_set.contains(dep.as_str()) {
                let severity = config.effective_severity_for_path(
                    "CON002",
                    &model.file_path,
                    Severity::Warning,
                );

                diags.push(CheckDiagnostic {
                    rule_id: "CON002".to_owned(),
                    message: format!(
                        "Model '{}' declares dependency '{}' in its header, \
                         but this model is not referenced via {{{{ ref('{}') }}}} in the SQL body.",
                        model.name, dep, dep
                    ),
                    severity,
                    category: CheckCategory::ModelConsistency,
                    file_path: model.file_path.clone(),
                    line: find_dependencies_line(model),
                    column: 0,
                    snippet: model.header.get("dependencies").map(|d| {
                        let truncated = if d.len() > 100 { &d[..100] } else { d };
                        format!("-- dependencies: {truncated}")
                    }),
                    suggestion: Some(format!(
                        "Either add {{{{ ref('{dep}') }}}} to the SQL query, \
                         or remove '{dep}' from the '-- dependencies:' header."
                    )),
                    doc_url: Some("https://docs.ironlayer.app/check/rules/CON002".to_owned()),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CON003: ref() call not in declared dependencies
// ---------------------------------------------------------------------------

/// CON003: A model uses `{{ ref('X') }}` but does not declare `X` in
/// its `-- dependencies:` header field.
fn check_con003(
    models: &[&DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !config.is_rule_enabled_for_path("CON003", &model.file_path, true) {
            continue;
        }

        // Only check models that have a dependencies header field
        if !model.header.contains_key("dependencies") {
            continue;
        }

        let declared_deps = parse_dependencies_field(model);
        let dep_set: HashSet<&str> = declared_deps.iter().map(|s| s.as_str()).collect();

        for ref_name in &model.ref_names {
            if !dep_set.contains(ref_name.as_str()) {
                let severity = config.effective_severity_for_path(
                    "CON003",
                    &model.file_path,
                    Severity::Warning,
                );

                diags.push(CheckDiagnostic {
                    rule_id: "CON003".to_owned(),
                    message: format!(
                        "Model '{}' uses {{{{ ref('{}') }}}} but '{}' is not listed in the \
                         '-- dependencies:' header field.",
                        model.name, ref_name, ref_name
                    ),
                    severity,
                    category: CheckCategory::ModelConsistency,
                    file_path: model.file_path.clone(),
                    line: find_ref_line(&model.content, ref_name),
                    column: 0,
                    snippet: Some(format!("{{{{ ref('{ref_name}') }}}}")),
                    suggestion: Some(format!(
                        "Add '{ref_name}' to the '-- dependencies:' header field, \
                         or remove the {{{{ ref('{ref_name}') }}}} call."
                    )),
                    doc_url: Some("https://docs.ironlayer.app/check/rules/CON003".to_owned()),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CON004: Orphan model (disabled by default)
// ---------------------------------------------------------------------------

/// CON004: A model is never referenced by any other model via `{{ ref() }}`.
///
/// **Intentionally disabled by default** because terminal mart models
/// consumed by BI tools (dashboards, reports) are legitimately never
/// referenced within the project.
fn check_con004(
    models: &[&DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Build set of all referenced model names
    let all_referenced: HashSet<&str> = models
        .iter()
        .flat_map(|m| m.ref_names.iter().map(|r| r.as_str()))
        .collect();

    for model in models {
        if !config.is_rule_enabled_for_path("CON004", &model.file_path, false) {
            continue;
        }

        if !all_referenced.contains(model.name.as_str()) {
            let severity =
                config.effective_severity_for_path("CON004", &model.file_path, Severity::Info);

            diags.push(CheckDiagnostic {
                rule_id: "CON004".to_owned(),
                message: format!(
                    "Orphan model: '{}' is not referenced by any other model in the project. \
                     If this is a terminal mart model consumed by external tools (dashboards, \
                     BI), this is expected and this rule can be disabled.",
                    model.name
                ),
                severity,
                category: CheckCategory::ModelConsistency,
                file_path: model.file_path.clone(),
                line: 1,
                column: 0,
                snippet: None,
                suggestion: Some(
                    "If this model is consumed externally, disable CON004 via config. \
                     Otherwise, this model may be unused and can be removed."
                        .to_owned(),
                ),
                doc_url: Some("https://docs.ironlayer.app/check/rules/CON004".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// CON005: Model has no declared owner (disabled by default)
// ---------------------------------------------------------------------------

/// CON005: A model does not have an `-- owner:` header field.
///
/// **Intentionally disabled by default** because `owner` is an optional field.
fn check_con005(
    models: &[&DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !config.is_rule_enabled_for_path("CON005", &model.file_path, false) {
            continue;
        }

        if !model.header.contains_key("owner") {
            let severity =
                config.effective_severity_for_path("CON005", &model.file_path, Severity::Info);

            diags.push(CheckDiagnostic {
                rule_id: "CON005".to_owned(),
                message: format!(
                    "Model '{}' has no declared owner. Adding an '-- owner:' header field \
                     helps track model ownership and accountability.",
                    model.name
                ),
                severity,
                category: CheckCategory::ModelConsistency,
                file_path: model.file_path.clone(),
                line: 1,
                column: 0,
                snippet: None,
                suggestion: Some(
                    "Add '-- owner: team-name' or '-- owner: user@example.com' to the model header."
                        .to_owned(),
                ),
                doc_url: Some("https://docs.ironlayer.app/check/rules/CON005".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Parse the `-- dependencies:` header field into a list of model names.
///
/// The dependencies field is a comma-separated list of model names:
/// `-- dependencies: stg_orders, stg_customers, dim_products`
fn parse_dependencies_field(model: &DiscoveredModel) -> Vec<String> {
    match model.header.get("dependencies") {
        Some(deps_str) => deps_str
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect(),
        None => Vec::new(),
    }
}

/// Find the line number where `-- name:` appears in the model content.
fn find_name_line_in_model(model: Option<&&DiscoveredModel>) -> u32 {
    match model {
        Some(m) => {
            for (idx, line) in m.content.lines().enumerate() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("--") {
                    let rest = rest.trim();
                    if rest.starts_with("name:") {
                        return (idx + 1) as u32;
                    }
                }
            }
            1
        }
        None => 1,
    }
}

/// Find the line number where `-- dependencies:` appears in model content.
fn find_dependencies_line(model: &DiscoveredModel) -> u32 {
    for (idx, line) in model.content.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("--") {
            let rest = rest.trim();
            if rest.starts_with("dependencies:") {
                return (idx + 1) as u32;
            }
        }
    }
    1
}

/// Find the line number where a specific `{{ ref('name') }}` appears.
fn find_ref_line(content: &str, ref_name: &str) -> u32 {
    let pattern_single = format!("ref('{ref_name}')");
    let pattern_double = format!("ref(\"{ref_name}\")");

    for (idx, line) in content.lines().enumerate() {
        if line.contains(&pattern_single) || line.contains(&pattern_double) {
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
    use std::collections::HashMap;

    fn make_model(
        name: &str,
        file_path: &str,
        content: &str,
        refs: Vec<&str>,
        header: HashMap<&str, &str>,
    ) -> DiscoveredModel {
        DiscoveredModel {
            name: name.to_owned(),
            file_path: file_path.to_owned(),
            content_hash: compute_sha256(content),
            ref_names: refs.into_iter().map(|s| s.to_owned()).collect(),
            header: header
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            content: content.to_owned(),
        }
    }

    fn default_config() -> CheckConfig {
        CheckConfig::default()
    }

    // --- CON001 tests ---

    #[test]
    fn test_con001_no_duplicates() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "stg_orders",
                "models/stg_orders.sql",
                "-- name: stg_orders\nSELECT 1",
                vec![],
                HashMap::from([("name", "stg_orders")]),
            ),
            make_model(
                "stg_customers",
                "models/stg_customers.sql",
                "-- name: stg_customers\nSELECT 1",
                vec![],
                HashMap::from([("name", "stg_customers")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON001"));
    }

    #[test]
    fn test_con001_duplicate_name() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "stg_orders",
                "models/v1/stg_orders.sql",
                "-- name: stg_orders\nSELECT 1",
                vec![],
                HashMap::from([("name", "stg_orders")]),
            ),
            make_model(
                "stg_orders",
                "models/v2/stg_orders.sql",
                "-- name: stg_orders\nSELECT 2",
                vec![],
                HashMap::from([("name", "stg_orders")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let con001_diags: Vec<_> = diags.iter().filter(|d| d.rule_id == "CON001").collect();
        assert_eq!(con001_diags.len(), 2); // One per duplicate
    }

    #[test]
    fn test_con001_severity_is_error() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "dup",
                "a.sql",
                "-- name: dup\nSELECT 1",
                vec![],
                HashMap::from([("name", "dup")]),
            ),
            make_model(
                "dup",
                "b.sql",
                "-- name: dup\nSELECT 2",
                vec![],
                HashMap::from([("name", "dup")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        for d in diags.iter().filter(|d| d.rule_id == "CON001") {
            assert_eq!(d.severity, Severity::Error);
        }
    }

    #[test]
    fn test_con001_disabled_via_config() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON001".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let models = vec![
            make_model("dup", "a.sql", "SELECT 1", vec![], HashMap::new()),
            make_model("dup", "b.sql", "SELECT 2", vec![], HashMap::new()),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON001"));
    }

    #[test]
    fn test_con001_triple_duplicate() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![
            make_model("dup", "a.sql", "SELECT 1", vec![], HashMap::new()),
            make_model("dup", "b.sql", "SELECT 2", vec![], HashMap::new()),
            make_model("dup", "c.sql", "SELECT 3", vec![], HashMap::new()),
        ];

        let diags = checker.check_project(&models, &config);
        let con001_diags: Vec<_> = diags.iter().filter(|d| d.rule_id == "CON001").collect();
        assert_eq!(con001_diags.len(), 3); // One per file
    }

    // --- CON002 tests ---

    #[test]
    fn test_con002_deps_match_refs() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "fct_revenue",
            "models/fct_revenue.sql",
            "-- name: fct_revenue\n-- dependencies: stg_orders\nSELECT * FROM {{ ref('stg_orders') }}",
            vec!["stg_orders"],
            HashMap::from([("name", "fct_revenue"), ("dependencies", "stg_orders")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON002"));
    }

    #[test]
    fn test_con002_dep_not_in_refs() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "fct_revenue",
            "models/fct_revenue.sql",
            "-- name: fct_revenue\n-- dependencies: stg_orders, stg_customers\nSELECT * FROM {{ ref('stg_orders') }}",
            vec!["stg_orders"],
            HashMap::from([("name", "fct_revenue"), ("dependencies", "stg_orders, stg_customers")]),
        )];

        let diags = checker.check_project(&models, &config);
        let con002 = diags.iter().find(|d| d.rule_id == "CON002");
        assert!(con002.is_some());
        assert!(con002.unwrap().message.contains("stg_customers"));
    }

    #[test]
    fn test_con002_no_dependencies_field() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "-- name: stg_orders\nSELECT 1",
            vec![],
            HashMap::from([("name", "stg_orders")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON002"));
    }

    #[test]
    fn test_con002_disabled_via_config() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON002".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let models = vec![make_model(
            "fct",
            "fct.sql",
            "-- dependencies: missing\nSELECT 1",
            vec![],
            HashMap::from([("dependencies", "missing")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON002"));
    }

    #[test]
    fn test_con002_multiple_undeclared() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "fct",
            "models/fct.sql",
            "-- dependencies: a, b, c\nSELECT 1",
            vec![],
            HashMap::from([("dependencies", "a, b, c")]),
        )];

        let diags = checker.check_project(&models, &config);
        let con002: Vec<_> = diags.iter().filter(|d| d.rule_id == "CON002").collect();
        assert_eq!(con002.len(), 3);
    }

    // --- CON003 tests ---

    #[test]
    fn test_con003_refs_match_deps() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "fct",
            "models/fct.sql",
            "-- dependencies: stg_orders\nSELECT * FROM {{ ref('stg_orders') }}",
            vec!["stg_orders"],
            HashMap::from([("dependencies", "stg_orders")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON003"));
    }

    #[test]
    fn test_con003_ref_not_in_deps() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "fct",
            "models/fct.sql",
            "-- dependencies: stg_orders\nSELECT * FROM {{ ref('stg_orders') }} JOIN {{ ref('stg_customers') }}",
            vec!["stg_orders", "stg_customers"],
            HashMap::from([("dependencies", "stg_orders")]),
        )];

        let diags = checker.check_project(&models, &config);
        let con003 = diags.iter().find(|d| d.rule_id == "CON003");
        assert!(con003.is_some());
        assert!(con003.unwrap().message.contains("stg_customers"));
    }

    #[test]
    fn test_con003_no_dependencies_field_no_diagnostic() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        // No dependencies header → CON003 doesn't fire (it only checks drift)
        let models = vec![make_model(
            "fct",
            "models/fct.sql",
            "SELECT * FROM {{ ref('stg_orders') }}",
            vec!["stg_orders"],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON003"));
    }

    #[test]
    fn test_con003_disabled_via_config() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON003".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let models = vec![make_model(
            "fct",
            "fct.sql",
            "SELECT * FROM {{ ref('missing') }}",
            vec!["missing"],
            HashMap::from([("dependencies", "")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON003"));
    }

    #[test]
    fn test_con003_severity_is_warning() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "fct",
            "models/fct.sql",
            "-- dependencies: a\nSELECT * FROM {{ ref('a') }} JOIN {{ ref('b') }}",
            vec!["a", "b"],
            HashMap::from([("dependencies", "a")]),
        )];

        let diags = checker.check_project(&models, &config);
        let con003 = diags.iter().find(|d| d.rule_id == "CON003");
        assert!(con003.is_some());
        assert_eq!(con003.unwrap().severity, Severity::Warning);
    }

    // --- CON004 tests ---

    #[test]
    fn test_con004_disabled_by_default() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "orphan",
            "models/orphan.sql",
            "SELECT 1",
            vec![],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON004"));
    }

    #[test]
    fn test_con004_enabled_finds_orphan() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON004".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let models = vec![
            make_model(
                "stg_orders",
                "models/stg_orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "fct_revenue",
                "models/fct_revenue.sql",
                "SELECT * FROM {{ ref('stg_orders') }}",
                vec!["stg_orders"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        // fct_revenue is orphan (nobody refs it), stg_orders is not
        let orphan_diags: Vec<_> = diags.iter().filter(|d| d.rule_id == "CON004").collect();
        assert_eq!(orphan_diags.len(), 1);
        assert!(orphan_diags[0].message.contains("fct_revenue"));
    }

    #[test]
    fn test_con004_enabled_no_orphans() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON004".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let models = vec![
            make_model(
                "stg_orders",
                "models/stg_orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "fct_revenue",
                "models/fct_revenue.sql",
                "SELECT * FROM {{ ref('stg_orders') }}",
                vec!["stg_orders"],
                HashMap::new(),
            ),
            make_model(
                "dashboard",
                "models/dashboard.sql",
                "SELECT * FROM {{ ref('fct_revenue') }}",
                vec!["fct_revenue"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        // Only "dashboard" is an orphan — stg_orders is ref'd by fct_revenue, fct_revenue by dashboard
        let orphan_diags: Vec<_> = diags.iter().filter(|d| d.rule_id == "CON004").collect();
        assert_eq!(orphan_diags.len(), 1);
        assert!(orphan_diags[0].message.contains("dashboard"));
    }

    #[test]
    fn test_con004_severity_is_info() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON004".to_owned(),
            crate::config::RuleSeverityOverride::Info,
        );

        let models = vec![make_model(
            "orphan",
            "models/orphan.sql",
            "SELECT 1",
            vec![],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        let con004 = diags.iter().find(|d| d.rule_id == "CON004");
        assert!(con004.is_some());
        assert_eq!(con004.unwrap().severity, Severity::Info);
    }

    // --- CON005 tests ---

    #[test]
    fn test_con005_disabled_by_default() {
        let checker = ModelConsistencyChecker;
        let config = default_config();

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "-- name: stg_orders\nSELECT 1",
            vec![],
            HashMap::from([("name", "stg_orders")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON005"));
    }

    #[test]
    fn test_con005_enabled_no_owner() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON005".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "-- name: stg_orders\nSELECT 1",
            vec![],
            HashMap::from([("name", "stg_orders")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(diags.iter().any(|d| d.rule_id == "CON005"));
    }

    #[test]
    fn test_con005_enabled_has_owner() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON005".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "-- name: stg_orders\n-- owner: data-eng\nSELECT 1",
            vec![],
            HashMap::from([("name", "stg_orders"), ("owner", "data-eng")]),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "CON005"));
    }

    #[test]
    fn test_con005_severity_is_info_by_default() {
        let checker = ModelConsistencyChecker;
        let mut config = default_config();
        config.rules.insert(
            "CON005".to_owned(),
            crate::config::RuleSeverityOverride::Info,
        );

        let models = vec![make_model(
            "stg_orders",
            "models/stg_orders.sql",
            "SELECT 1",
            vec![],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        let con005 = diags.iter().find(|d| d.rule_id == "CON005");
        assert!(con005.is_some());
        assert_eq!(con005.unwrap().severity, Severity::Info);
    }

    // --- Helper function tests ---

    #[test]
    fn test_parse_dependencies_field_comma_separated() {
        let model = make_model(
            "fct",
            "fct.sql",
            "SELECT 1",
            vec![],
            HashMap::from([("dependencies", "stg_orders, stg_customers, dim_products")]),
        );
        let deps = parse_dependencies_field(&model);
        assert_eq!(deps, vec!["stg_orders", "stg_customers", "dim_products"]);
    }

    #[test]
    fn test_parse_dependencies_field_empty() {
        let model = make_model("fct", "fct.sql", "SELECT 1", vec![], HashMap::new());
        let deps = parse_dependencies_field(&model);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_parse_dependencies_field_single() {
        let model = make_model(
            "fct",
            "fct.sql",
            "SELECT 1",
            vec![],
            HashMap::from([("dependencies", "stg_orders")]),
        );
        let deps = parse_dependencies_field(&model);
        assert_eq!(deps, vec!["stg_orders"]);
    }

    #[test]
    fn test_find_ref_line() {
        let content = "-- name: fct\nSELECT *\nFROM {{ ref('stg_orders') }}";
        assert_eq!(find_ref_line(content, "stg_orders"), 3);
    }

    #[test]
    fn test_find_ref_line_not_found() {
        let content = "SELECT 1";
        assert_eq!(find_ref_line(content, "nonexistent"), 0);
    }
}
