//! Test adequacy validation — rules TST001 through TST005.
//!
//! Validates that models declare sufficient tests for their configuration.
//! For example, models with a `unique_key` should have `unique()` and
//! `not_null()` tests, incremental models should have row-count tests, etc.
//!
//! All TST rules operate at the project level via `check_project()` since
//! they need cross-model context (the full `-- tests:` header field contents).

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Test adequacy checker.
pub struct TestAdequacyChecker;

impl Checker for TestAdequacyChecker {
    fn name(&self) -> &'static str {
        "test_adequacy"
    }

    fn check_file(
        &self,
        _file_path: &str,
        _content: &str,
        _model: Option<&DiscoveredModel>,
        _config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        // All TST rules are project-level checks.
        Vec::new()
    }

    fn check_project(
        &self,
        models: &[DiscoveredModel],
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        for model in models {
            check_tst001(model, config, &mut diags);
            check_tst002(model, config, &mut diags);
            check_tst003(model, config, &mut diags);
            check_tst004(model, config, &mut diags);
            check_tst005(model, config, &mut diags);
        }

        diags
    }
}

/// Helper: generate a documentation URL for a rule.
fn doc_url(rule_id: &str) -> Option<String> {
    Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}"))
}

/// Parse the `-- tests:` header value into individual test declarations.
///
/// Tests are comma-separated: `unique(id), not_null(id), row_count_min(1)`
fn parse_test_declarations(tests_value: &str) -> Vec<String> {
    if tests_value.trim().is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();

    for ch in tests_value.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim().to_owned();
                if !trimmed.is_empty() {
                    result.push(trimmed);
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let trimmed = current.trim().to_owned();
    if !trimmed.is_empty() {
        result.push(trimmed);
    }

    result
}

/// Check if any test declaration starts with the given test type name.
fn has_test_type(declarations: &[String], test_type: &str) -> bool {
    declarations
        .iter()
        .any(|d| d.trim().to_ascii_lowercase().starts_with(test_type))
}

/// Check if any test declaration is for a specific test type AND column.
fn has_test_for_column(declarations: &[String], test_type: &str, column: &str) -> bool {
    let prefix = format!("{}(", test_type);
    declarations.iter().any(|d| {
        let lower = d.trim().to_ascii_lowercase();
        if lower.starts_with(&prefix) {
            // Extract the column name from the parentheses
            let inner = &lower[prefix.len()..];
            let col_part = inner.split(')').next().unwrap_or("");
            col_part.trim() == column.to_ascii_lowercase()
        } else {
            false
        }
    })
}

/// Get the line number of the `-- tests:` header or default to 1.
fn tests_header_line(model: &DiscoveredModel) -> u32 {
    model
        .content
        .lines()
        .enumerate()
        .find(|(_, line)| {
            let trimmed = line.trim();
            trimmed.starts_with("-- tests:")
        })
        .map_or(1, |(i, _)| i as u32 + 1)
}

/// TST001: Model with unique_key but no unique() test.
///
/// If a model declares `-- unique_key: id`, there should be a
/// `unique(id)` test to validate the uniqueness constraint.
fn check_tst001(model: &DiscoveredModel, config: &CheckConfig, diags: &mut Vec<CheckDiagnostic>) {
    if !config.is_rule_enabled_for_path("TST001", &model.file_path, true) {
        return;
    }

    let unique_key = match model.header.get("unique_key") {
        Some(uk) => uk.clone(),
        None => return,
    };

    let tests_value = model.header.get("tests").map(|s| s.as_str()).unwrap_or("");
    let declarations = parse_test_declarations(tests_value);

    if !has_test_for_column(&declarations, "unique", &unique_key) {
        diags.push(CheckDiagnostic {
            rule_id: "TST001".to_owned(),
            message: format!(
                "Model '{}' declares unique_key '{}' but has no unique({}) test. \
                 Without a uniqueness test, duplicate keys may go undetected.",
                model.name, unique_key, unique_key
            ),
            severity: config.effective_severity_for_path(
                "TST001",
                &model.file_path,
                Severity::Warning,
            ),
            category: CheckCategory::TestAdequacy,
            file_path: model.file_path.clone(),
            line: tests_header_line(model),
            column: 0,
            snippet: Some(format!("-- unique_key: {unique_key}")),
            suggestion: Some(format!(
                "Add 'unique({unique_key})' to the -- tests: header field."
            )),
            doc_url: doc_url("TST001"),
        });
    }
}

/// TST002: Model with unique_key but no not_null() test on the key column.
///
/// NULL values in the unique_key column bypass uniqueness checks, so both
/// unique() and not_null() should be declared.
fn check_tst002(model: &DiscoveredModel, config: &CheckConfig, diags: &mut Vec<CheckDiagnostic>) {
    if !config.is_rule_enabled_for_path("TST002", &model.file_path, true) {
        return;
    }

    let unique_key = match model.header.get("unique_key") {
        Some(uk) => uk.clone(),
        None => return,
    };

    let tests_value = model.header.get("tests").map(|s| s.as_str()).unwrap_or("");
    let declarations = parse_test_declarations(tests_value);

    if !has_test_for_column(&declarations, "not_null", &unique_key) {
        diags.push(CheckDiagnostic {
            rule_id: "TST002".to_owned(),
            message: format!(
                "Model '{}' declares unique_key '{}' but has no not_null({}) test. \
                 NULL values in the unique key bypass uniqueness checks.",
                model.name, unique_key, unique_key
            ),
            severity: config.effective_severity_for_path(
                "TST002",
                &model.file_path,
                Severity::Warning,
            ),
            category: CheckCategory::TestAdequacy,
            file_path: model.file_path.clone(),
            line: tests_header_line(model),
            column: 0,
            snippet: Some(format!("-- unique_key: {unique_key}")),
            suggestion: Some(format!(
                "Add 'not_null({unique_key})' to the -- tests: header field."
            )),
            doc_url: doc_url("TST002"),
        });
    }
}

/// TST003: Incremental model with no row_count test.
///
/// Incremental models should have at least one row_count_min or row_count_max
/// test to detect data freshness issues (e.g., zero rows returned).
fn check_tst003(model: &DiscoveredModel, config: &CheckConfig, diags: &mut Vec<CheckDiagnostic>) {
    if !config.is_rule_enabled_for_path("TST003", &model.file_path, true) {
        return;
    }

    let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");
    if kind != "INCREMENTAL_BY_TIME_RANGE" && kind != "APPEND_ONLY" {
        return;
    }

    let tests_value = model.header.get("tests").map(|s| s.as_str()).unwrap_or("");
    let declarations = parse_test_declarations(tests_value);

    let has_row_count = has_test_type(&declarations, "row_count_min")
        || has_test_type(&declarations, "row_count_max");

    if !has_row_count {
        diags.push(CheckDiagnostic {
            rule_id: "TST003".to_owned(),
            message: format!(
                "Incremental model '{}' (kind: {kind}) has no row_count_min or \
                 row_count_max test. Without a row count test, the model may \
                 silently produce zero rows on incremental runs.",
                model.name
            ),
            severity: config.effective_severity_for_path(
                "TST003",
                &model.file_path,
                Severity::Info,
            ),
            category: CheckCategory::TestAdequacy,
            file_path: model.file_path.clone(),
            line: tests_header_line(model),
            column: 0,
            snippet: None,
            suggestion: Some(
                "Add 'row_count_min(1)' to the -- tests: header field to detect \
                 empty incremental runs."
                    .to_owned(),
            ),
            doc_url: doc_url("TST003"),
        });
    }
}

/// TST004: Model with contract_mode STRICT but zero tests.
///
/// If a model enforces strict schema contracts, it should also have tests
/// to validate data correctness.
fn check_tst004(model: &DiscoveredModel, config: &CheckConfig, diags: &mut Vec<CheckDiagnostic>) {
    if !config.is_rule_enabled_for_path("TST004", &model.file_path, true) {
        return;
    }

    let contract_mode = model
        .header
        .get("contract_mode")
        .map(|s| s.as_str())
        .unwrap_or("");

    if contract_mode != "STRICT" {
        return;
    }

    let tests_value = model.header.get("tests").map(|s| s.as_str()).unwrap_or("");
    let declarations = parse_test_declarations(tests_value);

    if declarations.is_empty() {
        diags.push(CheckDiagnostic {
            rule_id: "TST004".to_owned(),
            message: format!(
                "Model '{}' has contract_mode STRICT but declares no tests. \
                 Schema contracts enforce column types, but tests are needed \
                 to validate data correctness.",
                model.name
            ),
            severity: config.effective_severity_for_path(
                "TST004",
                &model.file_path,
                Severity::Warning,
            ),
            category: CheckCategory::TestAdequacy,
            file_path: model.file_path.clone(),
            line: tests_header_line(model),
            column: 0,
            snippet: Some(format!("-- contract_mode: {contract_mode}")),
            suggestion: Some(
                "Add tests for key columns (e.g., 'unique(id), not_null(id)') \
                 to the -- tests: header field."
                    .to_owned(),
            ),
            doc_url: doc_url("TST004"),
        });
    }
}

/// TST005: Model with no tests declared at all.
///
/// **Disabled by default.** Some projects legitimately have models without tests.
/// Enable via config if test coverage is a requirement.
fn check_tst005(model: &DiscoveredModel, config: &CheckConfig, diags: &mut Vec<CheckDiagnostic>) {
    // TST005 is disabled by default
    if !config.is_rule_enabled_for_path("TST005", &model.file_path, false) {
        return;
    }

    let tests_value = model.header.get("tests").map(|s| s.as_str()).unwrap_or("");
    let declarations = parse_test_declarations(tests_value);

    if declarations.is_empty() {
        diags.push(CheckDiagnostic {
            rule_id: "TST005".to_owned(),
            message: format!(
                "Model '{}' has no tests declared. Consider adding tests to \
                 validate data correctness.",
                model.name
            ),
            severity: config.effective_severity_for_path(
                "TST005",
                &model.file_path,
                Severity::Info,
            ),
            category: CheckCategory::TestAdequacy,
            file_path: model.file_path.clone(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: Some(
                "Add a '-- tests:' header field with test declarations \
                 (e.g., 'unique(id), not_null(id)')."
                    .to_owned(),
            ),
            doc_url: doc_url("TST005"),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckConfig;
    use std::collections::HashMap;

    fn make_model(name: &str, headers: &[(&str, &str)]) -> DiscoveredModel {
        let mut header = HashMap::new();
        let mut content_lines = Vec::new();
        for (k, v) in headers {
            header.insert((*k).to_owned(), (*v).to_owned());
            content_lines.push(format!("-- {k}: {v}"));
        }
        content_lines.push("SELECT 1".to_owned());
        DiscoveredModel {
            name: name.to_owned(),
            file_path: format!("models/{name}.sql"),
            content_hash: String::new(),
            ref_names: vec![],
            header,
            content: content_lines.join("\n"),
        }
    }

    fn check_models(models: &[DiscoveredModel]) -> Vec<CheckDiagnostic> {
        let checker = TestAdequacyChecker;
        let config = CheckConfig::default();
        checker.check_project(models, &config)
    }

    // --- TST001: unique_key without unique() test ---

    #[test]
    fn test_tst001_unique_key_no_unique_test_fires() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "MERGE_BY_KEY"),
                ("unique_key", "id"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(diags.iter().any(|d| d.rule_id == "TST001"));
    }

    #[test]
    fn test_tst001_unique_key_with_unique_test_no_fire() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "MERGE_BY_KEY"),
                ("unique_key", "id"),
                ("tests", "unique(id), not_null(id)"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST001"));
    }

    #[test]
    fn test_tst001_no_unique_key_no_fire() {
        let model = make_model(
            "my_model",
            &[("name", "my_model"), ("kind", "FULL_REFRESH")],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST001"));
    }

    // --- TST002: unique_key without not_null() test ---

    #[test]
    fn test_tst002_unique_key_no_not_null_fires() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "MERGE_BY_KEY"),
                ("unique_key", "id"),
                ("tests", "unique(id)"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(diags.iter().any(|d| d.rule_id == "TST002"));
    }

    #[test]
    fn test_tst002_unique_key_with_not_null_no_fire() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "MERGE_BY_KEY"),
                ("unique_key", "id"),
                ("tests", "unique(id), not_null(id)"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST002"));
    }

    // --- TST003: Incremental model without row_count test ---

    #[test]
    fn test_tst003_incremental_no_row_count_fires() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "INCREMENTAL_BY_TIME_RANGE"),
                ("time_column", "created_at"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(diags.iter().any(|d| d.rule_id == "TST003"));
    }

    #[test]
    fn test_tst003_incremental_with_row_count_no_fire() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "INCREMENTAL_BY_TIME_RANGE"),
                ("time_column", "created_at"),
                ("tests", "row_count_min(1)"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST003"));
    }

    #[test]
    fn test_tst003_full_refresh_no_fire() {
        let model = make_model(
            "my_model",
            &[("name", "my_model"), ("kind", "FULL_REFRESH")],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST003"));
    }

    // --- TST004: STRICT contract with no tests ---

    #[test]
    fn test_tst004_strict_no_tests_fires() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "FULL_REFRESH"),
                ("contract_mode", "STRICT"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(diags.iter().any(|d| d.rule_id == "TST004"));
    }

    #[test]
    fn test_tst004_strict_with_tests_no_fire() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "FULL_REFRESH"),
                ("contract_mode", "STRICT"),
                ("tests", "unique(id)"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST004"));
    }

    #[test]
    fn test_tst004_warn_contract_no_fire() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "FULL_REFRESH"),
                ("contract_mode", "WARN"),
            ],
        );
        let diags = check_models(&[model]);
        assert!(!diags.iter().any(|d| d.rule_id == "TST004"));
    }

    // --- TST005: Model with no tests at all (disabled by default) ---

    #[test]
    fn test_tst005_no_tests_disabled_by_default() {
        let model = make_model(
            "my_model",
            &[("name", "my_model"), ("kind", "FULL_REFRESH")],
        );
        let diags = check_models(&[model]);
        // TST005 is disabled by default, should not fire
        assert!(!diags.iter().any(|d| d.rule_id == "TST005"));
    }

    #[test]
    fn test_tst005_fires_when_enabled() {
        let model = make_model(
            "my_model",
            &[("name", "my_model"), ("kind", "FULL_REFRESH")],
        );
        let checker = TestAdequacyChecker;
        let mut config = CheckConfig::default();
        // Enable TST005 by setting severity to Info (any non-Off value enables it)
        config.rules.insert(
            "TST005".to_owned(),
            crate::config::RuleSeverityOverride::Info,
        );
        let diags = checker.check_project(&[model], &config);
        assert!(diags.iter().any(|d| d.rule_id == "TST005"));
    }

    // --- Helper function tests ---

    #[test]
    fn test_parse_test_declarations() {
        let tests = "unique(id), not_null(id), row_count_min(1)";
        let decls = parse_test_declarations(tests);
        assert_eq!(decls.len(), 3);
        assert_eq!(decls[0], "unique(id)");
        assert_eq!(decls[1], "not_null(id)");
        assert_eq!(decls[2], "row_count_min(1)");
    }

    #[test]
    fn test_parse_test_declarations_with_accepted_values() {
        let tests = "unique(id), accepted_values(status:active|inactive|pending)";
        let decls = parse_test_declarations(tests);
        assert_eq!(decls.len(), 2);
        assert_eq!(decls[1], "accepted_values(status:active|inactive|pending)");
    }

    #[test]
    fn test_parse_test_declarations_empty() {
        let decls = parse_test_declarations("");
        assert!(decls.is_empty());
    }

    #[test]
    fn test_has_test_for_column() {
        let decls = vec!["unique(id)".to_owned(), "not_null(user_id)".to_owned()];
        assert!(has_test_for_column(&decls, "unique", "id"));
        assert!(!has_test_for_column(&decls, "unique", "user_id"));
        assert!(has_test_for_column(&decls, "not_null", "user_id"));
    }

    // --- General tests ---

    #[test]
    fn test_all_diags_have_doc_url() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "MERGE_BY_KEY"),
                ("unique_key", "id"),
            ],
        );
        let diags = check_models(&[model]);
        for d in &diags {
            assert!(
                d.doc_url.is_some(),
                "Diagnostic {} missing doc_url",
                d.rule_id
            );
        }
    }

    #[test]
    fn test_all_diags_have_suggestion() {
        let model = make_model(
            "my_model",
            &[
                ("name", "my_model"),
                ("kind", "MERGE_BY_KEY"),
                ("unique_key", "id"),
            ],
        );
        let diags = check_models(&[model]);
        for d in &diags {
            assert!(
                d.suggestion.is_some(),
                "Diagnostic {} missing suggestion",
                d.rule_id
            );
        }
    }
}
