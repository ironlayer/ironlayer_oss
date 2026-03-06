//! Incremental model logic validation — rules INC001 through INC005.
//!
//! Validates that incremental models use their declared fields (time_column,
//! unique_key) correctly in the SQL body, ensuring the incremental strategy
//! actually works at runtime.

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::sql_lexer::{self, Token, TokenKind};
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Incremental logic checker.
pub struct IncrementalLogicChecker;

impl Checker for IncrementalLogicChecker {
    fn name(&self) -> &'static str {
        "incremental_logic"
    }

    fn check_file(
        &self,
        file_path: &str,
        content: &str,
        model: Option<&DiscoveredModel>,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        if !file_path.ends_with(".sql") {
            return Vec::new();
        }

        let model = match model {
            Some(m) => m,
            None => return Vec::new(),
        };

        let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");

        // Only applies to incremental model kinds
        if kind != "INCREMENTAL_BY_TIME_RANGE" && kind != "MERGE_BY_KEY" && kind != "APPEND_ONLY" {
            return Vec::new();
        }

        let body = sql_lexer::strip_header(content);
        let tokens = sql_lexer::tokenize(body);
        let meaningful = sql_lexer::meaningful_tokens(&tokens);

        let header_lines = content
            .lines()
            .take_while(|line| {
                let trimmed = line.trim();
                trimmed.is_empty() || trimmed.starts_with("--")
            })
            .count() as u32;

        let mut diags = Vec::new();

        check_inc001(
            file_path,
            &meaningful,
            header_lines,
            model,
            config,
            &mut diags,
        );
        check_inc002(
            file_path,
            body,
            &meaningful,
            header_lines,
            model,
            config,
            &mut diags,
        );
        check_inc003(
            file_path,
            &meaningful,
            header_lines,
            model,
            config,
            &mut diags,
        );
        check_inc004(
            file_path,
            &meaningful,
            header_lines,
            model,
            config,
            &mut diags,
        );
        check_inc005(file_path, model, config, &mut diags);

        diags
    }
}

/// Helper: check if a token is a keyword matching the given word.
fn is_kw(tok: &Token<'_>, word: &str) -> bool {
    tok.kind == TokenKind::Keyword && tok.text.eq_ignore_ascii_case(word)
}

/// Helper: check if a token is a keyword or identifier matching the given word.
fn is_kw_or_ident(tok: &Token<'_>, word: &str) -> bool {
    (tok.kind == TokenKind::Keyword || tok.kind == TokenKind::Identifier)
        && tok.text.eq_ignore_ascii_case(word)
}

/// Helper: generate a documentation URL for a rule.
fn doc_url(rule_id: &str) -> Option<String> {
    Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}"))
}

/// Check if the given column name appears anywhere in a WHERE clause in the tokens.
fn column_in_where_clause(meaningful: &[&Token<'_>], column_name: &str) -> bool {
    let mut in_where = false;
    let mut paren_depth: i32 = 0;

    for tok in meaningful {
        if is_kw(tok, "WHERE") {
            in_where = true;
            paren_depth = 0;
            continue;
        }

        if in_where {
            match tok.kind {
                TokenKind::LeftParen => paren_depth += 1,
                TokenKind::RightParen => {
                    paren_depth -= 1;
                    if paren_depth < 0 {
                        in_where = false;
                        continue;
                    }
                }
                _ => {}
            }

            // End WHERE on next clause keyword at paren depth 0
            if paren_depth == 0
                && (is_kw(tok, "GROUP")
                    || is_kw(tok, "ORDER")
                    || is_kw(tok, "HAVING")
                    || is_kw(tok, "LIMIT")
                    || is_kw(tok, "UNION")
                    || is_kw(tok, "EXCEPT")
                    || is_kw(tok, "INTERSECT"))
            {
                in_where = false;
                continue;
            }

            if is_kw_or_ident(tok, column_name) {
                return true;
            }
        }
    }

    false
}

/// Check if the given column name appears anywhere in a SELECT clause.
fn column_in_select_clause(meaningful: &[&Token<'_>], column_name: &str) -> bool {
    let mut in_select = false;

    for tok in meaningful {
        if is_kw(tok, "SELECT") {
            in_select = true;
            continue;
        }

        if in_select {
            // End SELECT on FROM
            if is_kw(tok, "FROM") {
                in_select = false;
                continue;
            }

            if is_kw_or_ident(tok, column_name) {
                return true;
            }
        }
    }

    false
}

/// INC001: INCREMENTAL_BY_TIME_RANGE model doesn't reference time_column in WHERE.
///
/// If the model declares `time_column`, it must appear in a WHERE clause to
/// actually filter incrementally.
fn check_inc001(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    model: &DiscoveredModel,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("INC001", file_path, true) {
        return;
    }

    let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");
    if kind != "INCREMENTAL_BY_TIME_RANGE" {
        return;
    }

    let time_column = match model.header.get("time_column") {
        Some(tc) => tc.as_str(),
        None => return, // HDR005 already catches this
    };

    if !column_in_where_clause(meaningful, time_column) {
        diags.push(CheckDiagnostic {
            rule_id: "INC001".to_owned(),
            message: format!(
                "Model '{}' declares kind INCREMENTAL_BY_TIME_RANGE with \
                 time_column '{time_column}', but '{time_column}' does not \
                 appear in any WHERE clause. The incremental filter will not work.",
                model.name
            ),
            severity: config.effective_severity_for_path("INC001", file_path, Severity::Error),
            category: CheckCategory::IncrementalLogic,
            file_path: file_path.to_owned(),
            line: header_lines + 1,
            column: 0,
            snippet: Some(format!("-- time_column: {time_column}")),
            suggestion: Some(format!(
                "Add 'WHERE {time_column} > ...' or 'WHERE {time_column} >= ...' \
                 to the SQL body to enable incremental filtering."
            )),
            doc_url: doc_url("INC001"),
        });
    }
}

/// INC002: MERGE_BY_KEY model doesn't reference unique_key in SQL body.
///
/// The unique_key must appear somewhere in the SQL to serve as the
/// merge/join condition.
fn check_inc002(
    file_path: &str,
    body: &str,
    _meaningful: &[&Token<'_>],
    header_lines: u32,
    model: &DiscoveredModel,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("INC002", file_path, true) {
        return;
    }

    let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");
    if kind != "MERGE_BY_KEY" {
        return;
    }

    let unique_key = match model.header.get("unique_key") {
        Some(uk) => uk.as_str(),
        None => return, // HDR006 already catches this
    };

    // Check if the unique_key column appears anywhere in the SQL body
    // (case-insensitive search in the body text)
    let body_lower = body.to_ascii_lowercase();
    let key_lower = unique_key.to_ascii_lowercase();

    if !body_lower.contains(&key_lower) {
        diags.push(CheckDiagnostic {
            rule_id: "INC002".to_owned(),
            message: format!(
                "Model '{}' declares kind MERGE_BY_KEY with unique_key '{unique_key}', \
                 but '{unique_key}' does not appear in the SQL body. The merge key \
                 must be referenced for deduplication or joining.",
                model.name
            ),
            severity: config.effective_severity_for_path("INC002", file_path, Severity::Error),
            category: CheckCategory::IncrementalLogic,
            file_path: file_path.to_owned(),
            line: header_lines + 1,
            column: 0,
            snippet: Some(format!("-- unique_key: {unique_key}")),
            suggestion: Some(format!(
                "Include '{unique_key}' in the SELECT list and ensure it's used \
                 in the merge/join condition."
            )),
            doc_url: doc_url("INC002"),
        });
    }
}

/// INC003: Incremental model with no WHERE clause.
///
/// INCREMENTAL_BY_TIME_RANGE and APPEND_ONLY models should have a WHERE
/// clause to filter data incrementally. Without one, every run processes
/// all data.
fn check_inc003(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    model: &DiscoveredModel,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("INC003", file_path, true) {
        return;
    }

    let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");
    if kind != "INCREMENTAL_BY_TIME_RANGE" && kind != "APPEND_ONLY" {
        return;
    }

    let has_where = meaningful.iter().any(|t| is_kw(t, "WHERE"));

    if !has_where {
        diags.push(CheckDiagnostic {
            rule_id: "INC003".to_owned(),
            message: format!(
                "Model '{}' has kind '{kind}' but contains no WHERE clause. \
                 Without a filter, every run will process all source data, \
                 negating the benefits of incremental loading.",
                model.name
            ),
            severity: config.effective_severity_for_path("INC003", file_path, Severity::Warning),
            category: CheckCategory::IncrementalLogic,
            file_path: file_path.to_owned(),
            line: header_lines + 1,
            column: 0,
            snippet: None,
            suggestion: Some(
                "Add a WHERE clause to filter on the incremental boundary \
                 (e.g., WHERE created_at > '{{start_time}}')."
                    .to_owned(),
            ),
            doc_url: doc_url("INC003"),
        });
    }
}

/// INC004: time_column in SELECT but not in WHERE.
///
/// If the time_column appears in the SELECT clause but not in the WHERE
/// clause, the model selects the column but doesn't filter on it.
fn check_inc004(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    model: &DiscoveredModel,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("INC004", file_path, true) {
        return;
    }

    let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");
    if kind != "INCREMENTAL_BY_TIME_RANGE" {
        return;
    }

    let time_column = match model.header.get("time_column") {
        Some(tc) => tc.as_str(),
        None => return,
    };

    let in_select = column_in_select_clause(meaningful, time_column);
    let in_where = column_in_where_clause(meaningful, time_column);

    if in_select && !in_where {
        diags.push(CheckDiagnostic {
            rule_id: "INC004".to_owned(),
            message: format!(
                "Model '{}' selects time_column '{time_column}' but does not \
                 filter on it in a WHERE clause. The incremental boundary \
                 won't be enforced.",
                model.name
            ),
            severity: config.effective_severity_for_path("INC004", file_path, Severity::Warning),
            category: CheckCategory::IncrementalLogic,
            file_path: file_path.to_owned(),
            line: header_lines + 1,
            column: 0,
            snippet: Some(format!("SELECT ... {time_column} ... (no WHERE)")),
            suggestion: Some(format!(
                "Add 'WHERE {time_column} >= ...' to enforce the incremental boundary."
            )),
            doc_url: doc_url("INC004"),
        });
    }
}

/// INC005: MERGE_BY_KEY with incompatible materialization.
///
/// MERGE_BY_KEY models should use TABLE or MERGE materialization. Using
/// VIEW doesn't make sense for a merge strategy.
fn check_inc005(
    file_path: &str,
    model: &DiscoveredModel,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("INC005", file_path, true) {
        return;
    }

    let kind = model.header.get("kind").map(|s| s.as_str()).unwrap_or("");
    if kind != "MERGE_BY_KEY" {
        return;
    }

    let materialization = match model.header.get("materialization") {
        Some(m) => m.as_str(),
        None => return, // No materialization declared, nothing to validate
    };

    if materialization == "VIEW" {
        let header_line = model
            .content
            .lines()
            .enumerate()
            .find(|(_, line)| {
                let trimmed = line.trim();
                trimmed.starts_with("-- materialization:")
            })
            .map_or(1, |(i, _)| i as u32 + 1);

        diags.push(CheckDiagnostic {
            rule_id: "INC005".to_owned(),
            message: format!(
                "Model '{}' has kind MERGE_BY_KEY but materialization VIEW. \
                 Views cannot be merge targets — use TABLE or MERGE materialization.",
                model.name
            ),
            severity: config.effective_severity_for_path("INC005", file_path, Severity::Warning),
            category: CheckCategory::IncrementalLogic,
            file_path: file_path.to_owned(),
            line: header_line,
            column: 0,
            snippet: Some(format!("-- materialization: {materialization}")),
            suggestion: Some(
                "Change materialization to TABLE or MERGE for MERGE_BY_KEY models.".to_owned(),
            ),
            doc_url: doc_url("INC005"),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckConfig;
    use std::collections::HashMap;

    fn make_model(kind: &str, extra: &[(&str, &str)]) -> DiscoveredModel {
        let mut header = HashMap::new();
        header.insert("name".to_owned(), "test_model".to_owned());
        header.insert("kind".to_owned(), kind.to_owned());
        for (k, v) in extra {
            header.insert((*k).to_owned(), (*v).to_owned());
        }
        DiscoveredModel {
            name: "test_model".to_owned(),
            file_path: "models/test.sql".to_owned(),
            content_hash: String::new(),
            ref_names: vec![],
            header,
            content: String::new(),
        }
    }

    fn check_with_model(sql: &str, model: &DiscoveredModel) -> Vec<CheckDiagnostic> {
        let checker = IncrementalLogicChecker;
        let config = CheckConfig::default();
        checker.check_file("models/test.sql", sql, Some(model), &config)
    }

    // --- INC001: time_column not in WHERE ---

    #[test]
    fn test_inc001_time_column_missing_from_where() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id, name, created_at FROM source";
        let diags = check_with_model(sql, &model);
        assert!(diags.iter().any(|d| d.rule_id == "INC001"));
    }

    #[test]
    fn test_inc001_time_column_in_where_no_fire() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id, name, created_at FROM source WHERE created_at > '2024-01-01'";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC001"));
    }

    #[test]
    fn test_inc001_full_refresh_no_fire() {
        let model = make_model("FULL_REFRESH", &[]);
        let sql = "-- name: test_model\n-- kind: FULL_REFRESH\n\
                   SELECT id FROM source";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC001"));
    }

    // --- INC002: unique_key not in SQL body ---

    #[test]
    fn test_inc002_unique_key_missing_from_body() {
        let model = make_model("MERGE_BY_KEY", &[("unique_key", "user_id")]);
        let sql = "-- name: test_model\n-- kind: MERGE_BY_KEY\n\
                   -- unique_key: user_id\n\
                   SELECT name, email FROM source";
        let diags = check_with_model(sql, &model);
        assert!(diags.iter().any(|d| d.rule_id == "INC002"));
    }

    #[test]
    fn test_inc002_unique_key_present_no_fire() {
        let model = make_model("MERGE_BY_KEY", &[("unique_key", "user_id")]);
        let sql = "-- name: test_model\n-- kind: MERGE_BY_KEY\n\
                   -- unique_key: user_id\n\
                   SELECT user_id, name, email FROM source";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC002"));
    }

    // --- INC003: Incremental model with no WHERE ---

    #[test]
    fn test_inc003_no_where_fires() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id, name, created_at FROM source";
        let diags = check_with_model(sql, &model);
        assert!(diags.iter().any(|d| d.rule_id == "INC003"));
    }

    #[test]
    fn test_inc003_has_where_no_fire() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id FROM source WHERE created_at > '2024-01-01'";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC003"));
    }

    #[test]
    fn test_inc003_merge_by_key_no_fire() {
        let model = make_model("MERGE_BY_KEY", &[("unique_key", "id")]);
        let sql = "-- name: test_model\n-- kind: MERGE_BY_KEY\n\
                   -- unique_key: id\n\
                   SELECT id, name FROM source";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC003"));
    }

    // --- INC004: time_column in SELECT but not WHERE ---

    #[test]
    fn test_inc004_in_select_not_where_fires() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id, created_at FROM source";
        let diags = check_with_model(sql, &model);
        assert!(diags.iter().any(|d| d.rule_id == "INC004"));
    }

    #[test]
    fn test_inc004_in_both_no_fire() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id, created_at FROM source WHERE created_at > '2024-01-01'";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC004"));
    }

    // --- INC005: MERGE_BY_KEY with VIEW materialization ---

    #[test]
    fn test_inc005_merge_by_key_view_fires() {
        let model = make_model(
            "MERGE_BY_KEY",
            &[("unique_key", "id"), ("materialization", "VIEW")],
        );
        let sql = "-- name: test_model\n-- kind: MERGE_BY_KEY\n\
                   -- unique_key: id\n-- materialization: VIEW\n\
                   SELECT id, name FROM source";
        let diags = check_with_model(sql, &model);
        assert!(diags.iter().any(|d| d.rule_id == "INC005"));
    }

    #[test]
    fn test_inc005_merge_by_key_table_no_fire() {
        let model = make_model(
            "MERGE_BY_KEY",
            &[("unique_key", "id"), ("materialization", "TABLE")],
        );
        let sql = "-- name: test_model\n-- kind: MERGE_BY_KEY\n\
                   -- unique_key: id\n-- materialization: TABLE\n\
                   SELECT id, name FROM source";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "INC005"));
    }

    // --- General tests ---

    #[test]
    fn test_non_sql_file_no_diags() {
        let checker = IncrementalLogicChecker;
        let config = CheckConfig::default();
        let diags = checker.check_file("schema.yml", "some yaml", None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_no_model_no_diags() {
        let checker = IncrementalLogicChecker;
        let config = CheckConfig::default();
        let diags = checker.check_file("models/test.sql", "SELECT 1", None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_all_diags_have_doc_url() {
        let model = make_model(
            "INCREMENTAL_BY_TIME_RANGE",
            &[("time_column", "created_at")],
        );
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   -- time_column: created_at\n\
                   SELECT id, created_at FROM source";
        let diags = check_with_model(sql, &model);
        for d in &diags {
            assert!(
                d.doc_url.is_some(),
                "Diagnostic {} missing doc_url",
                d.rule_id
            );
        }
    }
}
