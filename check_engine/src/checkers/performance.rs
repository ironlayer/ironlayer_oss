//! Performance anti-pattern detection — rules PERF001 through PERF007.
//!
//! Detects SQL patterns that are likely to cause performance issues at scale,
//! such as cartesian products, unnecessary sorting, and suboptimal join patterns.

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::sql_lexer::{self, Token, TokenKind};
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Performance anti-pattern checker.
pub struct PerformanceChecker;

impl Checker for PerformanceChecker {
    fn name(&self) -> &'static str {
        "performance"
    }

    fn check_file(
        &self,
        file_path: &str,
        content: &str,
        _model: Option<&DiscoveredModel>,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        if !file_path.ends_with(".sql") {
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

        check_perf001(file_path, &meaningful, header_lines, config, &mut diags);
        check_perf002(file_path, &meaningful, header_lines, config, &mut diags);
        check_perf003(file_path, &meaningful, header_lines, config, &mut diags);
        check_perf004(file_path, &meaningful, header_lines, config, &mut diags);
        check_perf005(file_path, &meaningful, header_lines, config, &mut diags);
        check_perf006(file_path, &meaningful, header_lines, config, &mut diags);
        check_perf007(file_path, &meaningful, header_lines, config, &mut diags);

        diags
    }
}

/// Helper: check if a token is a keyword matching the given word.
fn is_kw(tok: &Token<'_>, word: &str) -> bool {
    tok.kind == TokenKind::Keyword && tok.text.eq_ignore_ascii_case(word)
}

/// Helper: generate a documentation URL for a rule.
fn doc_url(rule_id: &str) -> Option<String> {
    Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}"))
}

/// PERF001: CROSS JOIN detected.
///
/// CROSS JOIN produces a cartesian product which can explode row counts.
/// Often indicates a missing join condition.
fn check_perf001(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF001", file_path, true) {
        return;
    }

    for (idx, tok) in meaningful.iter().enumerate() {
        if is_kw(tok, "CROSS") && idx + 1 < meaningful.len() && is_kw(meaningful[idx + 1], "JOIN") {
            let line = header_lines + tok.line;
            diags.push(CheckDiagnostic {
                rule_id: "PERF001".to_owned(),
                message: format!(
                    "CROSS JOIN at line {line}. This produces a cartesian product \
                     which can cause row count explosion. Ensure this is intentional."
                ),
                severity: config.effective_severity_for_path(
                    "PERF001",
                    file_path,
                    Severity::Warning,
                ),
                category: CheckCategory::Performance,
                file_path: file_path.to_owned(),
                line,
                column: tok.column,
                snippet: Some("CROSS JOIN".to_owned()),
                suggestion: Some(
                    "If you need a cross join, add a comment explaining why. \
                     Otherwise, use INNER JOIN with a join condition."
                        .to_owned(),
                ),
                doc_url: doc_url("PERF001"),
            });
        }
    }
}

/// PERF002: ORDER BY in subquery or CTE.
///
/// ORDER BY in a subquery or CTE is usually unnecessary because the outer
/// query doesn't preserve the ordering. The database may ignore it or
/// waste resources sorting.
fn check_perf002(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF002", file_path, true) {
        return;
    }

    // Track parenthesis depth to detect subqueries
    let mut paren_depth: i32 = 0;

    for (idx, tok) in meaningful.iter().enumerate() {
        match tok.kind {
            TokenKind::LeftParen => paren_depth += 1,
            TokenKind::RightParen => paren_depth -= 1,
            _ => {}
        }

        // ORDER BY inside parentheses (depth > 0) means it's in a subquery
        if paren_depth > 0
            && is_kw(tok, "ORDER")
            && idx + 1 < meaningful.len()
            && is_kw(meaningful[idx + 1], "BY")
        {
            // Skip if there's a ROW_NUMBER, RANK, etc. window function nearby
            // (ORDER BY in window functions is valid)
            let has_over_nearby = meaningful[idx..].iter().take(10).any(|t| is_kw(t, "OVER"));

            // Also check backwards for window function keywords
            let has_window_func = idx >= 2
                && meaningful[..idx].iter().rev().take(5).any(|t| {
                    is_kw(t, "OVER")
                        || t.text.eq_ignore_ascii_case("ROW_NUMBER")
                        || t.text.eq_ignore_ascii_case("RANK")
                        || t.text.eq_ignore_ascii_case("DENSE_RANK")
                        || t.text.eq_ignore_ascii_case("NTILE")
                        || t.text.eq_ignore_ascii_case("LAG")
                        || t.text.eq_ignore_ascii_case("LEAD")
                });

            // Skip if LIMIT follows nearby (ORDER BY + LIMIT in subquery is valid)
            let has_limit_nearby = meaningful[idx..]
                .iter()
                .take(15)
                .any(|t| is_kw(t, "LIMIT") || is_kw(t, "FETCH"));

            if !has_over_nearby && !has_window_func && !has_limit_nearby {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "PERF002".to_owned(),
                    message: format!(
                        "ORDER BY in subquery at line {line}. Subquery ordering is \
                         not preserved by the outer query and wastes sort resources."
                    ),
                    severity: config.effective_severity_for_path(
                        "PERF002",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::Performance,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some("ORDER BY (in subquery)".to_owned()),
                    suggestion: Some(
                        "Remove the ORDER BY from the subquery. Add it to the \
                         outermost query if ordering is needed."
                            .to_owned(),
                    ),
                    doc_url: doc_url("PERF002"),
                });
            }
        }
    }
}

/// PERF003: NOT IN with subquery.
///
/// NOT IN with a subquery has surprising NULL behavior and often performs
/// worse than NOT EXISTS with a correlated subquery.
fn check_perf003(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF003", file_path, true) {
        return;
    }

    for (idx, tok) in meaningful.iter().enumerate() {
        if is_kw(tok, "NOT")
            && idx + 1 < meaningful.len()
            && is_kw(meaningful[idx + 1], "IN")
            && idx + 2 < meaningful.len()
            && meaningful[idx + 2].kind == TokenKind::LeftParen
        {
            // Check if the next token after '(' is SELECT (subquery)
            if idx + 3 < meaningful.len() && is_kw(meaningful[idx + 3], "SELECT") {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "PERF003".to_owned(),
                    message: format!(
                        "NOT IN with subquery at line {line}. NOT IN has surprising \
                         behavior with NULLs and may perform worse than NOT EXISTS."
                    ),
                    severity: config.effective_severity_for_path(
                        "PERF003",
                        file_path,
                        Severity::Info,
                    ),
                    category: CheckCategory::Performance,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some("NOT IN (SELECT ...)".to_owned()),
                    suggestion: Some(
                        "Consider using NOT EXISTS (SELECT 1 FROM ... WHERE ...) \
                         instead. NOT EXISTS handles NULLs correctly and often \
                         performs better."
                            .to_owned(),
                    ),
                    doc_url: doc_url("PERF003"),
                });
            }
        }
    }
}

/// PERF004: SELECT * usage.
///
/// SELECT * pulls all columns which can be wasteful for wide tables and
/// makes the model fragile to upstream schema changes.
fn check_perf004(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF004", file_path, true) {
        return;
    }

    for (idx, tok) in meaningful.iter().enumerate() {
        if is_kw(tok, "SELECT") {
            // Check the next meaningful token for *
            if idx + 1 < meaningful.len()
                && meaningful[idx + 1].kind == TokenKind::Operator
                && meaningful[idx + 1].text == "*"
            {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "PERF004".to_owned(),
                    message: format!(
                        "SELECT * at line {line}. Selecting all columns pulls \
                         unnecessary data and makes the model fragile to upstream \
                         schema changes."
                    ),
                    severity: config.effective_severity_for_path(
                        "PERF004",
                        file_path,
                        Severity::Info,
                    ),
                    category: CheckCategory::Performance,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some("SELECT *".to_owned()),
                    suggestion: Some(
                        "List columns explicitly instead of using SELECT *.".to_owned(),
                    ),
                    doc_url: doc_url("PERF004"),
                });
            }
        }
    }
}

/// PERF005: Correlated subquery in SELECT list.
///
/// A correlated subquery in the SELECT list executes once per row of the
/// outer query, which can be extremely slow for large datasets.
fn check_perf005(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF005", file_path, true) {
        return;
    }

    // Track if we're in a SELECT list (between SELECT and FROM at depth 0)
    let mut in_select_list = false;
    let base_depth: i32 = 0;
    let mut paren_depth: i32 = 0;

    for (idx, tok) in meaningful.iter().enumerate() {
        if is_kw(tok, "SELECT") && paren_depth == base_depth {
            in_select_list = true;
            continue;
        }

        if in_select_list && is_kw(tok, "FROM") && paren_depth == base_depth {
            in_select_list = false;
            continue;
        }

        match tok.kind {
            TokenKind::LeftParen => paren_depth += 1,
            TokenKind::RightParen => paren_depth -= 1,
            _ => {}
        }

        // Detect (SELECT ...) in the SELECT list
        if in_select_list
            && tok.kind == TokenKind::LeftParen
            && idx + 1 < meaningful.len()
            && is_kw(meaningful[idx + 1], "SELECT")
        {
            let line = header_lines + tok.line;
            diags.push(CheckDiagnostic {
                rule_id: "PERF005".to_owned(),
                message: format!(
                    "Correlated subquery in SELECT list at line {line}. \
                     This executes once per row and can be extremely slow \
                     for large datasets."
                ),
                severity: config.effective_severity_for_path(
                    "PERF005",
                    file_path,
                    Severity::Warning,
                ),
                category: CheckCategory::Performance,
                file_path: file_path.to_owned(),
                line,
                column: tok.column,
                snippet: Some("(SELECT ... in SELECT list)".to_owned()),
                suggestion: Some(
                    "Rewrite as a LEFT JOIN or use a CTE to avoid per-row execution.".to_owned(),
                ),
                doc_url: doc_url("PERF005"),
            });
        }
    }
}

/// PERF006: DISTINCT on entire SELECT.
///
/// SELECT DISTINCT on all columns may indicate a join producing duplicates
/// that should be fixed at the source rather than masked with DISTINCT.
fn check_perf006(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF006", file_path, true) {
        return;
    }

    let mut paren_depth: i32 = 0;

    for (idx, tok) in meaningful.iter().enumerate() {
        match tok.kind {
            TokenKind::LeftParen => paren_depth += 1,
            TokenKind::RightParen => paren_depth -= 1,
            _ => {}
        }

        // Only flag top-level SELECT DISTINCT (not in subqueries)
        if paren_depth == 0
            && is_kw(tok, "SELECT")
            && idx + 1 < meaningful.len()
            && is_kw(meaningful[idx + 1], "DISTINCT")
        {
            // Don't flag SELECT DISTINCT ON (...) — that's a deliberate dedup
            if idx + 2 < meaningful.len() && is_kw(meaningful[idx + 2], "ON") {
                continue;
            }

            let line = header_lines + tok.line;
            diags.push(CheckDiagnostic {
                rule_id: "PERF006".to_owned(),
                message: format!(
                    "SELECT DISTINCT at line {line}. Using DISTINCT on the entire \
                     result set may indicate a join producing unintended duplicates."
                ),
                severity: config.effective_severity_for_path("PERF006", file_path, Severity::Info),
                category: CheckCategory::Performance,
                file_path: file_path.to_owned(),
                line,
                column: tok.column,
                snippet: Some("SELECT DISTINCT".to_owned()),
                suggestion: Some(
                    "Investigate whether the duplicates are caused by a join issue. \
                     Fix the join condition or use GROUP BY for explicit deduplication."
                        .to_owned(),
                ),
                doc_url: doc_url("PERF006"),
            });
        }
    }
}

/// PERF007: UNION instead of UNION ALL.
///
/// UNION performs an implicit DISTINCT which requires sorting. If duplicates
/// between the branches are not expected, UNION ALL is more efficient.
fn check_perf007(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("PERF007", file_path, true) {
        return;
    }

    for (idx, tok) in meaningful.iter().enumerate() {
        if is_kw(tok, "UNION") {
            // Check that the next token is NOT "ALL" — that means it's a bare UNION
            let is_union_all = idx + 1 < meaningful.len() && is_kw(meaningful[idx + 1], "ALL");

            if !is_union_all {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "PERF007".to_owned(),
                    message: format!(
                        "UNION without ALL at line {line}. UNION performs implicit \
                         deduplication which requires sorting and extra computation."
                    ),
                    severity: config.effective_severity_for_path(
                        "PERF007",
                        file_path,
                        Severity::Info,
                    ),
                    category: CheckCategory::Performance,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some("UNION".to_owned()),
                    suggestion: Some(
                        "Use UNION ALL if duplicates between branches are not expected. \
                         UNION ALL avoids the implicit DISTINCT sort."
                            .to_owned(),
                    ),
                    doc_url: doc_url("PERF007"),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckConfig;

    fn check(sql: &str) -> Vec<CheckDiagnostic> {
        let checker = PerformanceChecker;
        let config = CheckConfig::default();
        checker.check_file("models/test.sql", sql, None, &config)
    }

    // --- PERF001: CROSS JOIN ---

    #[test]
    fn test_perf001_cross_join_fires() {
        let diags = check("SELECT a.id, b.name FROM t1 a CROSS JOIN t2 b");
        assert!(diags.iter().any(|d| d.rule_id == "PERF001"));
    }

    #[test]
    fn test_perf001_inner_join_no_fire() {
        let diags = check("SELECT a.id FROM t1 a INNER JOIN t2 b ON a.id = b.id");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF001"));
    }

    #[test]
    fn test_perf001_in_comment_no_fire() {
        let diags = check("-- CROSS JOIN usage\nSELECT 1");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF001"));
    }

    // --- PERF002: ORDER BY in subquery ---

    #[test]
    fn test_perf002_order_by_in_subquery_fires() {
        let diags = check("SELECT * FROM (SELECT id, name FROM t ORDER BY name) sub");
        assert!(diags.iter().any(|d| d.rule_id == "PERF002"));
    }

    #[test]
    fn test_perf002_order_by_in_outer_no_fire() {
        let diags = check("SELECT id, name FROM t ORDER BY name");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF002"));
    }

    #[test]
    fn test_perf002_window_function_no_fire() {
        let diags = check("SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as rn FROM t");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF002"));
    }

    #[test]
    fn test_perf002_subquery_with_limit_no_fire() {
        let diags = check("SELECT * FROM (SELECT id FROM t ORDER BY id LIMIT 10) sub");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF002"));
    }

    // --- PERF003: NOT IN with subquery ---

    #[test]
    fn test_perf003_not_in_subquery_fires() {
        let diags = check("SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2)");
        assert!(diags.iter().any(|d| d.rule_id == "PERF003"));
    }

    #[test]
    fn test_perf003_not_in_literal_no_fire() {
        let diags = check("SELECT id FROM t1 WHERE status NOT IN ('active', 'pending')");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF003"));
    }

    #[test]
    fn test_perf003_in_subquery_no_fire() {
        let diags = check("SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF003"));
    }

    // --- PERF004: SELECT * ---

    #[test]
    fn test_perf004_select_star_fires() {
        let diags = check("SELECT * FROM my_table");
        assert!(diags.iter().any(|d| d.rule_id == "PERF004"));
    }

    #[test]
    fn test_perf004_explicit_columns_no_fire() {
        let diags = check("SELECT id, name, email FROM my_table");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF004"));
    }

    #[test]
    fn test_perf004_count_star_no_fire() {
        // COUNT(*) should not fire — the * is inside a function call
        let diags = check("SELECT COUNT(*) FROM my_table");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF004"));
    }

    // --- PERF005: Correlated subquery in SELECT ---

    #[test]
    fn test_perf005_correlated_subquery_fires() {
        let diags =
            check("SELECT id, (SELECT MAX(val) FROM t2 WHERE t2.id = t1.id) as max_val FROM t1");
        assert!(diags.iter().any(|d| d.rule_id == "PERF005"));
    }

    #[test]
    fn test_perf005_no_subquery_no_fire() {
        let diags = check("SELECT id, name FROM t1");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF005"));
    }

    // --- PERF006: SELECT DISTINCT ---

    #[test]
    fn test_perf006_select_distinct_fires() {
        let diags = check("SELECT DISTINCT id, name FROM t");
        assert!(diags.iter().any(|d| d.rule_id == "PERF006"));
    }

    #[test]
    fn test_perf006_select_no_distinct_no_fire() {
        let diags = check("SELECT id, name FROM t");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF006"));
    }

    #[test]
    fn test_perf006_subquery_distinct_no_fire() {
        // Only fires for top-level DISTINCT
        let diags = check("SELECT id FROM (SELECT DISTINCT id FROM t) sub");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF006"));
    }

    // --- PERF007: UNION without ALL ---

    #[test]
    fn test_perf007_union_fires() {
        let diags = check("SELECT id FROM t1 UNION SELECT id FROM t2");
        assert!(diags.iter().any(|d| d.rule_id == "PERF007"));
    }

    #[test]
    fn test_perf007_union_all_no_fire() {
        let diags = check("SELECT id FROM t1 UNION ALL SELECT id FROM t2");
        assert!(!diags.iter().any(|d| d.rule_id == "PERF007"));
    }

    // --- General tests ---

    #[test]
    fn test_non_sql_file_no_diags() {
        let checker = PerformanceChecker;
        let config = CheckConfig::default();
        let diags = checker.check_file("schema.yml", "some yaml", None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_all_diags_have_doc_url() {
        let sql = "SELECT * FROM t1 CROSS JOIN t2 UNION SELECT * FROM t3";
        let diags = check(sql);
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
        let sql = "SELECT * FROM t1 CROSS JOIN t2";
        let diags = check(sql);
        for d in &diags {
            assert!(
                d.suggestion.is_some(),
                "Diagnostic {} missing suggestion",
                d.rule_id
            );
        }
    }
}
