//! Databricks-specific SQL validation — rules DBK001 through DBK007.
//!
//! Detects Databricks anti-patterns and dialect-incompatible SQL that
//! should be caught before deployment to a Databricks workspace.

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::sql_lexer::{self, Token, TokenKind};
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Databricks-specific SQL checker.
pub struct DatabricksSqlChecker;

impl Checker for DatabricksSqlChecker {
    fn name(&self) -> &'static str {
        "databricks_sql"
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

        check_dbk001(file_path, &meaningful, header_lines, config, &mut diags);
        check_dbk002(file_path, &meaningful, header_lines, config, &mut diags);
        check_dbk003(file_path, &meaningful, header_lines, config, &mut diags);
        check_dbk004(file_path, &meaningful, header_lines, config, &mut diags);
        check_dbk005(
            file_path,
            body,
            &meaningful,
            header_lines,
            config,
            &mut diags,
        );
        check_dbk006(
            file_path,
            &meaningful,
            header_lines,
            model,
            config,
            &mut diags,
        );
        check_dbk007(file_path, &meaningful, header_lines, config, &mut diags);

        diags
    }
}

/// Helper: check if a token is a keyword matching the given word (case-insensitive).
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

/// DBK001: Hardcoded catalog/schema in table references.
///
/// Detects patterns like `catalog.schema.table` in FROM/JOIN clauses
/// that should use `ref()` or configuration-driven references instead.
fn check_dbk001(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK001", file_path, true) {
        return;
    }

    // Look for three-part names: ident.ident.ident after FROM or JOIN keywords
    let mut i = 0;
    while i < meaningful.len() {
        let is_from_or_join = is_kw(meaningful[i], "FROM")
            || is_kw(meaningful[i], "JOIN")
            || is_kw(meaningful[i], "INTO");

        if is_from_or_join {
            let j = i + 1;
            // Check for catalog.schema.table (three parts separated by dots)
            if j + 4 < meaningful.len()
                && (meaningful[j].kind == TokenKind::Identifier
                    || meaningful[j].kind == TokenKind::QuotedIdent
                    || meaningful[j].kind == TokenKind::Keyword)
                && meaningful[j + 1].kind == TokenKind::Dot
                && (meaningful[j + 2].kind == TokenKind::Identifier
                    || meaningful[j + 2].kind == TokenKind::QuotedIdent
                    || meaningful[j + 2].kind == TokenKind::Keyword)
                && meaningful[j + 3].kind == TokenKind::Dot
                && (meaningful[j + 4].kind == TokenKind::Identifier
                    || meaningful[j + 4].kind == TokenKind::QuotedIdent
                    || meaningful[j + 4].kind == TokenKind::Keyword)
            {
                let catalog = meaningful[j].text;
                let schema = meaningful[j + 2].text;
                let table = meaningful[j + 4].text;
                let fqn = format!("{catalog}.{schema}.{table}");
                let line = header_lines + meaningful[j].line;

                diags.push(CheckDiagnostic {
                    rule_id: "DBK001".to_owned(),
                    message: format!(
                        "Hardcoded three-part table reference '{fqn}' detected. \
                         Use ref() or configuration-driven catalog/schema references \
                         to support multi-environment deployments."
                    ),
                    severity: config.effective_severity_for_path(
                        "DBK001",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::DatabricksSql,
                    file_path: file_path.to_owned(),
                    line,
                    column: meaningful[j].column,
                    snippet: Some(fqn),
                    suggestion: Some(
                        "Replace with {{ ref('model_name') }} or use config-driven \
                         catalog/schema variables."
                            .to_owned(),
                    ),
                    doc_url: doc_url("DBK001"),
                });

                i = j + 5;
                continue;
            }
        }
        i += 1;
    }
}

/// DBK002: Non-deterministic MERGE.
///
/// Detects MERGE INTO statements where the ON condition may match multiple
/// source rows to the same target row, causing non-deterministic results.
/// Fires when MERGE INTO is found without a preceding DISTINCT, GROUP BY,
/// or ROW_NUMBER qualification on the source.
fn check_dbk002(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK002", file_path, true) {
        return;
    }

    let mut i = 0;
    while i < meaningful.len() {
        if is_kw(meaningful[i], "MERGE") {
            // Look for MERGE INTO
            if i + 1 < meaningful.len() && is_kw(meaningful[i + 1], "INTO") {
                // Scan from MERGE to end of statement (semicolon or EOF)
                let stmt_end = meaningful[i..]
                    .iter()
                    .position(|t| t.kind == TokenKind::Semicolon)
                    .map_or(meaningful.len(), |p| i + p);

                let stmt_tokens = &meaningful[i..stmt_end];

                // Check if there's a USING clause with a subquery
                let has_using = stmt_tokens.iter().any(|t| is_kw(t, "USING"));

                if has_using {
                    // Look for deduplication patterns in the USING source:
                    // DISTINCT, GROUP BY, ROW_NUMBER, or QUALIFY
                    let has_dedup = stmt_tokens.iter().any(|t| {
                        is_kw(t, "DISTINCT")
                            || is_kw_or_ident(t, "ROW_NUMBER")
                            || is_kw_or_ident(t, "QUALIFY")
                    });

                    let has_group_by = stmt_tokens
                        .windows(2)
                        .any(|w| is_kw(w[0], "GROUP") && is_kw(w[1], "BY"));

                    if !has_dedup && !has_group_by {
                        let line = header_lines + meaningful[i].line;
                        diags.push(CheckDiagnostic {
                            rule_id: "DBK002".to_owned(),
                            message: format!(
                                "MERGE INTO at line {line} has a USING source without \
                                 deduplication. If the source contains duplicate match \
                                 keys, the MERGE will be non-deterministic."
                            ),
                            severity: config.effective_severity_for_path(
                                "DBK002",
                                file_path,
                                Severity::Error,
                            ),
                            category: CheckCategory::DatabricksSql,
                            file_path: file_path.to_owned(),
                            line,
                            column: meaningful[i].column,
                            snippet: Some("MERGE INTO ... USING ...".to_owned()),
                            suggestion: Some(
                                "Add DISTINCT, GROUP BY, or ROW_NUMBER + QUALIFY to the \
                                 USING source to ensure each target row matches at most \
                                 one source row."
                                    .to_owned(),
                            ),
                            doc_url: doc_url("DBK002"),
                        });
                    }
                }

                i = stmt_end;
                continue;
            }
        }
        i += 1;
    }
}

/// DBK003: OPTIMIZE or VACUUM in model SQL.
///
/// These are maintenance operations that should be run separately,
/// not embedded in transformation model SQL.
fn check_dbk003(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK003", file_path, true) {
        return;
    }

    let mut i = 0;
    while i < meaningful.len() {
        let is_optimize = is_kw_or_ident(meaningful[i], "OPTIMIZE");
        let is_vacuum = is_kw_or_ident(meaningful[i], "VACUUM");

        if is_optimize || is_vacuum {
            let op_name = if is_optimize { "OPTIMIZE" } else { "VACUUM" };
            let line = header_lines + meaningful[i].line;

            diags.push(CheckDiagnostic {
                rule_id: "DBK003".to_owned(),
                message: format!(
                    "{op_name} detected at line {line}. This is a Delta Lake maintenance \
                     operation that should be run as a separate job, not embedded in a \
                     transformation model."
                ),
                severity: config.effective_severity_for_path("DBK003", file_path, Severity::Error),
                category: CheckCategory::DatabricksSql,
                file_path: file_path.to_owned(),
                line,
                column: meaningful[i].column,
                snippet: Some(op_name.to_owned()),
                suggestion: Some(format!(
                    "Move the {op_name} command to a separate maintenance job or \
                     Databricks workflow task."
                )),
                doc_url: doc_url("DBK003"),
            });
        }
        i += 1;
    }
}

/// DBK004: COPY INTO in model SQL.
///
/// COPY INTO is an ingestion operation and should not appear in
/// transformation model SQL.
fn check_dbk004(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK004", file_path, true) {
        return;
    }

    let mut i = 0;
    while i < meaningful.len() {
        if is_kw(meaningful[i], "COPY")
            && i + 1 < meaningful.len()
            && is_kw(meaningful[i + 1], "INTO")
        {
            let line = header_lines + meaningful[i].line;
            diags.push(CheckDiagnostic {
                rule_id: "DBK004".to_owned(),
                message: format!(
                    "COPY INTO detected at line {line}. This is an ingestion operation \
                     that should be handled by a separate data loading pipeline, not a \
                     transformation model."
                ),
                severity: config.effective_severity_for_path(
                    "DBK004",
                    file_path,
                    Severity::Warning,
                ),
                category: CheckCategory::DatabricksSql,
                file_path: file_path.to_owned(),
                line,
                column: meaningful[i].column,
                snippet: Some("COPY INTO".to_owned()),
                suggestion: Some(
                    "Move COPY INTO to a dedicated ingestion job or use Auto Loader \
                     for streaming file ingestion."
                        .to_owned(),
                ),
                doc_url: doc_url("DBK004"),
            });
            i += 2;
            continue;
        }
        i += 1;
    }
}

/// DBK005: Dialect-incompatible functions.
///
/// Detects SQL Server–style functions and syntax that don't work in Databricks:
/// - ISNULL() → use COALESCE() or IFNULL()
/// - DATEADD(part, n, date) → use DATE_ADD() or DATEADD in Databricks SQL syntax
/// - TOP N → use LIMIT N
/// - GETDATE() → use CURRENT_TIMESTAMP
/// - LEN() → use LENGTH()
fn check_dbk005(
    file_path: &str,
    body: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK005", file_path, true) {
        return;
    }

    let incompatible_functions: &[(&str, &str)] = &[
        ("ISNULL", "Use COALESCE() or IFNULL() instead of ISNULL()."),
        ("GETDATE", "Use CURRENT_TIMESTAMP instead of GETDATE()."),
        ("LEN", "Use LENGTH() instead of LEN()."),
        (
            "CHARINDEX",
            "Use LOCATE() or POSITION() instead of CHARINDEX().",
        ),
        ("NVARCHAR", "Use STRING instead of NVARCHAR."),
    ];

    for (func_name, suggestion) in incompatible_functions {
        for (idx, tok) in meaningful.iter().enumerate() {
            if is_kw_or_ident(tok, func_name)
                && idx + 1 < meaningful.len()
                && meaningful[idx + 1].kind == TokenKind::LeftParen
            {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "DBK005".to_owned(),
                    message: format!(
                        "Dialect-incompatible function '{}' at line {line}. \
                         This function is not native to Databricks SQL.",
                        tok.text
                    ),
                    severity: config.effective_severity_for_path(
                        "DBK005",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::DatabricksSql,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some(format!("{}(", tok.text)),
                    suggestion: Some((*suggestion).to_owned()),
                    doc_url: doc_url("DBK005"),
                });
            }
        }
    }

    // Check for TOP N (SQL Server style) — appears as keyword/ident TOP after SELECT
    for (idx, tok) in meaningful.iter().enumerate() {
        if idx > 0 && is_kw(meaningful[idx - 1], "SELECT") && is_kw_or_ident(tok, "TOP") {
            // Ensure it's not in a string literal (already filtered by meaningful_tokens)
            // and not part of a column alias (next token should be a number)
            if idx + 1 < meaningful.len() && meaningful[idx + 1].kind == TokenKind::NumberLiteral {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "DBK005".to_owned(),
                    message: format!(
                        "SQL Server–style 'SELECT TOP N' at line {line}. \
                         Databricks SQL uses LIMIT instead."
                    ),
                    severity: config.effective_severity_for_path(
                        "DBK005",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::DatabricksSql,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some(format!("SELECT TOP {}", meaningful[idx + 1].text)),
                    suggestion: Some(
                        "Use 'SELECT ... LIMIT N' instead of 'SELECT TOP N'.".to_owned(),
                    ),
                    doc_url: doc_url("DBK005"),
                });
            }
        }
    }

    // Check for NOLOCK hint (SQL Server table hint)
    let body_upper = body.to_ascii_uppercase();
    if body_upper.contains("NOLOCK") {
        for tok in meaningful.iter() {
            if is_kw_or_ident(tok, "NOLOCK") {
                let line = header_lines + tok.line;
                diags.push(CheckDiagnostic {
                    rule_id: "DBK005".to_owned(),
                    message: format!(
                        "SQL Server table hint 'NOLOCK' at line {line}. \
                         Databricks SQL does not support table hints."
                    ),
                    severity: config.effective_severity_for_path(
                        "DBK005",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::DatabricksSql,
                    file_path: file_path.to_owned(),
                    line,
                    column: tok.column,
                    snippet: Some("NOLOCK".to_owned()),
                    suggestion: Some(
                        "Remove the NOLOCK hint. Databricks handles read isolation \
                         automatically via Delta Lake MVCC."
                            .to_owned(),
                    ),
                    doc_url: doc_url("DBK005"),
                });
                // Only flag once
                break;
            }
        }
    }
}

/// DBK006: CREATE OR REPLACE in incremental model.
///
/// Using CREATE OR REPLACE TABLE in an INCREMENTAL_BY_TIME_RANGE or
/// MERGE_BY_KEY model defeats the purpose of incrementality.
fn check_dbk006(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    model: Option<&DiscoveredModel>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK006", file_path, true) {
        return;
    }

    // Only fire for incremental models
    let is_incremental = model.is_some_and(|m| {
        let kind = m.header.get("kind").map(|s| s.as_str()).unwrap_or("");
        kind == "INCREMENTAL_BY_TIME_RANGE" || kind == "MERGE_BY_KEY" || kind == "APPEND_ONLY"
    });

    if !is_incremental {
        return;
    }

    let mut i = 0;
    while i < meaningful.len() {
        if is_kw(meaningful[i], "CREATE") {
            // Look for CREATE OR REPLACE [TABLE|VIEW]
            if i + 2 < meaningful.len()
                && is_kw(meaningful[i + 1], "OR")
                && is_kw(meaningful[i + 2], "REPLACE")
            {
                let line = header_lines + meaningful[i].line;
                let kind_str = model
                    .and_then(|m| m.header.get("kind"))
                    .map_or("incremental", |s| s.as_str());

                diags.push(CheckDiagnostic {
                    rule_id: "DBK006".to_owned(),
                    message: format!(
                        "CREATE OR REPLACE in a '{kind_str}' model at line {line}. \
                         This drops and recreates the table on every run, defeating \
                         the purpose of incremental loading."
                    ),
                    severity: config.effective_severity_for_path(
                        "DBK006",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::DatabricksSql,
                    file_path: file_path.to_owned(),
                    line,
                    column: meaningful[i].column,
                    snippet: Some("CREATE OR REPLACE".to_owned()),
                    suggestion: Some(
                        "For incremental models, use INSERT INTO, MERGE INTO, or \
                         let the IronLayer engine handle materialization."
                            .to_owned(),
                    ),
                    doc_url: doc_url("DBK006"),
                });

                i += 3;
                continue;
            }
        }
        i += 1;
    }
}

/// DBK007: Non-standard MERGE syntax.
///
/// Detects MERGE statements that are missing WHEN MATCHED or
/// WHEN NOT MATCHED clauses, which may indicate incomplete merge logic.
fn check_dbk007(
    file_path: &str,
    meaningful: &[&Token<'_>],
    header_lines: u32,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("DBK007", file_path, true) {
        return;
    }

    let mut i = 0;
    while i < meaningful.len() {
        if is_kw(meaningful[i], "MERGE")
            && i + 1 < meaningful.len()
            && is_kw(meaningful[i + 1], "INTO")
        {
            let stmt_end = meaningful[i..]
                .iter()
                .position(|t| t.kind == TokenKind::Semicolon)
                .map_or(meaningful.len(), |p| i + p);

            let stmt_tokens = &meaningful[i..stmt_end];

            // Check for WHEN MATCHED and WHEN NOT MATCHED
            let has_when_matched = stmt_tokens
                .windows(2)
                .any(|w| is_kw(w[0], "WHEN") && is_kw_or_ident(w[1], "MATCHED"));

            let has_when_not_matched = stmt_tokens.windows(3).any(|w| {
                is_kw(w[0], "WHEN") && is_kw(w[1], "NOT") && is_kw_or_ident(w[2], "MATCHED")
            });

            if !has_when_matched && !has_when_not_matched {
                let line = header_lines + meaningful[i].line;
                diags.push(CheckDiagnostic {
                    rule_id: "DBK007".to_owned(),
                    message: format!(
                        "MERGE INTO at line {line} has no WHEN MATCHED or \
                         WHEN NOT MATCHED clauses. This MERGE statement has no \
                         effect without action clauses."
                    ),
                    severity: config.effective_severity_for_path(
                        "DBK007",
                        file_path,
                        Severity::Warning,
                    ),
                    category: CheckCategory::DatabricksSql,
                    file_path: file_path.to_owned(),
                    line,
                    column: meaningful[i].column,
                    snippet: Some("MERGE INTO ... (no WHEN clauses)".to_owned()),
                    suggestion: Some(
                        "Add WHEN MATCHED THEN UPDATE and/or WHEN NOT MATCHED \
                         THEN INSERT clauses to define merge behavior."
                            .to_owned(),
                    ),
                    doc_url: doc_url("DBK007"),
                });
            }

            i = stmt_end;
            continue;
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckConfig;

    fn check(sql: &str) -> Vec<CheckDiagnostic> {
        let checker = DatabricksSqlChecker;
        let config = CheckConfig::default();
        checker.check_file("models/test.sql", sql, None, &config)
    }

    fn check_with_model(sql: &str, model: &DiscoveredModel) -> Vec<CheckDiagnostic> {
        let checker = DatabricksSqlChecker;
        let config = CheckConfig::default();
        checker.check_file("models/test.sql", sql, Some(model), &config)
    }

    fn make_model(kind: &str) -> DiscoveredModel {
        let mut header = std::collections::HashMap::new();
        header.insert("name".to_owned(), "test_model".to_owned());
        header.insert("kind".to_owned(), kind.to_owned());
        DiscoveredModel {
            name: "test_model".to_owned(),
            file_path: "models/test.sql".to_owned(),
            content_hash: String::new(),
            ref_names: vec![],
            header,
            content: String::new(),
        }
    }

    // --- DBK001: Hardcoded catalog/schema ---

    #[test]
    fn test_dbk001_three_part_name_fires() {
        let diags = check("SELECT * FROM my_catalog.my_schema.my_table");
        assert!(diags.iter().any(|d| d.rule_id == "DBK001"));
    }

    #[test]
    fn test_dbk001_two_part_name_no_fire() {
        let diags = check("SELECT * FROM my_schema.my_table");
        assert!(!diags.iter().any(|d| d.rule_id == "DBK001"));
    }

    #[test]
    fn test_dbk001_ref_no_fire() {
        let diags = check("SELECT * FROM {{ ref('my_model') }}");
        assert!(!diags.iter().any(|d| d.rule_id == "DBK001"));
    }

    #[test]
    fn test_dbk001_join_three_part_fires() {
        let diags = check("SELECT a.id FROM t a JOIN prod.analytics.dim_customer b ON a.id = b.id");
        assert!(diags.iter().any(|d| d.rule_id == "DBK001"));
    }

    // --- DBK002: Non-deterministic MERGE ---

    #[test]
    fn test_dbk002_merge_without_dedup_fires() {
        let sql = "MERGE INTO target USING source ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET target.val = source.val";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "DBK002"));
    }

    #[test]
    fn test_dbk002_merge_with_distinct_no_fire() {
        let sql = "MERGE INTO target USING (SELECT DISTINCT id, val FROM source) s \
                   ON target.id = s.id \
                   WHEN MATCHED THEN UPDATE SET target.val = s.val";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "DBK002"));
    }

    #[test]
    fn test_dbk002_merge_with_group_by_no_fire() {
        let sql = "MERGE INTO target USING (SELECT id, MAX(val) as val FROM source GROUP BY id) s \
                   ON target.id = s.id \
                   WHEN MATCHED THEN UPDATE SET target.val = s.val";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "DBK002"));
    }

    // --- DBK003: OPTIMIZE/VACUUM ---

    #[test]
    fn test_dbk003_optimize_fires() {
        let diags = check("OPTIMIZE my_table");
        assert!(diags.iter().any(|d| d.rule_id == "DBK003"));
    }

    #[test]
    fn test_dbk003_vacuum_fires() {
        let diags = check("VACUUM my_table RETAIN 168 HOURS");
        assert!(diags.iter().any(|d| d.rule_id == "DBK003"));
    }

    #[test]
    fn test_dbk003_in_comment_no_fire() {
        let diags = check("-- OPTIMIZE table\nSELECT 1");
        assert!(!diags.iter().any(|d| d.rule_id == "DBK003"));
    }

    // --- DBK004: COPY INTO ---

    #[test]
    fn test_dbk004_copy_into_fires() {
        let diags = check("COPY INTO my_table FROM 's3://bucket/path'");
        assert!(diags.iter().any(|d| d.rule_id == "DBK004"));
    }

    #[test]
    fn test_dbk004_no_copy_no_fire() {
        let diags = check("SELECT * FROM my_table");
        assert!(!diags.iter().any(|d| d.rule_id == "DBK004"));
    }

    // --- DBK005: Dialect-incompatible functions ---

    #[test]
    fn test_dbk005_isnull_fires() {
        let diags = check("SELECT ISNULL(col, 0) FROM t");
        assert!(diags.iter().any(|d| d.rule_id == "DBK005"));
    }

    #[test]
    fn test_dbk005_getdate_fires() {
        let diags = check("SELECT GETDATE() AS now");
        assert!(diags.iter().any(|d| d.rule_id == "DBK005"));
    }

    #[test]
    fn test_dbk005_coalesce_no_fire() {
        let diags = check("SELECT COALESCE(col, 0) FROM t");
        assert!(!diags.iter().any(|d| d.rule_id == "DBK005"));
    }

    #[test]
    fn test_dbk005_select_top_fires() {
        let diags = check("SELECT TOP 10 col FROM t");
        assert!(diags.iter().any(|d| d.rule_id == "DBK005"));
    }

    #[test]
    fn test_dbk005_select_limit_no_fire() {
        let diags = check("SELECT col FROM t LIMIT 10");
        assert!(!diags.iter().any(|d| d.rule_id == "DBK005"));
    }

    // --- DBK006: CREATE OR REPLACE in incremental ---

    #[test]
    fn test_dbk006_create_or_replace_incremental_fires() {
        let model = make_model("INCREMENTAL_BY_TIME_RANGE");
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   CREATE OR REPLACE TABLE t AS SELECT * FROM src";
        let diags = check_with_model(sql, &model);
        assert!(diags.iter().any(|d| d.rule_id == "DBK006"));
    }

    #[test]
    fn test_dbk006_create_or_replace_full_refresh_no_fire() {
        let model = make_model("FULL_REFRESH");
        let sql = "-- name: test_model\n-- kind: FULL_REFRESH\n\
                   CREATE OR REPLACE TABLE t AS SELECT * FROM src";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "DBK006"));
    }

    #[test]
    fn test_dbk006_no_create_or_replace_no_fire() {
        let model = make_model("INCREMENTAL_BY_TIME_RANGE");
        let sql = "-- name: test_model\n-- kind: INCREMENTAL_BY_TIME_RANGE\n\
                   SELECT * FROM src WHERE created_at > '2024-01-01'";
        let diags = check_with_model(sql, &model);
        assert!(!diags.iter().any(|d| d.rule_id == "DBK006"));
    }

    // --- DBK007: Non-standard MERGE syntax ---

    #[test]
    fn test_dbk007_merge_no_when_clauses_fires() {
        let sql = "MERGE INTO target USING source ON target.id = source.id";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "DBK007"));
    }

    #[test]
    fn test_dbk007_merge_with_when_matched_no_fire() {
        let sql = "MERGE INTO target USING source ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET target.val = source.val";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "DBK007"));
    }

    #[test]
    fn test_dbk007_merge_with_when_not_matched_no_fire() {
        let sql = "MERGE INTO target USING source ON target.id = source.id \
                   WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "DBK007"));
    }

    // --- General tests ---

    #[test]
    fn test_non_sql_file_no_diags() {
        let checker = DatabricksSqlChecker;
        let config = CheckConfig::default();
        let diags = checker.check_file("schema.yml", "some yaml", None, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_all_diags_have_doc_url() {
        let sql = "OPTIMIZE t; VACUUM t; COPY INTO t FROM 's3://x'; \
                   SELECT ISNULL(a, 0) FROM t";
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
        let sql = "OPTIMIZE t; VACUUM t; COPY INTO t FROM 's3://x'";
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
