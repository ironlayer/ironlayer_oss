//! SQL syntax validation checker — rules SQL001 through SQL009.
//!
//! Uses the lightweight token stream from [`crate::sql_lexer`] to detect
//! structural issues in SQL files without building a full AST. All checks
//! operate on tokens produced by the lexer, ensuring that comment content
//! and string literal content do not produce false positives.
//!
//! | Rule   | Default  | Severity | Description                                        | Fixable |
//! |--------|----------|----------|----------------------------------------------------|---------|
//! | SQL001 | enabled  | error    | Unbalanced parentheses in SQL body                 | no      |
//! | SQL002 | enabled  | error    | Unclosed single-quoted string literal               | no      |
//! | SQL003 | enabled  | error    | Unclosed backtick-quoted identifier                 | no      |
//! | SQL004 | enabled  | warning  | `SELECT *` detected (non-terminal query)           | no      |
//! | SQL005 | enabled  | warning  | Missing `WHERE` clause on `DELETE` statement       | no      |
//! | SQL006 | disabled | info     | SQL body exceeds configured max line count          | no      |
//! | SQL007 | enabled  | warning  | Trailing semicolons in model SQL                   | yes     |
//! | SQL008 | enabled  | error    | Empty SQL body (no SQL after header)               | no      |
//! | SQL009 | disabled | warning  | Tab characters detected (prefer spaces)            | yes     |

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::sql_lexer::{self, Token, TokenKind};
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Default maximum SQL body line count for SQL006.
const DEFAULT_MAX_LINE_COUNT: usize = 500;

/// SQL syntax checker implementing SQL001-SQL009.
pub struct SqlSyntaxChecker;

/// Construct a [`CheckDiagnostic`] for a SQL syntax rule. Every check
/// function uses this to avoid repeating the category and doc URL fields.
#[allow(clippy::too_many_arguments)]
fn make_diag(
    rule_id: &str,
    message: String,
    severity: Severity,
    file_path: &str,
    line: u32,
    column: u32,
    snippet: Option<String>,
    suggestion: Option<String>,
) -> CheckDiagnostic {
    CheckDiagnostic {
        rule_id: rule_id.to_owned(),
        message,
        severity,
        category: CheckCategory::SqlSyntax,
        file_path: file_path.to_owned(),
        line,
        column,
        snippet,
        suggestion,
        doc_url: Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}")),
    }
}

impl Checker for SqlSyntaxChecker {
    fn name(&self) -> &'static str {
        "sql_syntax"
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

        let mut diags = Vec::new();

        check_sql008(file_path, body, &tokens, config, &mut diags);
        check_sql001(file_path, &tokens, config, &mut diags);
        check_sql002(file_path, &tokens, config, &mut diags);
        check_sql003(file_path, &tokens, config, &mut diags);
        check_sql004(file_path, &tokens, config, &mut diags);
        check_sql005(file_path, &tokens, config, &mut diags);
        check_sql006(file_path, body, config, &mut diags);
        check_sql007(file_path, &tokens, content, config, &mut diags);
        check_sql009(file_path, content, config, &mut diags);

        diags
    }
}

// ---------------------------------------------------------------------------
// SQL001: Unbalanced parentheses in SQL body
// ---------------------------------------------------------------------------

/// Count parenthesis depth, reporting the first unmatched paren.
fn check_sql001(
    file_path: &str,
    tokens: &[Token<'_>],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL001", file_path, true) {
        return;
    }

    let mut depth: i32 = 0;
    let mut first_unmatched_close: Option<&Token<'_>> = None;

    for tok in tokens {
        match tok.kind {
            TokenKind::LeftParen => {
                depth += 1;
            }
            TokenKind::RightParen => {
                depth -= 1;
                if depth < 0 && first_unmatched_close.is_none() {
                    first_unmatched_close = Some(tok);
                }
            }
            _ => {}
        }
    }

    let sev = config.effective_severity_for_path("SQL001", file_path, Severity::Error);

    if depth > 0 {
        // More opens than closes — find the first unmatched open via a stack
        let mut stack: Vec<&Token<'_>> = Vec::new();
        for tok in tokens {
            match tok.kind {
                TokenKind::LeftParen => stack.push(tok),
                TokenKind::RightParen => {
                    stack.pop();
                }
                _ => {}
            }
        }
        if let Some(u) = stack.first() {
            diags.push(make_diag(
                "SQL001",
                format!(
                    "Unbalanced parentheses: {} unclosed opening parenthesis(es). \
                     First unmatched '(' is at line {}, column {}.",
                    depth, u.line, u.column
                ),
                sev,
                file_path,
                u.line,
                u.column,
                Some(u.text.to_owned()),
                Some("Add the missing closing parenthesis ')'.".to_owned()),
            ));
        }
    } else if let Some(tok) = first_unmatched_close {
        diags.push(make_diag(
            "SQL001",
            format!(
                "Unbalanced parentheses: extra closing parenthesis ')' \
                 at line {}, column {}.",
                tok.line, tok.column
            ),
            sev,
            file_path,
            tok.line,
            tok.column,
            Some(tok.text.to_owned()),
            Some("Remove the extra closing parenthesis ')' or add a matching '('.".to_owned()),
        ));
    }
}

// ---------------------------------------------------------------------------
// SQL002: Unbalanced single quotes (unclosed string literal)
// ---------------------------------------------------------------------------

/// Detect unclosed single-quoted string literals. The lexer consumes until
/// it finds a closing quote or reaches EOF. An unclosed string token will
/// not end with a single quote.
fn check_sql002(
    file_path: &str,
    tokens: &[Token<'_>],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL002", file_path, true) {
        return;
    }

    let sev = config.effective_severity_for_path("SQL002", file_path, Severity::Error);

    for tok in tokens {
        if tok.kind == TokenKind::StringLiteral && !tok.text.ends_with('\'') {
            diags.push(make_diag(
                "SQL002",
                format!(
                    "Unclosed string literal starting at line {}, column {}. \
                     The single quote is not properly closed.",
                    tok.line, tok.column
                ),
                sev,
                file_path,
                tok.line,
                tok.column,
                Some(truncate_snippet(tok.text, 120)),
                Some("Add a closing single quote (') to terminate the string.".to_owned()),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// SQL003: Unbalanced backtick quotes
// ---------------------------------------------------------------------------

/// Detect unclosed backtick-quoted identifiers. The lexer produces a
/// `QuotedIdent` token. If it starts with `` ` `` but does not end with
/// `` ` `` (or has length < 2), it is unclosed.
fn check_sql003(
    file_path: &str,
    tokens: &[Token<'_>],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL003", file_path, true) {
        return;
    }

    let sev = config.effective_severity_for_path("SQL003", file_path, Severity::Error);

    for tok in tokens {
        if tok.kind == TokenKind::QuotedIdent
            && tok.text.starts_with('`')
            && (!tok.text.ends_with('`') || tok.text.len() < 2)
        {
            diags.push(make_diag(
                "SQL003",
                format!(
                    "Unclosed backtick-quoted identifier starting at line {}, \
                     column {}.",
                    tok.line, tok.column
                ),
                sev,
                file_path,
                tok.line,
                tok.column,
                Some(truncate_snippet(tok.text, 120)),
                Some("Add a closing backtick (`) to terminate the identifier.".to_owned()),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// SQL004: SELECT * detected (non-terminal query)
// ---------------------------------------------------------------------------

/// Detect `SELECT *` usage. Finds `Keyword("SELECT")` followed immediately
/// by `Operator("*")` in the meaningful token stream (skipping whitespace,
/// newlines, comments). Ignores occurrences inside Jinja blocks and avoids
/// false positives on `COUNT(*)` by checking the surrounding context.
fn check_sql004(
    file_path: &str,
    tokens: &[Token<'_>],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL004", file_path, true) {
        return;
    }

    let meaningful = collect_non_jinja_meaningful(tokens);

    for (i, tok) in meaningful.iter().enumerate() {
        if tok.kind == TokenKind::Keyword && tok.text.eq_ignore_ascii_case("SELECT") {
            if let Some(next) = meaningful.get(i + 1) {
                if next.kind == TokenKind::Operator && next.text == "*" {
                    // Avoid false positive on COUNT(*): if the token after
                    // * is ')' and the token two before SELECT is an
                    // aggregate function followed by '(', this is COUNT(*).
                    // Simpler heuristic: if the * is followed by ')' then
                    // it is part of an aggregate like COUNT(*), not SELECT *.
                    let star_followed_by_rparen = meaningful
                        .get(i + 2)
                        .is_some_and(|t| t.kind == TokenKind::RightParen);

                    if !star_followed_by_rparen {
                        diags.push(make_diag(
                            "SQL004",
                            format!(
                                "SELECT * detected at line {}, column {}. \
                                 Explicitly list columns instead of using * \
                                 for better maintainability and contract \
                                 enforcement.",
                                tok.line, tok.column
                            ),
                            config.effective_severity_for_path(
                                "SQL004",
                                file_path,
                                Severity::Warning,
                            ),
                            file_path,
                            tok.line,
                            tok.column,
                            Some("SELECT *".to_owned()),
                            Some("Replace 'SELECT *' with an explicit column list.".to_owned()),
                        ));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SQL005: Missing WHERE clause on DELETE statement
// ---------------------------------------------------------------------------

/// Detect `DELETE FROM` statements that lack a `WHERE` clause. Scans for
/// `Keyword("DELETE")` followed by `Keyword("FROM")`, then checks whether
/// `Keyword("WHERE")` appears before the next statement terminator
/// (`;` or EOF).
fn check_sql005(
    file_path: &str,
    tokens: &[Token<'_>],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL005", file_path, true) {
        return;
    }

    let meaningful = collect_non_jinja_meaningful(tokens);

    let mut i = 0;
    while i + 1 < meaningful.len() {
        let tok = meaningful[i];
        let next = meaningful[i + 1];

        if tok.kind == TokenKind::Keyword
            && tok.text.eq_ignore_ascii_case("DELETE")
            && next.kind == TokenKind::Keyword
            && next.text.eq_ignore_ascii_case("FROM")
        {
            // Scan forward for WHERE before end of statement
            let mut found_where = false;
            let mut j = i + 2;
            while j < meaningful.len() {
                let t = meaningful[j];
                if t.kind == TokenKind::Semicolon {
                    break;
                }
                if t.kind == TokenKind::Keyword && t.text.eq_ignore_ascii_case("WHERE") {
                    found_where = true;
                    break;
                }
                j += 1;
            }

            if !found_where {
                diags.push(make_diag(
                    "SQL005",
                    format!(
                        "DELETE FROM without WHERE clause at line {}, column {}. \
                         This will delete all rows from the table.",
                        tok.line, tok.column
                    ),
                    config.effective_severity_for_path("SQL005", file_path, Severity::Warning),
                    file_path,
                    tok.line,
                    tok.column,
                    Some("DELETE FROM ...".to_owned()),
                    Some("Add a WHERE clause to limit the rows affected.".to_owned()),
                ));
            }
            // Skip past this statement
            i = j;
        } else {
            i += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// SQL006: SQL body exceeds configured max line count (disabled by default)
// ---------------------------------------------------------------------------

/// Check whether the SQL body exceeds the configured maximum line count.
/// Default threshold is 500 lines. This rule is **disabled by default**.
fn check_sql006(
    file_path: &str,
    body: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL006", file_path, false) {
        return;
    }

    let line_count = body.lines().count();
    let max_lines = DEFAULT_MAX_LINE_COUNT;

    if line_count > max_lines {
        diags.push(make_diag(
            "SQL006",
            format!(
                "SQL body is {} lines long (max recommended: {}). \
                 Consider breaking this model into smaller, composable models.",
                line_count, max_lines
            ),
            config.effective_severity_for_path("SQL006", file_path, Severity::Info),
            file_path,
            1,
            0,
            None,
            Some(
                "Split this model into smaller models using {{ ref() }} to \
                 compose them."
                    .to_owned(),
            ),
        ));
    }
}

// ---------------------------------------------------------------------------
// SQL007: Trailing semicolons in model SQL (fixable)
// ---------------------------------------------------------------------------

/// Detect trailing semicolons in model SQL. IronLayer model files should not
/// end with semicolons, as the engine handles statement termination. A
/// trailing semicolon is one where no significant non-semicolon token
/// follows it (only whitespace, newlines, or comments trail it).
fn check_sql007(
    file_path: &str,
    tokens: &[Token<'_>],
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL007", file_path, true) {
        return;
    }

    // Walk backwards from end of token stream to find trailing semicolons.
    // Trailing means: only whitespace, newlines, or comments follow.
    let mut trailing_semis: Vec<&Token<'_>> = Vec::new();
    let mut found_significant = false;

    for tok in tokens.iter().rev() {
        match tok.kind {
            TokenKind::Whitespace
            | TokenKind::Newline
            | TokenKind::LineComment
            | TokenKind::BlockComment => {
                // Skip trivial trailing tokens
            }
            TokenKind::Semicolon => {
                if !found_significant {
                    trailing_semis.push(tok);
                }
            }
            _ => {
                found_significant = true;
            }
        }
        if found_significant {
            break;
        }
    }

    // Compute the header line offset so we can report the correct line
    // in the original file (tokens are based on the body after strip_header).
    let header_line_count = count_header_lines(content);

    let sev = config.effective_severity_for_path("SQL007", file_path, Severity::Warning);

    // Report in file order (reverse of our backward scan)
    for semi_tok in trailing_semis.iter().rev() {
        let actual_line = header_line_count + semi_tok.line;
        diags.push(make_diag(
            "SQL007",
            format!(
                "Trailing semicolon at line {}. IronLayer models should not \
                 end with semicolons — the engine handles statement \
                 termination.",
                actual_line
            ),
            sev,
            file_path,
            actual_line,
            semi_tok.column,
            Some(";".to_owned()),
            Some("Remove the trailing semicolon.".to_owned()),
        ));
    }
}

// ---------------------------------------------------------------------------
// SQL008: Empty SQL body (no SQL after header)
// ---------------------------------------------------------------------------

/// Check for empty SQL body — no SQL content after the header comments.
/// If the body contains only comments, whitespace, or is completely empty,
/// this rule fires.
fn check_sql008(
    file_path: &str,
    body: &str,
    tokens: &[Token<'_>],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL008", file_path, true) {
        return;
    }

    let sev = config.effective_severity_for_path("SQL008", file_path, Severity::Error);
    let suggestion = "Add a SQL SELECT statement after the header comments.".to_owned();
    let trimmed_body = body.trim();

    // Completely empty body after header
    if trimmed_body.is_empty() {
        diags.push(make_diag(
            "SQL008",
            "Empty SQL body — no SQL statements found after the header block.".to_owned(),
            sev,
            file_path,
            1,
            0,
            None,
            Some(suggestion),
        ));
        return;
    }

    // Body exists but may contain only comments — check for meaningful tokens
    let meaningful = sql_lexer::meaningful_tokens(tokens);
    let has_sql = meaningful.iter().any(|t| {
        !matches!(
            t.kind,
            TokenKind::JinjaOpen
                | TokenKind::JinjaClose
                | TokenKind::JinjaBlock
                | TokenKind::Semicolon
                | TokenKind::Unknown
        )
    });

    if !has_sql {
        diags.push(make_diag(
            "SQL008",
            "SQL body contains no meaningful SQL statements (only comments or whitespace)."
                .to_owned(),
            sev,
            file_path,
            1,
            0,
            None,
            Some(suggestion),
        ));
    }
}

// ---------------------------------------------------------------------------
// SQL009: Tab characters detected (fixable, disabled by default)
// ---------------------------------------------------------------------------

/// Detect tab characters in the SQL content. Reports one diagnostic per
/// line that contains tabs. This rule is **disabled by default**.
fn check_sql009(
    file_path: &str,
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("SQL009", file_path, false) {
        return;
    }

    let sev = config.effective_severity_for_path("SQL009", file_path, Severity::Warning);

    for (line_idx, line) in content.lines().enumerate() {
        if let Some(col) = line.find('\t') {
            let tab_count = line.chars().filter(|c| *c == '\t').count();
            diags.push(make_diag(
                "SQL009",
                format!(
                    "Tab character(s) detected at line {} ({} tab(s)). \
                     Use spaces for consistent indentation.",
                    line_idx + 1,
                    tab_count,
                ),
                sev,
                file_path,
                (line_idx + 1) as u32,
                (col + 1) as u32,
                Some(truncate_snippet(line, 120)),
                Some("Replace tab characters with 4 spaces.".to_owned()),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect meaningful tokens that are NOT inside Jinja `{{ ... }}` blocks.
/// Filters out whitespace, newlines, comments, and all Jinja tokens.
fn collect_non_jinja_meaningful<'a>(tokens: &'a [Token<'a>]) -> Vec<&'a Token<'a>> {
    let mut result = Vec::new();
    let mut jinja_depth: u32 = 0;

    for tok in tokens {
        match tok.kind {
            TokenKind::JinjaOpen => {
                jinja_depth += 1;
            }
            TokenKind::JinjaClose => {
                jinja_depth = jinja_depth.saturating_sub(1);
            }
            TokenKind::JinjaBlock
            | TokenKind::Whitespace
            | TokenKind::Newline
            | TokenKind::LineComment
            | TokenKind::BlockComment => {}
            _ => {
                if jinja_depth == 0 {
                    result.push(tok);
                }
            }
        }
    }

    result
}

/// Count the number of header lines (lines before the body starts).
/// The header block ends at the first line that is both non-empty AND
/// not a comment.
fn count_header_lines(content: &str) -> u32 {
    let mut count: u32 = 0;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            count += 1;
        } else {
            break;
        }
    }
    count
}

/// Truncate a text snippet to a max length, appending "..." if truncated.
fn truncate_snippet(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_owned()
    } else {
        // Find a safe char boundary
        let boundary = text
            .char_indices()
            .take_while(|(i, _)| *i < max_len.saturating_sub(3))
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        format!("{}...", &text[..boundary])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckConfig;

    fn default_config() -> CheckConfig {
        CheckConfig::default()
    }

    fn check(content: &str) -> Vec<CheckDiagnostic> {
        let checker = SqlSyntaxChecker;
        checker.check_file("test.sql", content, None, &default_config())
    }

    fn check_with_config(content: &str, config: &CheckConfig) -> Vec<CheckDiagnostic> {
        let checker = SqlSyntaxChecker;
        checker.check_file("test.sql", content, None, config)
    }

    fn by_rule<'a>(diags: &'a [CheckDiagnostic], rule: &str) -> Vec<&'a CheckDiagnostic> {
        diags.iter().filter(|d| d.rule_id == rule).collect()
    }

    fn sql006_config() -> CheckConfig {
        let mut config = default_config();
        config.rules.insert(
            "SQL006".to_owned(),
            crate::config::RuleSeverityOverride::Info,
        );
        config
    }

    fn sql009_config() -> CheckConfig {
        let mut config = default_config();
        config.rules.insert(
            "SQL009".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        config
    }

    // ── Non-SQL file ─────────────────────────────────────────────────────

    #[test]
    fn test_non_sql_file_ignored() {
        let checker = SqlSyntaxChecker;
        let diags = checker.check_file("schema.yml", "SELECT (", None, &default_config());
        assert!(diags.is_empty());
    }

    #[test]
    fn test_checker_name() {
        assert_eq!(SqlSyntaxChecker.name(), "sql_syntax");
    }

    // ── SQL001: Unbalanced parentheses ───────────────────────────────────

    #[test]
    fn test_sql001_balanced_parens() {
        let diags = check("SELECT (a + b) FROM t");
        assert!(by_rule(&diags, "SQL001").is_empty());
    }

    #[test]
    fn test_sql001_unclosed_left_paren() {
        let diags = check("SELECT (a + b FROM t");
        let hits = by_rule(&diags, "SQL001");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].message.contains("unclosed opening"));
        assert_eq!(hits[0].severity, Severity::Error);
    }

    #[test]
    fn test_sql001_extra_close_paren() {
        let diags = check("SELECT a + b) FROM t");
        let hits = by_rule(&diags, "SQL001");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].message.contains("extra closing"));
        assert_eq!(hits[0].severity, Severity::Error);
    }

    #[test]
    fn test_sql001_nested_balanced() {
        let diags = check("SELECT ((a + b) * (c + d)) FROM t");
        assert!(by_rule(&diags, "SQL001").is_empty());
    }

    #[test]
    fn test_sql001_deeply_nested() {
        let diags = check("SELECT COALESCE(NULLIF(a, ''), b, (c + d))");
        assert!(by_rule(&diags, "SQL001").is_empty());
    }

    #[test]
    fn test_sql001_multiple_unclosed() {
        let diags = check("SELECT ((a + b");
        let hits = by_rule(&diags, "SQL001");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].message.contains("2 unclosed"));
    }

    #[test]
    fn test_sql001_no_parens_at_all() {
        let diags = check("SELECT 1 FROM t");
        assert!(by_rule(&diags, "SQL001").is_empty());
    }

    #[test]
    fn test_sql001_disabled_via_config() {
        let mut config = default_config();
        config.rules.insert(
            "SQL001".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );
        let diags = check_with_config("SELECT (a FROM t", &config);
        assert!(by_rule(&diags, "SQL001").is_empty());
    }

    #[test]
    fn test_sql001_has_doc_url() {
        let diags = check("SELECT (a FROM t");
        let hits = by_rule(&diags, "SQL001");
        assert!(hits[0].doc_url.as_deref().unwrap().contains("SQL001"));
    }

    #[test]
    fn test_sql001_category() {
        let diags = check("SELECT (a FROM t");
        let hits = by_rule(&diags, "SQL001");
        assert_eq!(hits[0].category, CheckCategory::SqlSyntax);
    }

    // ── SQL002: Unclosed string literal ──────────────────────────────────

    #[test]
    fn test_sql002_balanced_strings() {
        let diags = check("SELECT 'hello' FROM t");
        assert!(by_rule(&diags, "SQL002").is_empty());
    }

    #[test]
    fn test_sql002_escaped_quotes() {
        let diags = check("SELECT 'it''s fine' FROM t");
        assert!(by_rule(&diags, "SQL002").is_empty());
    }

    #[test]
    fn test_sql002_unclosed_at_eof() {
        let diags = check("SELECT 'unclosed string");
        let hits = by_rule(&diags, "SQL002");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Error);
        assert!(hits[0].message.contains("Unclosed string literal"));
    }

    #[test]
    fn test_sql002_empty_string_ok() {
        let diags = check("SELECT '' FROM t");
        assert!(by_rule(&diags, "SQL002").is_empty());
    }

    #[test]
    fn test_sql002_multiple_valid_strings() {
        let diags = check("SELECT 'a', 'b', 'c' FROM t");
        assert!(by_rule(&diags, "SQL002").is_empty());
    }

    #[test]
    fn test_sql002_multiline_string_closed() {
        let diags = check("SELECT 'line1\nline2' FROM t");
        assert!(by_rule(&diags, "SQL002").is_empty());
    }

    #[test]
    fn test_sql002_has_suggestion() {
        let diags = check("SELECT 'unclosed");
        let hits = by_rule(&diags, "SQL002");
        assert!(hits[0].suggestion.is_some());
    }

    // ── SQL003: Unclosed backtick-quoted identifier ──────────────────────

    #[test]
    fn test_sql003_balanced_backticks() {
        let diags = check("SELECT `my column` FROM t");
        assert!(by_rule(&diags, "SQL003").is_empty());
    }

    #[test]
    fn test_sql003_unclosed_backtick() {
        let diags = check("SELECT `unclosed column");
        let hits = by_rule(&diags, "SQL003");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Error);
    }

    #[test]
    fn test_sql003_double_quoted_no_false_positive() {
        // Double-quoted idents are NOT backtick idents
        let diags = check("SELECT \"my column\" FROM t");
        assert!(by_rule(&diags, "SQL003").is_empty());
    }

    #[test]
    fn test_sql003_single_backtick_unclosed() {
        let diags = check("SELECT `");
        let hits = by_rule(&diags, "SQL003");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql003_multiple_valid() {
        let diags = check("SELECT `a`, `b`, `c` FROM `t`");
        assert!(by_rule(&diags, "SQL003").is_empty());
    }

    #[test]
    fn test_sql003_has_doc_url() {
        let diags = check("SELECT `unclosed");
        let hits = by_rule(&diags, "SQL003");
        assert!(hits[0].doc_url.as_deref().unwrap().contains("SQL003"));
    }

    // ── SQL004: SELECT * detected ────────────────────────────────────────

    #[test]
    fn test_sql004_select_star_fires() {
        let diags = check("SELECT * FROM t");
        let hits = by_rule(&diags, "SQL004");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Warning);
        assert!(hits[0].message.contains("SELECT *"));
    }

    #[test]
    fn test_sql004_select_columns_pass() {
        let diags = check("SELECT a, b FROM t");
        assert!(by_rule(&diags, "SQL004").is_empty());
    }

    #[test]
    fn test_sql004_select_star_inside_cte() {
        let sql = "WITH base AS (\n  SELECT * FROM raw\n)\nSELECT id FROM base";
        let diags = check(sql);
        let hits = by_rule(&diags, "SQL004");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql004_count_star_no_false_positive() {
        let diags = check("SELECT COUNT(*) FROM orders");
        assert!(by_rule(&diags, "SQL004").is_empty());
    }

    #[test]
    fn test_sql004_lowercase() {
        let diags = check("select * from orders");
        let hits = by_rule(&diags, "SQL004");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql004_star_in_multiplication_no_fire() {
        let diags = check("SELECT price * quantity FROM orders");
        assert!(by_rule(&diags, "SQL004").is_empty());
    }

    #[test]
    fn test_sql004_select_star_after_whitespace() {
        let diags = check("SELECT   *   FROM orders");
        let hits = by_rule(&diags, "SQL004");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql004_disabled_via_config() {
        let mut config = default_config();
        config.rules.insert(
            "SQL004".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );
        let diags = check_with_config("SELECT * FROM t", &config);
        assert!(by_rule(&diags, "SQL004").is_empty());
    }

    #[test]
    fn test_sql004_inside_jinja_no_fire() {
        // Inside {{ }}, the tokens are within jinja depth > 0
        let diags = check("{{ config(SELECT *) }}\nSELECT id FROM t");
        let hits = by_rule(&diags, "SQL004");
        assert!(hits.is_empty());
    }

    #[test]
    fn test_sql004_multiple_select_stars() {
        let sql = "SELECT * FROM t UNION ALL SELECT * FROM u";
        let diags = check(sql);
        let hits = by_rule(&diags, "SQL004");
        assert_eq!(hits.len(), 2);
    }

    // ── SQL005: DELETE FROM without WHERE ─────────────────────────────────

    #[test]
    fn test_sql005_delete_without_where() {
        let diags = check("DELETE FROM t");
        let hits = by_rule(&diags, "SQL005");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Warning);
        assert!(hits[0].message.contains("DELETE FROM without WHERE"));
    }

    #[test]
    fn test_sql005_delete_with_where_pass() {
        let diags = check("DELETE FROM t WHERE id = 1");
        assert!(by_rule(&diags, "SQL005").is_empty());
    }

    #[test]
    fn test_sql005_lowercase() {
        let diags = check("delete from orders");
        let hits = by_rule(&diags, "SQL005");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql005_delete_semicolon_then_more() {
        let diags = check("DELETE FROM a; SELECT 1 FROM b");
        let hits = by_rule(&diags, "SQL005");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql005_two_deletes_one_missing() {
        let diags = check("DELETE FROM a; DELETE FROM b WHERE id = 1");
        let hits = by_rule(&diags, "SQL005");
        // Only the first DELETE lacks WHERE
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql005_select_only_no_fire() {
        let diags = check("SELECT 1 FROM t");
        assert!(by_rule(&diags, "SQL005").is_empty());
    }

    #[test]
    fn test_sql005_has_suggestion() {
        let diags = check("DELETE FROM t");
        let hits = by_rule(&diags, "SQL005");
        assert!(hits[0].suggestion.is_some());
    }

    // ── SQL006: Max line count (disabled by default) ─────────────────────

    #[test]
    fn test_sql006_disabled_by_default() {
        let long_sql = "SELECT\n".repeat(600);
        let diags = check(&long_sql);
        assert!(by_rule(&diags, "SQL006").is_empty());
    }

    #[test]
    fn test_sql006_enabled_short_file_pass() {
        let config = sql006_config();
        let diags = check_with_config("SELECT 1\nFROM t", &config);
        assert!(by_rule(&diags, "SQL006").is_empty());
    }

    #[test]
    fn test_sql006_enabled_long_file_fires() {
        let config = sql006_config();
        let long_sql = "SELECT\n".repeat(600);
        let diags = check_with_config(&long_sql, &config);
        let hits = by_rule(&diags, "SQL006");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Info);
        assert!(hits[0].message.contains("600"));
    }

    #[test]
    fn test_sql006_exactly_500_lines_pass() {
        let config = sql006_config();
        let sql = (0..500)
            .map(|i| format!("SELECT {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let diags = check_with_config(&sql, &config);
        assert!(by_rule(&diags, "SQL006").is_empty());
    }

    #[test]
    fn test_sql006_501_lines_fires() {
        let config = sql006_config();
        let sql = (0..501)
            .map(|i| format!("SELECT {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let diags = check_with_config(&sql, &config);
        let hits = by_rule(&diags, "SQL006");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql006_header_excluded_from_count() {
        // 10 header lines + 495 body lines = 505 total but only 495 body
        let config = sql006_config();
        let header = "-- name: test\n".repeat(10);
        let body = (0..495)
            .map(|i| format!("SELECT {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let sql = format!("{header}{body}");
        let diags = check_with_config(&sql, &config);
        assert!(by_rule(&diags, "SQL006").is_empty());
    }

    // ── SQL007: Trailing semicolons ──────────────────────────────────────

    #[test]
    fn test_sql007_trailing_semicolon_fires() {
        let diags = check("SELECT 1;");
        let hits = by_rule(&diags, "SQL007");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Warning);
        assert!(hits[0].message.contains("Trailing semicolon"));
    }

    #[test]
    fn test_sql007_no_semicolon_pass() {
        let diags = check("SELECT 1");
        assert!(by_rule(&diags, "SQL007").is_empty());
    }

    #[test]
    fn test_sql007_semicolon_inside_string_no_fire() {
        let diags = check("SELECT 'a;b' FROM t");
        assert!(by_rule(&diags, "SQL007").is_empty());
    }

    #[test]
    fn test_sql007_mid_statement_semicolon_no_fire() {
        // Semicolons between statements are not trailing
        let diags = check("SELECT 1; SELECT 2");
        assert!(by_rule(&diags, "SQL007").is_empty());
    }

    #[test]
    fn test_sql007_trailing_with_whitespace_after() {
        let diags = check("SELECT 1;\n  \n");
        let hits = by_rule(&diags, "SQL007");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql007_trailing_with_comment_after() {
        let diags = check("SELECT 1;\n-- end of file");
        let hits = by_rule(&diags, "SQL007");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql007_multiple_trailing_semicolons() {
        let diags = check("SELECT 1;;");
        let hits = by_rule(&diags, "SQL007");
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn test_sql007_with_header_reports_correct_line() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1;";
        let diags = check(sql);
        let hits = by_rule(&diags, "SQL007");
        assert_eq!(hits.len(), 1);
        // Header has 2 lines, body line 1 => actual line 3
        assert_eq!(hits[0].line, 3);
    }

    // ── SQL008: Empty SQL body ───────────────────────────────────────────

    #[test]
    fn test_sql008_empty_body_with_header() {
        let diags = check("-- name: test\n-- kind: FULL_REFRESH\n");
        let hits = by_rule(&diags, "SQL008");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Error);
    }

    #[test]
    fn test_sql008_has_body_pass() {
        let diags = check("-- name: test\n-- kind: FULL_REFRESH\nSELECT 1");
        assert!(by_rule(&diags, "SQL008").is_empty());
    }

    #[test]
    fn test_sql008_completely_empty_file() {
        let diags = check("");
        let hits = by_rule(&diags, "SQL008");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql008_whitespace_only() {
        let diags = check("   \n  \n  ");
        let hits = by_rule(&diags, "SQL008");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql008_comments_only_body() {
        // After the header, the body is just a comment
        let sql = "-- header comment\n-- another comment";
        let diags = check(sql);
        let hits = by_rule(&diags, "SQL008");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql008_just_select_pass() {
        let diags = check("SELECT 1");
        assert!(by_rule(&diags, "SQL008").is_empty());
    }

    #[test]
    fn test_sql008_has_suggestion() {
        let diags = check("");
        let hits = by_rule(&diags, "SQL008");
        assert!(hits[0].suggestion.is_some());
    }

    // ── SQL009: Tab characters (disabled by default) ─────────────────────

    #[test]
    fn test_sql009_disabled_by_default() {
        let diags = check("\tSELECT 1");
        assert!(by_rule(&diags, "SQL009").is_empty());
    }

    #[test]
    fn test_sql009_enabled_detects_tabs() {
        let config = sql009_config();
        let diags = check_with_config("\tSELECT 1", &config);
        let hits = by_rule(&diags, "SQL009");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].severity, Severity::Warning);
        assert!(hits[0].message.contains("Tab character"));
    }

    #[test]
    fn test_sql009_spaces_pass() {
        let config = sql009_config();
        let diags = check_with_config("    SELECT 1", &config);
        assert!(by_rule(&diags, "SQL009").is_empty());
    }

    #[test]
    fn test_sql009_multiple_lines_with_tabs() {
        let config = sql009_config();
        let diags = check_with_config("SELECT\t1\nFROM\tt", &config);
        let hits = by_rule(&diags, "SQL009");
        // Should report one diagnostic per line
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn test_sql009_tab_in_string_still_fires() {
        // SQL009 operates on raw content lines, not tokens. A tab in a
        // string literal line will still be detected. This is intentional
        // because --fix replaces tabs file-wide.
        let config = sql009_config();
        let diags = check_with_config("SELECT 'tab\there' FROM t", &config);
        let hits = by_rule(&diags, "SQL009");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_sql009_multiple_tabs_single_line() {
        let config = sql009_config();
        let diags = check_with_config("\t\tSELECT 1", &config);
        let hits = by_rule(&diags, "SQL009");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].message.contains("2 tab(s)"));
    }

    #[test]
    fn test_sql009_has_suggestion() {
        let config = sql009_config();
        let diags = check_with_config("\tSELECT 1", &config);
        let hits = by_rule(&diags, "SQL009");
        assert!(hits[0].suggestion.as_deref().unwrap().contains("4 spaces"));
    }

    // ── Cross-rule / integration tests ───────────────────────────────────

    #[test]
    fn test_header_plus_valid_sql_no_syntax_errors() {
        let sql = "-- name: my_model\n-- kind: FULL_REFRESH\n\nSELECT id, name\nFROM orders";
        let diags = check(sql);
        let syntax: Vec<_> = diags
            .iter()
            .filter(|d| d.rule_id.starts_with("SQL"))
            .collect();
        assert!(syntax.is_empty());
    }

    #[test]
    fn test_all_diagnostics_have_category_sql_syntax() {
        let diags = check("SELECT (a FROM t");
        for d in &diags {
            if d.rule_id.starts_with("SQL") {
                assert_eq!(d.category, CheckCategory::SqlSyntax);
            }
        }
    }

    #[test]
    fn test_truncate_snippet_short() {
        assert_eq!(truncate_snippet("short", 80), "short");
    }

    #[test]
    fn test_truncate_snippet_long() {
        let long = "a".repeat(200);
        let truncated = truncate_snippet(&long, 20);
        assert!(truncated.len() <= 20);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn test_count_header_lines_with_header() {
        assert_eq!(count_header_lines("-- name: a\n-- kind: b\nSELECT 1"), 2);
    }

    #[test]
    fn test_count_header_lines_no_header() {
        assert_eq!(count_header_lines("SELECT 1"), 0);
    }
}
