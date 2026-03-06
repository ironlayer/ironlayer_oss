//! Lightweight SQL tokenizer for the IronLayer Check Engine.
//!
//! Produces a stream of [`Token`]s from SQL source text. Designed for
//! validation checks (bracket balance, safety keyword detection, etc.),
//! NOT for full AST construction.
//!
//! The lexer uses zero-copy `&str` slices into the source, tracks
//! 1-based line/column positions, and correctly handles:
//!
//! - SQL keywords (case-insensitive matching)
//! - Identifiers (including `$` for Databricks)
//! - Quoted identifiers: `` `backtick` `` and `"double-quoted"`
//! - String literals with `''` escape sequences
//! - Number literals (integer, decimal, scientific notation)
//! - All operators: `=`, `<>`, `!=`, `>=`, `<=`, `+`, `-`, `*`, `/`, `%`
//! - Punctuation: `(`, `)`, `,`, `;`, `.`
//! - Line comments: `-- ...`
//! - Block comments with nesting: `/* ... /* ... */ ... */`
//! - Jinja templates: `{{ ... }}`, `{% ... %}`, `{# ... #}`
//! - Whitespace (spaces, tabs) and newlines
//! - Unicode identifiers

/// A token kind produced by the SQL lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// SQL keyword: SELECT, FROM, WHERE, INSERT, DELETE, DROP, etc.
    Keyword,
    /// Unquoted identifier: table_name, column_name.
    Identifier,
    /// Quoted identifier: backtick-quoted or double-quoted.
    QuotedIdent,
    /// Single-quoted string literal (handles `''` escaping).
    StringLiteral,
    /// Numeric literal: integer, decimal, or scientific notation.
    NumberLiteral,
    /// Operator: `=`, `<>`, `!=`, `>=`, `<=`, `+`, `-`, `*`, `/`, `%`.
    Operator,
    /// Left parenthesis `(`.
    LeftParen,
    /// Right parenthesis `)`.
    RightParen,
    /// Comma `,`.
    Comma,
    /// Semicolon `;`.
    Semicolon,
    /// Dot `.`.
    Dot,
    /// Line comment `-- ...`.
    LineComment,
    /// Block comment `/* ... */` (supports nesting).
    BlockComment,
    /// Jinja open template `{{`.
    JinjaOpen,
    /// Jinja close template `}}`.
    JinjaClose,
    /// Jinja block `{% ... %}` or Jinja comment `{# ... #}`.
    JinjaBlock,
    /// Whitespace (spaces, tabs — not newlines).
    Whitespace,
    /// Newline character(s) (`\n` or `\r\n`).
    Newline,
    /// Unrecognized character.
    Unknown,
}

/// A single token with its kind, text slice, and position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token<'a> {
    /// The kind of token.
    pub kind: TokenKind,
    /// Zero-copy slice into the original source text.
    pub text: &'a str,
    /// Byte offset in the source.
    pub offset: usize,
    /// 1-based line number.
    pub line: u32,
    /// 1-based column number.
    pub column: u32,
}

/// SQL keywords recognized by the lexer (uppercase for case-insensitive match).
const SQL_KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BOOLEAN",
    "BY",
    "CASCADE",
    "CASE",
    "CAST",
    "CHAR",
    "CHECK",
    "CLUSTER",
    "COLUMN",
    "COMMENT",
    "COMMIT",
    "CONSTRAINT",
    "COPY",
    "COUNT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "DATABASE",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DISTINCT",
    "DISTRIBUTE",
    "DIV",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXCHANGE",
    "EXISTS",
    "EXPLAIN",
    "EXTENDED",
    "EXTERNAL",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FOR",
    "FORMAT",
    "FROM",
    "FULL",
    "FUNCTION",
    "GLOBAL",
    "GRANT",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IF",
    "IN",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LATERAL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LONG",
    "MACRO",
    "MAP",
    "MATCHED",
    "MERGE",
    "MINUS",
    "NATURAL",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OPTIMIZE",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "OVERWRITE",
    "PARTITION",
    "PERCENT",
    "PIVOT",
    "PRIMARY",
    "RANGE",
    "REBUILD",
    "REGEXP",
    "RENAME",
    "REPLACE",
    "RESET",
    "RESTRICT",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROLE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SCHEMA",
    "SELECT",
    "SEMI",
    "SET",
    "SHOW",
    "SMALLINT",
    "SOME",
    "SORT",
    "START",
    "STRING",
    "STRUCT",
    "SYNC",
    "TABLE",
    "TABLESAMPLE",
    "TEMP",
    "TEMPORARY",
    "THEN",
    "TIMESTAMP",
    "TINYINT",
    "TO",
    "TRANSFORM",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "UNBOUNDED",
    "UNCACHE",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNPIVOT",
    "UNSET",
    "UPDATE",
    "USE",
    "USER",
    "USING",
    "VACUUM",
    "VALUES",
    "VARCHAR",
    "VIEW",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

/// Check whether an identifier string is a SQL keyword (case-insensitive).
fn is_keyword(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    SQL_KEYWORDS.binary_search(&upper.as_str()).is_ok()
}

/// Tokenize a SQL source string into a vector of [`Token`]s.
///
/// Handles all 19 [`TokenKind`] variants, Jinja templates, nested block
/// comments, and string escape sequences.
#[must_use]
pub fn tokenize(source: &str) -> Vec<Token<'_>> {
    let mut tokens = Vec::new();
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut pos: usize = 0;
    let mut line: u32 = 1;
    let mut col: u32 = 1;

    while pos < len {
        let start = pos;
        let start_line = line;
        let start_col = col;
        let ch = bytes[pos];

        // ---------------------------------------------------------------
        // Newline: \r\n or \n
        // ---------------------------------------------------------------
        if ch == b'\n' {
            pos += 1;
            tokens.push(Token {
                kind: TokenKind::Newline,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            line += 1;
            col = 1;
            continue;
        }
        if ch == b'\r' {
            if pos + 1 < len && bytes[pos + 1] == b'\n' {
                pos += 2;
            } else {
                pos += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Newline,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            line += 1;
            col = 1;
            continue;
        }

        // ---------------------------------------------------------------
        // Whitespace: spaces and tabs (not newlines)
        // ---------------------------------------------------------------
        if ch == b' ' || ch == b'\t' {
            while pos < len && (bytes[pos] == b' ' || bytes[pos] == b'\t') {
                pos += 1;
                col += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Whitespace,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Line comment: -- ...
        // ---------------------------------------------------------------
        if ch == b'-' && pos + 1 < len && bytes[pos + 1] == b'-' {
            while pos < len && bytes[pos] != b'\n' && bytes[pos] != b'\r' {
                pos += 1;
            }
            let text = &source[start..pos];
            col += (pos - start) as u32;
            tokens.push(Token {
                kind: TokenKind::LineComment,
                text,
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Block comment: /* ... */ with nesting support
        // ---------------------------------------------------------------
        if ch == b'/' && pos + 1 < len && bytes[pos + 1] == b'*' {
            pos += 2;
            col += 2;
            let mut depth: u32 = 1;
            while pos < len && depth > 0 {
                if bytes[pos] == b'/' && pos + 1 < len && bytes[pos + 1] == b'*' {
                    depth += 1;
                    pos += 2;
                    col += 2;
                } else if bytes[pos] == b'*' && pos + 1 < len && bytes[pos + 1] == b'/' {
                    depth -= 1;
                    pos += 2;
                    col += 2;
                } else if bytes[pos] == b'\n' {
                    pos += 1;
                    line += 1;
                    col = 1;
                } else if bytes[pos] == b'\r' {
                    pos += 1;
                    if pos < len && bytes[pos] == b'\n' {
                        pos += 1;
                    }
                    line += 1;
                    col = 1;
                } else {
                    pos += 1;
                    col += 1;
                }
            }
            tokens.push(Token {
                kind: TokenKind::BlockComment,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Jinja templates: {{ ... }}, {% ... %}, {# ... #}
        // ---------------------------------------------------------------
        if ch == b'{' && pos + 1 < len {
            let next = bytes[pos + 1];

            // {{ ... }}
            if next == b'{' {
                pos += 2;
                col += 2;
                tokens.push(Token {
                    kind: TokenKind::JinjaOpen,
                    text: &source[start..pos],
                    offset: start,
                    line: start_line,
                    column: start_col,
                });
                continue;
            }

            // {% ... %} — consume to closing %}
            if next == b'%' {
                pos += 2;
                col += 2;
                while pos < len {
                    if bytes[pos] == b'%' && pos + 1 < len && bytes[pos + 1] == b'}' {
                        pos += 2;
                        col += 2;
                        break;
                    }
                    if bytes[pos] == b'\n' {
                        line += 1;
                        col = 1;
                        pos += 1;
                    } else if bytes[pos] == b'\r' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'\n' {
                            pos += 1;
                        }
                        line += 1;
                        col = 1;
                    } else {
                        pos += 1;
                        col += 1;
                    }
                }
                tokens.push(Token {
                    kind: TokenKind::JinjaBlock,
                    text: &source[start..pos],
                    offset: start,
                    line: start_line,
                    column: start_col,
                });
                continue;
            }

            // {# ... #} — Jinja comment
            if next == b'#' {
                pos += 2;
                col += 2;
                while pos < len {
                    if bytes[pos] == b'#' && pos + 1 < len && bytes[pos + 1] == b'}' {
                        pos += 2;
                        col += 2;
                        break;
                    }
                    if bytes[pos] == b'\n' {
                        line += 1;
                        col = 1;
                        pos += 1;
                    } else if bytes[pos] == b'\r' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'\n' {
                            pos += 1;
                        }
                        line += 1;
                        col = 1;
                    } else {
                        pos += 1;
                        col += 1;
                    }
                }
                tokens.push(Token {
                    kind: TokenKind::JinjaBlock,
                    text: &source[start..pos],
                    offset: start,
                    line: start_line,
                    column: start_col,
                });
                continue;
            }
        }

        // }} — Jinja close
        if ch == b'}' && pos + 1 < len && bytes[pos + 1] == b'}' {
            pos += 2;
            col += 2;
            tokens.push(Token {
                kind: TokenKind::JinjaClose,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // String literal: 'single quoted' with '' escape
        // ---------------------------------------------------------------
        if ch == b'\'' {
            pos += 1;
            col += 1;
            while pos < len {
                if bytes[pos] == b'\'' {
                    pos += 1;
                    col += 1;
                    // Escaped quote ''
                    if pos < len && bytes[pos] == b'\'' {
                        pos += 1;
                        col += 1;
                        continue;
                    }
                    break;
                }
                if bytes[pos] == b'\n' {
                    line += 1;
                    col = 1;
                    pos += 1;
                } else if bytes[pos] == b'\r' {
                    pos += 1;
                    if pos < len && bytes[pos] == b'\n' {
                        pos += 1;
                    }
                    line += 1;
                    col = 1;
                } else {
                    pos += 1;
                    col += 1;
                }
            }
            tokens.push(Token {
                kind: TokenKind::StringLiteral,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Quoted identifiers: `backtick` or "double-quoted"
        // ---------------------------------------------------------------
        if ch == b'`' {
            pos += 1;
            col += 1;
            while pos < len && bytes[pos] != b'`' {
                if bytes[pos] == b'\n' {
                    line += 1;
                    col = 1;
                } else {
                    col += 1;
                }
                pos += 1;
            }
            if pos < len {
                pos += 1; // closing backtick
                col += 1;
            }
            tokens.push(Token {
                kind: TokenKind::QuotedIdent,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b'"' {
            pos += 1;
            col += 1;
            while pos < len && bytes[pos] != b'"' {
                if bytes[pos] == b'\n' {
                    line += 1;
                    col = 1;
                } else {
                    col += 1;
                }
                pos += 1;
            }
            if pos < len {
                pos += 1; // closing quote
                col += 1;
            }
            tokens.push(Token {
                kind: TokenKind::QuotedIdent,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Number literals: digits, optional decimal point, optional exponent
        // ---------------------------------------------------------------
        if ch.is_ascii_digit() {
            while pos < len && bytes[pos].is_ascii_digit() {
                pos += 1;
                col += 1;
            }
            // Decimal part
            if pos < len && bytes[pos] == b'.' && pos + 1 < len && bytes[pos + 1].is_ascii_digit() {
                pos += 1; // dot
                col += 1;
                while pos < len && bytes[pos].is_ascii_digit() {
                    pos += 1;
                    col += 1;
                }
            }
            // Exponent part
            if pos < len && (bytes[pos] == b'e' || bytes[pos] == b'E') {
                pos += 1;
                col += 1;
                if pos < len && (bytes[pos] == b'+' || bytes[pos] == b'-') {
                    pos += 1;
                    col += 1;
                }
                while pos < len && bytes[pos].is_ascii_digit() {
                    pos += 1;
                    col += 1;
                }
            }
            tokens.push(Token {
                kind: TokenKind::NumberLiteral,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Identifiers / Keywords: letters, digits, underscore, $ (Databricks)
        // ---------------------------------------------------------------
        if ch.is_ascii_alphabetic() || ch == b'_' || ch == b'$' || ch > 127 {
            // For multi-byte UTF-8, use char-aware iteration
            if ch > 127 {
                let rest = &source[pos..];
                let first_char = rest.chars().next().expect("non-empty source after pos");
                pos += first_char.len_utf8();
                col += 1;
                while pos < len {
                    let byte = bytes[pos];
                    if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' {
                        pos += 1;
                        col += 1;
                    } else if byte > 127 {
                        let rest2 = &source[pos..];
                        if let Some(c) = rest2.chars().next() {
                            if c.is_alphanumeric() || c == '_' {
                                pos += c.len_utf8();
                                col += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            } else {
                pos += 1;
                col += 1;
                while pos < len
                    && (bytes[pos].is_ascii_alphanumeric()
                        || bytes[pos] == b'_'
                        || bytes[pos] == b'$'
                        || bytes[pos] > 127)
                {
                    if bytes[pos] > 127 {
                        // Multi-byte char in identifier
                        let rest = &source[pos..];
                        if let Some(c) = rest.chars().next() {
                            if c.is_alphanumeric() || c == '_' {
                                pos += c.len_utf8();
                                col += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        pos += 1;
                        col += 1;
                    }
                }
            }

            let text = &source[start..pos];
            let kind = if is_keyword(text) {
                TokenKind::Keyword
            } else {
                TokenKind::Identifier
            };
            tokens.push(Token {
                kind,
                text,
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Punctuation
        // ---------------------------------------------------------------
        if ch == b'(' {
            pos += 1;
            col += 1;
            tokens.push(Token {
                kind: TokenKind::LeftParen,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b')' {
            pos += 1;
            col += 1;
            tokens.push(Token {
                kind: TokenKind::RightParen,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b',' {
            pos += 1;
            col += 1;
            tokens.push(Token {
                kind: TokenKind::Comma,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b';' {
            pos += 1;
            col += 1;
            tokens.push(Token {
                kind: TokenKind::Semicolon,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b'.' {
            pos += 1;
            col += 1;
            tokens.push(Token {
                kind: TokenKind::Dot,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Operators: multi-char first, then single-char
        // ---------------------------------------------------------------
        if ch == b'<' {
            pos += 1;
            col += 1;
            if pos < len && (bytes[pos] == b'>' || bytes[pos] == b'=') {
                pos += 1;
                col += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Operator,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b'>' {
            pos += 1;
            col += 1;
            if pos < len && bytes[pos] == b'=' {
                pos += 1;
                col += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Operator,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b'!' && pos + 1 < len && bytes[pos + 1] == b'=' {
            pos += 2;
            col += 2;
            tokens.push(Token {
                kind: TokenKind::Operator,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }
        if ch == b'=' || ch == b'+' || ch == b'-' || ch == b'*' || ch == b'/' || ch == b'%' {
            pos += 1;
            col += 1;
            tokens.push(Token {
                kind: TokenKind::Operator,
                text: &source[start..pos],
                offset: start,
                line: start_line,
                column: start_col,
            });
            continue;
        }

        // ---------------------------------------------------------------
        // Unknown character
        // ---------------------------------------------------------------
        // Handle multi-byte UTF-8 correctly
        if ch > 127 {
            let rest = &source[pos..];
            if let Some(c) = rest.chars().next() {
                pos += c.len_utf8();
            } else {
                pos += 1;
            }
        } else {
            pos += 1;
        }
        col += 1;
        tokens.push(Token {
            kind: TokenKind::Unknown,
            text: &source[start..pos],
            offset: start,
            line: start_line,
            column: start_col,
        });
    }

    tokens
}

/// Strip the `-- key: value` header block from SQL content, returning only the
/// SQL body. The header block ends at the first line that is both non-empty AND
/// not a comment.
#[must_use]
pub fn strip_header(content: &str) -> &str {
    let mut body_start = 0;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            body_start += line.len();
            // Account for the newline character(s)
            let rest = &content[body_start..];
            if rest.starts_with("\r\n") {
                body_start += 2;
            } else if rest.starts_with('\n') || rest.starts_with('\r') {
                body_start += 1;
            }
            continue;
        }
        // First non-empty, non-comment line — this is where the body starts
        break;
    }
    if body_start >= content.len() {
        ""
    } else {
        &content[body_start..]
    }
}

/// Filter a token stream to only "meaningful" tokens — those that are not
/// whitespace, newlines, or comments. Preserves Jinja tokens.
#[must_use]
pub fn meaningful_tokens<'a>(tokens: &'a [Token<'a>]) -> Vec<&'a Token<'a>> {
    tokens
        .iter()
        .filter(|t| {
            !matches!(
                t.kind,
                TokenKind::Whitespace
                    | TokenKind::Newline
                    | TokenKind::LineComment
                    | TokenKind::BlockComment
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: collect token kinds from source.
    fn kinds(source: &str) -> Vec<TokenKind> {
        tokenize(source).iter().map(|t| t.kind).collect()
    }

    /// Helper: collect (kind, text) pairs, ignoring whitespace/newlines/comments.
    fn significant(source: &str) -> Vec<(TokenKind, &str)> {
        tokenize(source)
            .iter()
            .filter(|t| {
                t.kind != TokenKind::Whitespace
                    && t.kind != TokenKind::Newline
                    && t.kind != TokenKind::LineComment
                    && t.kind != TokenKind::BlockComment
            })
            .map(|t| (t.kind, t.text))
            .collect()
    }

    #[test]
    fn test_simple_select() {
        let tokens = significant("SELECT 1");
        assert_eq!(
            tokens,
            vec![
                (TokenKind::Keyword, "SELECT"),
                (TokenKind::NumberLiteral, "1"),
            ]
        );
    }

    #[test]
    fn test_keywords_case_insensitive() {
        let tokens = significant("select FROM where");
        assert_eq!(tokens.len(), 3);
        assert!(tokens.iter().all(|(k, _)| *k == TokenKind::Keyword));
    }

    #[test]
    fn test_identifiers() {
        let tokens = significant("table_name _private $dollar");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], (TokenKind::Identifier, "table_name"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "_private"));
        assert_eq!(tokens[2], (TokenKind::Identifier, "$dollar"));
    }

    #[test]
    fn test_quoted_ident_backtick() {
        let tokens = significant("`my table`");
        assert_eq!(tokens, vec![(TokenKind::QuotedIdent, "`my table`")]);
    }

    #[test]
    fn test_quoted_ident_double_quote() {
        let tokens = significant("\"my column\"");
        assert_eq!(tokens, vec![(TokenKind::QuotedIdent, "\"my column\"")]);
    }

    #[test]
    fn test_string_literal() {
        let tokens = significant("'hello world'");
        assert_eq!(tokens, vec![(TokenKind::StringLiteral, "'hello world'")]);
    }

    #[test]
    fn test_string_literal_escaped_quote() {
        let tokens = significant("'it''s fine'");
        assert_eq!(tokens, vec![(TokenKind::StringLiteral, "'it''s fine'")]);
    }

    #[test]
    fn test_number_integer() {
        let tokens = significant("42");
        assert_eq!(tokens, vec![(TokenKind::NumberLiteral, "42")]);
    }

    #[test]
    fn test_number_decimal() {
        let tokens = significant("3.14");
        assert_eq!(tokens, vec![(TokenKind::NumberLiteral, "3.14")]);
    }

    #[test]
    fn test_number_scientific() {
        let tokens = significant("1e10");
        assert_eq!(tokens, vec![(TokenKind::NumberLiteral, "1e10")]);
    }

    #[test]
    fn test_number_scientific_signed() {
        let tokens = significant("2.5E-3");
        assert_eq!(tokens, vec![(TokenKind::NumberLiteral, "2.5E-3")]);
    }

    #[test]
    fn test_operators() {
        let ops = significant("= <> != >= <= + - * / %");
        let expected_texts: Vec<&str> = vec!["=", "<>", "!=", ">=", "<=", "+", "-", "*", "/", "%"];
        assert_eq!(ops.len(), expected_texts.len());
        for (i, (kind, text)) in ops.iter().enumerate() {
            assert_eq!(*kind, TokenKind::Operator);
            assert_eq!(*text, expected_texts[i]);
        }
    }

    #[test]
    fn test_punctuation() {
        let tokens = significant("( ) , ; .");
        assert_eq!(tokens[0].0, TokenKind::LeftParen);
        assert_eq!(tokens[1].0, TokenKind::RightParen);
        assert_eq!(tokens[2].0, TokenKind::Comma);
        assert_eq!(tokens[3].0, TokenKind::Semicolon);
        assert_eq!(tokens[4].0, TokenKind::Dot);
    }

    #[test]
    fn test_line_comment() {
        let all = tokenize("-- this is a comment\nSELECT 1");
        let comment = all.iter().find(|t| t.kind == TokenKind::LineComment);
        assert!(comment.is_some());
        assert_eq!(comment.unwrap().text, "-- this is a comment");
    }

    #[test]
    fn test_block_comment() {
        let tokens = significant("/* comment */ SELECT");
        // Block comment is filtered by significant()
        assert_eq!(tokens, vec![(TokenKind::Keyword, "SELECT")]);
    }

    #[test]
    fn test_block_comment_nested() {
        let all = tokenize("/* outer /* inner */ still comment */");
        let bc = all.iter().find(|t| t.kind == TokenKind::BlockComment);
        assert!(bc.is_some());
        assert_eq!(bc.unwrap().text, "/* outer /* inner */ still comment */");
    }

    #[test]
    fn test_jinja_open_close() {
        let tokens = significant("{{ ref('model') }}");
        assert_eq!(tokens[0].0, TokenKind::JinjaOpen);
        assert_eq!(tokens[tokens.len() - 1].0, TokenKind::JinjaClose);
    }

    #[test]
    fn test_jinja_block() {
        let all = tokenize("{% if true %}SELECT 1{% endif %}");
        let blocks: Vec<_> = all
            .iter()
            .filter(|t| t.kind == TokenKind::JinjaBlock)
            .collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].text, "{% if true %}");
        assert_eq!(blocks[1].text, "{% endif %}");
    }

    #[test]
    fn test_jinja_comment() {
        let all = tokenize("{# this is a jinja comment #}");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].kind, TokenKind::JinjaBlock);
    }

    #[test]
    fn test_whitespace_and_newlines() {
        let all = kinds("  \n\t ");
        assert_eq!(
            all,
            vec![
                TokenKind::Whitespace,
                TokenKind::Newline,
                TokenKind::Whitespace,
            ]
        );
    }

    #[test]
    fn test_line_numbers() {
        let all = tokenize("SELECT\n  1\n  FROM t");
        let select = all.iter().find(|t| t.text == "SELECT").unwrap();
        assert_eq!(select.line, 1);
        assert_eq!(select.column, 1);

        let one = all.iter().find(|t| t.text == "1").unwrap();
        assert_eq!(one.line, 2);

        let from = all.iter().find(|t| t.text == "FROM").unwrap();
        assert_eq!(from.line, 3);
    }

    #[test]
    fn test_full_sql_query() {
        let sql = "SELECT id, name FROM {{ ref('stg_users') }} WHERE id > 0";
        let tokens = significant(sql);
        let kinds: Vec<TokenKind> = tokens.iter().map(|t| t.0).collect();
        assert_eq!(
            kinds,
            vec![
                TokenKind::Keyword,       // SELECT
                TokenKind::Identifier,    // id
                TokenKind::Comma,         // ,
                TokenKind::Identifier,    // name
                TokenKind::Keyword,       // FROM
                TokenKind::JinjaOpen,     // {{
                TokenKind::Identifier,    // ref
                TokenKind::LeftParen,     // (
                TokenKind::StringLiteral, // 'stg_users'
                TokenKind::RightParen,    // )
                TokenKind::JinjaClose,    // }}
                TokenKind::Keyword,       // WHERE
                TokenKind::Identifier,    // id
                TokenKind::Operator,      // >
                TokenKind::NumberLiteral, // 0
            ]
        );
    }

    #[test]
    fn test_empty_input() {
        assert!(tokenize("").is_empty());
    }

    #[test]
    fn test_semicolon_detection() {
        let all = tokenize("SELECT 1;\nSELECT 2;");
        let semis: Vec<_> = all
            .iter()
            .filter(|t| t.kind == TokenKind::Semicolon)
            .collect();
        assert_eq!(semis.len(), 2);
    }

    #[test]
    fn test_select_star() {
        let tokens = significant("SELECT * FROM t");
        assert_eq!(tokens[0], (TokenKind::Keyword, "SELECT"));
        assert_eq!(tokens[1], (TokenKind::Operator, "*"));
        assert_eq!(tokens[2], (TokenKind::Keyword, "FROM"));
    }

    #[test]
    fn test_unicode_identifier() {
        let tokens = significant("SELECT über_column FROM tëst");
        assert_eq!(tokens[0].0, TokenKind::Keyword);
        assert_eq!(tokens[1].1, "über_column");
        assert_eq!(tokens[1].0, TokenKind::Identifier);
        assert_eq!(tokens[3].1, "tëst");
        assert_eq!(tokens[3].0, TokenKind::Identifier);
    }

    #[test]
    fn test_crlf_newlines() {
        let all = tokenize("SELECT\r\n1");
        let newlines: Vec<_> = all
            .iter()
            .filter(|t| t.kind == TokenKind::Newline)
            .collect();
        assert_eq!(newlines.len(), 1);
        assert_eq!(newlines[0].text, "\r\n");
    }

    #[test]
    fn test_drop_table_sequence() {
        let tokens = significant("DROP TABLE my_table");
        assert_eq!(
            tokens,
            vec![
                (TokenKind::Keyword, "DROP"),
                (TokenKind::Keyword, "TABLE"),
                (TokenKind::Identifier, "my_table"),
            ]
        );
    }

    #[test]
    fn test_delete_without_where() {
        let tokens = significant("DELETE FROM my_table");
        assert_eq!(
            tokens,
            vec![
                (TokenKind::Keyword, "DELETE"),
                (TokenKind::Keyword, "FROM"),
                (TokenKind::Identifier, "my_table"),
            ]
        );
    }

    #[test]
    fn test_string_inside_does_not_leak() {
        // Keywords inside strings should be string tokens, not keywords
        let tokens = significant("SELECT 'DROP TABLE'");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].0, TokenKind::Keyword);
        assert_eq!(tokens[1].0, TokenKind::StringLiteral);
    }

    #[test]
    fn test_multiline_string() {
        let sql = "SELECT 'line1\nline2'";
        let all = tokenize(sql);
        let strings: Vec<_> = all
            .iter()
            .filter(|t| t.kind == TokenKind::StringLiteral)
            .collect();
        assert_eq!(strings.len(), 1);
        assert_eq!(strings[0].text, "'line1\nline2'");
    }

    #[test]
    fn test_tab_detection() {
        let sql = "SELECT\t1";
        let all = tokenize(sql);
        let ws: Vec<_> = all
            .iter()
            .filter(|t| t.kind == TokenKind::Whitespace)
            .collect();
        assert_eq!(ws.len(), 1);
        assert!(ws[0].text.contains('\t'));
    }
}
