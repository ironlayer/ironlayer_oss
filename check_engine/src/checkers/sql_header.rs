//! SQL header validation checker — rules HDR001 through HDR013.
//!
//! Validates the `-- key: value` comment block at the top of IronLayer-native
//! `.sql` files. Only active when the project type is IronLayer.
//!
//! Header parsing replicates the exact termination logic from
//! `model_loader.parse_yaml_header()`:
//! - `-- key: value` lines are metadata declarations
//! - `--` followed by text without a colon is a plain comment (skipped)
//! - `--` alone is a bare separator (skipped)
//! - Empty/whitespace-only lines are skipped
//! - The first line that is **both non-empty AND not a comment** terminates
//!   the header block

use std::collections::{HashMap, HashSet};

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// The 13 known header fields (matches `model_loader._KNOWN_FIELDS`).
const KNOWN_FIELDS: &[&str] = &[
    "name",
    "kind",
    "materialization",
    "time_column",
    "unique_key",
    "partition_by",
    "incremental_strategy",
    "owner",
    "tags",
    "dependencies",
    "contract_mode",
    "contract_columns",
    "tests",
];

/// Valid model kind values (matches `ModelKind` enum).
const VALID_KINDS: &[&str] = &[
    "FULL_REFRESH",
    "INCREMENTAL_BY_TIME_RANGE",
    "APPEND_ONLY",
    "MERGE_BY_KEY",
];

/// Valid materialization values (matches `Materialization` enum).
const VALID_MATERIALIZATIONS: &[&str] = &["TABLE", "VIEW", "MERGE", "INSERT_OVERWRITE"];

/// Valid contract mode values (matches `SchemaContractMode` enum).
const VALID_CONTRACT_MODES: &[&str] = &["DISABLED", "WARN", "STRICT"];

/// SQL header checker implementing HDR001-HDR013.
pub struct SqlHeaderChecker;

/// A parsed header field with its line number.
#[derive(Debug)]
struct HeaderField {
    /// The header key (e.g., `"name"`, `"kind"`).
    key: String,
    /// The header value (e.g., `"stg_orders"`, `"FULL_REFRESH"`).
    value: String,
    /// 1-based line number where this field was found.
    line: u32,
}

/// Parse the header block from the top of a SQL file.
///
/// Returns the list of parsed header fields in order of appearance,
/// including duplicates (for HDR013 detection).
fn parse_header(content: &str) -> Vec<HeaderField> {
    let mut fields = Vec::new();

    for (line_idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        let line_num = (line_idx + 1) as u32;

        // Empty/whitespace-only lines — skip, do NOT terminate
        if trimmed.is_empty() {
            continue;
        }

        // Must start with `--` to be part of the header
        if let Some(rest) = trimmed.strip_prefix("--") {
            let rest = rest.trim();

            // Bare `--` (empty after stripping) — skip, do NOT terminate
            if rest.is_empty() {
                continue;
            }

            // Check for `key: value` pattern
            if let Some(colon_pos) = rest.find(':') {
                let key = rest[..colon_pos].trim().to_lowercase();
                let value = rest[colon_pos + 1..].trim().to_owned();

                if !key.is_empty() {
                    fields.push(HeaderField {
                        key,
                        value,
                        line: line_num,
                    });
                    continue;
                }
            }

            // Comment without colon — skip, do NOT terminate
            continue;
        }

        // First non-empty, non-comment line terminates the header
        break;
    }

    fields
}

/// Build a map of key → value from parsed header fields (first occurrence wins).
fn build_header_map(fields: &[HeaderField]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for field in fields {
        map.entry(field.key.clone())
            .or_insert_with(|| field.value.clone());
    }
    map
}

/// Generate the doc URL for a given rule ID.
fn doc_url(rule_id: &str) -> Option<String> {
    Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}"))
}

impl Checker for SqlHeaderChecker {
    fn name(&self) -> &'static str {
        "sql_header"
    }

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

        let fields = parse_header(content);
        let header_map = build_header_map(&fields);

        let mut diags = Vec::new();

        // HDR001: Missing required field `name`
        check_hdr001(file_path, &header_map, config, &mut diags);

        // HDR002: Missing required field `kind`
        check_hdr002(file_path, &header_map, config, &mut diags);

        // HDR003: Invalid `kind` value
        check_hdr003(file_path, &header_map, &fields, config, &mut diags);

        // HDR004: Invalid `materialization` value
        check_hdr004(file_path, &header_map, &fields, config, &mut diags);

        // HDR005: INCREMENTAL_BY_TIME_RANGE requires `time_column`
        check_hdr005(file_path, &header_map, &fields, config, &mut diags);

        // HDR006: MERGE_BY_KEY requires `unique_key`
        check_hdr006(file_path, &header_map, &fields, config, &mut diags);

        // HDR007: Unrecognized header field (disabled by default)
        check_hdr007(file_path, &fields, config, &mut diags);

        // HDR008: Missing optional `owner` field (disabled by default)
        check_hdr008(file_path, &header_map, config, &mut diags);

        // HDR009: Missing optional `tags` field (disabled by default)
        check_hdr009(file_path, &header_map, config, &mut diags);

        // HDR010: Invalid `contract_mode` value
        check_hdr010(file_path, &header_map, &fields, config, &mut diags);

        // HDR011: Malformed `contract_columns` syntax
        check_hdr011(file_path, &header_map, &fields, config, &mut diags);

        // HDR012: Malformed `tests` syntax
        check_hdr012(file_path, &header_map, &fields, config, &mut diags);

        // HDR013: Duplicate header field
        check_hdr013(file_path, &fields, config, &mut diags);

        diags
    }
}

// ---------------------------------------------------------------------------
// HDR001: Missing required field `name`
// ---------------------------------------------------------------------------

fn check_hdr001(
    file_path: &str,
    header_map: &HashMap<String, String>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR001", file_path, true) {
        return;
    }

    if !header_map.contains_key("name") {
        diags.push(CheckDiagnostic {
            rule_id: "HDR001".to_owned(),
            message: "Missing required field 'name' in SQL header. Every IronLayer model \
                      must declare its name via '-- name: <model_name>'."
                .to_owned(),
            severity: config.effective_severity_for_path("HDR001", file_path, Severity::Error),
            category: CheckCategory::SqlHeader,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: Some(
                "Add '-- name: <model_name>' as the first line of the file.".to_owned(),
            ),
            doc_url: doc_url("HDR001"),
        });
    }
}

// ---------------------------------------------------------------------------
// HDR002: Missing required field `kind`
// ---------------------------------------------------------------------------

fn check_hdr002(
    file_path: &str,
    header_map: &HashMap<String, String>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR002", file_path, true) {
        return;
    }

    if !header_map.contains_key("kind") {
        diags.push(CheckDiagnostic {
            rule_id: "HDR002".to_owned(),
            message: "Missing required field 'kind' in SQL header. Every IronLayer model \
                      must declare its kind via '-- kind: <MODEL_KIND>'."
                .to_owned(),
            severity: config.effective_severity_for_path("HDR002", file_path, Severity::Error),
            category: CheckCategory::SqlHeader,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: Some(format!(
                "Add '-- kind: <kind>' where kind is one of: {}.",
                VALID_KINDS.join(", ")
            )),
            doc_url: doc_url("HDR002"),
        });
    }
}

// ---------------------------------------------------------------------------
// HDR003: Invalid `kind` value
// ---------------------------------------------------------------------------

fn check_hdr003(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR003", file_path, true) {
        return;
    }

    if let Some(kind_value) = header_map.get("kind") {
        let kind_upper = kind_value.trim().to_uppercase();
        if !VALID_KINDS.contains(&kind_upper.as_str()) {
            let kind_line = fields
                .iter()
                .find(|f| f.key == "kind")
                .map_or(1, |f| f.line);

            diags.push(CheckDiagnostic {
                rule_id: "HDR003".to_owned(),
                message: format!(
                    "Invalid kind value '{}'. Must be one of: {}.",
                    kind_value.trim(),
                    VALID_KINDS.join(", ")
                ),
                severity: config.effective_severity_for_path("HDR003", file_path, Severity::Error),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: kind_line,
                column: 0,
                snippet: Some(format!("-- kind: {}", kind_value.trim())),
                suggestion: Some(format!("Change to one of: {}.", VALID_KINDS.join(", "))),
                doc_url: doc_url("HDR003"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR004: Invalid `materialization` value
// ---------------------------------------------------------------------------

fn check_hdr004(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR004", file_path, true) {
        return;
    }

    if let Some(mat_value) = header_map.get("materialization") {
        let mat_upper = mat_value.trim().to_uppercase();
        if !VALID_MATERIALIZATIONS.contains(&mat_upper.as_str()) {
            let mat_line = fields
                .iter()
                .find(|f| f.key == "materialization")
                .map_or(1, |f| f.line);

            diags.push(CheckDiagnostic {
                rule_id: "HDR004".to_owned(),
                message: format!(
                    "Invalid materialization value '{}'. Must be one of: {}.",
                    mat_value.trim(),
                    VALID_MATERIALIZATIONS.join(", ")
                ),
                severity: config.effective_severity_for_path("HDR004", file_path, Severity::Error),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: mat_line,
                column: 0,
                snippet: Some(format!("-- materialization: {}", mat_value.trim())),
                suggestion: Some(format!(
                    "Change to one of: {}.",
                    VALID_MATERIALIZATIONS.join(", ")
                )),
                doc_url: doc_url("HDR004"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR005: INCREMENTAL_BY_TIME_RANGE requires `time_column`
// ---------------------------------------------------------------------------

fn check_hdr005(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR005", file_path, true) {
        return;
    }

    if let Some(kind_value) = header_map.get("kind") {
        if kind_value.trim().to_uppercase() == "INCREMENTAL_BY_TIME_RANGE"
            && !header_map.contains_key("time_column")
        {
            let kind_line = fields
                .iter()
                .find(|f| f.key == "kind")
                .map_or(1, |f| f.line);

            diags.push(CheckDiagnostic {
                rule_id: "HDR005".to_owned(),
                message: "kind: INCREMENTAL_BY_TIME_RANGE requires 'time_column' header field. \
                          Add '-- time_column: <column_name>' to the header."
                    .to_owned(),
                severity: config.effective_severity_for_path("HDR005", file_path, Severity::Error),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: kind_line,
                column: 0,
                snippet: Some(format!("-- kind: {}", kind_value.trim())),
                suggestion: Some(
                    "Add '-- time_column: created_at' (or your timestamp column).".to_owned(),
                ),
                doc_url: doc_url("HDR005"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR006: MERGE_BY_KEY requires `unique_key`
// ---------------------------------------------------------------------------

fn check_hdr006(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR006", file_path, true) {
        return;
    }

    if let Some(kind_value) = header_map.get("kind") {
        if kind_value.trim().to_uppercase() == "MERGE_BY_KEY"
            && !header_map.contains_key("unique_key")
        {
            let kind_line = fields
                .iter()
                .find(|f| f.key == "kind")
                .map_or(1, |f| f.line);

            diags.push(CheckDiagnostic {
                rule_id: "HDR006".to_owned(),
                message: "kind: MERGE_BY_KEY requires 'unique_key' header field. \
                          Add '-- unique_key: <column_name>' to the header."
                    .to_owned(),
                severity: config.effective_severity_for_path("HDR006", file_path, Severity::Error),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: kind_line,
                column: 0,
                snippet: Some(format!("-- kind: {}", kind_value.trim())),
                suggestion: Some(
                    "Add '-- unique_key: id' (or your primary key column).".to_owned(),
                ),
                doc_url: doc_url("HDR006"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR007: Unrecognized header field (disabled by default)
// ---------------------------------------------------------------------------

fn check_hdr007(
    file_path: &str,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Disabled by default — preserves forward-compatible header extensions
    if !config.is_rule_enabled_for_path("HDR007", file_path, false) {
        return;
    }

    let known: HashSet<&str> = KNOWN_FIELDS.iter().copied().collect();

    for field in fields {
        if !known.contains(field.key.as_str()) {
            diags.push(CheckDiagnostic {
                rule_id: "HDR007".to_owned(),
                message: format!(
                    "Unrecognized header field '{}'. Known fields are: {}.",
                    field.key,
                    KNOWN_FIELDS.join(", ")
                ),
                severity: config.effective_severity_for_path(
                    "HDR007",
                    file_path,
                    Severity::Warning,
                ),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: field.line,
                column: 0,
                snippet: Some(format!("-- {}: {}", field.key, field.value)),
                suggestion: Some(format!(
                    "Remove this field or use a known field: {}.",
                    KNOWN_FIELDS.join(", ")
                )),
                doc_url: doc_url("HDR007"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR008: Missing optional `owner` field (disabled by default)
// ---------------------------------------------------------------------------

fn check_hdr008(
    file_path: &str,
    header_map: &HashMap<String, String>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Disabled by default
    if !config.is_rule_enabled_for_path("HDR008", file_path, false) {
        return;
    }

    if !header_map.contains_key("owner") {
        diags.push(CheckDiagnostic {
            rule_id: "HDR008".to_owned(),
            message: "Missing recommended field 'owner' in SQL header. \
                      Adding an owner helps track model responsibility."
                .to_owned(),
            severity: config.effective_severity_for_path("HDR008", file_path, Severity::Warning),
            category: CheckCategory::SqlHeader,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: Some("Add '-- owner: <team_or_person>' to the header.".to_owned()),
            doc_url: doc_url("HDR008"),
        });
    }
}

// ---------------------------------------------------------------------------
// HDR009: Missing optional `tags` field (disabled by default)
// ---------------------------------------------------------------------------

fn check_hdr009(
    file_path: &str,
    header_map: &HashMap<String, String>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Disabled by default
    if !config.is_rule_enabled_for_path("HDR009", file_path, false) {
        return;
    }

    if !header_map.contains_key("tags") {
        diags.push(CheckDiagnostic {
            rule_id: "HDR009".to_owned(),
            message: "Missing recommended field 'tags' in SQL header. \
                      Tags help organize and filter models."
                .to_owned(),
            severity: config.effective_severity_for_path("HDR009", file_path, Severity::Warning),
            category: CheckCategory::SqlHeader,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: Some("Add '-- tags: finance, daily' to the header.".to_owned()),
            doc_url: doc_url("HDR009"),
        });
    }
}

// ---------------------------------------------------------------------------
// HDR010: Invalid `contract_mode` value
// ---------------------------------------------------------------------------

fn check_hdr010(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR010", file_path, true) {
        return;
    }

    if let Some(mode_value) = header_map.get("contract_mode") {
        let mode_upper = mode_value.trim().to_uppercase();
        if !VALID_CONTRACT_MODES.contains(&mode_upper.as_str()) {
            let mode_line = fields
                .iter()
                .find(|f| f.key == "contract_mode")
                .map_or(1, |f| f.line);

            diags.push(CheckDiagnostic {
                rule_id: "HDR010".to_owned(),
                message: format!(
                    "Invalid contract_mode value '{}'. Must be one of: {}.",
                    mode_value.trim(),
                    VALID_CONTRACT_MODES.join(", ")
                ),
                severity: config.effective_severity_for_path("HDR010", file_path, Severity::Error),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: mode_line,
                column: 0,
                snippet: Some(format!("-- contract_mode: {}", mode_value.trim())),
                suggestion: Some(format!(
                    "Change to one of: {}.",
                    VALID_CONTRACT_MODES.join(", ")
                )),
                doc_url: doc_url("HDR010"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR011: Malformed `contract_columns` syntax
// ---------------------------------------------------------------------------

/// Validate a single contract column entry: `name:TYPE` or `name:TYPE:NOT_NULL`.
fn validate_contract_column_entry(entry: &str) -> Result<(), String> {
    let parts: Vec<&str> = entry.split(':').collect();

    if parts.len() < 2 {
        return Err(format!(
            "Invalid contract_columns entry '{}': expected 'name:TYPE' or 'name:TYPE:NOT_NULL'.",
            entry
        ));
    }

    let col_name = parts[0].trim();
    let data_type = parts[1].trim();

    if col_name.is_empty() {
        return Err("Empty column name in contract_columns declaration.".to_owned());
    }

    if data_type.is_empty() {
        return Err(format!(
            "Empty data type for column '{}' in contract_columns.",
            col_name
        ));
    }

    if parts.len() >= 3 {
        let modifier = parts[2].trim().to_uppercase();
        if !modifier.is_empty() && modifier != "NOT_NULL" {
            return Err(format!(
                "Invalid contract_columns modifier '{}' for column '{}'. Expected 'NOT_NULL'.",
                parts[2].trim(),
                col_name
            ));
        }
    }

    Ok(())
}

fn check_hdr011(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR011", file_path, true) {
        return;
    }

    if let Some(columns_value) = header_map.get("contract_columns") {
        let columns_value = columns_value.trim();
        if columns_value.is_empty() {
            return;
        }

        let columns_line = fields
            .iter()
            .find(|f| f.key == "contract_columns")
            .map_or(1, |f| f.line);

        for entry in columns_value.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }

            if let Err(error_msg) = validate_contract_column_entry(entry) {
                diags.push(CheckDiagnostic {
                    rule_id: "HDR011".to_owned(),
                    message: error_msg,
                    severity: config.effective_severity_for_path(
                        "HDR011",
                        file_path,
                        Severity::Error,
                    ),
                    category: CheckCategory::SqlHeader,
                    file_path: file_path.to_owned(),
                    line: columns_line,
                    column: 0,
                    snippet: Some(format!("-- contract_columns: {columns_value}")),
                    suggestion: Some(
                        "Use format: 'name:TYPE' or 'name:TYPE:NOT_NULL'. Example: 'id:INT:NOT_NULL, name:STRING'."
                            .to_owned(),
                    ),
                    doc_url: doc_url("HDR011"),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HDR012: Malformed `tests` syntax
// ---------------------------------------------------------------------------

/// Known test types (matches `_TEST_TYPE_LOOKUP` in model_loader.py).
const KNOWN_TEST_TYPES: &[&str] = &[
    "not_null",
    "unique",
    "row_count_min",
    "row_count_max",
    "accepted_values",
    "custom_sql",
];

/// Validate test declarations from the `tests` header value.
///
/// Expected syntax: `test_type(arg)` optionally followed by `@SEVERITY`.
fn validate_test_declarations(value: &str) -> Vec<String> {
    let mut errors = Vec::new();

    if value.trim().is_empty() {
        return errors;
    }

    // Split declarations using paren-depth-aware parsing (same as Python)
    let declarations = split_test_declarations(value);

    for decl in &declarations {
        let decl = decl.trim();
        if decl.is_empty() {
            continue;
        }

        // Strip severity suffix: test_type(arg)@WARN
        let base_decl = if let Some(at_idx) = decl.rfind(")@") {
            &decl[..at_idx + 1]
        } else {
            decl
        };

        // Extract test_type and arg from `test_type(arg)`
        let paren_idx = match base_decl.find('(') {
            Some(idx) => idx,
            None => {
                errors.push(format!(
                    "Invalid test declaration '{}': expected 'test_type(arg)' syntax.",
                    decl
                ));
                continue;
            }
        };

        if !base_decl.ends_with(')') {
            errors.push(format!(
                "Invalid test declaration '{}': missing closing parenthesis.",
                decl
            ));
            continue;
        }

        let test_name = base_decl[..paren_idx].trim().to_lowercase();

        if !KNOWN_TEST_TYPES.contains(&test_name.as_str()) {
            errors.push(format!(
                "Unknown test type '{}' in declaration '{}'. Known types: {}.",
                test_name,
                decl,
                KNOWN_TEST_TYPES.join(", ")
            ));
        }
    }

    errors
}

/// Split test declarations using paren-depth-aware parsing.
///
/// Commas inside parentheses (e.g., in `custom_sql(...)`) are not split.
fn split_test_declarations(value: &str) -> Vec<String> {
    let mut declarations = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;

    for ch in value.chars() {
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
                    declarations.push(trimmed);
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let trailing = current.trim().to_owned();
    if !trailing.is_empty() {
        declarations.push(trailing);
    }

    declarations
}

fn check_hdr012(
    file_path: &str,
    header_map: &HashMap<String, String>,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR012", file_path, true) {
        return;
    }

    if let Some(tests_value) = header_map.get("tests") {
        let tests_line = fields
            .iter()
            .find(|f| f.key == "tests")
            .map_or(1, |f| f.line);

        let errors = validate_test_declarations(tests_value);
        for error_msg in errors {
            diags.push(CheckDiagnostic {
                rule_id: "HDR012".to_owned(),
                message: error_msg,
                severity: config.effective_severity_for_path("HDR012", file_path, Severity::Error),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: tests_line,
                column: 0,
                snippet: Some(format!("-- tests: {}", tests_value.trim())),
                suggestion: Some(
                    "Use format: 'test_type(arg)'. Example: 'not_null(id), unique(email)'."
                        .to_owned(),
                ),
                doc_url: doc_url("HDR012"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// HDR013: Duplicate header field
// ---------------------------------------------------------------------------

fn check_hdr013(
    file_path: &str,
    fields: &[HeaderField],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    if !config.is_rule_enabled_for_path("HDR013", file_path, true) {
        return;
    }

    let mut seen: HashMap<&str, u32> = HashMap::new();

    for field in fields {
        if let Some(&first_line) = seen.get(field.key.as_str()) {
            diags.push(CheckDiagnostic {
                rule_id: "HDR013".to_owned(),
                message: format!(
                    "Duplicate header field '{}'. First declared on line {}, duplicated here on line {}.",
                    field.key, first_line, field.line
                ),
                severity: config.effective_severity_for_path(
                    "HDR013",
                    file_path,
                    Severity::Warning,
                ),
                category: CheckCategory::SqlHeader,
                file_path: file_path.to_owned(),
                line: field.line,
                column: 0,
                snippet: Some(format!("-- {}: {}", field.key, field.value)),
                suggestion: Some(format!(
                    "Remove this duplicate '{}' declaration (keep the one on line {}).",
                    field.key, first_line
                )),
                doc_url: doc_url("HDR013"),
            });
        } else {
            seen.insert(&field.key, field.line);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    fn default_config() -> CheckConfig {
        CheckConfig::default()
    }

    fn check(content: &str) -> Vec<CheckDiagnostic> {
        let checker = SqlHeaderChecker;
        checker.check_file("test.sql", content, None, &default_config())
    }

    // ── HDR001 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr001_missing_name() {
        let sql = indoc! {"
            -- kind: FULL_REFRESH
            SELECT 1
        "};
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR001"));
    }

    #[test]
    fn test_hdr001_name_present() {
        let sql = indoc! {"
            -- name: stg_orders
            -- kind: FULL_REFRESH
            SELECT 1
        "};
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR001"));
    }

    #[test]
    fn test_hdr001_empty_file() {
        let diags = check("");
        assert!(diags.iter().any(|d| d.rule_id == "HDR001"));
    }

    #[test]
    fn test_hdr001_no_header() {
        let diags = check("SELECT 1");
        assert!(diags.iter().any(|d| d.rule_id == "HDR001"));
    }

    #[test]
    fn test_hdr001_severity_is_error() {
        let diags = check("SELECT 1");
        let hdr001 = diags.iter().find(|d| d.rule_id == "HDR001").unwrap();
        assert_eq!(hdr001.severity, Severity::Error);
    }

    // ── HDR002 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr002_missing_kind() {
        let sql = indoc! {"
            -- name: stg_orders
            SELECT 1
        "};
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_hdr002_kind_present() {
        let sql = indoc! {"
            -- name: stg_orders
            -- kind: FULL_REFRESH
            SELECT 1
        "};
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_hdr002_severity_is_error() {
        let sql = "-- name: test\nSELECT 1";
        let diags = check(sql);
        let hdr002 = diags.iter().find(|d| d.rule_id == "HDR002").unwrap();
        assert_eq!(hdr002.severity, Severity::Error);
    }

    // ── HDR003 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr003_invalid_kind() {
        let sql = indoc! {"
            -- name: test
            -- kind: INVALID_KIND
            SELECT 1
        "};
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_hdr003_valid_full_refresh() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_hdr003_valid_incremental() {
        let sql = "-- name: test\n-- kind: INCREMENTAL_BY_TIME_RANGE\n-- time_column: ts\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_hdr003_valid_append_only() {
        let sql = "-- name: test\n-- kind: APPEND_ONLY\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_hdr003_valid_merge_by_key() {
        let sql = "-- name: test\n-- kind: MERGE_BY_KEY\n-- unique_key: id\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_hdr003_case_insensitive() {
        // Kind comparison should be case-insensitive
        let sql = "-- name: test\n-- kind: full_refresh\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_hdr003_message_includes_invalid_value() {
        let sql = "-- name: test\n-- kind: BROKEN\nSELECT 1";
        let diags = check(sql);
        let hdr003 = diags.iter().find(|d| d.rule_id == "HDR003").unwrap();
        assert!(hdr003.message.contains("BROKEN"));
    }

    // ── HDR004 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr004_invalid_materialization() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- materialization: SNAPSHOT\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR004"));
    }

    #[test]
    fn test_hdr004_valid_table() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- materialization: TABLE\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR004"));
    }

    #[test]
    fn test_hdr004_valid_view() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- materialization: VIEW\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR004"));
    }

    #[test]
    fn test_hdr004_valid_merge() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- materialization: MERGE\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR004"));
    }

    #[test]
    fn test_hdr004_valid_insert_overwrite() {
        let sql =
            "-- name: test\n-- kind: FULL_REFRESH\n-- materialization: INSERT_OVERWRITE\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR004"));
    }

    #[test]
    fn test_hdr004_no_materialization_is_ok() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR004"));
    }

    // ── HDR005 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr005_incremental_missing_time_column() {
        let sql = "-- name: test\n-- kind: INCREMENTAL_BY_TIME_RANGE\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR005"));
    }

    #[test]
    fn test_hdr005_incremental_with_time_column() {
        let sql =
            "-- name: test\n-- kind: INCREMENTAL_BY_TIME_RANGE\n-- time_column: created_at\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR005"));
    }

    #[test]
    fn test_hdr005_full_refresh_no_time_column_ok() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR005"));
    }

    #[test]
    fn test_hdr005_append_only_no_time_column_ok() {
        let sql = "-- name: test\n-- kind: APPEND_ONLY\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR005"));
    }

    // ── HDR006 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr006_merge_missing_unique_key() {
        let sql = "-- name: test\n-- kind: MERGE_BY_KEY\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR006"));
    }

    #[test]
    fn test_hdr006_merge_with_unique_key() {
        let sql = "-- name: test\n-- kind: MERGE_BY_KEY\n-- unique_key: id\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR006"));
    }

    #[test]
    fn test_hdr006_full_refresh_no_unique_key_ok() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR006"));
    }

    // ── HDR007 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr007_disabled_by_default() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- custom_field: value\nSELECT 1";
        let diags = check(sql);
        // HDR007 is disabled by default, so no diagnostics
        assert!(!diags.iter().any(|d| d.rule_id == "HDR007"));
    }

    #[test]
    fn test_hdr007_enabled_detects_unknown_field() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- custom_field: value\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR007".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "HDR007"));
    }

    #[test]
    fn test_hdr007_known_fields_not_flagged() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- owner: team_a\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR007".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR007"));
    }

    // ── HDR008 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr008_disabled_by_default() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR008"));
    }

    #[test]
    fn test_hdr008_enabled_missing_owner() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR008".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "HDR008"));
    }

    #[test]
    fn test_hdr008_enabled_owner_present() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- owner: team_a\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR008".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR008"));
    }

    // ── HDR009 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr009_disabled_by_default() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR009"));
    }

    #[test]
    fn test_hdr009_enabled_missing_tags() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR009".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(diags.iter().any(|d| d.rule_id == "HDR009"));
    }

    #[test]
    fn test_hdr009_enabled_tags_present() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- tags: finance, daily\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR009".to_owned(),
            crate::config::RuleSeverityOverride::Warning,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR009"));
    }

    // ── HDR010 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr010_invalid_contract_mode() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_mode: INVALID\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR010"));
    }

    #[test]
    fn test_hdr010_valid_disabled() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_mode: DISABLED\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR010"));
    }

    #[test]
    fn test_hdr010_valid_warn() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_mode: WARN\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR010"));
    }

    #[test]
    fn test_hdr010_valid_strict() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_mode: STRICT\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR010"));
    }

    #[test]
    fn test_hdr010_no_contract_mode_is_ok() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR010"));
    }

    // ── HDR011 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr011_valid_contract_columns() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_columns: id:INT:NOT_NULL, name:STRING\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR011"));
    }

    #[test]
    fn test_hdr011_missing_type() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_columns: id\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR011"));
    }

    #[test]
    fn test_hdr011_invalid_modifier() {
        let sql =
            "-- name: test\n-- kind: FULL_REFRESH\n-- contract_columns: id:INT:NULLABLE\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR011"));
    }

    #[test]
    fn test_hdr011_empty_column_name() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_columns: :INT\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR011"));
    }

    #[test]
    fn test_hdr011_empty_data_type() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- contract_columns: id:\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR011"));
    }

    // ── HDR012 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr012_valid_tests() {
        let sql =
            "-- name: test\n-- kind: FULL_REFRESH\n-- tests: not_null(id), unique(email)\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR012"));
    }

    #[test]
    fn test_hdr012_unknown_test_type() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- tests: invalid_test(id)\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR012"));
    }

    #[test]
    fn test_hdr012_missing_parens() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- tests: not_null\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR012"));
    }

    #[test]
    fn test_hdr012_custom_sql_with_commas() {
        // custom_sql can contain commas inside parens — should parse correctly
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- tests: custom_sql(SELECT a, b FROM t WHERE c > 0)\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR012"));
    }

    #[test]
    fn test_hdr012_with_severity_suffix() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- tests: not_null(id)@WARN\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR012"));
    }

    // ── HDR013 tests ─────────────────────────────────────────────────────

    #[test]
    fn test_hdr013_duplicate_field() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- name: test2\nSELECT 1";
        let diags = check(sql);
        assert!(diags.iter().any(|d| d.rule_id == "HDR013"));
    }

    #[test]
    fn test_hdr013_no_duplicates() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR013"));
    }

    #[test]
    fn test_hdr013_severity_is_warning() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- name: test2\nSELECT 1";
        let diags = check(sql);
        let hdr013 = diags.iter().find(|d| d.rule_id == "HDR013").unwrap();
        assert_eq!(hdr013.severity, Severity::Warning);
    }

    #[test]
    fn test_hdr013_message_includes_line_numbers() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- name: test2\nSELECT 1";
        let diags = check(sql);
        let hdr013 = diags.iter().find(|d| d.rule_id == "HDR013").unwrap();
        assert!(hdr013.message.contains("line 1"));
        assert!(hdr013.message.contains("line 3"));
    }

    // ── Header parsing tests ─────────────────────────────────────────────

    #[test]
    fn test_header_blank_lines_do_not_terminate() {
        let sql = "-- name: test\n\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        // Both name and kind should be found (blank line doesn't terminate)
        assert!(!diags.iter().any(|d| d.rule_id == "HDR001"));
        assert!(!diags.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_header_bare_comment_does_not_terminate() {
        let sql = "-- name: test\n--\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        // Both name and kind should be found (bare -- doesn't terminate)
        assert!(!diags.iter().any(|d| d.rule_id == "HDR001"));
        assert!(!diags.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_header_plain_comment_does_not_terminate() {
        let sql = "-- name: test\n-- This is a comment\n-- kind: FULL_REFRESH\nSELECT 1";
        let diags = check(sql);
        // Both name and kind should be found (plain comment doesn't terminate)
        assert!(!diags.iter().any(|d| d.rule_id == "HDR001"));
        assert!(!diags.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_header_sql_terminates() {
        let sql = "-- name: test\nSELECT 1\n-- kind: FULL_REFRESH";
        let diags = check(sql);
        // `kind` is after the SQL — should NOT be found
        assert!(diags.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_non_sql_file_ignored() {
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("schema.yml", "key: value", None, &default_config());
        assert!(diags.is_empty());
    }

    #[test]
    fn test_complete_valid_header() {
        let sql = indoc! {"
            -- name: stg_orders
            -- kind: INCREMENTAL_BY_TIME_RANGE
            -- materialization: TABLE
            -- time_column: created_at
            -- owner: data_team
            -- tags: finance, daily
            -- contract_mode: STRICT
            -- contract_columns: id:INT:NOT_NULL, name:STRING
            -- tests: not_null(id), unique(id)
            SELECT * FROM raw.orders
        "};
        let diags = check(sql);
        // Should have zero HDR errors
        let hdr_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.rule_id.starts_with("HDR"))
            .collect();
        assert!(
            hdr_diags.is_empty(),
            "Unexpected HDR diagnostics: {hdr_diags:?}"
        );
    }

    #[test]
    fn test_header_with_unknown_fields_forward_compatible() {
        // Unknown fields should be silently ignored (HDR007 disabled by default)
        let sql = indoc! {"
            -- name: test
            -- kind: FULL_REFRESH
            -- future_field: some_value
            -- another_custom: data
            SELECT 1
        "};
        let diags = check(sql);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR007"));
    }

    // ── Validate test declarations parsing ───────────────────────────────

    #[test]
    fn test_split_test_declarations_simple() {
        let result = split_test_declarations("not_null(id), unique(email)");
        assert_eq!(result, vec!["not_null(id)", "unique(email)"]);
    }

    #[test]
    fn test_split_test_declarations_nested_parens() {
        let result = split_test_declarations("custom_sql(SELECT a, b FROM t), not_null(id)");
        assert_eq!(
            result,
            vec!["custom_sql(SELECT a, b FROM t)", "not_null(id)"]
        );
    }

    #[test]
    fn test_validate_contract_column_valid() {
        assert!(validate_contract_column_entry("id:INT").is_ok());
        assert!(validate_contract_column_entry("id:INT:NOT_NULL").is_ok());
        assert!(validate_contract_column_entry("name:STRING").is_ok());
    }

    #[test]
    fn test_validate_contract_column_invalid() {
        assert!(validate_contract_column_entry("id").is_err());
        assert!(validate_contract_column_entry(":INT").is_err());
        assert!(validate_contract_column_entry("id:").is_err());
        assert!(validate_contract_column_entry("id:INT:NULLABLE").is_err());
    }

    // ── Config override tests ────────────────────────────────────────────

    #[test]
    fn test_rule_disabled_via_config() {
        let sql = "-- name: test\n-- kind: INVALID\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR003".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "HDR003"));
    }

    #[test]
    fn test_severity_override_via_config() {
        let sql = "-- name: test\n-- kind: FULL_REFRESH\n-- name: test2\nSELECT 1";
        let mut config = default_config();
        config.rules.insert(
            "HDR013".to_owned(),
            crate::config::RuleSeverityOverride::Error,
        );
        let checker = SqlHeaderChecker;
        let diags = checker.check_file("test.sql", sql, None, &config);
        let hdr013 = diags.iter().find(|d| d.rule_id == "HDR013").unwrap();
        assert_eq!(hdr013.severity, Severity::Error);
    }
}
