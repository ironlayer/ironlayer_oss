//! JSON and SARIF output generation for the IronLayer Check Engine.
//!
//! Produces two output formats:
//!
//! 1. **JSON** — native check engine format via `serde_json` serialization
//!    of [`CheckResult`]. Used by `ironlayer check --json`.
//!
//! 2. **SARIF v2.1.0** — Static Analysis Results Interchange Format, compatible
//!    with GitHub Code Scanning (`github/codeql-action/upload-sarif@v3`).
//!    Maps [`CheckDiagnostic`] fields to SARIF result objects.
//!
//! ## SARIF Field Mapping
//!
//! | CheckDiagnostic field | SARIF location |
//! |---|---|
//! | `rule_id` | `result.ruleId` |
//! | `message` | `result.message.text` |
//! | `severity` | `result.level` (Error→error, Warning→warning, Info→note) |
//! | `file_path` | `result.locations[0].physicalLocation.artifactLocation.uri` |
//! | `line` | `result.locations[0].physicalLocation.region.startLine` |
//! | `column` | `result.locations[0].physicalLocation.region.startColumn` |
//! | `suggestion` | `result.fixes[0].description.text` |
//! | `snippet` | `result.locations[0].physicalLocation.contextRegion.snippet.text` |

use std::collections::HashMap;

use serde::Serialize;

use crate::types::{CheckDiagnostic, CheckResult, Severity};

/// Engine version for SARIF tool metadata.
const ENGINE_VERSION: &str = "0.3.0";

/// SARIF schema URL.
const SARIF_SCHEMA: &str =
    "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/main/sarif-2.1/schema/sarif-schema-2.1.0.json";

// ---------------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------------

/// Serialize a [`CheckResult`] to pretty-printed JSON.
///
/// # Errors
///
/// Returns an error message if serialization fails (should not happen for valid data).
pub fn to_json(result: &CheckResult) -> Result<String, String> {
    serde_json::to_string_pretty(result).map_err(|e| format!("JSON serialization failed: {e}"))
}

// ---------------------------------------------------------------------------
// SARIF output
// ---------------------------------------------------------------------------

/// Serialize a [`CheckResult`] to SARIF v2.1.0 JSON.
///
/// Produces a complete SARIF log with tool metadata, rule definitions,
/// and result objects mapped from check diagnostics.
///
/// # Errors
///
/// Returns an error message if serialization fails.
pub fn to_sarif(result: &CheckResult) -> Result<String, String> {
    let sarif = build_sarif_log(result);
    serde_json::to_string_pretty(&sarif).map_err(|e| format!("SARIF serialization failed: {e}"))
}

/// Build the SARIF log structure from a check result.
fn build_sarif_log(result: &CheckResult) -> SarifLog {
    // Collect unique rule IDs and build rule definitions
    let mut rule_map: HashMap<String, SarifRule> = HashMap::new();

    for diag in &result.diagnostics {
        rule_map
            .entry(diag.rule_id.clone())
            .or_insert_with(|| SarifRule {
                id: diag.rule_id.clone(),
                name: rule_id_to_name(&diag.rule_id),
                short_description: SarifMessage {
                    text: rule_id_to_short_description(&diag.rule_id),
                },
                help_uri: diag.doc_url.clone(),
                default_configuration: SarifDefaultConfiguration {
                    level: severity_to_sarif_level(diag.severity),
                },
            });
    }

    // Sort rules by ID for deterministic output
    let mut rules: Vec<SarifRule> = rule_map.into_values().collect();
    rules.sort_by(|a, b| a.id.cmp(&b.id));

    // Build results
    let results: Vec<SarifResult> = result.diagnostics.iter().map(build_sarif_result).collect();

    SarifLog {
        schema: SARIF_SCHEMA.to_owned(),
        version: "2.1.0".to_owned(),
        runs: vec![SarifRun {
            tool: SarifTool {
                driver: SarifDriver {
                    name: "ironlayer-check".to_owned(),
                    version: ENGINE_VERSION.to_owned(),
                    information_uri: "https://docs.ironlayer.app/check".to_owned(),
                    rules,
                },
            },
            results,
        }],
    }
}

/// Build a SARIF result object from a check diagnostic.
fn build_sarif_result(diag: &CheckDiagnostic) -> SarifResult {
    let mut locations = Vec::new();

    // Build physical location
    let mut location = SarifPhysicalLocation {
        artifact_location: SarifArtifactLocation {
            uri: diag.file_path.clone(),
        },
        region: None,
        context_region: None,
    };

    // Add region if we have line/column info
    if diag.line > 0 {
        location.region = Some(SarifRegion {
            start_line: diag.line,
            start_column: if diag.column > 0 {
                Some(diag.column)
            } else {
                None
            },
        });
    }

    // Add context region with snippet if available
    if let Some(ref snippet) = diag.snippet {
        location.context_region = Some(SarifContextRegion {
            snippet: SarifSnippet {
                text: snippet.clone(),
            },
        });
    }

    locations.push(SarifLocation {
        physical_location: location,
    });

    // Build fixes if suggestion is available
    let fixes = diag.suggestion.as_ref().map(|suggestion| {
        vec![SarifFix {
            description: SarifMessage {
                text: suggestion.clone(),
            },
        }]
    });

    SarifResult {
        rule_id: diag.rule_id.clone(),
        level: severity_to_sarif_level(diag.severity),
        message: SarifMessage {
            text: diag.message.clone(),
        },
        locations,
        fixes,
    }
}

/// Map check engine severity to SARIF level string.
fn severity_to_sarif_level(severity: Severity) -> String {
    match severity {
        Severity::Error => "error".to_owned(),
        Severity::Warning => "warning".to_owned(),
        Severity::Info => "note".to_owned(),
    }
}

/// Derive a PascalCase rule name from a rule ID.
///
/// Maps rule ID prefixes to descriptive names.
fn rule_id_to_name(rule_id: &str) -> String {
    let prefix = rule_id
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>();

    match prefix.as_str() {
        "HDR" => format!("Header{}", &rule_id[3..]),
        "SQL" => format!("SqlSyntax{}", &rule_id[3..]),
        "SAF" => format!("SqlSafety{}", &rule_id[3..]),
        "REF" => format!("RefResolution{}", &rule_id[3..]),
        "NAME" => format!("NamingConvention{}", &rule_id[4..]),
        "YML" => format!("YamlSchema{}", &rule_id[3..]),
        "DBT" => format!("DbtProject{}", &rule_id[3..]),
        "CON" => format!("ModelConsistency{}", &rule_id[3..]),
        _ => rule_id.to_owned(),
    }
}

/// Derive a short description from a rule ID.
fn rule_id_to_short_description(rule_id: &str) -> String {
    match rule_id {
        "HDR001" => "Missing required 'name' header field".to_owned(),
        "HDR002" => "Missing required 'kind' header field".to_owned(),
        "HDR003" => "Invalid model kind value".to_owned(),
        "HDR004" => "Invalid materialization value".to_owned(),
        "HDR005" => "INCREMENTAL_BY_TIME_RANGE requires time_column".to_owned(),
        "HDR006" => "MERGE_BY_KEY requires unique_key".to_owned(),
        "HDR007" => "Unrecognized header field".to_owned(),
        "HDR008" => "Missing optional owner field".to_owned(),
        "HDR009" => "Missing optional tags field".to_owned(),
        "HDR010" => "Invalid contract mode value".to_owned(),
        "HDR011" => "Malformed contract_columns syntax".to_owned(),
        "HDR012" => "Malformed tests syntax".to_owned(),
        "HDR013" => "Duplicate header field".to_owned(),
        "SQL001" => "Unbalanced brackets".to_owned(),
        "SQL002" => "Unbalanced string literal".to_owned(),
        "SQL003" => "Unbalanced block comment".to_owned(),
        "SQL004" => "SELECT * usage".to_owned(),
        "SQL005" => "Missing WHERE clause on DELETE".to_owned(),
        "SQL006" => "Empty SQL body".to_owned(),
        "SQL007" => "Trailing semicolon".to_owned(),
        "SQL008" => "Invalid UTF-8 file".to_owned(),
        "SQL009" => "Tab character in SQL".to_owned(),
        "SAF001" => "DROP TABLE detected".to_owned(),
        "SAF002" => "DROP VIEW detected".to_owned(),
        "SAF003" => "DROP SCHEMA detected".to_owned(),
        "SAF004" => "TRUNCATE detected".to_owned(),
        "SAF005" => "DELETE without WHERE".to_owned(),
        "SAF006" => "ALTER DROP COLUMN detected".to_owned(),
        "SAF007" => "GRANT detected".to_owned(),
        "SAF008" => "REVOKE detected".to_owned(),
        "SAF009" => "CREATE USER detected".to_owned(),
        "SAF010" => "INSERT OVERWRITE without PARTITION".to_owned(),
        "REF001" => "Undefined model reference".to_owned(),
        "REF002" => "Self-referencing model".to_owned(),
        "REF003" => "Circular reference chain".to_owned(),
        "REF004" => "Fully-qualified ref name".to_owned(),
        "REF005" => "Duplicate ref in single file".to_owned(),
        "REF006" => "Ambiguous model name".to_owned(),
        "NAME001" => "Staging model naming violation".to_owned(),
        "NAME002" => "Intermediate model naming violation".to_owned(),
        "NAME003" => "Mart model naming violation".to_owned(),
        "NAME004" => "Model in wrong layer directory".to_owned(),
        "NAME005" => "Invalid model name format".to_owned(),
        "NAME006" => "File name does not match model name".to_owned(),
        "NAME007" => "Directory name convention violation".to_owned(),
        "NAME008" => "Model name too long".to_owned(),
        "YML001" => "Invalid YAML syntax".to_owned(),
        "YML002" => "Missing required name in dbt_project.yml".to_owned(),
        "YML003" => "Missing required version in dbt_project.yml".to_owned(),
        "YML004" => "YAML model has no SQL file".to_owned(),
        "YML005" => "SQL file has no YAML documentation".to_owned(),
        "YML006" => "YAML column mismatch with SQL output".to_owned(),
        "YML007" => "Model has no tests".to_owned(),
        "YML008" => "Model has no column descriptions".to_owned(),
        "YML009" => "Profile references non-existent target".to_owned(),
        "DBT001" => "dbt_project.yml not found".to_owned(),
        "DBT002" => "Model not in configured model path".to_owned(),
        "DBT003" => "Undefined source reference".to_owned(),
        "DBT004" => "Unrecognized materialization".to_owned(),
        "DBT005" => "No unique test on any column".to_owned(),
        "DBT006" => "No not_null test on primary key".to_owned(),
        "CON001" => "Duplicate model name".to_owned(),
        "CON002" => "Declared dependency not in refs".to_owned(),
        "CON003" => "Ref not in declared dependencies".to_owned(),
        "CON004" => "Orphan model".to_owned(),
        "CON005" => "No declared owner".to_owned(),
        _ => format!("Check rule {rule_id}"),
    }
}

// ---------------------------------------------------------------------------
// SARIF v2.1.0 data structures
// ---------------------------------------------------------------------------

/// Top-level SARIF log structure.
#[derive(Debug, Serialize)]
struct SarifLog {
    #[serde(rename = "$schema")]
    schema: String,
    version: String,
    runs: Vec<SarifRun>,
}

/// A single SARIF run (one per tool invocation).
#[derive(Debug, Serialize)]
struct SarifRun {
    tool: SarifTool,
    results: Vec<SarifResult>,
}

/// Tool metadata.
#[derive(Debug, Serialize)]
struct SarifTool {
    driver: SarifDriver,
}

/// Tool driver (name, version, rules).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifDriver {
    name: String,
    version: String,
    information_uri: String,
    rules: Vec<SarifRule>,
}

/// A SARIF rule definition.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifRule {
    id: String,
    name: String,
    short_description: SarifMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    help_uri: Option<String>,
    default_configuration: SarifDefaultConfiguration,
}

/// Default configuration for a SARIF rule.
#[derive(Debug, Serialize)]
struct SarifDefaultConfiguration {
    level: String,
}

/// A single SARIF result (diagnostic).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifResult {
    rule_id: String,
    level: String,
    message: SarifMessage,
    locations: Vec<SarifLocation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fixes: Option<Vec<SarifFix>>,
}

/// A text message.
#[derive(Debug, Serialize)]
struct SarifMessage {
    text: String,
}

/// A SARIF location wrapper.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifLocation {
    physical_location: SarifPhysicalLocation,
}

/// Physical location in a file.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifPhysicalLocation {
    artifact_location: SarifArtifactLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<SarifRegion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    context_region: Option<SarifContextRegion>,
}

/// File path in SARIF format.
#[derive(Debug, Serialize)]
struct SarifArtifactLocation {
    uri: String,
}

/// Line/column region.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifRegion {
    start_line: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_column: Option<u32>,
}

/// Context region with a snippet.
#[derive(Debug, Serialize)]
struct SarifContextRegion {
    snippet: SarifSnippet,
}

/// A code snippet.
#[derive(Debug, Serialize)]
struct SarifSnippet {
    text: String,
}

/// A suggested fix.
#[derive(Debug, Serialize)]
struct SarifFix {
    description: SarifMessage,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CheckCategory, CheckDiagnostic, CheckResult, Severity};

    fn make_result(diagnostics: Vec<CheckDiagnostic>) -> CheckResult {
        let total_errors = diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .count() as u32;
        let total_warnings = diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Warning)
            .count() as u32;
        let total_infos = diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Info)
            .count() as u32;

        CheckResult {
            diagnostics,
            total_files_checked: 5,
            total_files_skipped_cache: 2,
            total_errors,
            total_warnings,
            total_infos,
            elapsed_ms: 100,
            project_type: "ironlayer".to_owned(),
            passed: total_errors == 0,
        }
    }

    fn make_diag(rule_id: &str, severity: Severity) -> CheckDiagnostic {
        CheckDiagnostic {
            rule_id: rule_id.to_owned(),
            message: format!("Test diagnostic for {rule_id}"),
            severity,
            category: CheckCategory::SqlHeader,
            file_path: "models/test.sql".to_owned(),
            line: 5,
            column: 10,
            snippet: Some("-- name: test".to_owned()),
            suggestion: Some("Fix it".to_owned()),
            doc_url: Some(format!("https://docs.ironlayer.app/check/rules/{rule_id}")),
        }
    }

    // --- JSON output tests ---

    #[test]
    fn test_json_output_valid() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let json = to_json(&result);
        assert!(json.is_ok());
    }

    #[test]
    fn test_json_output_contains_diagnostics() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let json = to_json(&result).unwrap();
        assert!(json.contains("HDR001"));
        assert!(json.contains("models/test.sql"));
    }

    #[test]
    fn test_json_output_empty_result() {
        let result = make_result(vec![]);
        let json = to_json(&result).unwrap();
        assert!(json.contains("\"diagnostics\": []"));
        assert!(json.contains("\"passed\": true"));
    }

    #[test]
    fn test_json_roundtrip() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let json = to_json(&result).unwrap();
        let parsed: CheckResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.diagnostics.len(), 1);
        assert_eq!(parsed.diagnostics[0].rule_id, "HDR001");
    }

    // --- SARIF output tests ---

    #[test]
    fn test_sarif_output_valid() {
        let result = make_result(vec![make_diag("REF001", Severity::Error)]);
        let sarif = to_sarif(&result);
        assert!(sarif.is_ok());
    }

    #[test]
    fn test_sarif_has_schema() {
        let result = make_result(vec![make_diag("REF001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("$schema"));
        assert!(sarif.contains("sarif-schema-2.1.0.json"));
    }

    #[test]
    fn test_sarif_has_version() {
        let result = make_result(vec![make_diag("REF001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"version\": \"2.1.0\""));
    }

    #[test]
    fn test_sarif_has_tool_info() {
        let result = make_result(vec![make_diag("REF001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("ironlayer-check"));
        assert!(sarif.contains(ENGINE_VERSION));
    }

    #[test]
    fn test_sarif_error_level() {
        let result = make_result(vec![make_diag("REF001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"level\": \"error\""));
    }

    #[test]
    fn test_sarif_warning_level() {
        let result = make_result(vec![make_diag("SQL004", Severity::Warning)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"level\": \"warning\""));
    }

    #[test]
    fn test_sarif_info_level() {
        let result = make_result(vec![make_diag("CON004", Severity::Info)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"level\": \"note\""));
    }

    #[test]
    fn test_sarif_has_rule_id() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"ruleId\": \"HDR001\""));
    }

    #[test]
    fn test_sarif_has_file_path() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("models/test.sql"));
    }

    #[test]
    fn test_sarif_has_region() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"startLine\": 5"));
        assert!(sarif.contains("\"startColumn\": 10"));
    }

    #[test]
    fn test_sarif_has_fixes() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("\"fixes\""));
        assert!(sarif.contains("Fix it"));
    }

    #[test]
    fn test_sarif_has_snippet() {
        let result = make_result(vec![make_diag("HDR001", Severity::Error)]);
        let sarif = to_sarif(&result).unwrap();
        assert!(sarif.contains("-- name: test"));
    }

    #[test]
    fn test_sarif_no_fixes_when_no_suggestion() {
        let mut diag = make_diag("HDR001", Severity::Error);
        diag.suggestion = None;
        let result = make_result(vec![diag]);
        let sarif = to_sarif(&result).unwrap();
        // Should not contain "fixes" key when suggestion is None
        let parsed: serde_json::Value = serde_json::from_str(&sarif).unwrap();
        let results = &parsed["runs"][0]["results"][0];
        assert!(results.get("fixes").is_none());
    }

    #[test]
    fn test_sarif_empty_result() {
        let result = make_result(vec![]);
        let sarif = to_sarif(&result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&sarif).unwrap();
        let results = &parsed["runs"][0]["results"];
        assert!(results.as_array().unwrap().is_empty());
    }

    #[test]
    fn test_sarif_multiple_rules_deduplicated() {
        let result = make_result(vec![
            make_diag("HDR001", Severity::Error),
            make_diag("HDR001", Severity::Error),
            make_diag("REF001", Severity::Error),
        ]);
        let sarif = to_sarif(&result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&sarif).unwrap();
        let rules = &parsed["runs"][0]["tool"]["driver"]["rules"];
        assert_eq!(rules.as_array().unwrap().len(), 2); // HDR001, REF001
    }

    #[test]
    fn test_sarif_rules_sorted_by_id() {
        let result = make_result(vec![
            make_diag("REF001", Severity::Error),
            make_diag("HDR001", Severity::Error),
        ]);
        let sarif = to_sarif(&result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&sarif).unwrap();
        let rules = parsed["runs"][0]["tool"]["driver"]["rules"]
            .as_array()
            .unwrap();
        assert_eq!(rules[0]["id"].as_str().unwrap(), "HDR001");
        assert_eq!(rules[1]["id"].as_str().unwrap(), "REF001");
    }

    // --- Helper function tests ---

    #[test]
    fn test_severity_to_sarif_level() {
        assert_eq!(severity_to_sarif_level(Severity::Error), "error");
        assert_eq!(severity_to_sarif_level(Severity::Warning), "warning");
        assert_eq!(severity_to_sarif_level(Severity::Info), "note");
    }

    #[test]
    fn test_rule_id_to_name() {
        assert_eq!(rule_id_to_name("HDR001"), "Header001");
        assert_eq!(rule_id_to_name("SQL004"), "SqlSyntax004");
        assert_eq!(rule_id_to_name("SAF001"), "SqlSafety001");
        assert_eq!(rule_id_to_name("REF001"), "RefResolution001");
        assert_eq!(rule_id_to_name("NAME005"), "NamingConvention005");
        assert_eq!(rule_id_to_name("YML001"), "YamlSchema001");
        assert_eq!(rule_id_to_name("DBT001"), "DbtProject001");
        assert_eq!(rule_id_to_name("CON001"), "ModelConsistency001");
    }

    #[test]
    fn test_rule_id_to_short_description_known() {
        assert_eq!(
            rule_id_to_short_description("HDR001"),
            "Missing required 'name' header field"
        );
        assert_eq!(
            rule_id_to_short_description("YML001"),
            "Invalid YAML syntax"
        );
        assert_eq!(
            rule_id_to_short_description("CON001"),
            "Duplicate model name"
        );
    }

    #[test]
    fn test_rule_id_to_short_description_unknown() {
        let desc = rule_id_to_short_description("UNKNOWN999");
        assert!(desc.contains("UNKNOWN999"));
    }

    #[test]
    fn test_sarif_no_region_when_line_zero() {
        let mut diag = make_diag("HDR001", Severity::Error);
        diag.line = 0;
        diag.column = 0;
        let result = make_result(vec![diag]);
        let sarif = to_sarif(&result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&sarif).unwrap();
        let location = &parsed["runs"][0]["results"][0]["locations"][0]["physicalLocation"];
        assert!(location.get("region").is_none());
    }

    #[test]
    fn test_sarif_phase3_rule_ids() {
        let yml_diag = CheckDiagnostic {
            rule_id: "YML001".into(),
            message: "Invalid YAML syntax".into(),
            severity: Severity::Error,
            category: CheckCategory::YamlSchema,
            file_path: "schema.yml".into(),
            line: 3,
            column: 1,
            snippet: None,
            suggestion: Some("Fix YAML indentation".into()),
            doc_url: Some("https://docs.ironlayer.app/check/rules/YML001".into()),
        };
        let dbt_diag = CheckDiagnostic {
            rule_id: "DBT001".into(),
            message: "Missing dbt_project.yml".into(),
            severity: Severity::Error,
            category: CheckCategory::DbtProject,
            file_path: ".".into(),
            line: 0,
            column: 0,
            snippet: None,
            suggestion: Some("Create a dbt_project.yml file".into()),
            doc_url: Some("https://docs.ironlayer.app/check/rules/DBT001".into()),
        };
        let con_diag = CheckDiagnostic {
            rule_id: "CON001".into(),
            message: "Duplicate model name 'stg_orders'".into(),
            severity: Severity::Error,
            category: CheckCategory::ModelConsistency,
            file_path: "models/stg_orders.sql".into(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: Some("https://docs.ironlayer.app/check/rules/CON001".into()),
        };
        let result = make_result(vec![yml_diag, dbt_diag, con_diag]);
        let sarif = to_sarif(&result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&sarif).unwrap();

        let rules = parsed["runs"][0]["tool"]["driver"]["rules"]
            .as_array()
            .unwrap();
        let rule_ids: Vec<&str> = rules.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert!(rule_ids.contains(&"CON001"));
        assert!(rule_ids.contains(&"DBT001"));
        assert!(rule_ids.contains(&"YML001"));

        let results = parsed["runs"][0]["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
    }
}
