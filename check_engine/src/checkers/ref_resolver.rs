//! Ref resolution checker — rules REF001 through REF006.
//!
//! Validates `{{ ref('...') }}` usage across the project, ensuring that
//! references resolve to existing models, no circular dependencies exist,
//! models don't reference themselves, fully-qualified names are flagged
//! when short names suffice, ambiguous short names are detected, and
//! hardcoded table references are identified.
//!
//! ## Rule Summary
//!
//! | Rule   | Default  | Severity | Description                                          | Phase         |
//! |--------|----------|----------|------------------------------------------------------|---------------|
//! | REF001 | enabled  | error    | Ref references a model that doesn't exist            | check_project |
//! | REF002 | enabled  | warning  | Circular ref dependency detected                     | check_project |
//! | REF003 | enabled  | warning  | Self-referential ref                                 | check_file    |
//! | REF004 | enabled  | info     | Fully-qualified name where short name suffices        | check_project |
//! | REF005 | enabled  | warning  | Ambiguous short name across schemas                  | check_project |
//! | REF006 | disabled | info     | Hardcoded table reference instead of ref()           | check_file    |

use std::collections::{HashMap, HashSet};

use crate::checkers::Checker;
use crate::config::CheckConfig;
use crate::sql_lexer::{tokenize, TokenKind};
use crate::types::{CheckCategory, CheckDiagnostic, DiscoveredModel, Severity};

/// Ref resolution checker validating `{{ ref('...') }}` usage.
pub struct RefResolverChecker;

impl Checker for RefResolverChecker {
    fn name(&self) -> &'static str {
        "ref_resolver"
    }

    /// Per-file checks: REF003 (self-ref) and REF006 (hardcoded table reference).
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

        let mut diags = Vec::new();

        // REF003: Self-referential ref
        if config.is_rule_enabled_for_path("REF003", file_path, true) {
            check_ref003(file_path, content, model, config, &mut diags);
        }

        // REF006: Hardcoded table reference (disabled by default)
        if config.is_rule_enabled_for_path("REF006", file_path, false) {
            check_ref006(file_path, content, config, &mut diags);
        }

        diags
    }

    /// Cross-file checks: REF001 (undefined ref), REF002 (circular),
    /// REF004 (fully-qualified), REF005 (ambiguous short name).
    fn check_project(
        &self,
        models: &[DiscoveredModel],
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        let sql_models: Vec<&DiscoveredModel> = models
            .iter()
            .filter(|m| m.file_path.ends_with(".sql"))
            .collect();

        let registry = build_model_registry(&sql_models);

        if config.is_rule_enabled("REF001", true) {
            check_ref001(&sql_models, &registry, config, &mut diags);
        }

        if config.is_rule_enabled("REF002", true) {
            check_ref002(&sql_models, &registry, config, &mut diags);
        }

        if config.is_rule_enabled("REF004", true) {
            check_ref004(&sql_models, &registry, config, &mut diags);
        }

        if config.is_rule_enabled("REF005", true) {
            check_ref005(&sql_models, config, &mut diags);
        }

        diags
    }
}

// ---------------------------------------------------------------------------
// Model registry
// ---------------------------------------------------------------------------

/// Maps model names (both short and canonical) to their canonical forms.
///
/// For model `"analytics.orders_daily"`:
///   - `"orders_daily"` -> `["analytics.orders_daily"]`
///   - `"analytics.orders_daily"` -> `["analytics.orders_daily"]`
///
/// For model `"stg_users"` (no dot):
///   - `"stg_users"` -> `["stg_users"]`
struct ModelRegistry {
    /// Maps short and canonical names to lists of canonical names.
    name_to_canonical: HashMap<String, Vec<String>>,
    /// Set of all canonical model names.
    all_canonical: HashSet<String>,
}

/// Build the model registry from discovered SQL models.
fn build_model_registry(models: &[&DiscoveredModel]) -> ModelRegistry {
    let mut name_to_canonical: HashMap<String, Vec<String>> = HashMap::new();
    let mut all_canonical: HashSet<String> = HashSet::new();

    for model in models {
        let canonical = model.name.clone();
        all_canonical.insert(canonical.clone());

        // Map canonical name -> itself
        name_to_canonical
            .entry(canonical.clone())
            .or_default()
            .push(canonical.clone());

        // If name has a dot, also map the short name
        if let Some(dot_pos) = model.name.rfind('.') {
            let short_name = &model.name[dot_pos + 1..];
            if !short_name.is_empty() {
                name_to_canonical
                    .entry(short_name.to_owned())
                    .or_default()
                    .push(canonical);
            }
        }
    }

    ModelRegistry {
        name_to_canonical,
        all_canonical,
    }
}

/// Check if a ref_name resolves to at least one model.
fn ref_resolves(registry: &ModelRegistry, ref_name: &str) -> bool {
    registry
        .name_to_canonical
        .get(ref_name)
        .is_some_and(|v| !v.is_empty())
}

/// Collect all known lookup names for fuzzy matching.
fn all_known_names(registry: &ModelRegistry) -> Vec<&str> {
    registry
        .name_to_canonical
        .keys()
        .map(|s| s.as_str())
        .collect()
}

/// Count distinct canonical names a short name maps to.
fn unique_canonical_count(registry: &ModelRegistry, name: &str) -> usize {
    registry.name_to_canonical.get(name).map_or(0, |v| {
        let unique: HashSet<&str> = v.iter().map(|s| s.as_str()).collect();
        unique.len()
    })
}

// ---------------------------------------------------------------------------
// Levenshtein distance (full Wagner-Fischer)
// ---------------------------------------------------------------------------

/// Compute the Levenshtein edit distance between two strings.
///
/// Uses the full Wagner-Fischer dynamic programming algorithm with two
/// rows for O(min(m,n)) space. NOT a simplified approximation.
fn levenshtein(a: &str, b: &str) -> usize {
    let n = a.len();
    let m = b.len();
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let mut prev = vec![0usize; m + 1];
    let mut curr = vec![0usize; m + 1];

    for (j, item) in prev.iter_mut().enumerate().take(m + 1) {
        *item = j;
    }

    for i in 1..=n {
        curr[0] = i;
        for j in 1..=m {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[m]
}

/// Find the closest match to `name` among `candidates` within `max_distance`.
fn find_closest_match<'a>(
    name: &str,
    candidates: &[&'a str],
    max_distance: usize,
) -> Option<(&'a str, usize)> {
    let mut best: Option<(&str, usize)> = None;

    for &candidate in candidates {
        if candidate == name {
            continue;
        }
        let dist = levenshtein(name, candidate);
        if dist <= max_distance {
            match best {
                Some((_, best_dist)) if dist < best_dist => {
                    best = Some((candidate, dist));
                }
                None => {
                    best = Some((candidate, dist));
                }
                _ => {}
            }
        }
    }

    best
}

// ---------------------------------------------------------------------------
// Helper: find the line number of a ref() call
// ---------------------------------------------------------------------------

/// Find the 1-based line number where `{{ ref('name') }}` appears.
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
// REF001: Undefined ref (check_project)
// ---------------------------------------------------------------------------

/// REF001: `{{ ref('model_name') }}` references a model that doesn't exist.
fn check_ref001(
    models: &[&DiscoveredModel],
    registry: &ModelRegistry,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let known_names = all_known_names(registry);

    for model in models {
        if !config.is_rule_enabled_for_path("REF001", &model.file_path, true) {
            continue;
        }

        for ref_name in &model.ref_names {
            if ref_resolves(registry, ref_name) {
                continue;
            }

            let severity =
                config.effective_severity_for_path("REF001", &model.file_path, Severity::Error);

            let suggestion = match find_closest_match(ref_name, &known_names, 3) {
                Some((match_name, _)) => Some(format!(
                    "Did you mean '{match_name}'? Check for typos in the model name."
                )),
                None => Some(
                    "Ensure the referenced model exists in the project and the \
                     name matches its '-- name:' header exactly."
                        .to_owned(),
                ),
            };

            let line = find_ref_line(&model.content, ref_name);

            diags.push(CheckDiagnostic {
                rule_id: "REF001".to_owned(),
                message: format!(
                    "Undefined ref: {{{{ ref('{}') }}}} in model '{}' references \
                     a model that does not exist in this project.",
                    ref_name, model.name
                ),
                severity,
                category: CheckCategory::RefResolution,
                file_path: model.file_path.clone(),
                line,
                column: 0,
                snippet: Some(format!("{{{{ ref('{ref_name}') }}}}")),
                suggestion,
                doc_url: Some("https://docs.ironlayer.app/check/rules/REF001".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// REF002: Circular dependency (check_project)
// ---------------------------------------------------------------------------

/// REF002: Circular ref dependency detected (e.g., A -> B -> A).
fn check_ref002(
    models: &[&DiscoveredModel],
    registry: &ModelRegistry,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Build adjacency list: model canonical name -> resolved ref targets
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    let model_name_to_file: HashMap<&str, &str> = models
        .iter()
        .map(|m| (m.name.as_str(), m.file_path.as_str()))
        .collect();

    for model in models {
        let mut targets = Vec::new();
        for ref_name in &model.ref_names {
            if let Some(canonicals) = registry.name_to_canonical.get(ref_name.as_str()) {
                for canonical in canonicals {
                    if registry.all_canonical.contains(canonical.as_str()) {
                        targets.push(canonical.as_str());
                    }
                }
            }
        }
        adjacency.insert(model.name.as_str(), targets);
    }

    // DFS cycle detection
    let all_names: Vec<&str> = models.iter().map(|m| m.name.as_str()).collect();
    let mut visited: HashSet<&str> = HashSet::new();
    let mut in_stack: HashSet<&str> = HashSet::new();
    let mut cycle_participants: HashSet<&str> = HashSet::new();

    for &start in &all_names {
        if !visited.contains(start) {
            dfs_detect_cycles(
                start,
                &adjacency,
                &mut visited,
                &mut in_stack,
                &mut cycle_participants,
                &mut Vec::new(),
            );
        }
    }

    for &participant in &cycle_participants {
        let file_path = match model_name_to_file.get(participant) {
            Some(fp) => *fp,
            None => continue,
        };

        if !config.is_rule_enabled_for_path("REF002", file_path, true) {
            continue;
        }

        let severity = config.effective_severity_for_path("REF002", file_path, Severity::Warning);

        let deps = adjacency
            .get(participant)
            .map_or(&[] as &[&str], |v| v.as_slice());
        let cyclic_deps: Vec<&str> = deps
            .iter()
            .filter(|d| cycle_participants.contains(**d))
            .copied()
            .collect();

        diags.push(CheckDiagnostic {
            rule_id: "REF002".to_owned(),
            message: format!(
                "Circular dependency detected: model '{}' is part of a \
                 dependency cycle involving: {}. Circular references prevent \
                 correct execution ordering.",
                participant,
                cyclic_deps.join(", ")
            ),
            severity,
            category: CheckCategory::RefResolution,
            file_path: file_path.to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: Some(
                "Break the circular dependency by refactoring one of the \
                 models to remove the cyclical ref() call."
                    .to_owned(),
            ),
            doc_url: Some("https://docs.ironlayer.app/check/rules/REF002".to_owned()),
        });
    }
}

/// DFS helper for cycle detection with visited set and recursion stack.
fn dfs_detect_cycles<'a>(
    node: &'a str,
    adjacency: &HashMap<&'a str, Vec<&'a str>>,
    visited: &mut HashSet<&'a str>,
    in_stack: &mut HashSet<&'a str>,
    cycle_participants: &mut HashSet<&'a str>,
    path: &mut Vec<&'a str>,
) {
    visited.insert(node);
    in_stack.insert(node);
    path.push(node);

    if let Some(neighbors) = adjacency.get(node) {
        for &neighbor in neighbors {
            if !visited.contains(neighbor) {
                dfs_detect_cycles(
                    neighbor,
                    adjacency,
                    visited,
                    in_stack,
                    cycle_participants,
                    path,
                );
            } else if in_stack.contains(neighbor) {
                // Found a cycle: mark all nodes from neighbor to end of path
                if let Some(start_idx) = path.iter().position(|&n| n == neighbor) {
                    for &cycle_node in &path[start_idx..] {
                        cycle_participants.insert(cycle_node);
                    }
                    cycle_participants.insert(neighbor);
                }
            }
        }
    }

    path.pop();
    in_stack.remove(node);
}

// ---------------------------------------------------------------------------
// REF003: Self-referential ref (check_file)
// ---------------------------------------------------------------------------

/// REF003: A model references itself via `{{ ref('own_name') }}`.
fn check_ref003(
    file_path: &str,
    content: &str,
    model: Option<&DiscoveredModel>,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let model = match model {
        Some(m) => m,
        None => return,
    };

    for ref_name in &model.ref_names {
        let is_self_ref = ref_name == &model.name
            || model
                .name
                .rfind('.')
                .map(|pos| &model.name[pos + 1..] == ref_name.as_str())
                .unwrap_or(false);

        if is_self_ref {
            let severity =
                config.effective_severity_for_path("REF003", file_path, Severity::Warning);
            let line = find_ref_line(content, ref_name);

            diags.push(CheckDiagnostic {
                rule_id: "REF003".to_owned(),
                message: format!(
                    "Self-referential ref: model '{}' references itself via \
                     {{{{ ref('{}') }}}}. A model cannot depend on its own output.",
                    model.name, ref_name
                ),
                severity,
                category: CheckCategory::RefResolution,
                file_path: file_path.to_owned(),
                line,
                column: 0,
                snippet: Some(format!("{{{{ ref('{ref_name}') }}}}")),
                suggestion: Some(
                    "Remove the self-referential ref() call. If you need to \
                     reference the model's own data, consider using a CTE or \
                     a separate staging model."
                        .to_owned(),
                ),
                doc_url: Some("https://docs.ironlayer.app/check/rules/REF003".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// REF004: Fully-qualified ref (check_project)
// ---------------------------------------------------------------------------

/// REF004: A ref uses a fully-qualified name (e.g., `schema.model`) where
/// the short name alone would uniquely resolve.
fn check_ref004(
    models: &[&DiscoveredModel],
    registry: &ModelRegistry,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    for model in models {
        if !config.is_rule_enabled_for_path("REF004", &model.file_path, true) {
            continue;
        }

        for ref_name in &model.ref_names {
            if let Some(dot_pos) = ref_name.rfind('.') {
                let short_name = &ref_name[dot_pos + 1..];
                if short_name.is_empty() {
                    continue;
                }

                // Only fire if the short name uniquely resolves
                if unique_canonical_count(registry, short_name) == 1 {
                    let severity = config.effective_severity_for_path(
                        "REF004",
                        &model.file_path,
                        Severity::Info,
                    );
                    let line = find_ref_line(&model.content, ref_name);

                    diags.push(CheckDiagnostic {
                        rule_id: "REF004".to_owned(),
                        message: format!(
                            "Fully-qualified ref '{}' can be simplified to \
                             '{}'. The short name uniquely resolves to a \
                             single model.",
                            ref_name, short_name
                        ),
                        severity,
                        category: CheckCategory::RefResolution,
                        file_path: model.file_path.clone(),
                        line,
                        column: 0,
                        snippet: Some(format!("{{{{ ref('{ref_name}') }}}}")),
                        suggestion: Some(format!(
                            "Replace {{{{ ref('{ref_name}') }}}} with \
                             {{{{ ref('{short_name}') }}}}."
                        )),
                        doc_url: Some("https://docs.ironlayer.app/check/rules/REF004".to_owned()),
                    });
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// REF005: Ambiguous short name (check_project)
// ---------------------------------------------------------------------------

/// REF005: Two or more models share the same short name but have different
/// fully-qualified (canonical) names.
fn check_ref005(
    models: &[&DiscoveredModel],
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    // Group by short name -> (canonical_name, file_path)
    let mut short_to_models: HashMap<String, Vec<(&str, &str)>> = HashMap::new();

    for model in models {
        let short_name = if let Some(dot_pos) = model.name.rfind('.') {
            &model.name[dot_pos + 1..]
        } else {
            model.name.as_str()
        };

        if !short_name.is_empty() {
            short_to_models
                .entry(short_name.to_owned())
                .or_default()
                .push((model.name.as_str(), model.file_path.as_str()));
        }
    }

    for (short_name, model_list) in &short_to_models {
        let unique_canonicals: HashSet<&str> = model_list.iter().map(|(name, _)| *name).collect();

        if unique_canonicals.len() < 2 {
            continue;
        }

        for &(canonical_name, file_path) in model_list {
            if !config.is_rule_enabled_for_path("REF005", file_path, true) {
                continue;
            }

            let severity =
                config.effective_severity_for_path("REF005", file_path, Severity::Warning);

            let other_canonicals: Vec<&str> = unique_canonicals
                .iter()
                .filter(|&&n| n != canonical_name)
                .copied()
                .collect();

            diags.push(CheckDiagnostic {
                rule_id: "REF005".to_owned(),
                message: format!(
                    "Ambiguous short name: '{}' resolves to multiple models: \
                     {}. Use fully-qualified names to avoid ambiguity.",
                    short_name,
                    unique_canonicals
                        .iter()
                        .copied()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                severity,
                category: CheckCategory::RefResolution,
                file_path: file_path.to_owned(),
                line: 1,
                column: 0,
                snippet: None,
                suggestion: Some(format!(
                    "Use the fully-qualified name (e.g., \
                     {{{{ ref('{}') }}}}) instead of the short name '{}'. \
                     Other models with this name: {}.",
                    canonical_name,
                    short_name,
                    other_canonicals.join(", ")
                )),
                doc_url: Some("https://docs.ironlayer.app/check/rules/REF005".to_owned()),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// REF006: Hardcoded table reference (check_file, disabled by default)
// ---------------------------------------------------------------------------

/// REF006: A SQL file uses a direct table reference (`FROM schema.table` or
/// `JOIN schema.table`) instead of `{{ ref('...') }}`.
///
/// Uses the SQL lexer to detect `FROM`/`JOIN` followed by
/// `Identifier.Identifier` outside of Jinja `{{ }}` blocks.
fn check_ref006(
    file_path: &str,
    content: &str,
    config: &CheckConfig,
    diags: &mut Vec<CheckDiagnostic>,
) {
    let tokens = tokenize(content);
    let significant: Vec<_> = tokens
        .iter()
        .filter(|t| {
            t.kind != TokenKind::Whitespace
                && t.kind != TokenKind::Newline
                && t.kind != TokenKind::LineComment
                && t.kind != TokenKind::BlockComment
        })
        .collect();

    let mut jinja_depth: u32 = 0;
    let mut i = 0;

    while i < significant.len() {
        let tok = significant[i];

        if tok.kind == TokenKind::JinjaOpen {
            jinja_depth += 1;
            i += 1;
            continue;
        }
        if tok.kind == TokenKind::JinjaClose {
            jinja_depth = jinja_depth.saturating_sub(1);
            i += 1;
            continue;
        }

        if jinja_depth > 0 {
            i += 1;
            continue;
        }

        // Detect FROM/JOIN keyword followed by Ident.Ident
        if tok.kind == TokenKind::Keyword {
            let upper = tok.text.to_ascii_uppercase();
            let is_table_source = upper == "FROM" || upper == "JOIN";

            // For INNER/LEFT/RIGHT/FULL/CROSS, look ahead for JOIN
            let join_offset = if !is_table_source
                && (upper == "INNER"
                    || upper == "LEFT"
                    || upper == "RIGHT"
                    || upper == "FULL"
                    || upper == "CROSS")
            {
                find_join_after(i, &significant)
            } else {
                None
            };

            let source_idx = if is_table_source {
                Some(i)
            } else {
                join_offset
            };

            if let Some(src) = source_idx {
                let after = src + 1;
                if after + 2 < significant.len()
                    && significant[after].kind == TokenKind::Identifier
                    && significant[after + 1].kind == TokenKind::Dot
                    && significant[after + 2].kind == TokenKind::Identifier
                {
                    let schema = significant[after].text;
                    let table = significant[after + 2].text;
                    let severity =
                        config.effective_severity_for_path("REF006", file_path, Severity::Info);

                    diags.push(CheckDiagnostic {
                        rule_id: "REF006".to_owned(),
                        message: format!(
                            "Hardcoded table reference '{schema}.{table}' \
                             found. Consider using {{{{ ref('{table}') }}}} \
                             instead for dependency tracking and environment \
                             portability."
                        ),
                        severity,
                        category: CheckCategory::RefResolution,
                        file_path: file_path.to_owned(),
                        line: significant[after].line,
                        column: significant[after].column,
                        snippet: Some(format!("{schema}.{table}")),
                        suggestion: Some(format!(
                            "Replace '{schema}.{table}' with \
                             {{{{ ref('{table}') }}}}."
                        )),
                        doc_url: Some("https://docs.ironlayer.app/check/rules/REF006".to_owned()),
                    });
                }
            }
        }

        i += 1;
    }
}

/// Look ahead from position `i` (which is INNER/LEFT/etc.) for a JOIN keyword
/// within the next 3 tokens.
fn find_join_after(i: usize, tokens: &[&crate::sql_lexer::Token<'_>]) -> Option<usize> {
    let start = i + 1;
    let end = tokens.len().min(i + 4);
    tokens[start..end]
        .iter()
        .enumerate()
        .find(|(_, t)| t.kind == TokenKind::Keyword && t.text.eq_ignore_ascii_case("JOIN"))
        .map(|(offset, _)| start + offset)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuleSeverityOverride;
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

    // -------------------------------------------------------------------
    // Levenshtein tests
    // -------------------------------------------------------------------

    #[test]
    fn test_levenshtein_identical() {
        assert_eq!(levenshtein("hello", "hello"), 0);
    }

    #[test]
    fn test_levenshtein_empty() {
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("", ""), 0);
    }

    #[test]
    fn test_levenshtein_substitution() {
        assert_eq!(levenshtein("cat", "bat"), 1);
    }

    #[test]
    fn test_levenshtein_insertion() {
        assert_eq!(levenshtein("cat", "cats"), 1);
    }

    #[test]
    fn test_levenshtein_deletion() {
        assert_eq!(levenshtein("cats", "cat"), 1);
    }

    #[test]
    fn test_levenshtein_complex() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
    }

    // -------------------------------------------------------------------
    // REF001: Undefined ref
    // -------------------------------------------------------------------

    #[test]
    fn test_ref001_undefined_ref_fires() {
        let checker = RefResolverChecker;
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
                "fct_revenue",
                "models/fct_revenue.sql",
                "-- name: fct_revenue\nSELECT * FROM {{ ref('nonexistent_model') }}",
                vec!["nonexistent_model"],
                HashMap::from([("name", "fct_revenue")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let ref001: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF001").collect();
        assert_eq!(ref001.len(), 1);
        assert!(ref001[0].message.contains("nonexistent_model"));
        assert_eq!(ref001[0].severity, Severity::Error);
        assert_eq!(ref001[0].category, CheckCategory::RefResolution);
    }

    #[test]
    fn test_ref001_valid_ref_passes() {
        let checker = RefResolverChecker;
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
                "fct_revenue",
                "models/fct_revenue.sql",
                "-- name: fct_revenue\nSELECT * FROM {{ ref('stg_orders') }}",
                vec!["stg_orders"],
                HashMap::from([("name", "fct_revenue")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF001"));
    }

    #[test]
    fn test_ref001_fuzzy_suggestion() {
        let checker = RefResolverChecker;
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
                "fct_revenue",
                "models/fct_revenue.sql",
                "-- name: fct_revenue\nSELECT * FROM {{ ref('stg_orderz') }}",
                vec!["stg_orderz"],
                HashMap::from([("name", "fct_revenue")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let ref001: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF001").collect();
        assert_eq!(ref001.len(), 1);
        let suggestion = ref001[0]
            .suggestion
            .as_ref()
            .expect("should have suggestion");
        assert!(
            suggestion.contains("stg_orders"),
            "Suggestion should suggest 'stg_orders', got: {suggestion}"
        );
    }

    #[test]
    fn test_ref001_disabled_via_config() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF001".to_owned(), RuleSeverityOverride::Off);

        let models = vec![make_model(
            "fct",
            "models/fct.sql",
            "SELECT * FROM {{ ref('missing') }}",
            vec!["missing"],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF001"));
    }

    #[test]
    fn test_ref001_short_name_resolves_to_qualified() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "analytics.orders_daily",
                "models/orders_daily.sql",
                "-- name: analytics.orders_daily\nSELECT 1",
                vec![],
                HashMap::from([("name", "analytics.orders_daily")]),
            ),
            make_model(
                "fct_summary",
                "models/fct_summary.sql",
                "-- name: fct_summary\nSELECT * FROM {{ ref('orders_daily') }}",
                vec!["orders_daily"],
                HashMap::from([("name", "fct_summary")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(
            !diags.iter().any(|d| d.rule_id == "REF001"),
            "Short name should resolve to the qualified model"
        );
    }

    // -------------------------------------------------------------------
    // REF002: Circular dependency
    // -------------------------------------------------------------------

    #[test]
    fn test_ref002_ab_cycle_detected() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "model_a",
                "models/model_a.sql",
                "SELECT * FROM {{ ref('model_b') }}",
                vec!["model_b"],
                HashMap::new(),
            ),
            make_model(
                "model_b",
                "models/model_b.sql",
                "SELECT * FROM {{ ref('model_a') }}",
                vec!["model_a"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let ref002: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF002").collect();
        assert_eq!(ref002.len(), 2, "Both models in cycle should be flagged");
        assert!(ref002.iter().any(|d| d.message.contains("model_a")));
        assert!(ref002.iter().any(|d| d.message.contains("model_b")));
        assert_eq!(ref002[0].severity, Severity::Warning);
    }

    #[test]
    fn test_ref002_no_cycle_passes() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "stg_users",
                "models/stg_users.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "fct_users",
                "models/fct_users.sql",
                "SELECT * FROM {{ ref('stg_users') }}",
                vec!["stg_users"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF002"));
    }

    #[test]
    fn test_ref002_three_node_cycle() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "model_a",
                "models/model_a.sql",
                "SELECT * FROM {{ ref('model_b') }}",
                vec!["model_b"],
                HashMap::new(),
            ),
            make_model(
                "model_b",
                "models/model_b.sql",
                "SELECT * FROM {{ ref('model_c') }}",
                vec!["model_c"],
                HashMap::new(),
            ),
            make_model(
                "model_c",
                "models/model_c.sql",
                "SELECT * FROM {{ ref('model_a') }}",
                vec!["model_a"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let ref002: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF002").collect();
        assert_eq!(ref002.len(), 3, "All 3 models in cycle should be flagged");
    }

    #[test]
    fn test_ref002_disabled_via_config() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF002".to_owned(), RuleSeverityOverride::Off);

        let models = vec![
            make_model(
                "a",
                "models/a.sql",
                "SELECT * FROM {{ ref('b') }}",
                vec!["b"],
                HashMap::new(),
            ),
            make_model(
                "b",
                "models/b.sql",
                "SELECT * FROM {{ ref('a') }}",
                vec!["a"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF002"));
    }

    // -------------------------------------------------------------------
    // REF003: Self-referential ref
    // -------------------------------------------------------------------

    #[test]
    fn test_ref003_self_ref_fires() {
        let checker = RefResolverChecker;
        let config = default_config();
        let content = "-- name: stg_orders\nSELECT * FROM {{ ref('stg_orders') }}";
        let model = make_model(
            "stg_orders",
            "models/stg_orders.sql",
            content,
            vec!["stg_orders"],
            HashMap::from([("name", "stg_orders")]),
        );

        let diags = checker.check_file("models/stg_orders.sql", content, Some(&model), &config);
        let ref003: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF003").collect();
        assert_eq!(ref003.len(), 1);
        assert_eq!(ref003[0].severity, Severity::Warning);
    }

    #[test]
    fn test_ref003_non_self_ref_passes() {
        let checker = RefResolverChecker;
        let config = default_config();
        let content = "-- name: fct_revenue\nSELECT * FROM {{ ref('stg_orders') }}";
        let model = make_model(
            "fct_revenue",
            "models/fct_revenue.sql",
            content,
            vec!["stg_orders"],
            HashMap::from([("name", "fct_revenue")]),
        );

        let diags = checker.check_file("models/fct_revenue.sql", content, Some(&model), &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF003"));
    }

    #[test]
    fn test_ref003_qualified_name_self_ref() {
        let checker = RefResolverChecker;
        let config = default_config();
        let content = "-- name: analytics.orders\nSELECT * FROM {{ ref('orders') }}";
        let model = make_model(
            "analytics.orders",
            "models/orders.sql",
            content,
            vec!["orders"],
            HashMap::from([("name", "analytics.orders")]),
        );

        let diags = checker.check_file("models/orders.sql", content, Some(&model), &config);
        let ref003: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF003").collect();
        assert_eq!(
            ref003.len(),
            1,
            "Short name matching qualified model should fire"
        );
    }

    #[test]
    fn test_ref003_no_model_no_diagnostic() {
        let checker = RefResolverChecker;
        let config = default_config();
        let content = "SELECT * FROM {{ ref('stg_orders') }}";

        let diags = checker.check_file("models/unknown.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF003"));
    }

    #[test]
    fn test_ref003_disabled_via_config() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF003".to_owned(), RuleSeverityOverride::Off);
        let content = "-- name: stg_orders\nSELECT * FROM {{ ref('stg_orders') }}";
        let model = make_model(
            "stg_orders",
            "models/stg_orders.sql",
            content,
            vec!["stg_orders"],
            HashMap::from([("name", "stg_orders")]),
        );

        let diags = checker.check_file("models/stg_orders.sql", content, Some(&model), &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF003"));
    }

    // -------------------------------------------------------------------
    // REF004: Fully-qualified ref
    // -------------------------------------------------------------------

    #[test]
    fn test_ref004_fully_qualified_fires() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "analytics.orders_daily",
                "models/orders_daily.sql",
                "-- name: analytics.orders_daily\nSELECT 1",
                vec![],
                HashMap::from([("name", "analytics.orders_daily")]),
            ),
            make_model(
                "fct_summary",
                "models/fct_summary.sql",
                "-- name: fct_summary\nSELECT * FROM {{ ref('analytics.orders_daily') }}",
                vec!["analytics.orders_daily"],
                HashMap::from([("name", "fct_summary")]),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let ref004: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF004").collect();
        assert_eq!(ref004.len(), 1);
        assert_eq!(ref004[0].severity, Severity::Info);
        assert!(ref004[0].message.contains("orders_daily"));
    }

    #[test]
    fn test_ref004_short_name_passes() {
        let checker = RefResolverChecker;
        let config = default_config();

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
        assert!(!diags.iter().any(|d| d.rule_id == "REF004"));
    }

    #[test]
    fn test_ref004_ambiguous_short_no_fire() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "analytics.orders",
                "models/analytics/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "staging.orders",
                "models/staging/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "fct_summary",
                "models/fct_summary.sql",
                "SELECT * FROM {{ ref('analytics.orders') }}",
                vec!["analytics.orders"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(
            !diags.iter().any(|d| d.rule_id == "REF004"),
            "REF004 should not fire when short name is ambiguous"
        );
    }

    #[test]
    fn test_ref004_disabled_via_config() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF004".to_owned(), RuleSeverityOverride::Off);

        let models = vec![
            make_model(
                "analytics.orders",
                "models/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "fct",
                "models/fct.sql",
                "SELECT * FROM {{ ref('analytics.orders') }}",
                vec!["analytics.orders"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF004"));
    }

    // -------------------------------------------------------------------
    // REF005: Ambiguous short name
    // -------------------------------------------------------------------

    #[test]
    fn test_ref005_ambiguous_names_detected() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "analytics.orders",
                "models/analytics/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "staging.orders",
                "models/staging/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        let ref005: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF005").collect();
        assert_eq!(
            ref005.len(),
            2,
            "Both models with ambiguous name should fire"
        );
        assert_eq!(ref005[0].severity, Severity::Warning);
    }

    #[test]
    fn test_ref005_unique_names_pass() {
        let checker = RefResolverChecker;
        let config = default_config();

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
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF005"));
    }

    #[test]
    fn test_ref005_same_canonical_not_ambiguous() {
        let checker = RefResolverChecker;
        let config = default_config();

        // Same canonical names => CON001's job, not REF005
        let models = vec![
            make_model(
                "orders",
                "models/v1/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "orders",
                "models/v2/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(
            !diags.iter().any(|d| d.rule_id == "REF005"),
            "Same canonical name is not REF005"
        );
    }

    #[test]
    fn test_ref005_disabled_via_config() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF005".to_owned(), RuleSeverityOverride::Off);

        let models = vec![
            make_model(
                "analytics.orders",
                "models/analytics/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
            make_model(
                "staging.orders",
                "models/staging/orders.sql",
                "SELECT 1",
                vec![],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF005"));
    }

    // -------------------------------------------------------------------
    // REF006: Hardcoded table reference
    // -------------------------------------------------------------------

    #[test]
    fn test_ref006_hardcoded_table_fires_when_enabled() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF006".to_owned(), RuleSeverityOverride::Info);

        let content = "SELECT * FROM staging.orders WHERE 1=1";

        let diags = checker.check_file("models/fct.sql", content, None, &config);
        let ref006: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF006").collect();
        assert_eq!(ref006.len(), 1);
        assert!(ref006[0].message.contains("staging.orders"));
        assert_eq!(ref006[0].severity, Severity::Info);
    }

    #[test]
    fn test_ref006_ref_usage_passes() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF006".to_owned(), RuleSeverityOverride::Info);

        let content = "SELECT * FROM {{ ref('orders') }} WHERE 1=1";

        let diags = checker.check_file("models/fct.sql", content, None, &config);
        assert!(!diags.iter().any(|d| d.rule_id == "REF006"));
    }

    #[test]
    fn test_ref006_disabled_by_default() {
        let checker = RefResolverChecker;
        let config = default_config();

        let content = "SELECT * FROM staging.orders WHERE 1=1";

        let diags = checker.check_file("models/fct.sql", content, None, &config);
        assert!(
            !diags.iter().any(|d| d.rule_id == "REF006"),
            "REF006 should not fire with default config"
        );
    }

    #[test]
    fn test_ref006_join_hardcoded() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF006".to_owned(), RuleSeverityOverride::Info);

        let content = "SELECT * FROM {{ ref('orders') }} JOIN analytics.customers ON 1=1";

        let diags = checker.check_file("models/fct.sql", content, None, &config);
        let ref006: Vec<_> = diags.iter().filter(|d| d.rule_id == "REF006").collect();
        assert_eq!(ref006.len(), 1);
        assert!(ref006[0].message.contains("analytics.customers"));
    }

    #[test]
    fn test_ref006_non_sql_file_skipped() {
        let checker = RefResolverChecker;
        let mut config = default_config();
        config
            .rules
            .insert("REF006".to_owned(), RuleSeverityOverride::Info);

        let content = "SELECT * FROM staging.orders WHERE 1=1";

        let diags = checker.check_file("models/schema.yml", content, None, &config);
        assert!(diags.is_empty());
    }

    // -------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------

    #[test]
    fn test_empty_project_no_diagnostics() {
        let checker = RefResolverChecker;
        let config = default_config();
        let models: Vec<DiscoveredModel> = Vec::new();

        let diags = checker.check_project(&models, &config);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_yaml_files_excluded() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![make_model(
            "schema",
            "models/schema.yml",
            "version: 2",
            vec!["nonexistent"],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        assert!(
            !diags.iter().any(|d| d.rule_id == "REF001"),
            "YAML files should be excluded from ref resolution"
        );
    }

    #[test]
    fn test_all_diagnostics_have_doc_url() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![
            make_model(
                "model_a",
                "models/model_a.sql",
                "SELECT * FROM {{ ref('model_b') }}",
                vec!["model_b"],
                HashMap::new(),
            ),
            make_model(
                "model_b",
                "models/model_b.sql",
                "SELECT * FROM {{ ref('model_a') }}",
                vec!["model_a"],
                HashMap::new(),
            ),
        ];

        let diags = checker.check_project(&models, &config);
        for d in &diags {
            assert!(
                d.doc_url.is_some(),
                "Diagnostic {} should have a doc_url",
                d.rule_id
            );
            let url = d.doc_url.as_ref().expect("doc_url missing");
            assert!(
                url.starts_with("https://docs.ironlayer.app/check/rules/"),
                "doc_url format mismatch: {url}"
            );
        }
    }

    #[test]
    fn test_all_diagnostics_have_category() {
        let checker = RefResolverChecker;
        let config = default_config();

        let models = vec![make_model(
            "model_a",
            "models/model_a.sql",
            "SELECT * FROM {{ ref('missing') }}",
            vec!["missing"],
            HashMap::new(),
        )];

        let diags = checker.check_project(&models, &config);
        for d in &diags {
            assert_eq!(
                d.category,
                CheckCategory::RefResolution,
                "All ref_resolver diagnostics should be RefResolution"
            );
        }
    }
}
