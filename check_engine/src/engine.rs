//! Check engine orchestrator — the main entry point for running checks.
//!
//! Coordinates file discovery, caching, header parsing, ref extraction,
//! per-file checks, cross-file checks, and result assembly.
//!
//! Supports `--changed-only` mode for incremental checking (git integration),
//! `--fix` mode for auto-fixing fixable rules, and `--sarif` for SARIF output.
//!
//! Every per-file check dispatch is wrapped in `catch_unwind` so that a
//! panic in one checker emits an `INTERNAL` diagnostic instead of crashing
//! the Python process.

use std::collections::HashMap;
use std::io::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;
use regex::Regex;

use crate::cache::CheckCache;
use crate::checkers::{build_checker_registry, Checker};
use crate::config::CheckConfig;
use crate::discovery::{
    compute_sha256, detect_project_type, get_changed_files, read_file_content, stat_known_files,
    walk_file_metadata,
};
use crate::types::{
    CheckCategory, CheckDiagnostic, CheckResult, DiscoveredFile, DiscoveredFileMeta,
    DiscoveredModel, ProjectType, Severity,
};

/// The main check engine orchestrator.
///
/// Discovers files, checks them against all registered checkers, manages
/// caching, and assembles the final result.
pub struct CheckEngine {
    /// The check configuration.
    config: CheckConfig,
    /// All registered checkers.
    checkers: Vec<Box<dyn Checker>>,
}

impl CheckEngine {
    /// Create a new check engine with the given configuration.
    #[must_use]
    pub fn new(config: CheckConfig) -> Self {
        let checkers = build_checker_registry();
        Self { config, checkers }
    }

    /// Create a merged config from the project root's config files and CLI overrides.
    ///
    /// Loads config from the project root (ironlayer.check.toml, pyproject.toml, etc.)
    /// and then applies CLI-provided overrides (fix, changed_only, no_cache, etc.).
    fn merged_config(&self, root: &Path) -> CheckConfig {
        // Load project config from disk (4-level resolution)
        let mut config = match CheckConfig::load_from_project(root) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Failed to load project config: {}. Using defaults.", e);
                CheckConfig::default()
            }
        };

        // Apply CLI overrides from self.config (CLI flags take precedence)
        config.fix = self.config.fix;
        config.changed_only = self.config.changed_only;
        config.no_cache = self.config.no_cache;
        if self.config.select.is_some() {
            config.select = self.config.select.clone();
        }
        if self.config.exclude_rules.is_some() {
            config.exclude_rules = self.config.exclude_rules.clone();
        }
        if self.config.fail_on_warnings {
            config.fail_on_warnings = true;
        }
        if self.config.max_diagnostics != 0 {
            config.max_diagnostics = self.config.max_diagnostics;
        }
        // Merge per-rule severity overrides (CLI overrides project config)
        for (rule_id, severity) in &self.config.rules {
            config.rules.insert(rule_id.clone(), severity.clone());
        }

        config
    }

    /// Run all checks on the project at the given root path.
    ///
    /// This is the main entry point called from Python via PyO3.
    ///
    /// # Panics
    ///
    /// This function catches panics from individual checkers via `catch_unwind`.
    /// A panic in one checker emits an `INTERNAL` diagnostic and does not
    /// prevent other checkers from running.
    pub fn check(&self, root: &Path) -> CheckResult {
        let start = Instant::now();

        // 0. Merge project config with CLI config (CLI flags take precedence)
        let config = self.merged_config(root);

        // 1. Detect project type
        let project_type = detect_project_type(root);

        // 2. Initialize cache (load from disk if available)
        let mut cache = CheckCache::new(root, &config);

        // 3. File discovery: use targeted stat for warm cache, full walk for cold
        //
        // On warm runs with a valid cache, we stat only the known cached paths
        // instead of walking the entire directory tree. This reduces warm-cache
        // discovery from O(walk tree) to O(stat N) — typically ~5ms vs ~200ms.
        //
        // On cold runs (empty cache), we fall back to the full walk.
        // If any cached files are missing (deleted), we also need the full walk
        // to pick up any new files that might have been added.
        let all_metas = if cache.has_entries() && !config.changed_only {
            let known = cache.cached_paths();
            let metas = stat_known_files(root, &known);

            // If all cached files still exist, we can skip the full walk.
            // If some are missing (deleted), fall back to the full walk
            // to also discover any new files.
            if metas.len() == known.len() {
                metas
            } else {
                walk_file_metadata(root, &config)
            }
        } else {
            walk_file_metadata(root, &config)
        };

        // 3b. If --changed-only, filter to only git-modified files
        let check_metas: Vec<&DiscoveredFileMeta> = if config.changed_only {
            match get_changed_files(root) {
                Some(changed) => all_metas
                    .iter()
                    .filter(|m| changed.contains(&m.rel_path))
                    .collect(),
                None => {
                    log::warn!(
                        "--changed-only requested but git is unavailable. Checking all files."
                    );
                    all_metas.iter().collect()
                }
            }
        } else {
            all_metas.iter().collect()
        };

        // 4. Fast-partition using mtime+size
        let check_paths: std::collections::HashSet<&str> =
            check_metas.iter().map(|m| m.rel_path.as_str()).collect();

        let (fast_cached, needs_read): (Vec<&DiscoveredFileMeta>, Vec<&DiscoveredFileMeta>) = {
            let mut cached = Vec::new();
            let mut uncached = Vec::new();
            for meta in &all_metas {
                if !check_paths.contains(meta.rel_path.as_str()) {
                    continue;
                }
                if cache.is_fast_cached(meta) {
                    cached.push(meta);
                } else {
                    uncached.push(meta);
                }
            }
            (cached, uncached)
        };

        // ── Fast-path: all files cached → skip I/O, checkers, and flush ──
        if needs_read.is_empty() {
            let mut all_diags: Vec<CheckDiagnostic> = Vec::new();
            for meta in &fast_cached {
                if let Some(cached_diags) = cache.get_fast_cached_diagnostics(meta) {
                    for cd in cached_diags {
                        all_diags.push(cd.to_diagnostic());
                    }
                }
            }

            all_diags.sort_by(|a, b| {
                a.file_path
                    .cmp(&b.file_path)
                    .then(a.line.cmp(&b.line))
                    .then(a.column.cmp(&b.column))
            });

            let max_diags = config.max_diagnostics;
            if max_diags > 0 && all_diags.len() > max_diags {
                all_diags.truncate(max_diags);
            }

            let total_errors = all_diags
                .iter()
                .filter(|d| d.severity == Severity::Error)
                .count() as u32;
            let total_warnings = all_diags
                .iter()
                .filter(|d| d.severity == Severity::Warning)
                .count() as u32;
            let total_infos = all_diags
                .iter()
                .filter(|d| d.severity == Severity::Info)
                .count() as u32;

            let passed = if config.fail_on_warnings {
                total_errors == 0 && total_warnings == 0
            } else {
                total_errors == 0
            };

            // No flush needed — nothing changed
            let elapsed = start.elapsed();
            return CheckResult {
                diagnostics: all_diags,
                total_files_checked: 0,
                total_files_skipped_cache: fast_cached.len() as u32,
                total_errors,
                total_warnings,
                total_infos,
                elapsed_ms: elapsed.as_millis() as u64,
                project_type: project_type.to_string(),
                passed,
            };
        }

        // ── Slow path: some files need reading and checking ──

        // 4. Read content only for cache-miss files (the expensive I/O step)
        let uncached_files: Vec<DiscoveredFile> = needs_read
            .par_iter()
            .filter_map(|meta| read_file_content(root, meta))
            .collect();

        // 5. Build model registry:
        //    - Fast-cached files: reconstruct DiscoveredModel from cached metadata
        //    - Uncached files: parse headers from content
        //    We need ALL files' models for cross-file checks (ref resolution, etc.)
        let mut models: Vec<DiscoveredModel> = Vec::new();

        // Models from cache (no content read needed)
        for meta in &all_metas {
            if let Some(cm) = cache.get_cached_model(meta) {
                models.push(DiscoveredModel {
                    name: cm.name.clone(),
                    file_path: meta.rel_path.clone(),
                    content_hash: String::new(),
                    ref_names: cm.ref_names.clone(),
                    header: cm.header.clone(),
                    content: String::new(),
                });
            }
        }

        // Models from freshly-read content (overwrite any cache entries for same path)
        let fresh_model_paths: std::collections::HashSet<&str> =
            uncached_files.iter().map(|f| f.rel_path.as_str()).collect();
        models.retain(|m| !fresh_model_paths.contains(m.file_path.as_str()));

        for file in &uncached_files {
            if file.rel_path.ends_with(".sql")
                || file.rel_path.ends_with(".yml")
                || file.rel_path.ends_with(".yaml")
            {
                models.push(discover_model(file));
            }
        }

        // 6. Run per-file checks on uncached files (parallel via rayon)
        let max_diags = config.max_diagnostics;

        let model_map: HashMap<&str, &DiscoveredModel> =
            models.iter().map(|m| (m.file_path.as_str(), m)).collect();

        // Build a meta lookup for mtime/size to store in cache
        let meta_map: HashMap<&str, &DiscoveredFileMeta> =
            all_metas.iter().map(|m| (m.rel_path.as_str(), m)).collect();

        let file_results: Vec<(String, String, u64, i64, Vec<CheckDiagnostic>)> = uncached_files
            .par_iter()
            .map(|file| {
                let model = model_map.get(file.rel_path.as_str()).copied();
                let file_diags = self.run_per_file_checks(file, model, &project_type, &config);
                let (size, mtime) = meta_map
                    .get(file.rel_path.as_str())
                    .map(|m| (m.size, m.mtime_secs))
                    .unwrap_or((0, 0));
                (
                    file.rel_path.clone(),
                    file.content_hash.clone(),
                    size,
                    mtime,
                    file_diags,
                )
            })
            .collect();

        // Sequentially update cache and collect diagnostics
        let mut all_diags: Vec<CheckDiagnostic> = Vec::new();
        for (rel_path, content_hash, size, mtime, file_diags) in &file_results {
            let model = model_map.get(rel_path.as_str()).copied();
            cache.update(rel_path, content_hash, *size, *mtime, file_diags, model);
            all_diags.extend(file_diags.iter().cloned());

            if max_diags > 0 && all_diags.len() >= max_diags {
                break;
            }
        }

        // 7. Replay cached diagnostics for fast-cached files
        for meta in &fast_cached {
            if let Some(cached_diags) = cache.get_fast_cached_diagnostics(meta) {
                for cd in cached_diags {
                    all_diags.push(cd.to_diagnostic());
                }
            }
        }

        // 8. Run cross-file checks (sequential, needs full model list)
        let project_diags = self.run_cross_file_checks(&models, &project_type, &config);
        all_diags.extend(project_diags);

        // 9. Apply --fix if requested (5-step workflow)
        if config.fix {
            let fixed = apply_fixes(root, &all_diags);
            if !fixed.is_empty() {
                // Re-discover and re-check fixed files
                let mut recheck_diags = Vec::new();
                for fixed_path in &fixed {
                    let abs_path = root.join(fixed_path);
                    let content = match std::fs::read_to_string(&abs_path) {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    let content_hash = compute_sha256(&content);
                    let file = DiscoveredFile {
                        rel_path: fixed_path.clone(),
                        content,
                        content_hash,
                    };
                    let model = discover_model(&file);
                    let file_diags =
                        self.run_per_file_checks(&file, Some(&model), &project_type, &config);
                    recheck_diags.extend(file_diags);
                }

                // Replace diagnostics for fixed files with re-check results
                all_diags.retain(|d| !fixed.contains(&d.file_path));
                all_diags.extend(recheck_diags);
            }
        }

        // 10. Sort diagnostics by (file_path, line, column)
        all_diags.sort_by(|a, b| {
            a.file_path
                .cmp(&b.file_path)
                .then(a.line.cmp(&b.line))
                .then(a.column.cmp(&b.column))
        });

        // 11. Truncate to max_diagnostics
        if max_diags > 0 && all_diags.len() > max_diags {
            all_diags.truncate(max_diags);
        }

        // 12. Compute summary counts
        let total_errors = all_diags
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .count() as u32;
        let total_warnings = all_diags
            .iter()
            .filter(|d| d.severity == Severity::Warning)
            .count() as u32;
        let total_infos = all_diags
            .iter()
            .filter(|d| d.severity == Severity::Info)
            .count() as u32;

        // 13. Determine pass/fail
        let passed = if config.fail_on_warnings {
            total_errors == 0 && total_warnings == 0
        } else {
            total_errors == 0
        };

        // 14. Flush cache to disk
        cache.flush();

        let elapsed = start.elapsed();

        CheckResult {
            diagnostics: all_diags,
            total_files_checked: uncached_files.len() as u32,
            total_files_skipped_cache: fast_cached.len() as u32,
            total_errors,
            total_warnings,
            total_infos,
            elapsed_ms: elapsed.as_millis() as u64,
            project_type: project_type.to_string(),
            passed,
        }
    }

    /// Run all per-file checkers on a single file, wrapped in catch_unwind.
    fn run_per_file_checks(
        &self,
        file: &DiscoveredFile,
        model: Option<&DiscoveredModel>,
        project_type: &ProjectType,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        for checker in &self.checkers {
            // Skip checkers that don't apply to this project type
            if !should_run_checker(checker.name(), project_type) {
                continue;
            }

            let result = catch_unwind(AssertUnwindSafe(|| {
                checker.check_file(&file.rel_path, &file.content, model, config)
            }));

            match result {
                Ok(checker_diags) => diags.extend(checker_diags),
                Err(panic_info) => {
                    let panic_msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                        (*s).to_owned()
                    } else {
                        "unknown panic".to_owned()
                    };

                    diags.push(CheckDiagnostic {
                        rule_id: "INTERNAL".to_owned(),
                        message: format!(
                            "Internal error in checker '{}': {}. \
                             This is a bug — please report it.",
                            checker.name(),
                            panic_msg
                        ),
                        severity: Severity::Warning,
                        category: CheckCategory::FileStructure,
                        file_path: file.rel_path.clone(),
                        line: 0,
                        column: 0,
                        snippet: None,
                        suggestion: Some(
                            "This file was skipped by this checker due to an internal error."
                                .to_owned(),
                        ),
                        doc_url: None,
                    });
                }
            }
        }

        diags
    }

    /// Run all cross-file (project-level) checks, wrapped in catch_unwind.
    fn run_cross_file_checks(
        &self,
        models: &[DiscoveredModel],
        project_type: &ProjectType,
        config: &CheckConfig,
    ) -> Vec<CheckDiagnostic> {
        let mut diags = Vec::new();

        for checker in &self.checkers {
            if !should_run_checker(checker.name(), project_type) {
                continue;
            }

            let result = catch_unwind(AssertUnwindSafe(|| checker.check_project(models, config)));

            match result {
                Ok(checker_diags) => diags.extend(checker_diags),
                Err(panic_info) => {
                    let panic_msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                        (*s).to_owned()
                    } else {
                        "unknown panic".to_owned()
                    };

                    diags.push(CheckDiagnostic {
                        rule_id: "INTERNAL".to_owned(),
                        message: format!(
                            "Internal error in checker '{}' during project-level checks: {}. \
                             This is a bug — please report it.",
                            checker.name(),
                            panic_msg
                        ),
                        severity: Severity::Warning,
                        category: CheckCategory::FileStructure,
                        file_path: String::new(),
                        line: 0,
                        column: 0,
                        snippet: None,
                        suggestion: None,
                        doc_url: None,
                    });
                }
            }
        }

        diags
    }
}

/// Determine if a checker should run for a given project type.
///
/// - `sql_header`, `incremental_logic`, `test_adequacy` only run for IronLayer
///   projects (they depend on `-- key: value` header fields).
/// - `dbt_project` only runs for dbt projects.
/// - All other checkers (sql_syntax, sql_safety, ref_resolver, naming,
///   yaml_schema, model_consistency, databricks_sql, performance) run for
///   all project types.
fn should_run_checker(checker_name: &str, project_type: &ProjectType) -> bool {
    match checker_name {
        "sql_header" | "incremental_logic" | "test_adequacy" => {
            *project_type == ProjectType::IronLayer
        }
        "dbt_project" => *project_type == ProjectType::Dbt,
        _ => true,
    }
}

/// Extract model metadata from a discovered SQL file.
///
/// Parses the header block for `-- key: value` fields and extracts
/// `{{ ref('...') }}` references from the SQL body.
fn discover_model(file: &DiscoveredFile) -> DiscoveredModel {
    let header = parse_header_map(&file.content);
    let ref_names = extract_ref_names(&file.content);

    // Model name: prefer `-- name:` header, fall back to filename stem
    let name = header.get("name").cloned().unwrap_or_else(|| {
        Path::new(&file.rel_path)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default()
    });

    DiscoveredModel {
        name,
        file_path: file.rel_path.clone(),
        content_hash: file.content_hash.clone(),
        ref_names,
        header,
        content: file.content.clone(),
    }
}

/// Parse the header block into a key→value map (first occurrence wins).
fn parse_header_map(content: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Empty lines don't terminate
        if trimmed.is_empty() {
            continue;
        }

        // Must start with `--`
        if let Some(rest) = trimmed.strip_prefix("--") {
            let rest = rest.trim();

            // Bare `--`
            if rest.is_empty() {
                continue;
            }

            // `key: value` pattern
            if let Some(colon_pos) = rest.find(':') {
                let key = rest[..colon_pos].trim().to_lowercase();
                let value = rest[colon_pos + 1..].trim().to_owned();

                if !key.is_empty() {
                    map.entry(key).or_insert(value);
                    continue;
                }
            }

            // Plain comment — skip
            continue;
        }

        // Non-empty, non-comment → terminate header
        break;
    }

    map
}

/// Extract `{{ ref('...') }}` references from SQL content.
///
/// Uses the exact regex from `ref_resolver._REF_PATTERN`:
/// `r"\{\{\s*ref\s*\(\s*(?:'([^']+)'|\"([^\"]+)\")\s*\)\s*\}\}"`
fn extract_ref_names(content: &str) -> Vec<String> {
    // This regex matches the Python ref_resolver._REF_PATTERN exactly
    let re = Regex::new(r#"\{\{\s*ref\s*\(\s*(?:'([^']+)'|"([^"]+)")\s*\)\s*\}\}"#)
        .expect("ref pattern regex is valid");

    let mut refs = Vec::new();
    for cap in re.captures_iter(content) {
        // Group 1 = single-quoted, Group 2 = double-quoted
        if let Some(m) = cap.get(1) {
            refs.push(m.as_str().to_owned());
        } else if let Some(m) = cap.get(2) {
            refs.push(m.as_str().to_owned());
        }
    }
    refs
}

// ---------------------------------------------------------------------------
// --fix support
// ---------------------------------------------------------------------------

/// Set of rule IDs that can be auto-fixed.
const FIXABLE_RULES: &[&str] = &["SQL007", "SQL009", "HDR013", "REF004"];

/// Check if a diagnostic is for a fixable rule.
#[must_use]
fn is_fixable(diag: &CheckDiagnostic) -> bool {
    FIXABLE_RULES.contains(&diag.rule_id.as_str())
}

/// Apply auto-fixes for fixable diagnostics.
///
/// Follows the 5-step workflow:
/// 1. Dry-run: diagnostics already collected
/// 2. Filter: only fixable rules
/// 3. Apply: read file → apply fixes in reverse line order → atomic write
/// 4. Re-check: caller re-runs checks on modified files
/// 5. Report: caller reports which fixes were applied
///
/// Returns the set of file paths that were modified.
fn apply_fixes(root: &Path, diags: &[CheckDiagnostic]) -> Vec<String> {
    // Step 2: Filter to fixable diagnostics only
    let fixable: Vec<&CheckDiagnostic> = diags.iter().filter(|d| is_fixable(d)).collect();
    if fixable.is_empty() {
        return Vec::new();
    }

    // Group fixable diagnostics by file path
    let mut by_file: HashMap<String, Vec<&CheckDiagnostic>> = HashMap::new();
    for diag in &fixable {
        by_file
            .entry(diag.file_path.clone())
            .or_default()
            .push(diag);
    }

    let mut modified_files = Vec::new();

    // Step 3: Apply fixes per file in reverse line order
    for (file_path, mut file_diags) in by_file {
        let abs_path = root.join(&file_path);
        let content = match std::fs::read_to_string(&abs_path) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("--fix: could not read {}: {}", file_path, e);
                continue;
            }
        };

        // Sort in reverse line order so that fixes don't shift line numbers
        file_diags.sort_by(|a, b| b.line.cmp(&a.line));

        let mut lines: Vec<String> = content.lines().map(|l| l.to_owned()).collect();
        let mut any_changed = false;

        for diag in &file_diags {
            let changed = apply_single_fix(&mut lines, diag);
            if changed {
                any_changed = true;
            }
        }

        if any_changed {
            // Atomic write: write to temp file, then rename
            if let Err(e) = atomic_write_lines(&abs_path, &lines) {
                log::warn!("--fix: could not write {}: {}", file_path, e);
                continue;
            }
            modified_files.push(file_path);
        }
    }

    modified_files
}

/// Apply a single fix to the lines of a file.
///
/// Returns `true` if any modification was made.
fn apply_single_fix(lines: &mut Vec<String>, diag: &CheckDiagnostic) -> bool {
    match diag.rule_id.as_str() {
        "HDR013" => fix_hdr013(lines, diag),
        "SQL007" => fix_sql007(lines, diag),
        "SQL009" => fix_sql009(lines, diag),
        "REF004" => fix_ref004(lines, diag),
        _ => false,
    }
}

/// HDR013 fix: Remove duplicate header lines (keep first occurrence).
///
/// The diagnostic's line number points to the duplicate. Remove that line.
fn fix_hdr013(lines: &mut Vec<String>, diag: &CheckDiagnostic) -> bool {
    if diag.line == 0 || diag.line as usize > lines.len() {
        return false;
    }
    // Remove the duplicate header line (1-based index)
    lines.remove(diag.line as usize - 1);
    true
}

/// SQL007 fix: Remove trailing semicolons from SQL body.
///
/// The diagnostic's line number points to the line with the trailing semicolon.
fn fix_sql007(lines: &mut [String], diag: &CheckDiagnostic) -> bool {
    if diag.line == 0 || diag.line as usize > lines.len() {
        return false;
    }
    let idx = diag.line as usize - 1;
    let trimmed = lines[idx].trim_end();
    if trimmed.ends_with(';') {
        lines[idx] = trimmed.trim_end_matches(';').to_owned();
        true
    } else {
        false
    }
}

/// SQL009 fix: Replace tab characters with 4 spaces.
///
/// The diagnostic's line number points to the line with tab characters.
fn fix_sql009(lines: &mut [String], diag: &CheckDiagnostic) -> bool {
    if diag.line == 0 || diag.line as usize > lines.len() {
        return false;
    }
    let idx = diag.line as usize - 1;
    if lines[idx].contains('\t') {
        lines[idx] = lines[idx].replace('\t', "    ");
        true
    } else {
        false
    }
}

/// REF004 fix: Replace fully-qualified ref name with short name where unambiguous.
///
/// The diagnostic's snippet should contain the fully-qualified ref pattern.
/// The suggestion contains the short name to use.
fn fix_ref004(lines: &mut [String], diag: &CheckDiagnostic) -> bool {
    if diag.line == 0 || diag.line as usize > lines.len() {
        return false;
    }

    let (snippet, suggestion) = match (&diag.snippet, &diag.suggestion) {
        (Some(s), Some(sug)) => (s, sug),
        _ => return false,
    };

    let idx = diag.line as usize - 1;
    if lines[idx].contains(snippet.as_str()) {
        lines[idx] = lines[idx].replace(snippet.as_str(), suggestion.as_str());
        true
    } else {
        false
    }
}

/// Atomically write lines to a file (write to temp, then rename).
///
/// Uses `.tmp.{pid}` suffix for the temp file, then renames. On POSIX,
/// rename is atomic. No backups are created (assumes files are under VCS).
///
/// # Errors
///
/// Returns an error if the temp file cannot be written or renamed.
fn atomic_write_lines(path: &PathBuf, lines: &[String]) -> std::io::Result<()> {
    let pid = std::process::id();
    let tmp_path = path.with_extension(format!("tmp.{pid}"));

    let mut file = std::fs::File::create(&tmp_path)?;
    for (i, line) in lines.iter().enumerate() {
        file.write_all(line.as_bytes())?;
        if i < lines.len() - 1 {
            file.write_all(b"\n")?;
        }
    }
    // Preserve trailing newline if the original file likely had one
    file.write_all(b"\n")?;
    file.flush()?;
    drop(file);

    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::compute_sha256;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_extract_ref_names_single_quotes() {
        let content = "SELECT * FROM {{ ref('stg_orders') }}";
        let refs = extract_ref_names(content);
        assert_eq!(refs, vec!["stg_orders"]);
    }

    #[test]
    fn test_extract_ref_names_double_quotes() {
        let content = r#"SELECT * FROM {{ ref("stg_orders") }}"#;
        let refs = extract_ref_names(content);
        assert_eq!(refs, vec!["stg_orders"]);
    }

    #[test]
    fn test_extract_ref_names_multiple() {
        let content = "SELECT * FROM {{ ref('stg_orders') }} JOIN {{ ref('stg_customers') }}";
        let refs = extract_ref_names(content);
        assert_eq!(refs, vec!["stg_orders", "stg_customers"]);
    }

    #[test]
    fn test_extract_ref_names_with_whitespace() {
        let content = "SELECT * FROM {{  ref(  'stg_orders'  )  }}";
        let refs = extract_ref_names(content);
        assert_eq!(refs, vec!["stg_orders"]);
    }

    #[test]
    fn test_extract_ref_names_no_refs() {
        let content = "SELECT * FROM raw_orders";
        let refs = extract_ref_names(content);
        assert!(refs.is_empty());
    }

    #[test]
    fn test_parse_header_map() {
        let content = "-- name: stg_orders\n-- kind: FULL_REFRESH\nSELECT 1";
        let map = parse_header_map(content);
        assert_eq!(map.get("name"), Some(&"stg_orders".to_owned()));
        assert_eq!(map.get("kind"), Some(&"FULL_REFRESH".to_owned()));
    }

    #[test]
    fn test_parse_header_map_first_occurrence_wins() {
        let content = "-- name: first\n-- name: second\nSELECT 1";
        let map = parse_header_map(content);
        assert_eq!(map.get("name"), Some(&"first".to_owned()));
    }

    #[test]
    fn test_parse_header_map_blank_lines() {
        let content = "-- name: test\n\n-- kind: FULL_REFRESH\nSELECT 1";
        let map = parse_header_map(content);
        assert_eq!(map.get("name"), Some(&"test".to_owned()));
        assert_eq!(map.get("kind"), Some(&"FULL_REFRESH".to_owned()));
    }

    #[test]
    fn test_parse_header_map_bare_comment() {
        let content = "-- name: test\n--\n-- kind: FULL_REFRESH\nSELECT 1";
        let map = parse_header_map(content);
        assert!(map.contains_key("name"));
        assert!(map.contains_key("kind"));
    }

    #[test]
    fn test_discover_model_from_header() {
        let file = DiscoveredFile {
            rel_path: "models/stg_orders.sql".to_owned(),
            content:
                "-- name: stg_orders\n-- kind: FULL_REFRESH\nSELECT * FROM {{ ref('raw_orders') }}"
                    .to_owned(),
            content_hash: compute_sha256("test"),
        };
        let model = discover_model(&file);
        assert_eq!(model.name, "stg_orders");
        assert_eq!(model.ref_names, vec!["raw_orders"]);
        assert_eq!(model.header.get("kind"), Some(&"FULL_REFRESH".to_owned()));
    }

    #[test]
    fn test_discover_model_fallback_to_filename() {
        let file = DiscoveredFile {
            rel_path: "models/stg_orders.sql".to_owned(),
            content: "SELECT 1".to_owned(),
            content_hash: compute_sha256("test"),
        };
        let model = discover_model(&file);
        assert_eq!(model.name, "stg_orders");
    }

    #[test]
    fn test_check_engine_ironlayer_project() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("stg_orders.sql"),
            "-- name: stg_orders\n-- kind: FULL_REFRESH\nSELECT 1",
        )
        .unwrap();

        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());

        assert_eq!(result.project_type, "ironlayer");
        assert!(result.passed);
        assert_eq!(result.total_errors, 0);
    }

    #[test]
    fn test_check_engine_detects_errors() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // Missing kind → HDR002
        fs::write(
            models_dir.join("stg_orders.sql"),
            "-- name: stg_orders\nSELECT 1",
        )
        .unwrap();

        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());

        assert!(!result.passed);
        assert!(result.total_errors > 0);
        assert!(result.diagnostics.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_check_engine_empty_project() {
        let dir = tempdir().unwrap();
        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());

        assert!(result.passed);
        assert_eq!(result.total_files_checked, 0);
    }

    #[test]
    fn test_check_engine_non_ironlayer_skips_hdr() {
        let dir = tempdir().unwrap();
        // No ironlayer.yaml → raw_sql project type
        fs::write(dir.path().join("query.sql"), "SELECT 1").unwrap();

        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());

        // HDR rules should not fire for raw_sql project type
        assert!(!result
            .diagnostics
            .iter()
            .any(|d| d.rule_id.starts_with("HDR")));
    }

    #[test]
    fn test_check_engine_max_diagnostics() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        // Create many files with errors
        for i in 0..20 {
            fs::write(
                models_dir.join(format!("model_{i}.sql")),
                "SELECT 1", // Missing name and kind
            )
            .unwrap();
        }

        let mut config = CheckConfig::default();
        config.max_diagnostics = 5;
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());

        assert!(result.diagnostics.len() <= 5);
    }

    #[test]
    fn test_check_engine_fail_on_warnings() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // Duplicate header → HDR013 (warning)
        fs::write(
            models_dir.join("stg_orders.sql"),
            "-- name: stg_orders\n-- kind: FULL_REFRESH\n-- name: stg_orders2\nSELECT 1",
        )
        .unwrap();

        // Without fail_on_warnings: should pass (only warnings)
        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());
        assert!(result.passed);

        // With fail_on_warnings: should fail
        let mut config_strict = CheckConfig::default();
        config_strict.fail_on_warnings = true;
        let engine_strict = CheckEngine::new(config_strict);
        let result_strict = engine_strict.check(dir.path());
        assert!(!result_strict.passed);
    }

    #[test]
    fn test_should_run_checker() {
        assert!(should_run_checker("sql_header", &ProjectType::IronLayer));
        assert!(!should_run_checker("sql_header", &ProjectType::Dbt));
        assert!(!should_run_checker("sql_header", &ProjectType::RawSql));
        assert!(should_run_checker("dbt_project", &ProjectType::Dbt));
        assert!(!should_run_checker("dbt_project", &ProjectType::IronLayer));
        assert!(should_run_checker("sql_syntax", &ProjectType::IronLayer));
        assert!(should_run_checker("sql_syntax", &ProjectType::Dbt));
        assert!(should_run_checker("sql_syntax", &ProjectType::RawSql));
        // New checkers
        assert!(should_run_checker(
            "incremental_logic",
            &ProjectType::IronLayer
        ));
        assert!(!should_run_checker("incremental_logic", &ProjectType::Dbt));
        assert!(!should_run_checker(
            "incremental_logic",
            &ProjectType::RawSql
        ));
        assert!(should_run_checker("test_adequacy", &ProjectType::IronLayer));
        assert!(!should_run_checker("test_adequacy", &ProjectType::Dbt));
        assert!(should_run_checker(
            "databricks_sql",
            &ProjectType::IronLayer
        ));
        assert!(should_run_checker("databricks_sql", &ProjectType::Dbt));
        assert!(should_run_checker("databricks_sql", &ProjectType::RawSql));
        assert!(should_run_checker("performance", &ProjectType::IronLayer));
        assert!(should_run_checker("performance", &ProjectType::Dbt));
        assert!(should_run_checker("performance", &ProjectType::RawSql));
    }

    #[test]
    fn test_diagnostics_sorted() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("b_model.sql"),
            "-- name: b\nSELECT 1", // Missing kind
        )
        .unwrap();
        fs::write(
            models_dir.join("a_model.sql"),
            "-- name: a\nSELECT 1", // Missing kind
        )
        .unwrap();

        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());

        // Should be sorted by file_path
        if result.diagnostics.len() >= 2 {
            assert!(result.diagnostics[0].file_path <= result.diagnostics[1].file_path);
        }
    }

    // -----------------------------------------------------------------------
    // --fix mechanics tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_fixable_hdr013() {
        let diag = CheckDiagnostic {
            rule_id: "HDR013".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlHeader,
            file_path: "test.sql".to_owned(),
            line: 3,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(is_fixable(&diag));
    }

    #[test]
    fn test_is_fixable_sql007() {
        let diag = CheckDiagnostic {
            rule_id: "SQL007".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlSyntax,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(is_fixable(&diag));
    }

    #[test]
    fn test_is_fixable_non_fixable_rule() {
        let diag = CheckDiagnostic {
            rule_id: "HDR001".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Error,
            category: CheckCategory::SqlHeader,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(!is_fixable(&diag));
    }

    #[test]
    fn test_fix_hdr013_removes_duplicate_line() {
        let mut lines = vec![
            "-- name: test".to_owned(),
            "-- kind: FULL_REFRESH".to_owned(),
            "-- name: test2".to_owned(),
            "SELECT 1".to_owned(),
        ];
        let diag = CheckDiagnostic {
            rule_id: "HDR013".to_owned(),
            message: "Duplicate header".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlHeader,
            file_path: "test.sql".to_owned(),
            line: 3, // 1-based: the duplicate "-- name: test2"
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(fix_hdr013(&mut lines, &diag));
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "-- name: test");
        assert_eq!(lines[1], "-- kind: FULL_REFRESH");
        assert_eq!(lines[2], "SELECT 1");
    }

    #[test]
    fn test_fix_hdr013_out_of_range() {
        let mut lines = vec!["-- name: test".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "HDR013".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlHeader,
            file_path: "test.sql".to_owned(),
            line: 10, // Out of range
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(!fix_hdr013(&mut lines, &diag));
    }

    #[test]
    fn test_fix_sql007_removes_trailing_semicolon() {
        let mut lines = vec!["-- name: test".to_owned(), "SELECT 1;".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "SQL007".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlSyntax,
            file_path: "test.sql".to_owned(),
            line: 2,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(fix_sql007(&mut lines, &diag));
        assert_eq!(lines[1], "SELECT 1");
    }

    #[test]
    fn test_fix_sql007_no_semicolon() {
        let mut lines = vec!["SELECT 1".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "SQL007".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlSyntax,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(!fix_sql007(&mut lines, &diag));
    }

    #[test]
    fn test_fix_sql009_replaces_tabs() {
        let mut lines = vec!["\tSELECT 1".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "SQL009".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlSyntax,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(fix_sql009(&mut lines, &diag));
        assert_eq!(lines[0], "    SELECT 1");
    }

    #[test]
    fn test_fix_sql009_no_tabs() {
        let mut lines = vec!["    SELECT 1".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "SQL009".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlSyntax,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(!fix_sql009(&mut lines, &diag));
    }

    #[test]
    fn test_fix_ref004_replaces_snippet() {
        let mut lines = vec!["SELECT * FROM {{ ref('schema.stg_orders') }}".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "REF004".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::RefResolution,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: Some("ref('schema.stg_orders')".to_owned()),
            suggestion: Some("ref('stg_orders')".to_owned()),
            doc_url: None,
        };
        assert!(fix_ref004(&mut lines, &diag));
        assert!(lines[0].contains("ref('stg_orders')"));
        assert!(!lines[0].contains("schema.stg_orders"));
    }

    #[test]
    fn test_fix_ref004_no_snippet() {
        let mut lines = vec!["SELECT 1".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "REF004".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::RefResolution,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(!fix_ref004(&mut lines, &diag));
    }

    #[test]
    fn test_apply_fixes_empty_diags() {
        let dir = tempdir().unwrap();
        let result = apply_fixes(dir.path(), &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_apply_fixes_non_fixable_diags_ignored() {
        let dir = tempdir().unwrap();
        let diags = vec![CheckDiagnostic {
            rule_id: "HDR001".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Error,
            category: CheckCategory::SqlHeader,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        }];
        let result = apply_fixes(dir.path(), &diags);
        assert!(result.is_empty());
    }

    #[test]
    fn test_atomic_write_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sql");
        let lines = vec!["-- name: test".to_owned(), "SELECT 1".to_owned()];
        atomic_write_lines(&path, &lines).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("-- name: test\nSELECT 1\n"));
    }

    #[test]
    fn test_apply_single_fix_unknown_rule() {
        let mut lines = vec!["SELECT 1".to_owned()];
        let diag = CheckDiagnostic {
            rule_id: "UNKNOWN".to_owned(),
            message: "test".to_owned(),
            severity: Severity::Warning,
            category: CheckCategory::SqlSyntax,
            file_path: "test.sql".to_owned(),
            line: 1,
            column: 0,
            snippet: None,
            suggestion: None,
            doc_url: None,
        };
        assert!(!apply_single_fix(&mut lines, &diag));
    }

    // -----------------------------------------------------------------------
    // --fix integration test
    // -----------------------------------------------------------------------

    #[test]
    fn test_fix_mode_removes_duplicate_header() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("stg_orders.sql"),
            "-- name: stg_orders\n-- kind: FULL_REFRESH\n-- name: stg_orders2\nSELECT 1",
        )
        .unwrap();

        let mut config = CheckConfig::default();
        config.fix = true;
        let engine = CheckEngine::new(config);
        let _result = engine.check(dir.path());

        // Verify the file was fixed
        let content = fs::read_to_string(models_dir.join("stg_orders.sql")).unwrap();
        assert!(
            !content.contains("-- name: stg_orders2"),
            "Duplicate header line should be removed by --fix"
        );
        assert!(content.contains("-- name: stg_orders"));
        assert!(content.contains("-- kind: FULL_REFRESH"));
    }

    // -----------------------------------------------------------------------
    // Cross-file check scenarios
    // -----------------------------------------------------------------------

    #[test]
    fn test_con001_duplicate_names_detected() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("a.sql"),
            "-- name: dup_model\n-- kind: FULL_REFRESH\nSELECT 1",
        )
        .unwrap();
        fs::write(
            models_dir.join("b.sql"),
            "-- name: dup_model\n-- kind: FULL_REFRESH\nSELECT 2",
        )
        .unwrap();

        let engine = CheckEngine::new(CheckConfig::default());
        let result = engine.check(dir.path());
        assert!(result.diagnostics.iter().any(|d| d.rule_id == "CON001"));
    }

    #[test]
    fn test_yml005_undocumented_model() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("stg.sql"),
            "-- name: stg\n-- kind: FULL_REFRESH\nSELECT 1",
        )
        .unwrap();

        let engine = CheckEngine::new(CheckConfig::default());
        let result = engine.check(dir.path());
        assert!(
            result.diagnostics.iter().any(|d| d.rule_id == "YML005"),
            "YML005 should fire for SQL without YAML docs"
        );
    }

    #[test]
    fn test_yml004_yaml_model_no_sql_file() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("schema.yml"),
            "models:\n  - name: phantom_model\n    columns:\n      - name: id\n",
        )
        .unwrap();

        let engine = CheckEngine::new(CheckConfig::default());
        let result = engine.check(dir.path());
        assert!(
            result.diagnostics.iter().any(|d| d.rule_id == "YML004"),
            "YML004 should fire for YAML model with no SQL"
        );
    }

    #[test]
    fn test_dbt_project_skips_hdr_rules() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("dbt_project.yml"),
            "name: my_project\nversion: '1.0'\n",
        )
        .unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("stg.sql"), "SELECT 1").unwrap();

        let engine = CheckEngine::new(CheckConfig::default());
        let result = engine.check(dir.path());
        assert_eq!(result.project_type, "dbt");
        assert!(
            !result
                .diagnostics
                .iter()
                .any(|d| d.rule_id.starts_with("HDR")),
            "HDR rules should not fire for dbt projects"
        );
    }

    #[test]
    fn test_config_disables_rule() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("test.sql"), "SELECT 1").unwrap();

        let mut config = CheckConfig::default();
        config.rules.insert(
            "HDR001".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );
        config.rules.insert(
            "HDR002".to_owned(),
            crate::config::RuleSeverityOverride::Off,
        );

        let engine = CheckEngine::new(config);
        let result = engine.check(dir.path());
        assert!(!result.diagnostics.iter().any(|d| d.rule_id == "HDR001"));
        assert!(!result.diagnostics.iter().any(|d| d.rule_id == "HDR002"));
    }

    #[test]
    fn test_hdr007_disabled_by_default_unknown_field_passes() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("stg.sql"),
            "-- name: stg\n-- kind: FULL_REFRESH\n-- foobar: something\nSELECT 1",
        )
        .unwrap();

        let engine = CheckEngine::new(CheckConfig::default());
        let result = engine.check(dir.path());
        assert!(
            !result.diagnostics.iter().any(|d| d.rule_id == "HDR007"),
            "HDR007 should NOT fire by default"
        );
    }

    #[test]
    fn test_cache_invalidation_on_content_change() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("stg.sql"),
            "-- name: stg\n-- kind: FULL_REFRESH\nSELECT 1",
        )
        .unwrap();

        let config = CheckConfig::default();
        let engine = CheckEngine::new(config.clone());
        let r1 = engine.check(dir.path());
        assert!(r1.passed);

        // Modify file to break it
        fs::write(models_dir.join("stg.sql"), "SELECT 1").unwrap();

        let engine2 = CheckEngine::new(config);
        let r2 = engine2.check(dir.path());
        assert!(!r2.passed, "Should fail after header removed");
    }

    #[test]
    fn test_warm_cache_skips_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        for i in 0..5 {
            let content = format!(
                "-- name: model_{i}\n-- kind: FULL_REFRESH\n-- materialization: TABLE\nSELECT 1\n"
            );
            fs::write(models_dir.join(format!("model_{i}.sql")), content).unwrap();
        }

        // Cold run — should check all files (5 SQL + ironlayer.yaml = 6)
        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let r1 = engine.check(dir.path());
        let cold_checked = r1.total_files_checked;
        assert!(cold_checked > 0, "Cold run: should check some files");
        assert_eq!(
            r1.total_files_skipped_cache, 0,
            "Cold run: no files skipped"
        );

        // Warm run — files unchanged, should be served from cache
        let config2 = CheckConfig::default();
        let engine2 = CheckEngine::new(config2);
        let r2 = engine2.check(dir.path());
        assert_eq!(
            r2.total_files_skipped_cache, cold_checked,
            "Warm run: all {} files should be cache hits (got {} checked, {} cached)",
            cold_checked, r2.total_files_checked, r2.total_files_skipped_cache
        );
        assert_eq!(
            r2.total_files_checked, 0,
            "Warm run: no files should need checking"
        );
    }

    #[test]
    fn test_warm_cache_500_models_fast() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        let kinds = [
            "FULL_REFRESH",
            "INCREMENTAL_BY_TIME_RANGE",
            "APPEND_ONLY",
            "MERGE_BY_KEY",
        ];

        for i in 0..500 {
            let kind = kinds[i % kinds.len()];
            let extra = match kind {
                "INCREMENTAL_BY_TIME_RANGE" => "\n-- time_column: created_at",
                "MERGE_BY_KEY" => "\n-- unique_key: id",
                _ => "",
            };
            let content = format!(
                "-- name: model_{i}\n-- kind: {kind}{extra}\n-- materialization: TABLE\n\
                 SELECT id, name FROM {{{{ ref('model_{}') }}}}\n",
                if i > 0 { i - 1 } else { 0 }
            );
            fs::write(models_dir.join(format!("model_{i}.sql")), content).unwrap();
        }

        // Prime cache
        let config = CheckConfig::default();
        let engine = CheckEngine::new(config);
        let r1 = engine.check(dir.path());
        assert!(r1.total_files_checked > 0);

        // Warm run
        let config2 = CheckConfig::default();
        let engine2 = CheckEngine::new(config2);
        let r2 = engine2.check(dir.path());

        assert_eq!(r2.total_files_checked, 0, "All files should be cached");
        assert!(r2.total_files_skipped_cache > 0, "Should have cache hits");
    }
}
