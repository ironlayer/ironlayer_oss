//! File discovery and project type detection for the IronLayer Check Engine.
//!
//! Uses the `ignore` crate (same library used by ripgrep) for `.gitignore`-aware
//! file walking, plus support for `.ironlayerignore` files with gitignore-compatible
//! syntax.
//!
//! Project type detection uses a priority order:
//! 1. `ironlayer.yaml` / `ironlayer.yml` → IronLayer
//! 2. `dbt_project.yml` → dbt
//! 3. Sample `.sql` files for IronLayer-style headers → IronLayer
//! 4. Fallback → RawSql

use std::path::Path;

use ignore::WalkBuilder;
use sha2::{Digest, Sha256};

use crate::config::CheckConfig;
use crate::types::{DiscoveredFile, DiscoveredFileMeta, ProjectType};

/// Hardcoded directory names that are always excluded from file walking.
const HARDCODED_EXCLUDES: &[&str] = &[
    "target",
    "dbt_packages",
    "dbt_modules",
    "logs",
    ".venv",
    "node_modules",
    "__pycache__",
    ".git",
    ".ironlayer",
];

/// File extensions that are checked by default.
const DEFAULT_EXTENSIONS: &[&str] = &["sql", "yml", "yaml"];

/// Detect the project type by examining the project root.
///
/// Priority order (first match wins):
/// 1. `ironlayer.yaml` or `ironlayer.yml` exists → [`ProjectType::IronLayer`]
/// 2. `dbt_project.yml` exists → [`ProjectType::Dbt`]
/// 3. Any `.sql` file in the first 5 found has IronLayer-style `-- name:` headers → [`ProjectType::IronLayer`]
/// 4. Fallback → [`ProjectType::RawSql`]
pub fn detect_project_type(root: &Path) -> ProjectType {
    if root.join("ironlayer.yaml").is_file() || root.join("ironlayer.yml").is_file() {
        return ProjectType::IronLayer;
    }

    if root.join("dbt_project.yml").is_file() {
        return ProjectType::Dbt;
    }

    // Sample up to 5 .sql files for IronLayer-style headers
    if has_ironlayer_headers(root) {
        return ProjectType::IronLayer;
    }

    ProjectType::RawSql
}

/// Check if any SQL file in the first 5 found has IronLayer-style `-- name:` headers.
fn has_ironlayer_headers(root: &Path) -> bool {
    let walker = WalkBuilder::new(root).max_depth(Some(5)).build();

    let mut count = 0;
    for entry in walker.flatten() {
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
            count += 1;
            if count > 5 {
                break;
            }
            if let Ok(content) = std::fs::read_to_string(path) {
                if has_header_name_field(&content) {
                    return true;
                }
            }
        }
    }
    false
}

/// Check whether a SQL file content contains a `-- name:` header field.
fn has_header_name_field(content: &str) -> bool {
    for line in content.lines() {
        let trimmed = line.trim();

        // Empty lines don't terminate the header
        if trimmed.is_empty() {
            continue;
        }

        // Comment lines
        if let Some(rest) = trimmed.strip_prefix("--") {
            let rest = rest.trim();
            if rest.starts_with("name:") {
                return true;
            }
            // Other comments or bare `--` — continue scanning header
            continue;
        }

        // Non-empty, non-comment line terminates the header
        break;
    }
    false
}

/// Walk the project directory and discover all files to check.
///
/// Respects `.gitignore`, `.ironlayerignore`, hardcoded exclusions, and
/// config-defined exclusions. Returns only files with matching extensions.
///
/// # Arguments
///
/// * `root` — Project root directory.
/// * `config` — Check configuration (for `exclude` and `extra_extensions`).
///
/// # Returns
///
/// A vector of [`DiscoveredFile`] with relative paths, content, and content hashes.
pub fn walk_files(root: &Path, config: &CheckConfig) -> Vec<DiscoveredFile> {
    let mut builder = WalkBuilder::new(root);

    // Enable .gitignore (default) and add .ironlayerignore support
    builder.git_ignore(true);
    builder.git_global(false);
    builder.git_exclude(false);

    // Add custom ignore files
    let ironlayer_ignore = root.join(".ironlayerignore");
    if ironlayer_ignore.is_file() {
        builder.add_ignore(&ironlayer_ignore);
    }

    // Build the set of allowed extensions
    let mut extensions: Vec<&str> = DEFAULT_EXTENSIONS.to_vec();
    for ext in &config.extra_extensions {
        extensions.push(ext.as_str());
    }

    let mut files = Vec::new();

    for entry in builder.build().flatten() {
        let path = entry.path();

        // Skip directories
        if !path.is_file() {
            continue;
        }

        // Check if the path is in a hardcoded-excluded directory
        if is_in_excluded_dir(root, path) {
            continue;
        }

        // Check config-level exclusions
        if is_config_excluded(root, path, config) {
            continue;
        }

        // Check file extension
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        if !extensions.contains(&ext) {
            continue;
        }

        // Read file content
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Skipping file {} — read error: {}", path.display(), e);
                continue;
            }
        };

        // Compute relative path with forward slashes
        let rel_path = match path.strip_prefix(root) {
            Ok(rel) => rel.to_string_lossy().replace('\\', "/"),
            Err(_) => path.to_string_lossy().replace('\\', "/"),
        };

        // Compute SHA-256 hash
        let content_hash = compute_sha256(&content);

        files.push(DiscoveredFile {
            rel_path,
            content,
            content_hash,
        });
    }

    files
}

/// Walk the project directory collecting only file metadata (no content reads).
///
/// This is the fast-path entry point for warm cache runs. It calls `stat()` on
/// each file to get mtime and size, but never reads file content. The caller
/// uses the returned metadata to check the cache, and only reads content for
/// files that are cache misses.
///
/// # Arguments
///
/// * `root` — Project root directory.
/// * `config` — Check configuration (for `exclude` and `extra_extensions`).
///
/// # Returns
///
/// A vector of [`DiscoveredFileMeta`] with relative paths, sizes, and mtimes.
pub fn walk_file_metadata(root: &Path, config: &CheckConfig) -> Vec<DiscoveredFileMeta> {
    let mut builder = WalkBuilder::new(root);

    builder.git_ignore(true);
    builder.git_global(false);
    builder.git_exclude(false);

    let ironlayer_ignore = root.join(".ironlayerignore");
    if ironlayer_ignore.is_file() {
        builder.add_ignore(&ironlayer_ignore);
    }

    let mut extensions: Vec<&str> = DEFAULT_EXTENSIONS.to_vec();
    for ext in &config.extra_extensions {
        extensions.push(ext.as_str());
    }

    let mut metas = Vec::new();

    for entry in builder.build().flatten() {
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        if is_in_excluded_dir(root, path) {
            continue;
        }

        if is_config_excluded(root, path, config) {
            continue;
        }

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !extensions.contains(&ext) {
            continue;
        }

        // stat() only — no read
        let metadata = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("Skipping file {} — stat error: {}", path.display(), e);
                continue;
            }
        };

        let rel_path = match path.strip_prefix(root) {
            Ok(rel) => rel.to_string_lossy().replace('\\', "/"),
            Err(_) => path.to_string_lossy().replace('\\', "/"),
        };

        let mtime_secs = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        metas.push(DiscoveredFileMeta {
            rel_path,
            size: metadata.len(),
            mtime_secs,
        });
    }

    metas
}

/// Read a single file's content and compute its SHA-256 hash.
///
/// Used to materialize a [`DiscoveredFile`] from a [`DiscoveredFileMeta`] when
/// the file is a cache miss and needs to be checked.
///
/// # Arguments
///
/// * `root` — Project root directory.
/// * `meta` — File metadata from [`walk_file_metadata`].
///
/// # Returns
///
/// `Some(DiscoveredFile)` if the file was read successfully, `None` on I/O error.
pub fn read_file_content(root: &Path, meta: &DiscoveredFileMeta) -> Option<DiscoveredFile> {
    let abs_path = root.join(&meta.rel_path);
    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(e) => {
            log::warn!("Skipping file {} — read error: {}", meta.rel_path, e);
            return None;
        }
    };

    let content_hash = compute_sha256(&content);

    Some(DiscoveredFile {
        rel_path: meta.rel_path.clone(),
        content,
        content_hash,
    })
}

/// Stat a specific set of known file paths, returning their metadata.
///
/// This is the warm-cache fast path: instead of walking the entire directory
/// tree, we stat only the files we already know about from the cache. This
/// reduces warm-cache overhead from O(walk entire tree) to O(stat N files).
///
/// Files that no longer exist are silently omitted from the result.
///
/// # Arguments
///
/// * `root` — Project root directory.
/// * `rel_paths` — Relative paths of files to stat.
///
/// # Returns
///
/// A vector of [`DiscoveredFileMeta`] for files that still exist.
pub fn stat_known_files(root: &Path, rel_paths: &[&str]) -> Vec<DiscoveredFileMeta> {
    let mut metas = Vec::with_capacity(rel_paths.len());

    for rel_path in rel_paths {
        let abs_path = root.join(rel_path);
        let metadata = match std::fs::metadata(&abs_path) {
            Ok(m) if m.is_file() => m,
            _ => continue, // File deleted or inaccessible — treat as cache miss
        };

        let mtime_secs = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        metas.push(DiscoveredFileMeta {
            rel_path: rel_path.to_string(),
            size: metadata.len(),
            mtime_secs,
        });
    }

    metas
}

/// Check if a path is inside a hardcoded-excluded directory.
fn is_in_excluded_dir(root: &Path, path: &Path) -> bool {
    let rel = match path.strip_prefix(root) {
        Ok(r) => r,
        Err(_) => return false,
    };

    for component in rel.components() {
        if let std::path::Component::Normal(name) = component {
            let name_str = name.to_string_lossy();
            if HARDCODED_EXCLUDES.contains(&name_str.as_ref()) {
                return true;
            }
        }
    }
    false
}

/// Check if a path matches any config-level exclusion pattern.
fn is_config_excluded(root: &Path, path: &Path, config: &CheckConfig) -> bool {
    let rel = match path.strip_prefix(root) {
        Ok(r) => r,
        Err(_) => return false,
    };
    let rel_str = rel.to_string_lossy().replace('\\', "/");

    for pattern in &config.exclude {
        // Simple prefix matching for directory patterns (e.g., "target/")
        let normalized = pattern.trim_end_matches('/');
        if rel_str.starts_with(normalized)
            || rel_str.starts_with(&format!("{normalized}/"))
            || rel_str.contains(&format!("/{normalized}/"))
        {
            return true;
        }

        // Glob matching for more complex patterns
        if let Ok(glob) = globset::Glob::new(pattern) {
            let matcher = glob.compile_matcher();
            if matcher.is_match(&rel_str) {
                return true;
            }
        }
    }
    false
}

/// Compute the SHA-256 hex digest of a string.
#[must_use]
pub fn compute_sha256(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// Get the set of changed files in a git repository for `--changed-only` mode.
///
/// Runs `git diff --name-only HEAD` and `git diff --name-only --staged` to
/// collect both modified/untracked files and staged files. Returns their union.
///
/// Falls back to `None` if git is not available or the directory is not a git repo.
///
/// # Arguments
///
/// * `root` — Project root directory (must be inside a git repo).
///
/// # Returns
///
/// `Some(set_of_changed_paths)` if git is available and working,
/// `None` if git is unavailable or directory is not a git repo.
pub fn get_changed_files(root: &Path) -> Option<std::collections::HashSet<String>> {
    use std::collections::HashSet;
    use std::process::Command;

    let mut changed = HashSet::new();

    // Get modified + untracked files relative to HEAD
    let unstaged = Command::new("git")
        .args(["diff", "--name-only", "HEAD"])
        .current_dir(root)
        .output();

    match unstaged {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    changed.insert(trimmed.replace('\\', "/"));
                }
            }
        }
        _ => {
            // git not available or not a git repo — try ls-files for untracked
            // If that also fails, return None
            let untracked = Command::new("git")
                .args(["ls-files", "--others", "--exclude-standard"])
                .current_dir(root)
                .output();

            match untracked {
                Ok(output) if output.status.success() => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    for line in stdout.lines() {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            changed.insert(trimmed.replace('\\', "/"));
                        }
                    }
                }
                _ => return None,
            }
        }
    }

    // Get staged files
    let staged = Command::new("git")
        .args(["diff", "--name-only", "--staged"])
        .current_dir(root)
        .output();

    if let Ok(output) = staged {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    changed.insert(trimmed.replace('\\', "/"));
                }
            }
        }
    }

    Some(changed)
}

/// Filter a list of discovered files to only those that appear in the
/// changed-files set (for `--changed-only` mode).
///
/// Only filters `.sql`, `.yml`, and `.yaml` files. The full list is still needed
/// for building the model registry.
pub fn filter_changed_only(
    files: &[DiscoveredFile],
    changed: &std::collections::HashSet<String>,
) -> Vec<DiscoveredFile> {
    files
        .iter()
        .filter(|f| changed.contains(&f.rel_path))
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_detect_ironlayer_yaml() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::IronLayer);
    }

    #[test]
    fn test_detect_ironlayer_yml() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yml"), "version: 1\n").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::IronLayer);
    }

    #[test]
    fn test_detect_dbt() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("dbt_project.yml"), "name: my_project\n").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Dbt);
    }

    #[test]
    fn test_detect_ironlayer_priority_over_dbt() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("ironlayer.yaml"), "version: 1\n").unwrap();
        fs::write(dir.path().join("dbt_project.yml"), "name: test\n").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::IronLayer);
    }

    #[test]
    fn test_detect_raw_sql() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("query.sql"), "SELECT 1").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::RawSql);
    }

    #[test]
    fn test_detect_ironlayer_from_headers() {
        let dir = tempdir().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("stg_orders.sql"),
            "-- name: stg_orders\n-- kind: FULL_REFRESH\nSELECT 1",
        )
        .unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::IronLayer);
    }

    #[test]
    fn test_detect_empty_dir() {
        let dir = tempdir().unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::RawSql);
    }

    #[test]
    fn test_walk_files_basic() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("model.sql"), "SELECT 1").unwrap();
        fs::write(dir.path().join("config.yml"), "key: val\n").unwrap();
        fs::write(dir.path().join("readme.md"), "# Hello\n").unwrap();

        let config = CheckConfig::default();
        let files = walk_files(dir.path(), &config);
        assert_eq!(files.len(), 2); // .sql and .yml, not .md
    }

    #[test]
    fn test_walk_files_excludes_gitignored() {
        let dir = tempdir().unwrap();
        // The ignore crate requires a .git dir to respect .gitignore
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".gitignore"), "ignored.sql\n").unwrap();
        fs::write(dir.path().join("good.sql"), "SELECT 1").unwrap();
        fs::write(dir.path().join("ignored.sql"), "SELECT 2").unwrap();

        let config = CheckConfig::default();
        let files = walk_files(dir.path(), &config);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_path, "good.sql");
    }

    #[test]
    fn test_walk_files_excludes_hardcoded_dirs() {
        let dir = tempdir().unwrap();
        let target_dir = dir.path().join("target");
        fs::create_dir_all(&target_dir).unwrap();
        fs::write(target_dir.join("compiled.sql"), "SELECT 1").unwrap();
        fs::write(dir.path().join("model.sql"), "SELECT 1").unwrap();

        let config = CheckConfig::default();
        let files = walk_files(dir.path(), &config);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_path, "model.sql");
    }

    #[test]
    fn test_walk_files_forward_slash_paths() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("models").join("staging");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("stg_orders.sql"), "SELECT 1").unwrap();

        let config = CheckConfig::default();
        let files = walk_files(dir.path(), &config);
        assert_eq!(files.len(), 1);
        assert!(files[0].rel_path.contains('/'));
        assert!(!files[0].rel_path.contains('\\'));
    }

    #[test]
    fn test_compute_sha256() {
        let hash = compute_sha256("SELECT 1");
        assert!(hash.starts_with("sha256:"));
        assert_eq!(hash.len(), 7 + 64); // "sha256:" + 64 hex chars
    }

    #[test]
    fn test_compute_sha256_deterministic() {
        let hash1 = compute_sha256("SELECT 1");
        let hash2 = compute_sha256("SELECT 1");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_compute_sha256_different_content() {
        let hash1 = compute_sha256("SELECT 1");
        let hash2 = compute_sha256("SELECT 2");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_has_header_name_field() {
        assert!(has_header_name_field("-- name: stg_orders\nSELECT 1"));
        assert!(has_header_name_field(
            "-- kind: FULL_REFRESH\n-- name: stg_orders\nSELECT 1"
        ));
        assert!(!has_header_name_field("SELECT 1"));
        assert!(!has_header_name_field("-- kind: FULL_REFRESH\nSELECT 1"));
    }

    #[test]
    fn test_has_header_name_field_with_blank_lines() {
        // Blank lines don't terminate the header
        let content = "\n\n-- name: stg_orders\nSELECT 1";
        assert!(has_header_name_field(content));
    }

    #[test]
    fn test_filter_changed_only_includes_changed() {
        let files = vec![
            DiscoveredFile {
                rel_path: "models/a.sql".into(),
                content: "SELECT 1".into(),
                content_hash: compute_sha256("SELECT 1"),
            },
            DiscoveredFile {
                rel_path: "models/b.sql".into(),
                content: "SELECT 2".into(),
                content_hash: compute_sha256("SELECT 2"),
            },
        ];
        let mut changed = std::collections::HashSet::new();
        changed.insert("models/a.sql".to_string());
        let filtered = filter_changed_only(&files, &changed);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].rel_path, "models/a.sql");
    }

    #[test]
    fn test_filter_changed_only_empty_changed_set() {
        let files = vec![DiscoveredFile {
            rel_path: "models/a.sql".into(),
            content: "SELECT 1".into(),
            content_hash: compute_sha256("SELECT 1"),
        }];
        let changed = std::collections::HashSet::new();
        let filtered = filter_changed_only(&files, &changed);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_walk_files_ironlayerignore() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(".ironlayerignore"), "legacy_*.sql\n").unwrap();
        fs::write(dir.path().join("model.sql"), "SELECT 1").unwrap();
        fs::write(dir.path().join("legacy_old.sql"), "SELECT 2").unwrap();

        let config = CheckConfig::default();
        let files = walk_files(dir.path(), &config);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_path, "model.sql");
    }
}
