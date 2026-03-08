"""Type stubs for the ironlayer_check_engine Rust extension module.

This module is built from Rust via PyO3/maturin and provides fast
parallel validation of SQL models, YAML schemas, naming conventions,
ref() integrity, and project structure.
"""

from enum import IntEnum
from typing import Optional

class Dialect(IntEnum):
    """SQL dialect for dialect-aware checks.

    Mirrors ``core_engine.sql_toolkit._types.Dialect``.
    """

    Databricks = ...
    DuckDB = ...
    Redshift = ...

class Severity(IntEnum):
    """Check engine unified severity level.

    Unifies ``sql_guard.Severity`` and ``schema_validator.ViolationSeverity``.
    """

    Error = ...
    Warning = ...
    Info = ...

class CheckCategory(IntEnum):
    """Category of check rule that produced a diagnostic."""

    SqlSyntax = ...
    SqlSafety = ...
    SqlHeader = ...
    RefResolution = ...
    SchemaContract = ...
    YamlSchema = ...
    NamingConvention = ...
    DbtProject = ...
    ModelConsistency = ...
    FileStructure = ...
    DatabricksSql = ...
    IncrementalLogic = ...
    Performance = ...
    TestAdequacy = ...

class CheckDiagnostic:
    """A single diagnostic (error, warning, or info) produced by a check rule."""

    @property
    def rule_id(self) -> str:
        """Rule identifier (e.g., ``"SQL001"``, ``"REF002"``)."""
        ...
    @property
    def message(self) -> str:
        """Human-readable, actionable message describing the issue."""
        ...
    @property
    def severity(self) -> Severity:
        """Severity level for this diagnostic."""
        ...
    @property
    def category(self) -> CheckCategory:
        """Check category that produced this diagnostic."""
        ...
    @property
    def file_path(self) -> str:
        """File path relative to project root (forward slashes)."""
        ...
    @property
    def line(self) -> int:
        """1-based line number (0 if not applicable)."""
        ...
    @property
    def column(self) -> int:
        """1-based column number (0 if not applicable)."""
        ...
    @property
    def snippet(self) -> Optional[str]:
        """Offending text snippet (max 120 chars), if available."""
        ...
    @property
    def suggestion(self) -> Optional[str]:
        """Suggested fix, if one exists."""
        ...
    @property
    def doc_url(self) -> Optional[str]:
        """URL to documentation for this rule."""
        ...
    def __repr__(self) -> str: ...

class CheckResult:
    """Aggregate result of running all checks on a project."""

    @property
    def diagnostics(self) -> list[CheckDiagnostic]:
        """All diagnostics, sorted by ``(file_path, line, column)``."""
        ...
    @property
    def total_files_checked(self) -> int:
        """Number of files that were checked (not cached)."""
        ...
    @property
    def total_files_skipped_cache(self) -> int:
        """Number of files skipped due to cache hit."""
        ...
    @property
    def total_errors(self) -> int:
        """Count of diagnostics with severity ``Error``."""
        ...
    @property
    def total_warnings(self) -> int:
        """Count of diagnostics with severity ``Warning``."""
        ...
    @property
    def total_infos(self) -> int:
        """Count of diagnostics with severity ``Info``."""
        ...
    @property
    def elapsed_ms(self) -> int:
        """Wall-clock milliseconds elapsed for the entire check run."""
        ...
    @property
    def project_type(self) -> str:
        """Auto-detected project type: ``"ironlayer"``, ``"dbt"``, or ``"raw_sql"``."""
        ...
    @property
    def passed(self) -> bool:
        """Whether the check passed (zero errors)."""
        ...
    def to_json(self) -> str:
        """Serialize the result to JSON (spec §8.2 format).

        Raises:
            ValueError: If serialization fails.
        """
        ...
    def to_sarif_json(self) -> str:
        """Serialize the result to SARIF v2.1.0 JSON.

        Suitable for GitHub Code Scanning integration.

        Raises:
            ValueError: If serialization fails.
        """
        ...
    def __repr__(self) -> str: ...

class CheckConfig:
    """Complete check engine configuration.

    Loaded via a 4-level resolution order:
    1. ``ironlayer.check.toml`` (project root)
    2. ``[tool.ironlayer.check]`` in ``pyproject.toml``
    3. ``[check]`` in ``ironlayer.yaml``
    4. Built-in defaults
    """

    def __init__(self) -> None:
        """Create a new CheckConfig with built-in defaults."""
        ...
    @property
    def fail_on_warnings(self) -> bool:
        """Whether warnings should cause a non-zero exit code."""
        ...
    @fail_on_warnings.setter
    def fail_on_warnings(self, value: bool) -> None: ...
    @property
    def max_diagnostics(self) -> int:
        """Maximum number of diagnostics to report (0 = unlimited)."""
        ...
    @max_diagnostics.setter
    def max_diagnostics(self, value: int) -> None: ...
    @property
    def changed_only(self) -> bool:
        """Whether to use ``--changed-only`` mode."""
        ...
    @changed_only.setter
    def changed_only(self, value: bool) -> None: ...
    @property
    def fix(self) -> bool:
        """Whether to auto-fix fixable rules."""
        ...
    @fix.setter
    def fix(self, value: bool) -> None: ...
    @property
    def no_cache(self) -> bool:
        """Whether to disable the cache entirely."""
        ...
    @no_cache.setter
    def no_cache(self, value: bool) -> None: ...
    @property
    def select(self) -> Optional[str]:
        """Comma-separated rule IDs or categories to select."""
        ...
    @select.setter
    def select(self, value: Optional[str]) -> None: ...
    @property
    def exclude_rules(self) -> Optional[str]:
        """Comma-separated rule IDs or categories to exclude."""
        ...
    @exclude_rules.setter
    def exclude_rules(self, value: Optional[str]) -> None: ...
    @property
    def dialect(self) -> Dialect:
        """SQL dialect for dialect-aware checks."""
        ...
    @dialect.setter
    def dialect(self, value: Dialect) -> None: ...
    def __repr__(self) -> str: ...

class CheckEngine:
    """Main check engine orchestrator.

    Usage::

        from ironlayer_check_engine import CheckEngine, CheckConfig

        config = CheckConfig()
        engine = CheckEngine(config)
        result = engine.check("/path/to/project")
    """

    def __init__(self, config: CheckConfig) -> None:
        """Create a new CheckEngine with the given configuration."""
        ...
    def check(self, path: str) -> CheckResult:
        """Run all checks on the project at the given path.

        Args:
            path: Path to the project root directory.

        Returns:
            A ``CheckResult`` containing all diagnostics and summary counts.

        Raises:
            ValueError: If the path is not a directory.
        """
        ...
    def __repr__(self) -> str: ...

def quick_check(path: str) -> CheckResult:
    """One-shot check function (no config required).

    Equivalent to ``CheckEngine(CheckConfig()).check(path)``.

    Args:
        path: Path to the project root directory.

    Returns:
        A ``CheckResult`` with all diagnostics.

    Raises:
        ValueError: If the path is not a valid directory.
    """
    ...
