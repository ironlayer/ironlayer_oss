# AGENTS.md — IronLayer

This file provides AI coding agents (Cursor, GitHub Copilot, Claude Code, etc.) with project context.

---

## Project Overview

IronLayer is an open-source SQL control plane that runs BEFORE your transformation framework (dbt, SQLMesh).
It provides deterministic execution plans, column-level lineage, cost modeling, and schema guardrails.

---

## Build and Test Commands

```bash
# Run tests
uv run pytest tests/ -v

# Lint
uv run ruff check .
uv run mypy . --ignore-missing-imports

# Pre-commit
pre-commit run --all-files

# IronLayer CLI
ironlayer plan    # Validated execution plan
ironlayer diff    # Schema/data diff
ironlayer status  # System health
```

---

## Coding Conventions

### Python
- Type hints required, docstrings on public methods
- `rich` for CLI output, `typer` for CLI structure
- `structlog` for structured logging
- Never hardcode credentials — use environment variables
- `uv run` for all commands

### SQL / dbt / SQLMesh
- UPPERCASE keywords, CTE pattern
- IronLayer is framework-agnostic — validates both dbt and SQLMesh projects

### Git
```
type(scope): description
Types: feat, fix, docs, style, refactor, test, chore, ci, perf
```

---

## Contributing

See CONTRIBUTING.md for details on how to contribute to IronLayer.
