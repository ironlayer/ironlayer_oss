# CLAUDE.md — IronLayer

> Agent context for the IronLayer repo. Read by Claude Code.
> For workspace-level context, see `iron-layer-dev-workspace/ai/claude/CLAUDE.md`.
> Last updated: March 2026

---

## What IronLayer Is

IronLayer is an **AI-native SQL transformation control plane** for Databricks — the `terraform plan` / `terraform apply` for your dbt models.

```
Tagline: "Know what will run. Know what it costs. Know what it breaks."
License: Apache 2.0 (core)
Repo:    public — ironlayer/ (ironlayer/ironlayer_oss on GitHub)
```

```
Git repo → git diff → model loader → SQL parser → DAG builder
                                                      ↓
State store ← watermarks ← interval planner ← structural diff
    ↑                                                   ↓
Execution records                          plan JSON → executor → Databricks
                                                     ↓ (optional)
                                              AI engine → advisory JSON
```

---

## Two-Layer Architecture

### Layer A — Deterministic Core (`core_engine/`)

Same input → same output. Always.

- Content-based IDs (SHA-256 of plan content)
- Sorted JSON keys, stable SQL canonicalization
- No timestamps in plan payloads
- No LLM in the execution path

**Core modules:**

| Module | Responsibility |
|--------|---------------|
| `loader/` | Load models (raw .sql, dbt, SQLMesh), ref resolution, YAML headers |
| `parser/` | SQL parsing via SQLGlot, normalization, AST |
| `graph/` | DAG build (NetworkX), topological sort, upstream/downstream, column lineage |
| `diff/` | Structural diff (content hashes), AST diff, cosmetic vs structural detection |
| `planner/` | Interval planning, plan serialization, run types |
| `executor/` | Local (DuckDB), Databricks; cluster templates, retry, schema introspection |
| `sql_toolkit/` | SQLGlot: transpiler, qualifier, scope analysis, safety checks |
| `contracts/` | Schema contract validation |
| `state/` | DB layer, Alembic migrations, repository pattern |
| `models/` | `ModelDefinition`, `Plan`, `PlanStep`, `DiffResult` dataclasses |

### Layer B — AI Advisory (`ai_engine/`)

**Advisory only.** AI annotates plans; it never changes them.

- Cost prediction (from execution telemetry)
- Risk scoring (structural vs cosmetic diff, downstream impact)
- SQL optimization suggestions
- Non-blocking: plans are valid without AI advisory

---

## Model Kinds and Materialization

```python
# Model kinds (how IronLayer tracks and executes)
class ModelKind(str, Enum):
    FULL_REFRESH           = "FULL_REFRESH"
    INCREMENTAL_BY_TIME    = "INCREMENTAL_BY_TIME_RANGE"
    APPEND_ONLY            = "APPEND_ONLY"
    MERGE_BY_KEY           = "MERGE_BY_KEY"

# Materialization (how it lands in Databricks)
class Materialization(str, Enum):
    TABLE              = "TABLE"
    VIEW               = "VIEW"
    MERGE              = "MERGE"
    INSERT_OVERWRITE   = "INSERT_OVERWRITE"
```

---

## CLI Reference

```bash
ironlayer init my-project           # create project
ironlayer models ./my-project       # list all models with metadata
ironlayer plan ./project HEAD~1 HEAD  # generate execution plan from git diff
ironlayer plan ./project HEAD~1 HEAD --output plan.json
ironlayer show plan.json            # display a plan file
ironlayer apply plan.json           # execute a plan
ironlayer apply plan.json --auto-approve  # skip confirmation
ironlayer lineage ./project --model staging.orders   # table lineage
ironlayer diff ./project HEAD~1 HEAD --model orders  # semantic diff
ironlayer validate ./project        # schema contract validation
ironlayer transpile query.sql --from redshift --to databricks
ironlayer mcp serve                 # start MCP server (stdio)
ironlayer mcp serve --transport sse --port 3333  # SSE transport
```

---

## MCP Tools (8)

Invoked via `ironlayer mcp serve` (stdio default):

| Tool | Description |
|------|------------|
| `ironlayer_plan` | Plan from git diff — returns steps, cost estimate, risk score |
| `ironlayer_show` | Load and display a saved plan JSON |
| `ironlayer_lineage` | Upstream/downstream table lineage |
| `ironlayer_column_lineage` | Column-level lineage (single or all columns) |
| `ironlayer_diff` | Semantic SQL diff (cosmetic vs structural) |
| `ironlayer_validate` | Schema contract and safety validation |
| `ironlayer_models` | List all models with metadata (kind, materialization, last run) |
| `ironlayer_transpile` | SQL dialect conversion (Redshift → Databricks, etc.) |

MCP dependency: `pip install ironlayer[mcp]` (optional extra — pulls `mcp`, `starlette`, `uvicorn`).

---

## State Management

IronLayer tracks execution state to power incremental runs:

- **Watermarks** — last executed timestamp per model
- **Content hashes** — detect structural changes
- **Execution records** — success/failure, duration, rows processed
- **Database** — SQLite (local dev) or PostgreSQL (production, multi-tenant API)
- **Migrations** — Alembic, `core_engine/state/migrations/`

---

## Testing Strategy

```bash
uv run pytest tests/unit/          # fast, no external deps
uv run pytest tests/integration/   # requires DuckDB (local) or Databricks
uv run pytest tests/e2e/           # full plan → execute → verify cycle
uv run pytest tests/benchmark/     # performance benchmarks
```

Coverage target: 85%+ for `core_engine/` (deterministic code = highly testable).

```python
# Mock Databricks for unit tests
@pytest.fixture
def mock_databricks():
    with patch('core_engine.executor.WorkspaceClient') as mock:
        yield mock.return_value
```

---

## Repo Structure

```
ironlayer/
├── core_engine/        Layer A — deterministic (ironlayer-core PyPI package)
├── ai_engine/          Layer B — AI advisory
├── cli/                Typer CLI + MCP server
│   └── cli/mcp/        tools.py, server.py
├── api/                FastAPI control plane (multi-tenant, billing)
├── frontend/           React SPA (plan viewer, lineage explorer)
├── docs/               Architecture, quickstart, CLI reference
└── examples/           Demo project, GitHub Action workflow
```

---

## PR Requirements

```bash
uv run pytest tests/ -v -x
uv run ruff check . && uv run ruff format --check .
uv run mypy core_engine/ cli/ ai_engine/
# Plans must be deterministic — add tests for new planner logic
```
