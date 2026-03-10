# IronLayer

**AI-native transformation control plane for Databricks.**

IronLayer sits between your SQL models and your Databricks warehouse. It generates deterministic execution plans from git diffs, manages incremental state, and layers AI advisory analysis on top -- cost predictions, risk scoring, and optimization suggestions.

Workspace: [iron-layer-dev-workspace](https://github.com/ironlayer/iron-layer-dev-workspace)

## Quick Install

```bash
pip install ironlayer
```

That's it. The `ironlayer` package pulls in `ironlayer-core` automatically.

### Basic Usage

```bash
# Initialize a new IronLayer project
ironlayer init my-project

# List all discovered models
ironlayer models ./my-project

# Generate a deterministic execution plan from a git diff
ironlayer plan ./my-project HEAD~1 HEAD

# Execute the plan
ironlayer apply plan.json --repo ./my-project --auto-approve

# Trace upstream and downstream lineage
ironlayer lineage ./my-project --model staging.orders
```

### CI/CD Integration

Add IronLayer to your GitHub Actions workflow to get automatic plan comments on pull requests:

```yaml
# .github/workflows/ironlayer.yml
name: IronLayer Plan
on:
  pull_request:
    paths:
      - "models/**"

jobs:
  plan:
    runs-on: ubuntu-latest
    permissions:
      pull-requests: write
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: ironlayer/ironlayer/.github/actions/ironlayer-plan@main
        with:
          repo-path: "."
          # Optional: connect to IronLayer Cloud for AI-powered analysis
          # api-token: ${{ secrets.IRONLAYER_API_TOKEN }}
```

### Connect to IronLayer Cloud (Optional)

Get AI-powered cost estimates, risk scoring, and SQL optimization:

```bash
ironlayer login
```

## Developer Quickstart

For local development with the full stack:

```bash
# Install locally in editable mode
pip install -e core_engine/ -e api/ -e cli/

# Start local dev server (SQLite + DuckDB, no Docker required)
docker compose up -d

# Log in to a running instance
ironlayer login

# Generate an execution plan from a git diff
ironlayer plan . HEAD~1 HEAD

# Execute the plan
ironlayer apply plan.json --auto-approve
```

See [docs/quickstart.md](docs/quickstart.md) for the full guide.

## Architecture

IronLayer is a dual-engine system:

```
                   +--------------+     +--------------+
                   |   CLI (Typer)|     | Frontend (React)
                   +--------------+     +--------------+
                         |                    |
              +----------+--------------------+
              |                               |
    +---------+---------+          +----------+---------+
    | Layer A (Core)    |          | Layer B (AI)       |
    | Deterministic     |          | Advisory           |
    | - Git diff        |          | - Cost predictor   |
    | - DAG builder     |          | - Risk scorer      |
    | - Interval planner|          | - SQL optimizer    |
    | - SQL executor    |          | - LLM client       |
    +---------+---------+          +----------+---------+
              |                               |
    +---------+---------+          +----------+---------+
    | PostgreSQL        |          | FastAPI API        |
    | (state store)     |          | (control plane)    |
    +-------------------+          +--------------------+
```

**Layer A** (core_engine) is purely deterministic: same inputs always produce identical plan JSON. No timestamps, sorted keys, content-based IDs.

**Layer B** (ai_engine) provides advisory-only intelligence. AI never mutates plans -- it annotates with cost predictions, risk scores, and optimization suggestions. Customers bring their own LLM API key (per-tenant, encrypted at rest).

## Features

- **Deterministic planning** -- Git diff to execution plan with zero ambiguity
- **Incremental state** -- Watermark tracking for time-range partitions
- **Multi-tenant** -- Row-level security, per-tenant isolation from day 1
- **AI advisory** -- Cost prediction, risk scoring, SQL optimization (non-mutating)
- **Auth & billing** -- Email/password signup, JWT tokens, Stripe subscriptions
- **Per-tenant LLM keys** -- Customers bring their own Anthropic key, encrypted at rest
- **Onboarding wizard** -- Databricks connection, plan selection, environment setup
- **Migration tools** -- Import from dbt, SQLMesh, or raw SQL
- **Local development** -- Full stack on SQLite + DuckDB, no Docker required
- **Structured telemetry** -- PII-scrubbed, per-run compute metrics
- **Audit trail** -- Hash-chained, tamper-evident audit log

## Monorepo Structure

```
ironlayer/
  core_engine/    # Deterministic core (models, loader, parser, graph, planner, executor)
  ai_engine/      # AI advisory service (classifier, cost predictor, risk scorer, optimizer)
  api/            # FastAPI control plane (routers, services, middleware, security, billing)
  cli/            # Typer CLI (login, plan, show, apply, backfill, models, lineage)
  frontend/       # React + TypeScript + Tailwind SPA
  infra/          # Docker, Terraform (Azure), CI/CD pipeline
  examples/       # Demo project and example models
  docs/           # Architecture, quickstart, CLI reference, API reference, deployment
```

## Documentation

- [Quickstart Guide](docs/quickstart.md) -- Install and run in 5 minutes
- [Architecture](docs/architecture.md) -- Dual-engine design, determinism invariant, multi-tenant model
- [CLI Reference](docs/cli-reference.md) -- Every command with flags, env vars, exit codes
- [API Reference](docs/api-reference.md) -- REST API authentication and endpoints
- [Deployment Guide](docs/deployment.md) -- Docker Compose, Terraform (Azure Container Apps)
- [Azure VM Setup](docs/azure-vm-setup.md) -- Deployment runner VM
- [Release & Deployment Verification](docs/release-verification.md) -- How to verify PyPI, Cloudflare, and docs are live

## Tech Stack

- **Python 3.11+** with uv package management
- **SQLAlchemy 2.0** async with Alembic migrations (15 versioned)
- **FastAPI** + Pydantic v2
- **SQLGlot** (Databricks dialect) for SQL parsing and canonicalization
- **NetworkX** for DAG operations
- **PostgreSQL 16** (production) / **SQLite** (local dev) for metadata
- **DuckDB** for local SQL execution
- **React 18** + TypeScript + Tailwind CSS + ReactFlow
- **Azure Container Apps** / **Docker Compose** for deployment
- **Stripe** for billing and subscriptions

## License

Apache 2.0
