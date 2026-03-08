# Quickstart Guide

Get IronLayer running locally in under 5 minutes. No Docker, no PostgreSQL, no Databricks required.

## Prerequisites

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Git

## 1. Install IronLayer

### From PyPI (users)

```bash
uv pip install ironlayer
```

Or with pip:

```bash
pip install ironlayer
```

### From source (contributors)

```bash
# Clone the repository
git clone https://github.com/ironlayer/ironlayer.git
cd ironlayer

# Install all workspace packages in development mode
uv sync --all-packages
```

Verify the installation:

```bash
ironlayer --help
```

## 2. Create a Project

```bash
mkdir my-pipeline && cd my-pipeline
ironlayer init
```

You'll be prompted for:
- **Project name** (default: directory name)
- **State store** (local or postgres)
- **Databricks host** (optional, skip for local dev)
- **AI engine** (optional)

This creates:
```
my-pipeline/
  .ironlayer/config.yaml   # project configuration
  .env                      # environment variables
  .gitignore
  models/
    raw/source_orders.sql         # example source model
    staging/stg_orders.sql        # staging transformation
    analytics/orders_daily.sql    # incremental analytics
    analytics/revenue_summary.sql # dashboard model
```

For non-interactive setup:

```bash
ironlayer init --non-interactive --name my-pipeline
```

## 3. Start the Dev Server

```bash
ironlayer dev
```

This starts:
- **API server** on http://127.0.0.1:8000 (SQLite-backed, no Postgres)
- **DuckDB** for local SQL execution
- **Dev auth mode** (no JWT required)

The API docs are at http://127.0.0.1:8000/docs and readiness probe at http://127.0.0.1:8000/ready.

## 4. Explore Your Models

```bash
# List all models
ironlayer models models/

# View lineage for a specific model
ironlayer lineage models/ -m analytics.revenue_summary
```

## 5. Generate a Plan

Make a change to a model, commit it, then generate a plan:

```bash
# Initialize git (if not already)
git init && git add -A && git commit -m "Initial models"

# Edit a model (e.g., add a column)
echo "-- name: analytics.orders_daily
-- kind: INCREMENTAL_BY_TIME_RANGE
-- materialization: INSERT_OVERWRITE
-- time_column: order_date
-- dependencies: staging.stg_orders

SELECT
    order_date,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value  -- NEW COLUMN
FROM {{ ref('staging.stg_orders') }}
WHERE order_date >= '{{ start_date }}'
    AND order_date < '{{ end_date }}'
GROUP BY order_date" > models/analytics/orders_daily.sql

git add -A && git commit -m "Add avg_order_value column"

# Generate the execution plan
ironlayer plan . HEAD~1 HEAD
```

## 6. View and Apply the Plan

```bash
# Inspect the plan
ironlayer show plan.json

# Execute the plan locally
ironlayer apply plan.json --repo . --auto-approve
```

## 7. Try the Demo Project

For a pre-built example with git history:

```bash
cd examples/demo-project
bash setup.sh
cd demo
ironlayer plan . HEAD~1 HEAD
```

## Options Reference

| Command | Key Flags |
|---------|-----------|
| `ironlayer init` | `--non-interactive`, `--state-store`, `--no-ai`, `--no-git` |
| `ironlayer dev` | `--port`, `--ai-port`, `--no-ai`, `--no-ui`, `--reload` |
| `ironlayer plan` | `--out`, `--as-of-date`, `--json` |
| `ironlayer show` | `--json` |
| `ironlayer apply` | `--auto-approve`, `--approve-by`, `--override-cluster` |
| `ironlayer models` | `--json` |
| `ironlayer lineage` | `--model`, `--json` |

## Connecting to a Deployed Instance

If your team has a IronLayer API server deployed (e.g., on Azure), authenticate via the CLI:

```bash
# Login (stores credentials in ~/.ironlayer/credentials.json)
ironlayer login --api-url https://api.yourdomain.com

# Verify your identity
ironlayer whoami

# All subsequent commands use the stored token automatically
ironlayer plan . HEAD~1 HEAD

# Logout when done
ironlayer logout
```

Alternatively, set the `IRONLAYER_API_TOKEN` environment variable with an API key (created via the web UI or `POST /auth/api-keys`).

## Next Steps

- Read the [CLI Reference](cli-reference.md) for all commands and flags
- See the [Architecture Guide](architecture.md) for how IronLayer works
- Check the [Deployment Guide](deployment.md) for production setup (Azure Container Apps)
- Explore the [API Reference](api-reference.md) for the REST API
