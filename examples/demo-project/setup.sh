#!/usr/bin/env bash
# setup.sh — Initialise the IronLayer demo project with git history.
#
# This script creates a self-contained git repository with two commits
# so that ``platform plan . HEAD~1 HEAD`` produces a meaningful plan.
#
# Usage:
#   cd examples/demo-project
#   bash setup.sh          # creates ./demo/ with git repo
#   cd demo
#   platform plan . HEAD~1 HEAD
#
# The first commit contains the initial 8-model pipeline.
# The second commit modifies two models (adds a column, changes
# materialization) so the planner has something to diff.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="${SCRIPT_DIR}/demo"

# Clean any previous run.
if [ -d "${DEMO_DIR}" ]; then
    echo "Removing previous demo directory..."
    rm -rf "${DEMO_DIR}"
fi

echo "=== IronLayer Demo Project Setup ==="
echo ""

# --------------------------------------------------------------------------
# Step 1: Create the project directory and initialise git.
# --------------------------------------------------------------------------
mkdir -p "${DEMO_DIR}/models/raw"
mkdir -p "${DEMO_DIR}/models/staging"
mkdir -p "${DEMO_DIR}/models/analytics"
mkdir -p "${DEMO_DIR}/.ironlayer"

cd "${DEMO_DIR}"

git init --quiet
git config user.email "demo@example.com"
git config user.name "IronLayer Demo"

# --------------------------------------------------------------------------
# Step 2: Create the IronLayer configuration file.
# --------------------------------------------------------------------------
cat > .ironlayer/config.yaml <<'EOF'
project:
  name: demo-ecommerce
  state_store: local
  ai_engine:
    enabled: false
EOF

cat > .env <<'EOF'
PLATFORM_ENV=dev
PLATFORM_STATE_STORE_TYPE=local
EOF

cat > .gitignore <<'EOF'
.ironlayer/state.db
.ironlayer/local.duckdb
*.duckdb
__pycache__/
EOF

# --------------------------------------------------------------------------
# Step 3: Copy the initial model files (commit 1 — baseline).
# --------------------------------------------------------------------------
cp "${SCRIPT_DIR}/models/raw/source_events.sql"                models/raw/
cp "${SCRIPT_DIR}/models/raw/source_orders.sql"                models/raw/
cp "${SCRIPT_DIR}/models/staging/stg_events.sql"               models/staging/
cp "${SCRIPT_DIR}/models/staging/stg_customers.sql"            models/staging/
cp "${SCRIPT_DIR}/models/staging/stg_orders.sql"               models/staging/
cp "${SCRIPT_DIR}/models/analytics/orders_daily.sql"           models/analytics/
cp "${SCRIPT_DIR}/models/analytics/customer_lifetime_value.sql" models/analytics/
cp "${SCRIPT_DIR}/models/analytics/revenue_summary.sql"        models/analytics/

git add -A
git commit --quiet -m "Initial pipeline: 8 models across raw/staging/analytics

- raw.source_events: event ingestion from source system
- raw.source_orders: order ingestion from source system
- staging.stg_events: enriched events with user dimensions
- staging.stg_customers: customer lifecycle staging
- staging.stg_orders: orders with customer dimensions
- analytics.orders_daily: daily order aggregations (incremental)
- analytics.customer_lifetime_value: CLV per customer (full refresh)
- analytics.revenue_summary: executive dashboard metrics"

echo "[1/2] Baseline commit created (8 models)"

# --------------------------------------------------------------------------
# Step 4: Modify two models to create a diff (commit 2 — changes).
# --------------------------------------------------------------------------

# Change 1: Add a new column to orders_daily (schema change).
cat > models/analytics/orders_daily.sql <<'EOF'
-- name: analytics.orders_daily
-- kind: INCREMENTAL_BY_TIME_RANGE
-- materialization: INSERT_OVERWRITE
-- time_column: order_date
-- owner: analytics
-- tags: analytics, orders, sla
-- dependencies: staging.stg_orders

SELECT
    order_date,
    customer_country,
    customer_segment,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_orders,
    COUNT(CASE WHEN status = 'refunded' THEN 1 END) AS refunded_orders,
    -- NEW: net revenue after refunds
    SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END)
        - SUM(CASE WHEN status = 'refunded' THEN total_amount ELSE 0 END) AS net_revenue
FROM {{ ref('staging.stg_orders') }}
WHERE order_date >= '{{ start_date }}'
    AND order_date < '{{ end_date }}'
GROUP BY order_date, customer_country, customer_segment
EOF

# Change 2: Add user_metrics model (new model in the DAG).
cp "${SCRIPT_DIR}/models/analytics/user_metrics.sql" models/analytics/

# Change 3: Update revenue_summary to include user_metrics.
cat > models/analytics/revenue_summary.sql <<'EOF'
-- name: analytics.revenue_summary
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: analytics
-- tags: analytics, revenue, executive-dashboard
-- dependencies: analytics.orders_daily, analytics.customer_lifetime_value, analytics.user_metrics

SELECT
    o.order_date,
    o.customer_country,
    o.total_orders,
    o.unique_customers,
    o.total_revenue,
    o.avg_order_value,
    o.completed_orders,
    o.refunded_orders,
    o.net_revenue,
    clv.total_customers,
    clv.avg_lifetime_value,
    um.total_active_users,
    o.total_revenue / NULLIF(clv.total_customers, 0) AS revenue_per_customer,
    o.total_revenue / NULLIF(um.total_active_users, 0) AS revenue_per_active_user
FROM {{ ref('analytics.orders_daily') }} o
CROSS JOIN (
    SELECT
        COUNT(DISTINCT customer_id) AS total_customers,
        AVG(lifetime_value) AS avg_lifetime_value
    FROM {{ ref('analytics.customer_lifetime_value') }}
) clv
CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total_active_users
    FROM {{ ref('analytics.user_metrics') }}
    WHERE active_days > 0
) um
ORDER BY o.order_date DESC
EOF

git add -A
git commit --quiet -m "Add user_metrics, net_revenue column, update revenue_summary

Changes:
- analytics.orders_daily: add net_revenue column (completed - refunded)
- analytics.user_metrics: new model for user engagement metrics
- analytics.revenue_summary: add revenue_per_active_user from user_metrics"

echo "[2/2] Change commit created (2 modified + 1 new model)"

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
echo ""
echo "=== Demo Project Ready ==="
echo ""
echo "  Location: ${DEMO_DIR}"
echo ""
echo "  Try:"
echo "    cd ${DEMO_DIR}"
echo "    platform plan . HEAD~1 HEAD    # generate a migration plan"
echo "    platform show plan.json        # view the plan"
echo "    platform models models/        # list all models"
echo "    platform lineage models/       # view dependency graph"
echo ""
echo "  Git log:"
git log --oneline
echo ""
