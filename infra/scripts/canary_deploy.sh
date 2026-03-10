#!/usr/bin/env bash
# canary_deploy.sh — Progressive canary deployment for Azure Container Apps.
#
# Usage:
#   canary_deploy.sh <app-name> <new-image> <health-path> [revision-suffix]
#
# Example:
#   RESOURCE_GROUP=ironlayer-prod \
#   canary_deploy.sh ironlayer-api ghcr.io/org/api:v1.2.3 /api/v1/health v1-2-3
#
# Traffic progression:  0% (smoke test) → 10% (60s) → 50% (60s) → 100%
# On any failure:       traffic is restored to the previous revision, exit 1.
#
# Environment variables required:
#   RESOURCE_GROUP — Azure resource group name
#
# Optional:
#   PROMETHEUS_URL             — Prometheus query endpoint (e.g. http://prometheus:9090).
#                                 When set, error rate is checked between traffic shifts.
#   CANARY_ERROR_RATE_THRESHOLD — Max 5xx error rate (default 0.015 = 1.5%).
#                                 At 5% errors (~old default), a 99.9% availability SLO
#                                 would burn 3.6× the entire monthly error budget in <30 min.
#                                 1.5% keeps budget burn within a single deployment window.

set -euo pipefail

APP="${1:?First argument: Azure Container App name required}"
NEW_IMAGE="${2:?Second argument: new container image required}"
HEALTH_PATH="${3:?Third argument: health check path required (e.g. /api/v1/health)}"
SUFFIX="${4:-$(date +%Y%m%d%H%M%S)}"
RG="${RESOURCE_GROUP:?RESOURCE_GROUP environment variable required}"

# ---------------------------------------------------------------------------
# BL-083: Concurrency lock and cooldown guard.
#
# Prevents concurrent deploys (which can interleave traffic routing steps)
# and rapid successive deploys (which can exhaust Azure Container Apps quota).
# ---------------------------------------------------------------------------
LOCK_FILE="/tmp/ironlayer-deploy-${APP}.lock"
STAMP_FILE="/tmp/ironlayer-deploy-${APP}.timestamp"
COOLDOWN_SECONDS=300  # 5-minute minimum between deploys

# Register cleanup on all exit paths before acquiring the lock.
trap "rm -f '${LOCK_FILE}'" EXIT INT TERM

# Check for concurrent deploy.
if [ -f "${LOCK_FILE}" ]; then
  LOCK_PID=$(cat "${LOCK_FILE}" 2>/dev/null || echo "unknown")
  echo "ERROR: Another deploy is already in progress (PID ${LOCK_PID})." >&2
  echo "       Lock file: ${LOCK_FILE}" >&2
  exit 1
fi

# Check cooldown period.
if [ -f "${STAMP_FILE}" ]; then
  LAST_DEPLOY=$(cat "${STAMP_FILE}" 2>/dev/null || echo "0")
  NOW=$(date +%s)
  ELAPSED=$(( NOW - LAST_DEPLOY ))
  if [ "${ELAPSED}" -lt "${COOLDOWN_SECONDS}" ]; then
    REMAINING=$(( COOLDOWN_SECONDS - ELAPSED ))
    echo "ERROR: Cooldown period active. Last deploy was ${ELAPSED}s ago." >&2
    echo "       Wait ${REMAINING}s before deploying again." >&2
    exit 1
  fi
fi

# Acquire lock and record start time.
echo $$ > "${LOCK_FILE}"

echo "═══════════════════════════════════════════════════════════"
echo "  Canary deploy: ${APP}"
echo "  Image:  ${NEW_IMAGE}"
echo "  Suffix: ${SUFFIX}"
echo "═══════════════════════════════════════════════════════════"

# ---------------------------------------------------------------------------
# 1. Enable multiple-revision mode (idempotent; required for traffic splitting).
# ---------------------------------------------------------------------------
echo "▸ Enabling multiple-revision mode..."
az containerapp update \
  --name "$APP" \
  --resource-group "$RG" \
  --revisions-mode multiple \
  --output none 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Record the currently active revision (the one carrying production traffic).
# ---------------------------------------------------------------------------
OLD_REVISION=$(az containerapp revision list \
  --name "$APP" \
  --resource-group "$RG" \
  --query "[?properties.active && properties.trafficWeight > \`0\`].name | [0]" \
  --output tsv 2>/dev/null || echo "")

echo "▸ Active revision: ${OLD_REVISION:-none}"

# ---------------------------------------------------------------------------
# Helper: on any ERR, restore 100% traffic to the old revision, then exit 1.
# ---------------------------------------------------------------------------
rollback() {
  local CODE=$?
  echo ""
  echo "!!! DEPLOYMENT FAILED (exit ${CODE}) — rolling back ${APP} !!!"
  if [ -n "$OLD_REVISION" ]; then
    echo "!!! Restoring 100% traffic to: ${OLD_REVISION}"
    az containerapp ingress traffic set \
      --name "$APP" \
      --resource-group "$RG" \
      --revision-weight "${OLD_REVISION}=100" \
      --output none 2>/dev/null || true
    echo "!!! Rollback complete."
  else
    echo "!!! No previous revision recorded — traffic state may be inconsistent."
  fi
  exit 1
}
trap rollback ERR

# ---------------------------------------------------------------------------
# 3. BL-144: Capture baseline metrics before deploying the new revision.
# ---------------------------------------------------------------------------
capture_baseline_metrics

# ---------------------------------------------------------------------------
# 4. Deploy the new revision.
# ---------------------------------------------------------------------------
echo "▸ Deploying new revision (suffix: ${SUFFIX})..."
az containerapp update \
  --name "$APP" \
  --resource-group "$RG" \
  --image "$NEW_IMAGE" \
  --revision-suffix "$SUFFIX" \
  --output none

# ---------------------------------------------------------------------------
# 4. Get the new revision name and immediately lock it to 0% traffic.
# ---------------------------------------------------------------------------
NEW_REVISION=$(az containerapp show \
  --name "$APP" \
  --resource-group "$RG" \
  --query "properties.latestRevisionName" \
  --output tsv)

echo "▸ New revision: ${NEW_REVISION}"

if [ -n "$OLD_REVISION" ]; then
  echo "▸ Locking new revision to 0% traffic (old revision holds 100%)..."
  az containerapp ingress traffic set \
    --name "$APP" \
    --resource-group "$RG" \
    --revision-weight "${OLD_REVISION}=100" "${NEW_REVISION}=0" \
    --output none
fi

# ---------------------------------------------------------------------------
# 5. Wait for the new revision to reach Running state (max 3 minutes).
# ---------------------------------------------------------------------------
echo "▸ Waiting for ${NEW_REVISION} to reach Running state..."
for i in $(seq 1 18); do
  REV_STATE=$(az containerapp revision show \
    --name "$APP" \
    --resource-group "$RG" \
    --revision "$NEW_REVISION" \
    --query "properties.runningState" \
    --output tsv 2>/dev/null || echo "Unknown")

  if [ "$REV_STATE" = "Running" ]; then
    echo "  ✓ Revision is Running (attempt ${i}/18)"
    break
  fi

  if [ "$i" = "18" ]; then
    echo "  ✗ Revision did not reach Running state after 3 min (last: ${REV_STATE})"
    false  # triggers rollback trap
  fi

  echo "  Attempt ${i}/18: state=${REV_STATE}, waiting 10s..."
  sleep 10
done

# ---------------------------------------------------------------------------
# 6. Smoke test the new revision directly via its revision FQDN.
# ---------------------------------------------------------------------------
CANARY_FQDN=$(az containerapp revision show \
  --name "$APP" \
  --resource-group "$RG" \
  --revision "$NEW_REVISION" \
  --query "properties.fqdn" \
  --output tsv 2>/dev/null || echo "")

if [ -n "$CANARY_FQDN" ]; then
  echo "▸ Smoke testing canary at https://${CANARY_FQDN}${HEALTH_PATH}"
  SMOKE_OK=false
  for i in 1 2 3 4 5; do
    HTTP_STATUS=$(curl -sf -o /dev/null -w '%{http_code}' \
      --max-time 3 \
      "https://${CANARY_FQDN}${HEALTH_PATH}" 2>/dev/null || echo "000")
    if [ "$HTTP_STATUS" = "200" ]; then
      echo "  ✓ Canary smoke test passed (attempt ${i}/5, HTTP 200)"
      SMOKE_OK=true
      break
    fi
    echo "  Attempt ${i}/5: HTTP ${HTTP_STATUS}, retrying in 10s..."
    sleep 10
  done
  if [ "$SMOKE_OK" != "true" ]; then
    echo "  ✗ Canary smoke test failed after 5 attempts"
    false  # triggers rollback trap
  fi
else
  echo "▸ No revision FQDN available — skipping direct canary smoke test"
fi

# ---------------------------------------------------------------------------
# 7. Progressive traffic shift: 10% → 50% → 100%.
#
# Between each shift, the script waits 60 seconds and then checks the
# 5xx error rate from Prometheus (if PROMETHEUS_URL is set).  When the
# error rate exceeds the threshold the ERR trap fires the rollback.
# ---------------------------------------------------------------------------

# BL-112: Error rate threshold tightened from 5% → 1.5% to align with the
# 99.9% availability SLO.  At 5% errors the monthly error budget would be
# exhausted in under 30 minutes; 1.5% caps the risk within a single deploy window.
# Override via CANARY_ERROR_RATE_THRESHOLD env var (e.g. export CANARY_ERROR_RATE_THRESHOLD=0.02).
ERROR_RATE_THRESHOLD="${CANARY_ERROR_RATE_THRESHOLD:-0.015}"

# BL-143: p95 latency threshold in milliseconds (converted to seconds for Prometheus).
LATENCY_THRESHOLD_MS="${CANARY_LATENCY_THRESHOLD_MS:-500}"

# BL-141: When CANARY_PROMETHEUS_REQUIRED=true (default), a Prometheus outage
# aborts the deploy rather than silently allowing a potentially broken release
# to proceed.  Set to "false" in local/CI environments without Prometheus.
CANARY_PROMETHEUS_REQUIRED="${CANARY_PROMETHEUS_REQUIRED:-true}"

# BL-144: Baseline metrics (captured before deploying the new revision).
BASELINE_ERROR_RATE="0"
BASELINE_P95_LATENCY="0"

_prometheus_query() {
  # Run a Prometheus instant query; print the first result value or "0" on no data.
  # Returns 1 (fail) on curl error so callers can decide fail-open vs fail-closed.
  local query="$1"
  local result
  result=$(curl -sf --max-time 10 \
    "${PROMETHEUS_URL}/api/v1/query" \
    --data-urlencode "query=${query}" 2>/dev/null) || return 1

  echo "$result" | python3 -c "
import json, sys
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
if not results:
    print('0')
else:
    val = results[0].get('value', [0, '0'])[1]
    print(val if val not in ('NaN', 'Inf', '-Inf') else '0')
" 2>/dev/null || echo "0"
}

capture_baseline_metrics() {
  # BL-144: Snapshot error rate and p95 latency from the current production revision.
  [ -z "${PROMETHEUS_URL:-}" ] && return 0
  echo "▸ Capturing baseline metrics from current production traffic..."
  BASELINE_ERROR_RATE=$(_prometheus_query \
    "sum(rate(ironlayer_http_requests_total{job=\"${APP}\",status_code=~\"5..\"}[5m]))/sum(rate(ironlayer_http_requests_total{job=\"${APP}\"}[5m]))" \
    2>/dev/null) || BASELINE_ERROR_RATE="0"
  BASELINE_P95_LATENCY=$(_prometheus_query \
    "histogram_quantile(0.95, rate(ironlayer_http_request_duration_seconds_bucket{job=\"${APP}\"}[5m]))" \
    2>/dev/null) || BASELINE_P95_LATENCY="0"
  echo "  Baseline error rate: ${BASELINE_ERROR_RATE}"
  echo "  Baseline p95 latency: ${BASELINE_P95_LATENCY}s"
}

check_error_rate() {
  # BL-141: Fail-closed when Prometheus is unreachable and CANARY_PROMETHEUS_REQUIRED=true.
  [ -z "${PROMETHEUS_URL:-}" ] && return 0

  local query
  query="sum(rate(ironlayer_http_requests_total{job=\"${APP}\",status_code=~\"5..\"}[2m]))/sum(rate(ironlayer_http_requests_total{job=\"${APP}\"}[2m]))"

  local error_rate
  error_rate=$(_prometheus_query "$query") || {
    if [ "${CANARY_PROMETHEUS_REQUIRED}" = "true" ]; then
      echo "!!! Prometheus unreachable — cannot validate error rate. Aborting canary."
      return 1
    else
      echo "  ⚠ Prometheus query failed — continuing (fail-open; CANARY_PROMETHEUS_REQUIRED=false)"
      return 0
    fi
  }

  echo "  ▸ Error rate: ${error_rate} (threshold: ${ERROR_RATE_THRESHOLD}, baseline: ${BASELINE_ERROR_RATE})"

  # BL-144: Gate fires if rate exceeds BOTH the static threshold AND 2× baseline.
  python3 -c "
import sys
rate       = float('${error_rate}')
threshold  = float('${ERROR_RATE_THRESHOLD}')
baseline   = float('${BASELINE_ERROR_RATE}')
effective  = max(threshold, baseline * 2)
if rate > effective:
    print(f'  ✗ Error rate {rate:.4f} exceeds effective threshold {effective:.4f}')
    sys.exit(1)
" || {
    echo "  ✗ Error rate too high — triggering rollback"
    false  # triggers rollback trap
  }
}

check_latency() {
  # BL-143: Abort if p95 latency exceeds CANARY_LATENCY_THRESHOLD_MS.
  [ -z "${PROMETHEUS_URL:-}" ] && return 0

  local query
  query="histogram_quantile(0.95, rate(ironlayer_http_request_duration_seconds_bucket{job=\"${APP}\"}[2m]))"

  local latency_s
  latency_s=$(_prometheus_query "$query") || {
    if [ "${CANARY_PROMETHEUS_REQUIRED}" = "true" ]; then
      echo "!!! Prometheus unreachable — cannot validate latency. Aborting canary."
      return 1
    else
      echo "  ⚠ Latency query failed — continuing (fail-open)"
      return 0
    fi
  }

  local threshold_s
  threshold_s=$(python3 -c "print(${LATENCY_THRESHOLD_MS} / 1000)")

  echo "  ▸ p95 latency: ${latency_s}s (threshold: ${threshold_s}s, baseline: ${BASELINE_P95_LATENCY}s)"

  python3 -c "
import sys
latency   = float('${latency_s}')
threshold = float('${threshold_s}')
baseline  = float('${BASELINE_P95_LATENCY}')
effective = max(threshold, baseline * 2)
if latency > effective:
    print(f'  ✗ p95 latency {latency:.3f}s exceeds effective threshold {effective:.3f}s')
    sys.exit(1)
" || {
    echo "  ✗ Latency too high — triggering rollback"
    false  # triggers rollback trap
  }
}

check_ai_readiness() {
  # BL-142: Poll the AI engine /readiness endpoint before shifting traffic.
  # Only runs when APP contains "ai" in its name.
  [[ "${APP}" != *ai* ]] && return 0

  local ai_fqdn
  ai_fqdn=$(az containerapp revision show \
    --name "$APP" \
    --resource-group "$RG" \
    --revision "$NEW_REVISION" \
    --query "properties.fqdn" \
    --output tsv 2>/dev/null || echo "")

  if [ -z "$ai_fqdn" ]; then
    echo "  ⚠ Could not resolve AI engine FQDN — skipping readiness check"
    return 0
  fi

  echo "▸ Polling AI engine readiness at https://${ai_fqdn}/readiness (max 100s)..."
  for i in $(seq 1 10); do
    HTTP_STATUS=$(curl -sf -o /dev/null -w '%{http_code}' \
      --max-time 5 \
      "https://${ai_fqdn}/readiness" 2>/dev/null || echo "000")
    if [ "$HTTP_STATUS" = "200" ]; then
      echo "  ✓ AI engine ready (attempt ${i}/10, HTTP 200)"
      return 0
    fi
    echo "  Attempt ${i}/10: HTTP ${HTTP_STATUS}, waiting 10s..."
    sleep 10
  done

  echo "  ✗ AI engine readiness check failed after 100s — triggering rollback"
  false  # triggers rollback trap
}

# BL-142: Verify AI engine model is warmed before any traffic reaches it.
check_ai_readiness

for WEIGHT in 10 50 100; do
  OLD_WEIGHT=$((100 - WEIGHT))

  if [ -n "$OLD_REVISION" ] && [ "$OLD_WEIGHT" -gt 0 ]; then
    az containerapp ingress traffic set \
      --name "$APP" \
      --resource-group "$RG" \
      --revision-weight "${OLD_REVISION}=${OLD_WEIGHT}" "${NEW_REVISION}=${WEIGHT}" \
      --output none
  else
    az containerapp ingress traffic set \
      --name "$APP" \
      --resource-group "$RG" \
      --revision-weight "${NEW_REVISION}=100" \
      --output none
  fi

  echo "▸ Traffic: ${WEIGHT}% on ${NEW_REVISION}$( [ "$OLD_WEIGHT" -gt 0 ] && echo ", ${OLD_WEIGHT}% on ${OLD_REVISION}" )"

  # Pause between shifts, then validate error rate and latency (skip at 100%).
  if [ "$WEIGHT" -lt 100 ]; then
    sleep 60
    check_error_rate
    check_latency  # BL-143
  fi
done

trap - ERR  # Clear error trap — deployment succeeded.

# ---------------------------------------------------------------------------
# 8. BL-112: Post-100% shift smoke test.
#    Re-verify the health endpoint now that all traffic is on the new revision.
#    Issues that only surface under full load are caught here before cooldown.
# ---------------------------------------------------------------------------
if [ -n "$CANARY_FQDN" ]; then
  echo "▸ Post-100%-shift smoke test at https://${CANARY_FQDN}${HEALTH_PATH}"
  for i in 1 2 3; do
    HTTP_STATUS=$(curl -sf -o /dev/null -w '%{http_code}' \
      --max-time 3 \
      "https://${CANARY_FQDN}${HEALTH_PATH}" 2>/dev/null || echo "000")
    if [ "$HTTP_STATUS" = "200" ]; then
      echo "  ✓ Post-100% smoke test passed (HTTP 200)"
      break
    fi
    if [ "$i" = "3" ]; then
      echo "  ✗ Post-100% smoke test failed (HTTP ${HTTP_STATUS}) — triggering rollback"
      trap rollback ERR
      false
    fi
    echo "  Attempt ${i}/3: HTTP ${HTTP_STATUS}, retrying in 5s..."
    sleep 5
  done
fi

# Record successful deploy timestamp for cooldown enforcement.
date +%s > "${STAMP_FILE}"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ✓ ${APP} deployed successfully at 100% traffic"
echo "    Revision: ${NEW_REVISION}"
echo "═══════════════════════════════════════════════════════════"
