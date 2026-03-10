#!/usr/bin/env bash
# =============================================================================
# IronLayer Terraform State Bootstrap (BL-126)
# =============================================================================
#
# Creates the Azure Storage Account and container used for Terraform remote
# state BEFORE running terraform init. This avoids the chicken-and-egg problem
# of managing the state backend with the same config that uses it.
#
# Run this ONCE per environment before the first `terraform init -reconfigure`.
#
# Usage:
#   ENVIRONMENT=production RG=ironlayer-production ./bootstrap.sh
#
# Required env vars:
#   ENVIRONMENT   — production | staging | development
#   RG            — resource group where the tfstate SA will be created
#                   (recommend a separate RG, e.g. ironlayer-tfstate)
#   LOCATION      — Azure region (default: eastus2)
#
# After running this script, run:
#   terraform init -reconfigure \
#     -backend-config="resource_group_name=ironlayer-tfstate" \
#     -backend-config="storage_account_name=ironlayertfstate" \
#     -backend-config="container_name=tfstate" \
#     -backend-config="key=${ENVIRONMENT}/terraform.tfstate"
# =============================================================================

set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:-production}"
LOCATION="${LOCATION:-eastus2}"
TFSTATE_RG="${TFSTATE_RG:-ironlayer-tfstate}"
TFSTATE_SA="${TFSTATE_SA:-ironlayertfstate}"
TFSTATE_CONTAINER="tfstate"

echo "=== IronLayer Terraform State Bootstrap ==="
echo "  Environment : ${ENVIRONMENT}"
echo "  Resource RG : ${TFSTATE_RG}"
echo "  Storage acct: ${TFSTATE_SA}"
echo "  Location    : ${LOCATION}"
echo ""

# --- 1. Create dedicated resource group for Terraform state ------------------
echo "▸ Creating resource group ${TFSTATE_RG}..."
az group create \
  --name "${TFSTATE_RG}" \
  --location "${LOCATION}" \
  --output none

# --- 2. Create storage account -----------------------------------------------
echo "▸ Creating storage account ${TFSTATE_SA}..."
az storage account create \
  --name "${TFSTATE_SA}" \
  --resource-group "${TFSTATE_RG}" \
  --location "${LOCATION}" \
  --sku Standard_GRS \
  --kind StorageV2 \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --allow-shared-key-access false \
  --default-action Deny \
  --output none

echo "  ✓ Storage account created"

# --- 3. Enable versioning and soft-delete for state protection ---------------
echo "▸ Enabling blob versioning and soft-delete (30 days)..."
az storage account blob-service-properties update \
  --account-name "${TFSTATE_SA}" \
  --resource-group "${TFSTATE_RG}" \
  --enable-versioning true \
  --delete-retention-days 30 \
  --output none

echo "  ✓ Blob versioning and soft-delete enabled"

# --- 4. Create tfstate container ---------------------------------------------
echo "▸ Creating blob container ${TFSTATE_CONTAINER}..."
az storage container create \
  --name "${TFSTATE_CONTAINER}" \
  --account-name "${TFSTATE_SA}" \
  --auth-mode login \
  --output none

echo "  ✓ Container created"

# --- 5. Assign Storage Blob Data Contributor to current principal ------------
CURRENT_USER=$(az ad signed-in-user show --query id --output tsv 2>/dev/null || true)
if [ -n "${CURRENT_USER}" ]; then
  echo "▸ Granting Storage Blob Data Contributor to current user..."
  SA_ID=$(az storage account show \
    --name "${TFSTATE_SA}" \
    --resource-group "${TFSTATE_RG}" \
    --query id --output tsv)
  az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee "${CURRENT_USER}" \
    --scope "${SA_ID}" \
    --output none
  echo "  ✓ Role assigned"
fi

# --- 6. Enable diagnostic logs for state access auditing ---------------------
echo "▸ Enabling diagnostic logs (requires existing Log Analytics workspace)..."
SA_ID=$(az storage account show \
  --name "${TFSTATE_SA}" \
  --resource-group "${TFSTATE_RG}" \
  --query id --output tsv)

# Attempt to wire up Log Analytics if a workspace exists in the target RG.
RG="${RG:-ironlayer-production}"
WORKSPACE_ID=$(az monitor log-analytics workspace list \
  --resource-group "${RG}" \
  --query "[0].id" --output tsv 2>/dev/null || echo "")

if [ -n "${WORKSPACE_ID}" ]; then
  az monitor diagnostic-settings create \
    --name "tfstate-audit" \
    --resource "${SA_ID}/blobServices/default" \
    --workspace "${WORKSPACE_ID}" \
    --logs '[{"category":"StorageRead","enabled":true},{"category":"StorageWrite","enabled":true},{"category":"StorageDelete","enabled":true}]' \
    --output none
  echo "  ✓ Diagnostic logs wired to Log Analytics"
else
  echo "  ⚠ No Log Analytics workspace found in ${RG}; skipping diagnostic logs."
  echo "    Wire up manually after Terraform first run creates the workspace."
fi

# --- Done --------------------------------------------------------------------
echo ""
echo "=== Bootstrap complete ==="
echo ""
echo "Next steps:"
echo "  1. cd infra/terraform"
echo "  2. terraform init -reconfigure \\"
echo "       -backend-config=\"resource_group_name=${TFSTATE_RG}\" \\"
echo "       -backend-config=\"storage_account_name=${TFSTATE_SA}\" \\"
echo "       -backend-config=\"container_name=${TFSTATE_CONTAINER}\" \\"
echo "       -backend-config=\"key=${ENVIRONMENT}/terraform.tfstate\""
echo "  3. terraform plan"
