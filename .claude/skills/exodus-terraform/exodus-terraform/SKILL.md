---
name: exodus-terraform
description: >
  Generate Terraform modules and resources for Exodus Foundation multi-cloud infrastructure.
  Use when creating new modules, adding AWS/Azure/GCP/Databricks resources, modifying
  environments, or reviewing Terraform code.
triggers:
  - "create terraform module"
  - "add terraform resource"
  - "provision infrastructure"
  - "add aws resource"
  - "add azure resource"
  - "add databricks resource"
outputs:
  - "terraform/modules/{name}/main.tf"
  - "terraform/modules/{name}/variables.tf"
  - "terraform/modules/{name}/outputs.tf"
  - "terraform/modules/{name}/versions.tf"
  - "terraform/modules/{name}/README.md"
---

# Terraform Module Generator

> Generate modules that follow Exodus conventions: `for_each`, shared tagging,
> module-per-concern, environment composition, and multi-cloud support.

---

## Before You Start

```bash
ls terraform/modules/
ls terraform/environments/
cat terraform/modules/_shared/tags.tf 2>/dev/null || ls terraform/modules/
```

Read the closest existing module end-to-end before generating a new one.

---

## Required Module File Structure

```
terraform/modules/{name}/
├── main.tf         # Primary resources
├── variables.tf    # Inputs with descriptions and types
├── outputs.tf      # Outputs
├── versions.tf     # Provider version constraints
└── README.md       # Required: purpose, inputs, outputs, example usage
```

---

## `versions.tf` — Always First

```hcl
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }
}
```

---

## `variables.tf` — Typed, Validated, Described

```hcl
variable "client_name" {
  description = "Client slug — must match config/client.yml client.name"
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_-]*$", var.client_name))
    error_message = "client_name must be lowercase alphanumeric with underscores/hyphens."
  }
}

variable "environment" {
  description = "Deployment environment"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "cloud" {
  description = "Target cloud provider"
  type        = string

  validation {
    condition     = contains(["aws", "azure", "gcp"], var.cloud)
    error_message = "cloud must be aws, azure, or gcp."
  }
}
```

---

## `main.tf` — for_each, Never count

```hcl
locals {
  # Load client config from YAML
  client = yamldecode(file("${path.root}/../../config/client.yml"))

  common_tags = {
    Project     = var.client_name
    Environment = var.environment
    Product     = "exodus-foundation"
    ManagedBy   = "terraform"
    Module      = "module-name"
  }

  # Named maps for for_each — never use lists with count
  catalogs = {
    raw  = { name = "foundation_raw",  comment = "Raw source data (Bronze)" }
    dev  = { name = "foundation_dev",  comment = "Development outputs (Silver+Gold)" }
    ci   = { name = "foundation_ci",   comment = "Ephemeral CI schemas" }
    prod = { name = "foundation_prod", comment = "Production outputs (Silver+Gold)" }
  }
}

# CORRECT — for_each with named map
resource "databricks_catalog" "medallion" {
  for_each = local.catalogs
  name     = each.value.name
  comment  = each.value.comment
}

# WRONG — never use count for named resources
# resource "databricks_catalog" "medallion" {
#   count = length(var.catalog_names)
# }
```

## Security Rules (Non-Negotiable)

```hcl
# S3 — always block public, version, encrypt
resource "aws_s3_bucket_public_access_block" "foundation" {
  bucket                  = aws_s3_bucket.foundation.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "foundation" {
  bucket = aws_s3_bucket.foundation.id
  versioning_configuration { status = "Enabled" }
}

# IAM — no wildcard actions on sensitive resources
resource "aws_iam_policy" "databricks" {
  name = "foundation-databricks-${var.environment}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]  # specific, not "*"
      Resource = ["${aws_s3_bucket.foundation.arn}/*"]
    }]
  })
}

# No 0.0.0.0/0 ingress without explicit justification comment
# KMS — enable rotation
resource "aws_kms_key" "foundation" {
  enable_key_rotation = true
}
```

---

## State Backend

```hcl
terraform {
  backend "s3" {
    bucket         = "exodus-terraform-state"
    key            = "${var.client_name}/${var.environment}/module-name.tfstate"
    region         = "us-east-2"
    dynamodb_table = "exodus-terraform-locks"
    encrypt        = true
  }
}
```

---

## `outputs.tf` — Always Output Key IDs

```hcl
output "catalog_ids" {
  description = "Unity Catalog catalog IDs, keyed by layer name"
  value       = { for k, v in databricks_catalog.medallion : k => v.id }
}

output "s3_bucket_arn" {
  description = "ARN of the Foundation raw data S3 bucket"
  value       = aws_s3_bucket.foundation.arn
}

output "warehouse_id" {
  description = "Databricks SQL Warehouse ID for user-facing queries"
  value       = databricks_sql_endpoint.foundation.id
  sensitive   = false
}
```

---

## Multi-Cloud Provider Setup

```hcl
# For Databricks modules — two provider aliases max
provider "databricks" {
  alias = "account"
  host  = "https://accounts.azuredatabricks.net"
}

provider "databricks" {
  alias = "workspace"
  host  = var.workspace_url
}
```

---

## Verify

```bash
terraform fmt -check -recursive terraform/modules/{name}/
terraform validate
terraform plan -var-file=test.tfvars   # with test values
```
