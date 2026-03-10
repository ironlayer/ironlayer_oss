# -----------------------------------------------------------------------------
# General
# -----------------------------------------------------------------------------

variable "project_name" {
  description = "Name of the project, used as prefix for all resources"
  type        = string
  default     = "ironlayer"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.project_name))
    error_message = "Project name must be lowercase alphanumeric with hyphens, 2-21 characters, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment (production, staging, development)"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["production", "staging", "development"], var.environment)
    error_message = "Environment must be one of: production, staging, development."
  }
}

variable "azure_location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus2"
}

variable "resource_group_name" {
  description = "Name of the Azure resource group (must already exist)"
  type        = string
  default     = "ironlayer-production"
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------

variable "vnet_address_space" {
  description = "Address space for the virtual network"
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrhost(var.vnet_address_space, 0))
    error_message = "VNet address space must be a valid IPv4 CIDR block."
  }
}

# -----------------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------------

variable "postgresql_sku_name" {
  description = "SKU name for Azure Database for PostgreSQL Flexible Server"
  type        = string
  default     = "GP_Standard_D2s_v3"
}

variable "postgresql_zone_redundant" {
  description = "Enable zone-redundant HA for PostgreSQL"
  type        = bool
  default     = true
}

variable "postgresql_storage_mb" {
  description = "Storage size in MB for PostgreSQL (32768 = 32 GB)"
  type        = number
  default     = 32768
}

variable "db_name" {
  description = "Name of the PostgreSQL database"
  type        = string
  default     = "ironlayer"

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]{0,62}$", var.db_name))
    error_message = "Database name must start with a letter and contain only alphanumeric characters and underscores."
  }
}

variable "db_username" {
  description = "Administrator username for the PostgreSQL server"
  type        = string
  default     = "ironlayer"
  sensitive   = true

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]{0,62}$", var.db_username))
    error_message = "Database username must start with a letter and contain only alphanumeric characters and underscores."
  }
}

# -----------------------------------------------------------------------------
# Container Apps — API Service
# -----------------------------------------------------------------------------

variable "api_cpu" {
  description = "CPU cores for the API container (0.5 = half core)"
  type        = number
  default     = 0.5
}

variable "api_memory" {
  description = "Memory (Gi) for the API container"
  type        = string
  default     = "1Gi"
}

variable "api_min_replicas" {
  description = "Minimum number of API replicas"
  type        = number
  default     = 1

  validation {
    condition     = var.api_min_replicas >= 0
    error_message = "API min replicas must be >= 0."
  }
}

variable "api_max_replicas" {
  description = "Maximum number of API replicas"
  type        = number
  default     = 4
}

variable "api_image_tag" {
  description = "Docker image tag for the API service"
  type        = string
  default     = "latest"
}

# -----------------------------------------------------------------------------
# Container Apps — AI Engine Service
# -----------------------------------------------------------------------------

variable "ai_cpu" {
  description = "CPU cores for the AI engine container"
  type        = number
  default     = 0.5
}

variable "ai_memory" {
  description = "Memory (Gi) for the AI engine container"
  type        = string
  default     = "1Gi"
}

variable "ai_min_replicas" {
  description = "Minimum number of AI engine replicas"
  type        = number
  default     = 1
}

variable "ai_max_replicas" {
  description = "Maximum number of AI engine replicas"
  type        = number
  default     = 2
}

variable "ai_image_tag" {
  description = "Docker image tag for the AI engine service"
  type        = string
  default     = "latest"
}

# -----------------------------------------------------------------------------
# Container Apps — Frontend Service
# -----------------------------------------------------------------------------

variable "frontend_cpu" {
  description = "CPU cores for the frontend container"
  type        = number
  default     = 0.25
}

variable "frontend_memory" {
  description = "Memory (Gi) for the frontend container"
  type        = string
  default     = "0.5Gi"
}

variable "frontend_min_replicas" {
  description = "Minimum number of frontend replicas"
  type        = number
  default     = 1
}

variable "frontend_max_replicas" {
  description = "Maximum number of frontend replicas"
  type        = number
  default     = 4
}

variable "frontend_image_tag" {
  description = "Docker image tag for the frontend service"
  type        = string
  default     = "latest"
}

# -----------------------------------------------------------------------------
# TLS / Domain
# -----------------------------------------------------------------------------

variable "domain_name" {
  description = "Custom domain name for the application. Leave empty to use Azure-provided FQDN."
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# Secrets
# -----------------------------------------------------------------------------

variable "jwt_secret" {
  description = "JWT signing secret. If not provided, a random 64-character secret is auto-generated."
  type        = string
  default     = ""
  sensitive   = true
}

variable "llm_enabled" {
  description = "Enable the LLM integration for the AI engine"
  type        = bool
  default     = false
}

variable "llm_api_key" {
  description = "API key for the LLM provider (required when llm_enabled is true)"
  type        = string
  default     = ""
  sensitive   = true
}

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------

variable "cors_origins" {
  description = "Allowed CORS origins for the API service"
  type        = list(string)
  default     = ["https://app.ironlayer.app"]
}

# -----------------------------------------------------------------------------
# Billing (Stripe)
# -----------------------------------------------------------------------------

variable "billing_enabled" {
  description = "Enable Stripe billing integration"
  type        = bool
  default     = false
}

variable "stripe_secret_key" {
  description = "Stripe secret API key (required when billing_enabled is true)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "stripe_webhook_secret" {
  description = "Stripe webhook endpoint signing secret (required when billing_enabled is true)"
  type        = string
  default     = ""
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Observability
# -----------------------------------------------------------------------------

variable "log_retention_days" {
  description = "Log Analytics workspace retention in days"
  type        = number
  default     = 30

  validation {
    condition     = var.log_retention_days >= 30 && var.log_retention_days <= 730
    error_message = "Log retention must be between 30 and 730 days."
  }
}

# -----------------------------------------------------------------------------
# Security
# -----------------------------------------------------------------------------

variable "additional_kv_ip_rules" {
  description = "Additional IP CIDR ranges allowed to access Key Vault (e.g. office IPs, CI runner IPs). Format: [\"203.0.113.0/24\"]"
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for ip in var.additional_kv_ip_rules : can(cidrhost(ip, 0))])
    error_message = "Each entry in additional_kv_ip_rules must be a valid CIDR block."
  }
}
