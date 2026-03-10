# =============================================================================
# IronLayer Infrastructure — Azure Container Apps
# =============================================================================
#
# Provisions: Virtual Network, Container App Environment, PostgreSQL Flexible
# Server, Container Registry, Key Vault, Log Analytics, WAF Policy, and all
# supporting networking / identity resources.
#
# Equivalent functionality to the previous AWS ECS Fargate configuration,
# mapped 1:1 to Azure services.
# =============================================================================

# -----------------------------------------------------------------------------
# Data Sources
# -----------------------------------------------------------------------------

data "azurerm_resource_group" "this" {
  name = var.resource_group_name
}

data "azurerm_client_config" "current" {}

# -----------------------------------------------------------------------------
# Locals
# -----------------------------------------------------------------------------

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  db_port     = 5432

  common_tags = merge(var.tags, {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  })

  # Subnet CIDR allocation within the VNet address space
  # /16 → four /20 subnets: apps, database, gateway, reserved
  subnet_apps     = cidrsubnet(var.vnet_address_space, 4, 0) # 10.0.0.0/20
  subnet_database = cidrsubnet(var.vnet_address_space, 4, 1) # 10.0.16.0/20
  subnet_gateway  = cidrsubnet(var.vnet_address_space, 4, 2) # 10.0.32.0/20

  # Effective secrets
  effective_jwt_secret = var.jwt_secret != "" ? var.jwt_secret : random_password.jwt_secret[0].result
  database_url         = "postgresql+asyncpg://${var.db_username}:${random_password.db_password.result}@${azurerm_postgresql_flexible_server.this.fqdn}:${local.db_port}/${var.db_name}?ssl=require"
}

# =============================================================================
# Virtual Network
# =============================================================================

resource "azurerm_virtual_network" "this" {
  name                = "${local.name_prefix}-vnet"
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name
  address_space       = [var.vnet_address_space]

  tags = local.common_tags
}

# --- Apps Subnet (Container Apps Environment) --------------------------------

resource "azurerm_subnet" "apps" {
  name                 = "${local.name_prefix}-apps"
  resource_group_name  = data.azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [local.subnet_apps]

  delegation {
    name = "container-apps"
    service_delegation {
      name    = "Microsoft.App/environments"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

# --- Database Subnet ---------------------------------------------------------

resource "azurerm_subnet" "database" {
  name                 = "${local.name_prefix}-database"
  resource_group_name  = data.azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [local.subnet_database]

  delegation {
    name = "postgresql"
    service_delegation {
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }

  service_endpoints = ["Microsoft.Storage"]
}

# --- Gateway Subnet (for Application Gateway / future use) -------------------

resource "azurerm_subnet" "gateway" {
  name                 = "${local.name_prefix}-gateway"
  resource_group_name  = data.azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [local.subnet_gateway]
}

# --- NAT Gateway (outbound connectivity for Container Apps) ------------------

resource "azurerm_public_ip" "nat" {
  name                = "${local.name_prefix}-nat-pip"
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = local.common_tags
}

resource "azurerm_nat_gateway" "this" {
  name                    = "${local.name_prefix}-nat"
  location                = data.azurerm_resource_group.this.location
  resource_group_name     = data.azurerm_resource_group.this.name
  sku_name                = "Standard"
  idle_timeout_in_minutes = 10

  tags = local.common_tags
}

resource "azurerm_nat_gateway_public_ip_association" "this" {
  nat_gateway_id       = azurerm_nat_gateway.this.id
  public_ip_address_id = azurerm_public_ip.nat.id
}

resource "azurerm_subnet_nat_gateway_association" "apps" {
  subnet_id      = azurerm_subnet.apps.id
  nat_gateway_id = azurerm_nat_gateway.this.id
}

# --- Network Security Group (Database Subnet) --------------------------------

resource "azurerm_network_security_group" "database" {
  name                = "${local.name_prefix}-database-nsg"
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name

  security_rule {
    name                       = "allow-postgresql-from-apps"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5432"
    source_address_prefix      = local.subnet_apps
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "deny-all-inbound"
    priority                   = 4096
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = local.common_tags
}

resource "azurerm_subnet_network_security_group_association" "database" {
  subnet_id                 = azurerm_subnet.database.id
  network_security_group_id = azurerm_network_security_group.database.id
}

# =============================================================================
# Log Analytics Workspace
# =============================================================================

resource "azurerm_log_analytics_workspace" "this" {
  name                = "${local.name_prefix}-logs"
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.common_tags
}

# =============================================================================
# Container Registry (ACR)
# =============================================================================

resource "azurerm_container_registry" "this" {
  name                = replace("${var.project_name}${var.environment}acr", "-", "")
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name
  sku                 = "Standard"
  admin_enabled       = false

  tags = local.common_tags
}

# =============================================================================
# User-Assigned Managed Identity
# =============================================================================
#
# Shared identity for all Container Apps — grants ACR pull and Key Vault read.

resource "azurerm_user_assigned_identity" "apps" {
  name                = "${local.name_prefix}-apps-identity"
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name

  tags = local.common_tags
}

# --- ACR Pull role -----------------------------------------------------------

resource "azurerm_role_assignment" "acr_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.apps.principal_id
}

# =============================================================================
# Key Vault
# =============================================================================

resource "azurerm_key_vault" "this" {
  name                       = "${local.name_prefix}-kv"
  location                   = data.azurerm_resource_group.this.location
  resource_group_name        = data.azurerm_resource_group.this.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 90
  purge_protection_enabled   = var.environment == "production"

  rbac_authorization_enabled = true

  # BL-127: Deny all by default; allow only apps subnet + optional IP allowlist.
  network_acls {
    default_action             = "Deny"
    bypass                     = "AzureServices"
    virtual_network_subnet_ids = [azurerm_subnet.apps.id]
    ip_rules                   = var.additional_kv_ip_rules
  }

  tags = local.common_tags
}

# --- Key Vault Diagnostic Settings (BL-128) ----------------------------------

resource "azurerm_monitor_diagnostic_setting" "key_vault" {
  name                       = "${local.name_prefix}-kv-diagnostics"
  target_resource_id         = azurerm_key_vault.this.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category = "AuditEvent"
  }

  enabled_log {
    category = "AzureSDKOperational"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# --- Key Vault RBAC: Terraform SP can manage secrets -------------------------

resource "azurerm_role_assignment" "kv_terraform" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# --- Key Vault RBAC: Container Apps identity can read secrets ----------------

resource "azurerm_role_assignment" "kv_apps" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.apps.principal_id
}

# =============================================================================
# Key Vault Secrets
# =============================================================================

resource "random_password" "db_password" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "random_password" "jwt_secret" {
  count = var.jwt_secret == "" ? 1 : 0

  length  = 64
  special = false
}

resource "azurerm_key_vault_secret" "database_url" {
  name         = "database-url"
  value        = local.database_url
  key_vault_id = azurerm_key_vault.this.id

  tags = local.common_tags

  depends_on = [
    azurerm_role_assignment.kv_terraform,
    azurerm_postgresql_flexible_server.this,
  ]
}

resource "azurerm_key_vault_secret" "jwt_secret" {
  name         = "jwt-secret"
  value        = local.effective_jwt_secret
  key_vault_id = azurerm_key_vault.this.id

  tags = local.common_tags

  depends_on = [azurerm_role_assignment.kv_terraform]
}

resource "azurerm_key_vault_secret" "llm_api_key" {
  count = var.llm_enabled ? 1 : 0

  name         = "llm-api-key"
  value        = var.llm_api_key
  key_vault_id = azurerm_key_vault.this.id

  tags = local.common_tags

  depends_on = [azurerm_role_assignment.kv_terraform]
}

resource "azurerm_key_vault_secret" "stripe_secret_key" {
  count = var.billing_enabled ? 1 : 0

  name         = "stripe-secret-key"
  value        = var.stripe_secret_key
  key_vault_id = azurerm_key_vault.this.id

  tags = local.common_tags

  depends_on = [azurerm_role_assignment.kv_terraform]
}

resource "azurerm_key_vault_secret" "stripe_webhook_secret" {
  count = var.billing_enabled ? 1 : 0

  name         = "stripe-webhook-secret"
  value        = var.stripe_webhook_secret
  key_vault_id = azurerm_key_vault.this.id

  tags = local.common_tags

  depends_on = [azurerm_role_assignment.kv_terraform]
}

# =============================================================================
# PostgreSQL Flexible Server
# =============================================================================

resource "azurerm_private_dns_zone" "postgresql" {
  name                = "${local.name_prefix}.postgres.database.azure.com"
  resource_group_name = data.azurerm_resource_group.this.name

  tags = local.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgresql" {
  name                  = "${local.name_prefix}-pg-dns-link"
  resource_group_name   = data.azurerm_resource_group.this.name
  private_dns_zone_name = azurerm_private_dns_zone.postgresql.name
  virtual_network_id    = azurerm_virtual_network.this.id

  tags = local.common_tags
}

resource "azurerm_postgresql_flexible_server" "this" {
  name                          = "${local.name_prefix}-postgres"
  location                      = data.azurerm_resource_group.this.location
  resource_group_name           = data.azurerm_resource_group.this.name
  version                       = "16"
  delegated_subnet_id           = azurerm_subnet.database.id
  private_dns_zone_id           = azurerm_private_dns_zone.postgresql.id
  public_network_access_enabled = false
  administrator_login           = var.db_username
  administrator_password        = random_password.db_password.result
  storage_mb                    = var.postgresql_storage_mb
  sku_name                      = var.postgresql_sku_name
  # BL-129: Retain 35 days in production, 7 days elsewhere.
  # Geo-redundant backup enabled for production and staging (not dev) and only
  # on non-Burstable SKUs which do not support geo-redundancy.
  backup_retention_days        = var.environment == "production" ? 35 : 7
  geo_redundant_backup_enabled = var.environment != "development" && !startswith(var.postgresql_sku_name, "B_")
  auto_grow_enabled             = true

  zone = "1"

  dynamic "high_availability" {
    for_each = var.postgresql_zone_redundant ? [1] : []
    content {
      mode                      = "ZoneRedundant"
      standby_availability_zone = "2"
    }
  }

  maintenance_window {
    day_of_week  = 0 # Sunday
    start_hour   = 4
    start_minute = 30
  }

  tags = local.common_tags

  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgresql]

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [zone, high_availability[0].standby_availability_zone]
  }
}

# --- PostgreSQL Configuration ------------------------------------------------

resource "azurerm_postgresql_flexible_server_configuration" "log_statement" {
  name      = "log_statement"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "all"
}

resource "azurerm_postgresql_flexible_server_configuration" "log_min_duration" {
  name      = "log_min_duration_statement"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "250"
}

resource "azurerm_postgresql_flexible_server_configuration" "shared_preload" {
  name      = "shared_preload_libraries"
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = "pg_stat_statements"
}

# --- PostgreSQL Database -----------------------------------------------------

resource "azurerm_postgresql_flexible_server_database" "this" {
  name      = var.db_name
  server_id = azurerm_postgresql_flexible_server.this.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# --- PostgreSQL Diagnostic Settings ------------------------------------------

resource "azurerm_monitor_diagnostic_setting" "postgresql" {
  name                       = "${local.name_prefix}-pg-diagnostics"
  target_resource_id         = azurerm_postgresql_flexible_server.this.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category = "PostgreSQLLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# =============================================================================
# Container App Environment
# =============================================================================

resource "azurerm_container_app_environment" "this" {
  name                       = "${local.name_prefix}-env"
  location                   = data.azurerm_resource_group.this.location
  resource_group_name        = data.azurerm_resource_group.this.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id
  infrastructure_subnet_id   = azurerm_subnet.apps.id

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      infrastructure_resource_group_name,
      workload_profile,
    ]
  }
}

# =============================================================================
# Container Apps — API Service
# =============================================================================

resource "azurerm_container_app" "api" {
  name                         = "${local.name_prefix}-api"
  container_app_environment_id = azurerm_container_app_environment.this.id
  resource_group_name          = data.azurerm_resource_group.this.name
  revision_mode                = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.apps.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.apps.id
  }

  template {
    min_replicas = var.api_min_replicas
    max_replicas = var.api_max_replicas

    container {
      name   = "api"
      image  = "${azurerm_container_registry.this.login_server}/${var.project_name}-api:${var.api_image_tag}"
      cpu    = var.api_cpu
      memory = var.api_memory

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "PROJECT_NAME"
        value = var.project_name
      }
      env {
        name  = "PORT"
        value = "8000"
      }
      env {
        name  = "AI_ENGINE_URL"
        value = "https://${azurerm_container_app.ai.ingress[0].fqdn}"
      }
      env {
        name  = "LLM_ENABLED"
        value = tostring(var.llm_enabled)
      }
      env {
        name        = "DATABASE_URL"
        secret_name = "database-url"
      }
      env {
        name        = "JWT_SECRET"
        secret_name = "jwt-secret"
      }
      env {
        name  = "AUTH_MODE"
        value = "jwt"
      }
      env {
        name  = "API_PLATFORM_ENV"
        value = var.environment
      }
      env {
        name  = "API_BILLING_ENABLED"
        value = tostring(var.billing_enabled)
      }

      dynamic "env" {
        for_each = var.billing_enabled ? [1] : []
        content {
          name        = "STRIPE_SECRET_KEY"
          secret_name = "stripe-secret-key"
        }
      }
      dynamic "env" {
        for_each = var.billing_enabled ? [1] : []
        content {
          name        = "STRIPE_WEBHOOK_SECRET"
          secret_name = "stripe-webhook-secret"
        }
      }

      env {
        name  = "API_STRUCTURED_LOGGING"
        value = "true"
      }
      env {
        name  = "API_RATE_LIMIT_ENABLED"
        value = "true"
      }
      env {
        name  = "API_CORS_ORIGINS"
        value = jsonencode(var.cors_origins)
      }

      liveness_probe {
        transport = "HTTP"
        path      = "/api/v1/health"
        port      = 8000

        initial_delay           = 30
        interval_seconds        = 30
        timeout                 = 5
        failure_count_threshold = 3
      }

      readiness_probe {
        transport = "HTTP"
        path      = "/ready"
        port      = 8000

        interval_seconds        = 10
        timeout                 = 5
        failure_count_threshold = 3
      }

      startup_probe {
        transport = "HTTP"
        path      = "/ready"
        port      = 8000

        interval_seconds        = 5
        timeout                 = 3
        failure_count_threshold = 30
      }
    }
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    transport        = "auto"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  secret {
    name                = "database-url"
    key_vault_secret_id = azurerm_key_vault_secret.database_url.id
    identity            = azurerm_user_assigned_identity.apps.id
  }

  secret {
    name                = "jwt-secret"
    key_vault_secret_id = azurerm_key_vault_secret.jwt_secret.id
    identity            = azurerm_user_assigned_identity.apps.id
  }

  dynamic "secret" {
    for_each = var.billing_enabled ? [1] : []
    content {
      name                = "stripe-secret-key"
      key_vault_secret_id = azurerm_key_vault_secret.stripe_secret_key[0].id
      identity            = azurerm_user_assigned_identity.apps.id
    }
  }

  dynamic "secret" {
    for_each = var.billing_enabled ? [1] : []
    content {
      name                = "stripe-webhook-secret"
      key_vault_secret_id = azurerm_key_vault_secret.stripe_webhook_secret[0].id
      identity            = azurerm_user_assigned_identity.apps.id
    }
  }

  tags = merge(local.common_tags, {
    Service = "api"
  })

  depends_on = [
    azurerm_role_assignment.acr_pull,
    azurerm_role_assignment.kv_apps,
    azurerm_container_app.ai,
  ]
}

# =============================================================================
# Container Apps — AI Engine Service
# =============================================================================

resource "azurerm_container_app" "ai" {
  name                         = "${local.name_prefix}-ai"
  container_app_environment_id = azurerm_container_app_environment.this.id
  resource_group_name          = data.azurerm_resource_group.this.name
  revision_mode                = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.apps.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.apps.id
  }

  template {
    min_replicas = var.ai_min_replicas
    max_replicas = var.ai_max_replicas

    container {
      name   = "ai"
      image  = "${azurerm_container_registry.this.login_server}/${var.project_name}-ai:${var.ai_image_tag}"
      cpu    = var.ai_cpu
      memory = var.ai_memory

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "PROJECT_NAME"
        value = var.project_name
      }
      env {
        name  = "PORT"
        value = "8001"
      }
      env {
        name  = "LLM_ENABLED"
        value = tostring(var.llm_enabled)
      }
      env {
        name        = "DATABASE_URL"
        secret_name = "database-url"
      }

      dynamic "env" {
        for_each = var.llm_enabled ? [1] : []
        content {
          name        = "LLM_API_KEY"
          secret_name = "llm-api-key"
        }
      }

      liveness_probe {
        transport = "HTTP"
        path      = "/health"
        port      = 8001

        initial_delay           = 30
        interval_seconds        = 30
        timeout                 = 5
        failure_count_threshold = 3
      }

      # BL-100: /readiness returns 503 until CostPredictor warms up.
      readiness_probe {
        transport = "HTTP"
        path      = "/readiness"
        port      = 8001

        interval_seconds        = 10
        timeout                 = 5
        failure_count_threshold = 3
      }

      # BL-147: Startup probe gives the AI engine up to 5 min to load models.
      startup_probe {
        transport = "HTTP"
        path      = "/readiness"
        port      = 8001

        interval_seconds        = 10
        timeout                 = 5
        failure_count_threshold = 30
      }
    }
  }

  ingress {
    external_enabled = false
    target_port      = 8001
    transport        = "auto"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  secret {
    name                = "database-url"
    key_vault_secret_id = azurerm_key_vault_secret.database_url.id
    identity            = azurerm_user_assigned_identity.apps.id
  }

  dynamic "secret" {
    for_each = var.llm_enabled ? [1] : []
    content {
      name                = "llm-api-key"
      key_vault_secret_id = azurerm_key_vault_secret.llm_api_key[0].id
      identity            = azurerm_user_assigned_identity.apps.id
    }
  }

  tags = merge(local.common_tags, {
    Service = "ai"
  })

  depends_on = [
    azurerm_role_assignment.acr_pull,
    azurerm_role_assignment.kv_apps,
  ]
}

# =============================================================================
# Container Apps — Frontend Service
# =============================================================================

resource "azurerm_container_app" "frontend" {
  name                         = "${local.name_prefix}-frontend"
  container_app_environment_id = azurerm_container_app_environment.this.id
  resource_group_name          = data.azurerm_resource_group.this.name
  revision_mode                = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.apps.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.apps.id
  }

  template {
    min_replicas = var.frontend_min_replicas
    max_replicas = var.frontend_max_replicas

    container {
      name   = "frontend"
      image  = "${azurerm_container_registry.this.login_server}/${var.project_name}-frontend:${var.frontend_image_tag}"
      cpu    = var.frontend_cpu
      memory = var.frontend_memory

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }
      env {
        name  = "API_URL"
        value = "https://${azurerm_container_app.api.ingress[0].fqdn}"
      }

      liveness_probe {
        transport = "HTTP"
        path      = "/"
        port      = 3000

        initial_delay           = 15
        interval_seconds        = 30
        timeout                 = 5
        failure_count_threshold = 3
      }

      readiness_probe {
        transport = "HTTP"
        path      = "/"
        port      = 3000

        interval_seconds        = 10
        timeout                 = 5
        failure_count_threshold = 3
      }
    }
  }

  ingress {
    external_enabled = true
    target_port      = 3000
    transport        = "auto"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  tags = merge(local.common_tags, {
    Service = "frontend"
  })

  depends_on = [
    azurerm_role_assignment.acr_pull,
    azurerm_container_app.api,
  ]
}

# =============================================================================
# Container App Job — Database Migrations
# =============================================================================
#
# Manually triggered job that runs Alembic migrations against the production
# PostgreSQL instance.  Uses the API image (which includes core_engine +
# alembic + psycopg3).  Triggered by CI/CD before deploying new revisions.

resource "azurerm_container_app_job" "migrate" {
  name                         = "${local.name_prefix}-migrate"
  resource_group_name          = data.azurerm_resource_group.this.name
  location                     = data.azurerm_resource_group.this.location
  container_app_environment_id = azurerm_container_app_environment.this.id

  replica_timeout_in_seconds = 600
  replica_retry_limit        = 1

  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "migrate"
      image  = "${azurerm_container_registry.this.login_server}/${var.project_name}-api:${var.api_image_tag}"
      cpu    = 0.25
      memory = "0.5Gi"

      command = [
        "python", "-m", "alembic",
        "-c", "core_engine/state/migrations/alembic.ini",
        "upgrade", "head",
      ]

      env {
        name        = "ALEMBIC_DATABASE_URL"
        secret_name = "database-url"
      }
    }
  }

  secret {
    name                = "database-url"
    key_vault_secret_id = azurerm_key_vault_secret.database_url.id
    identity            = azurerm_user_assigned_identity.apps.id
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.apps.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.apps.id
  }

  tags = merge(local.common_tags, {
    Service = "migrate"
  })

  depends_on = [
    azurerm_role_assignment.acr_pull,
    azurerm_role_assignment.kv_apps,
  ]
}

# =============================================================================
# Storage Account (Diagnostic Logs)
# =============================================================================

resource "azurerm_storage_account" "logs" {
  name                     = replace("${var.project_name}${var.environment}logs", "-", "")
  location                 = data.azurerm_resource_group.this.location
  resource_group_name      = data.azurerm_resource_group.this.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = local.common_tags
}

# =============================================================================
# WAF Policy (for future Application Gateway)
# =============================================================================
#
# Container Apps have built-in ingress with TLS. This WAF policy is pre-created
# for when Application Gateway is added in front for custom domain + WAF.

resource "azurerm_web_application_firewall_policy" "this" {
  name                = "${local.name_prefix}-waf"
  location            = data.azurerm_resource_group.this.location
  resource_group_name = data.azurerm_resource_group.this.name

  policy_settings {
    enabled                     = true
    mode                        = "Prevention"
    request_body_check          = true
    max_request_body_size_in_kb = 128
    file_upload_limit_in_mb     = 100
  }

  # --- Rate limiting (custom rule) -------------------------------------------

  custom_rules {
    name      = "RateLimitPerIP"
    priority  = 1
    rule_type = "RateLimitRule"
    action    = "Block"

    rate_limit_duration  = "FiveMins"
    rate_limit_threshold = 2000
    group_rate_limit_by  = "ClientAddr"

    match_conditions {
      match_variables {
        variable_name = "RemoteAddr"
      }
      operator           = "IPMatch"
      negation_condition = false
      match_values       = ["0.0.0.0/0"]
    }
  }

  # --- Managed rule set (OWASP 3.2) -----------------------------------------

  managed_rules {
    managed_rule_set {
      type    = "OWASP"
      version = "3.2"
    }
  }

  tags = local.common_tags
}
