terraform {
  required_version = ">= 1.5"

  # Remote state backend — Azure Storage Account (BL-126)
  #
  # BOOTSTRAP REQUIRED before first use:
  #   Run infra/terraform/bootstrap.sh to create the storage account, then:
  #   terraform init -reconfigure \
  #     -backend-config="resource_group_name=<rg>" \
  #     -backend-config="storage_account_name=<account>" \
  #     -backend-config="container_name=tfstate" \
  #     -backend-config="key=infrastructure/terraform.tfstate"
  #
  # To migrate existing local state: terraform init -migrate-state
  backend "azurerm" {
    resource_group_name  = "ironlayer-tfstate"
    storage_account_name = "ironlayertfstate"
    container_name       = "tfstate"
    key                  = "infrastructure/terraform.tfstate"
    use_oidc             = true
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.90"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}
