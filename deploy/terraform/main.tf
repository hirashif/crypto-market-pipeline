# aks cluster for the pipeline (free control plane + 1 cheap node)
terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.116"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "rg" {
  name     = "crypto-market-pipeline-rg"
  location = "centralus"
}

resource "azurerm_kubernetes_cluster" "aks" {
  name                = "cmp-aks"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  dns_prefix          = "cmpaks"

  default_node_pool {
    name       = "default"
    node_count = 1
    vm_size    = "Standard_D2s_v3" # 2 vcpu / 8gb amd64 (matches our amd64 images; allowed on student subs)
  }

  identity {
    type = "SystemAssigned"
  }
}

# run this after apply to point kubectl at the cluster
output "get_credentials" {
  value = "az aks get-credentials --resource-group ${azurerm_resource_group.rg.name} --name ${azurerm_kubernetes_cluster.aks.name}"
}
