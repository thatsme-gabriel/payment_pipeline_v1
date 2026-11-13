terraform {
  required_version = ">= 1.6, < 2.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.88.0"
    }
  }
}

provider "confluent" {
  # CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET no ambiente do processo.
}
