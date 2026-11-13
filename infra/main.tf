resource "confluent_environment" "challenge" {
  display_name = "desafio-final"
  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "challenge" {
  display_name = "desafio-basic"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = "us-east-1"
  basic {}
  environment { id = confluent_environment.challenge.id }
}

data "confluent_schema_registry_cluster" "challenge" {
  environment { id = confluent_environment.challenge.id }
}

data "confluent_flink_region" "challenge" {
  cloud  = "AWS"
  region = "us-east-1"
}

locals {
  identities = toset(["setup", "producer", "consumer", "flink"])
  topics = {
    "desafio-cdc.public.customers"    = "compact,delete"
    "desafio-cdc.public.accounts"     = "compact,delete"
    "desafio-cdc.public.cards"        = "compact,delete"
    "desafio-cdc.public.merchants"    = "delete"
    "desafio-cdc.public.transactions" = "delete"
    "desafio-enriched-transactions"   = "delete"
    "desafio.fraud.detected"          = "delete"
  }
}

resource "confluent_service_account" "app" {
  for_each     = local.identities
  display_name = "desafio-${each.key}"
  description  = "Pipeline de pagamentos: identidade ${each.key}"
}

resource "confluent_role_binding" "setup" {
  principal   = "User:${confluent_service_account.app["setup"].id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.challenge.resource_name
}

resource "confluent_role_binding" "flink" {
  principal   = "User:${confluent_service_account.app["flink"].id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.challenge.resource_name
}

# Basic nao oferece RBAC granular Kafka e Flink nao usa ACLs para autorizar SQL.
# A SA Flink recebe acesso somente ao cluster EXCLUSIVO do desafio.
resource "confluent_role_binding" "flink_cluster" {
  principal   = "User:${confluent_service_account.app["flink"].id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.challenge.rbac_crn
}

resource "confluent_role_binding" "schema_access" {
  for_each = {
    producer = "DeveloperWrite"
    consumer = "DeveloperRead"
    flink    = "ResourceOwner"
  }
  principal   = "User:${confluent_service_account.app[each.key].id}"
  role_name   = each.value
  crn_pattern = "${data.confluent_schema_registry_cluster.challenge.resource_name}/subject=desafio*"
}

resource "confluent_api_key" "kafka" {
  for_each     = toset(["setup", "producer", "consumer"])
  display_name = "desafio-${each.key}-kafka"
  owner {
    id          = confluent_service_account.app[each.key].id
    api_version = confluent_service_account.app[each.key].api_version
    kind        = confluent_service_account.app[each.key].kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.challenge.id
    api_version = confluent_kafka_cluster.challenge.api_version
    kind        = confluent_kafka_cluster.challenge.kind
    environment { id = confluent_environment.challenge.id }
  }
  depends_on = [confluent_role_binding.setup]
}

resource "confluent_api_key" "schema" {
  for_each     = toset(["setup", "consumer"])
  display_name = "desafio-${each.key}-schema-registry"
  owner {
    id          = confluent_service_account.app[each.key].id
    api_version = confluent_service_account.app[each.key].api_version
    kind        = confluent_service_account.app[each.key].kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.challenge.id
    api_version = data.confluent_schema_registry_cluster.challenge.api_version
    kind        = data.confluent_schema_registry_cluster.challenge.kind
    environment { id = confluent_environment.challenge.id }
  }
  depends_on = [confluent_role_binding.setup, confluent_role_binding.schema_access]
}

resource "confluent_api_key" "flink" {
  display_name = "desafio-flink-api"
  owner {
    id          = confluent_service_account.app["flink"].id
    api_version = confluent_service_account.app["flink"].api_version
    kind        = confluent_service_account.app["flink"].kind
  }
  managed_resource {
    id          = data.confluent_flink_region.challenge.id
    api_version = data.confluent_flink_region.challenge.api_version
    kind        = data.confluent_flink_region.challenge.kind
    environment { id = confluent_environment.challenge.id }
  }
  depends_on = [confluent_role_binding.flink, confluent_role_binding.flink_cluster]
}

resource "confluent_kafka_topic" "challenge" {
  for_each = local.topics
  kafka_cluster { id = confluent_kafka_cluster.challenge.id }
  topic_name       = each.key
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.challenge.rest_endpoint
  config = {
    "cleanup.policy" = each.value
    "retention.ms"   = "604800000"
  }
  credentials {
    key    = confluent_api_key.kafka["setup"].id
    secret = confluent_api_key.kafka["setup"].secret
  }
}

locals {
  acls = {
    producer_write = {
      account = "producer", type = "TOPIC", name = "desafio-", pattern = "PREFIXED", op = "WRITE"
    }
    producer_create = {
      account = "producer", type = "TOPIC", name = "desafio-", pattern = "PREFIXED", op = "CREATE"
    }
    producer_describe = {
      account = "producer", type = "CLUSTER", name = "kafka-cluster", pattern = "LITERAL", op = "DESCRIBE"
    }
    consumer_read = {
      account = "consumer", type = "TOPIC", name = "desafio-", pattern = "PREFIXED", op = "READ"
    }
    consumer_fraud = {
      account = "consumer", type = "TOPIC", name = "desafio.fraud.detected", pattern = "LITERAL", op = "READ"
    }
    consumer_group = {
      account = "consumer", type = "GROUP", name = "desafio-", pattern = "PREFIXED", op = "READ"
    }
  }
}

resource "confluent_kafka_acl" "challenge" {
  for_each = local.acls
  kafka_cluster { id = confluent_kafka_cluster.challenge.id }
  resource_type = each.value.type
  resource_name = each.value.name
  pattern_type  = each.value.pattern
  principal     = "User:${confluent_service_account.app[each.value.account].id}"
  host          = "*"
  operation     = each.value.op
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.challenge.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka["setup"].id
    secret = confluent_api_key.kafka["setup"].secret
  }
}

resource "confluent_flink_compute_pool" "challenge" {
  display_name = "desafio-flink"
  cloud        = "AWS"
  region       = "us-east-1"
  max_cfu      = 5
  environment { id = confluent_environment.challenge.id }
}

resource "confluent_connector" "cdc" {
  environment { id = confluent_environment.challenge.id }
  kafka_cluster { id = confluent_kafka_cluster.challenge.id }
  config_nonsensitive = merge(
    jsondecode(file("${path.module}/../connectors/cdc.json")),
    var.database,
    { "kafka.api.key" = confluent_api_key.kafka["producer"].id }
  )
  config_sensitive = {
    "database.password" = var.cdc_password
    "kafka.api.secret"  = confluent_api_key.kafka["producer"].secret
  }
  depends_on = [confluent_kafka_acl.challenge, confluent_kafka_topic.challenge, confluent_role_binding.schema_access]
}
