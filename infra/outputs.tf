output "runtime" {
  sensitive = true
  value = {
    environment_id   = confluent_environment.challenge.id
    environment_name = confluent_environment.challenge.display_name
    cluster_id       = confluent_kafka_cluster.challenge.id
    cluster_name     = confluent_kafka_cluster.challenge.display_name
    bootstrap        = confluent_kafka_cluster.challenge.bootstrap_endpoint
    kafka_rest       = confluent_kafka_cluster.challenge.rest_endpoint
    schema_rest      = data.confluent_schema_registry_cluster.challenge.rest_endpoint
    schema_id        = data.confluent_schema_registry_cluster.challenge.id
    flink_rest       = data.confluent_flink_region.challenge.rest_endpoint
    compute_pool_id  = confluent_flink_compute_pool.challenge.id
    connector_name   = "desafio-cdc"
    accounts         = { for name, sa in confluent_service_account.app : name => sa.id }
    kafka_keys = { for name, key in confluent_api_key.kafka : name => {
      key = key.id, secret = key.secret
    } }
    schema_keys = { for name, key in confluent_api_key.schema : name => {
      key = key.id, secret = key.secret
    } }
    flink_key = { key = confluent_api_key.flink.id, secret = confluent_api_key.flink.secret }
  }
}
