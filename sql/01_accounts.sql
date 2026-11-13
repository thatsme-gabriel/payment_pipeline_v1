-- A chave Avro {id: BIGINT} e inferida como key_id pelo Confluent Flink.
-- O topico compactado fornece uma tabela atualizavel com PK inferida.
ALTER TABLE `desafio-cdc.public.accounts` SET (
  'value.format' = 'avro-debezium-registry',
  'changelog.mode' = 'upsert',
  'scan.startup.mode' = 'earliest-offset'
);
