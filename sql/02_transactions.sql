-- Transacoes do dominio sao imutaveis. Updates/deletes de evidencia usam merchants.
-- append e necessario para MATCH_RECOGNIZE depois do enriquecimento.
ALTER TABLE `desafio-cdc.public.transactions` SET (
  'value.format' = 'avro-debezium-registry',
  'changelog.mode' = 'append',
  'scan.startup.mode' = 'earliest-offset'
);
