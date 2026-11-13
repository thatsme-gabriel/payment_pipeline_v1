INSERT INTO `desafio.fraud.detected`
  (key_alert_id, alert_id, rule_id, card_id, first_transaction_id,
   second_transaction_id, last_transaction_id, first_at, last_at, transaction_count)
SELECT
  SHA2(CONCAT('RULE_VELOCITY:', CAST(card_id AS STRING), ':',
       CAST(first_id AS STRING), ':', CAST(last_id AS STRING)), 256),
  SHA2(CONCAT('RULE_VELOCITY:', CAST(card_id AS STRING), ':',
       CAST(first_id AS STRING), ':', CAST(last_id AS STRING)), 256),
  'RULE_VELOCITY', card_id, first_id, second_id, last_id, first_at, last_at, tx_count
FROM `desafio-enriched-transactions`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY occurred_at
  MEASURES
    FIRST(T.transaction_id) AS first_id,
    FIRST(T.transaction_id, 1) AS second_id,
    LAST(T.transaction_id) AS last_id,
    FIRST(T.occurred_at) AS first_at,
    LAST(T.occurred_at) AS last_at,
    COUNT(T.transaction_id) AS tx_count
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS T.amount > 0
    AND T.occurred_at < FIRST(T.occurred_at) + INTERVAL '60' SECOND
);
