INSERT INTO `desafio-enriched-transactions`
  (key_transaction_id, transaction_id, account_id, card_id, merchant_id,
   customer_id, amount, currency, account_status, occurred_at)
SELECT t.id, t.id, t.account_id, t.card_id, t.merchant_id,
       a.customer_id, t.amount, t.currency, a.status,
       CAST(t.occurred_at AS TIMESTAMP_LTZ(3))
FROM `desafio-cdc.public.transactions` AS t
JOIN `desafio-cdc.public.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.key_id;
