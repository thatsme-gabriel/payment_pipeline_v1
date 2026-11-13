-- Usa o horario da compra. Um snapshot rapido nao transforma compras antigas em fraude.
ALTER TABLE `desafio-enriched-transactions`
MODIFY WATERMARK FOR occurred_at AS occurred_at - INTERVAL '1' SECOND;
