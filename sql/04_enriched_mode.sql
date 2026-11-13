ALTER TABLE `desafio-enriched-transactions` SET (
  'changelog.mode' = 'append',
  'scan.startup.mode' = 'earliest-offset'
);
