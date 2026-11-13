# Evidências da entrega

Esta pasta não contém resultados fictícios de nuvem. Os JSONs são criados somente por chamadas reais aos serviços. Todos os arquivos gerados nesta pasta, incluindo JSON, CSV, logs e capturas de tela, são excluídos do Git porque podem conter dados de clientes, identificadores de infraestrutura e informações de cobrança. Somente este README é público.

| Arquivo | Como é gerado | O que comprova |
| --- | --- | --- |
| `00-seed.json` | `setup --apply` | Contagens lidas do Postgres e seed=42 |
| `01-envelopes-cdc.json` | `demo` | INSERT, UPDATE, DELETE e tombstone realmente lidos do Kafka |
| `02-fraud-input.json` | `demo` | Três compras inseridas no Postgres para o alerta |
| `02-fraud-alert.json` | `demo` | Alerta Kafka com os três IDs correspondentes |
| `03-compatibility.json` | `setup --apply` | Aceitação e rejeição pela API do Schema Registry e correção do PDF |
| `04-cdc-schemas.json` | `setup --apply` | Envelopes registrados pelo Debezium e classificação dos campos |
| `05-flink-statements.json` | `setup --apply` | Statements concluídos/em execução reportados pelo Flink |
| `06-security.json` | `evidence` | Service accounts, ACLs e papéis |
| `07-metrics.json` | `evidence` | Consultas, amostras e picos de tráfego/lag |
| `08-costs.json` | `evidence` ou `billing` | Lançamentos e soma do ambiente, ainda sujeitos a consolidação |
| `09-flink-empty.json` | `teardown --apply` | Ausência dos statements pertencentes ao projeto |
| `09-connect-empty.json` | `teardown --apply` | Ausência do conector do projeto antes de excluir o cluster |
| `09-teardown.json` | `teardown --apply` | Estado Terraform vazio e slot/publication removidos |

O mapa de permissões, a explicação das garantias e as alavancas de custo ficam em `docs/OPERACAO.md`.

Os resultados da referência local ficam em `output/`, com indicação explícita de que não validam Kafka, Debezium ou Flink.

Antes de compartilhar evidências fora do repositório, remova credenciais e anonimize dados de clientes, IDs de contas e detalhes de cobrança que não sejam necessários para a avaliação. Não use `git add -f` para publicar exportações sem essa revisão.

Os comandos CLI de listagem do PDF são complementares às coletas equivalentes via API. Não são geradas saídas de terminal falsas com `None found.`: esse texto só é salvo depois de a API confirmar a ausência do recurso verificado. No relatório final, diferencie ausência dos recursos do projeto de ausência global de recursos da conta.
