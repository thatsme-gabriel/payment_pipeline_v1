# Pipeline de pagamentos

Projeto do desafio: **Neon/Postgres → Debezium CDC → Kafka/Avro → Confluent Flink → consumidor**.

O código, os contratos, a infraestrutura e os scripts estão implementados. Os testes locais passaram; a infraestrutura e o SQL Flink ainda precisam ser executados e validados em uma conta real. Nenhuma evidência de nuvem foi inventada.

- [Explicação detalhada de tudo que foi implementado](IMPLEMENTACAO.md)
- [Mapa mental em linguagem simples](docs/MAPA_MENTAL.md)
- [Operação: observabilidade, segurança, custos e confiabilidade](docs/OPERACAO.md)
- [Lista de evidências para a entrega](evidencias/README.md)

## Executar localmente

Requer Python 3.11 ou superior. Na pasta do projeto, em PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
.\.venv\Scripts\python.exe -m payments check
.\.venv\Scripts\python.exe -m payments demo-local
.\.venv\Scripts\python.exe -m pytest
```

`check` confere seed e compatibilidade. `demo-local` usa uma referência Python da regra, faz a ida e volta pelo formato Avro e grava em SQLite. Ela **não executa Kafka, Debezium ou Flink**. Os resultados ficam em `output/`.

Para instalar usando as faixas de versão de desenvolvimento, em vez do arquivo fixado:

```powershell
.\.venv\Scripts\python.exe -m pip install -e '.[dev]'
```

## Executar na nuvem

1. Disponibilizar Terraform >= 1.6 no PATH.
2. Criar um banco exclusivo no Neon, habilitar replicação lógica e usar o endereço direto, sem `-pooler`.
3. Copiar `.env.example` para `.env` e preencher os acessos apenas nesse arquivo local.
4. Executar os comandos abaixo. A opção `-Apply` cria recursos que podem gerar cobranças.

```powershell
if (-not (Test-Path -LiteralPath .env)) {
    Copy-Item -LiteralPath .env.example -Destination .env
}
# Preencher .env antes de continuar.
.\setup.ps1             # Mostra o plano de etapas, sem conectar aos serviços.
.\setup.ps1 -Apply      # Cria o banco do desafio e os recursos Confluent.
.\.venv\Scripts\python.exe -m payments demo
.\.venv\Scripts\python.exe -m payments consume --seconds 60
.\.venv\Scripts\python.exe -m payments evidence
.\teardown.ps1 -Apply
# Depois da consolidação do faturamento:
.\.venv\Scripts\python.exe -m payments billing
```

Em Linux/macOS ou Git Bash, os equivalentes são `bash setup.sh --apply` e `bash teardown.sh --apply`. Sem `--apply`, os scripts apenas descrevem as etapas. As dependências precisam estar instaladas no ambiente Python utilizado.

O projeto usa Confluent Cloud na **AWS/us-east-1**. Não é preciso fornecer credenciais da AWS: não há recursos criados diretamente na sua conta AWS.

## Acessos necessários

| Acesso | Finalidade |
| --- | --- |
| Cloud API key e secret do Confluent | Criar e remover os recursos isolados do desafio |
| ID da organização Confluent | Submeter e listar statements Flink |
| `DATABASE_URL` do proprietário do banco Neon | Criar tabelas, publicação e usuário de replicação |
| `CDC_PASSWORD` | Senha exclusiva do usuário `cdc_user` |
| Permissão de métricas e faturamento | Coletar as evidências operacionais; as chaves podem ser separadas |

O setup cria chaves de serviço para o uso diário. A chave de gerenciamento não é utilizada pelo consumidor. O estado Terraform e `.state/` contêm segredos e estão excluídos do Git.

## Duas correções importantes do enunciado

- Em Avro/BACKWARD, remover um campo obrigatório pode ser compatível. O projeto comprova isso e usa a adição de um campo obrigatório **sem default** como caso de rejeição.
- O prefixo `desafio-*` não inclui o tópico `desafio.fraud.detected`. Uma ACL literal adicional permite ao consumidor ler esse tópico.

Veja a justificativa e as referências oficiais em [IMPLEMENTACAO.md](IMPLEMENTACAO.md).

## Antes de publicar no Git

Mantenha `.env.example` sem credenciais e use `.env` para os valores reais. A configuração do
conector contém apenas parâmetros públicos; senhas são fornecidas por variáveis de ambiente.

O `.gitignore` exclui estado e planos Terraform, credenciais, bancos locais, logs e as pastas
`.state/`, `data/`, `output/` e `tmp/`. Em `evidencias/`, somente o README é público: os demais
arquivos podem conter dados pessoais, IDs de contas, capturas de tela e informações de cobrança.

Depois de inicializar o repositório e preparar os arquivos para commit, confira:

```powershell
git status --short
git diff --cached --stat
git diff --cached
```

Arquivos que já estavam rastreados continuam no Git mesmo após uma regra de exclusão ser
adicionada. Remova-os do índice antes de publicar. Se uma credencial real já tiver sido
publicada, revogue/substitua a credencial e trate também o histórico.
