"""Terraform cuida do ciclo de vida; Python coordena banco, contratos e SQL."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from urllib.parse import unquote, urlsplit

from .config import ROOT, database_coordinates, require, runtime, write_json


def terraform_binary(config) -> str:
    requested = config.get("TERRAFORM", "terraform")
    found = shutil.which(requested)
    if not found:
        raise ValueError(
            "Terraform nao encontrado. Instale Terraform >=1.6 ou defina TERRAFORM "
            "com o caminho do executavel. Nenhum recurso foi criado."
        )
    return found


def terraform_env(config) -> dict:
    result = dict(os.environ)
    result.update(
        {
            "CONFLUENT_CLOUD_API_KEY": config["CONFLUENT_CLOUD_API_KEY"],
            "CONFLUENT_CLOUD_API_SECRET": config["CONFLUENT_CLOUD_API_SECRET"],
            "TF_VAR_database": json.dumps(database_coordinates(config)),
            "TF_VAR_cdc_password": config["CDC_PASSWORD"],
            "TF_IN_AUTOMATION": "1",
            "TF_INPUT": "0",
        }
    )
    result.pop("TF_LOG", None)
    result.pop("TF_LOG_PATH", None)
    return result


def scrub(text: str, config) -> str:
    secrets = [v for k, v in config.items() if ("SECRET" in k or "PASSWORD" in k) and v]
    password = urlsplit(config.get("DATABASE_URL", "")).password
    if password:
        secrets += [password, unquote(password)]
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    return text


def terraform(config, *args, private=False) -> str:
    command = [terraform_binary(config), "-chdir=" + str(ROOT / "infra"), *args]
    process = subprocess.run(
        command,
        env=terraform_env(config),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if not private:
        print(scrub(process.stdout, config))
    if process.returncode:
        if not private:
            print(scrub(process.stderr, config))
        raise RuntimeError(
            "Terraform falhou. Estado parcial foi preservado; use teardown --apply "
            "para remover somente recursos gerenciados neste projeto."
        )
    return process.stdout


def setup(config, apply=False) -> None:
    if not apply:
        print(
            "Plano de execucao (sem conexoes externas):\n"
            "1. Verificar credenciais e replicacao logica\n"
            "2. Criar tabelas, role CDC, publication e seed=42\n"
            "3. Terraform: ambiente, cluster Basic, SAs, ACLs, topicos, Flink e conector\n"
            "4. Registrar contratos Avro e tags; testar compatibilidade\n"
            "5. Configurar tabelas e iniciar os dois statements Flink\n"
            "Use setup --apply somente com o .env preenchido e um banco exclusivo."
        )
        return
    require(
        config,
        "CONFLUENT_CLOUD_API_KEY",
        "CONFLUENT_CLOUD_API_SECRET",
        "CONFLUENT_ORGANIZATION_ID",
        "DATABASE_URL",
        "CDC_PASSWORD",
    )
    terraform_binary(config)
    database_coordinates(config)
    if int(config.get("SEED", 42)) != 42:
        raise ValueError("O desafio exige SEED=42")
    if (ROOT / ".state/teardown-progress.json").exists():
        raise ValueError("Ha um teardown incompleto. Termine teardown --apply antes de novo setup")
    (ROOT / ".state").mkdir(exist_ok=True)
    from . import contracts, database, evidence, flink

    database.bootstrap(config)
    terraform(config, "init", "-input=false", "-no-color")
    terraform(config, "validate", "-no-color")
    plan = str(ROOT / ".state/setup.tfplan")
    terraform(config, "plan", "-input=false", "-no-color", "-out=" + plan)
    terraform(config, "apply", "-input=false", "-no-color", plan)
    state = json.loads(terraform(config, "output", "-json", "runtime", private=True))
    write_json(ROOT / ".state/runtime.json", state)
    contracts.register(state)
    evidence.wait_cdc(state, config)
    contracts.tag_cdc_schemas(state)
    contracts.cloud_compatibility(state)
    flink.deploy(state, config)
    print("Setup concluido. Execute 'python -m payments demo' e depois 'consume'.")


def teardown(config, apply=False) -> None:
    if not apply:
        print(
            "Remocao prevista: statements do manifesto, recursos Terraform deste projeto "
            "e slot/publication CDC. Tabelas e dados Neon permanecem. "
            "Execute teardown --apply para remover."
        )
        return
    require(
        config,
        "CONFLUENT_CLOUD_API_KEY",
        "CONFLUENT_CLOUD_API_SECRET",
        "CONFLUENT_ORGANIZATION_ID",
        "DATABASE_URL",
        "CDC_PASSWORD",
    )
    terraform_binary(config)
    from . import database, evidence, flink

    state = runtime() if (ROOT / ".state/runtime.json").exists() else None
    progress_path = ROOT / ".state/teardown-progress.json"
    progress = json.loads(progress_path.read_text()) if progress_path.exists() else {}
    if state and not progress.get("cloud_cleanup_done"):
        flink.stop(state, config)
        # Remove o conector e comprova a lista vazia antes de apagar o cluster pai.
        rest = evidence.cloud(config)
        prefix = evidence.connector_prefix(state)
        from .http import ApiError

        try:
            rest.request("DELETE", prefix + "/desafio-cdc")
        except ApiError as exc:
            if exc.status != 404:
                raise
        import time

        deadline = time.monotonic() + 180
        while True:
            connectors = rest.request("GET", prefix)
            if "desafio-cdc" not in connectors:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Conector ainda aparece na listagem")
            time.sleep(3)
        write_json(
            ROOT / "evidencias/09-connect-empty.json",
            {
                "source": "connect-live",
                "owned_connectors_remaining": [],
                "all_connectors": connectors,
                "result": "None found.",
            },
        )
        progress["cloud_cleanup_done"] = True
        write_json(progress_path, progress)
        write_json(
            ROOT / ".state/billing-context.json",
            {"environment_id": state["environment_id"], "cluster_id": state["cluster_id"]},
        )
    if not progress.get("resources_destroyed"):
        terraform(config, "init", "-input=false", "-no-color")
        terraform(config, "destroy", "-auto-approve", "-input=false", "-no-color")
        remaining = terraform(config, "state", "list", private=True).strip()
        if remaining:
            raise RuntimeError("Terraform ainda lista recursos; teardown incompleto")
        progress["resources_destroyed"] = True
        write_json(progress_path, progress)
    database.cleanup_replication(config)
    write_json(
        ROOT / "evidencias/09-teardown.json",
        {
            "source": "live",
            "terraform_state_remaining": [],
            "replication_slot": "removed-or-absent",
            "publication": "removed-or-absent",
            "neon_tables": "preserved",
            "billing": "recheck-after-72h",
        },
    )
    # Mantem IDs para reconciliar billing, mas revoga o uso local de credenciais apagadas.
    if state:
        write_json(
            ROOT / ".state/billing-context.json",
            {"environment_id": state["environment_id"], "cluster_id": state["cluster_id"]},
        )
        (ROOT / ".state/runtime.json").unlink()
    if (ROOT / ".state/statements.json").exists():
        (ROOT / ".state/statements.json").unlink()
    progress_path.unlink(missing_ok=True)
    print("Teardown concluido. Consulte billing novamente apos a consolidacao das cobrancas.")
