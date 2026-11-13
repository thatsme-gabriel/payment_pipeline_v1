from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from .config import ROOT, write_json
from .http import ApiError, Rest


def api(state, config):
    prefix = (
        f"/sql/v1/organizations/{config['CONFLUENT_ORGANIZATION_ID']}"
        f"/environments/{state['environment_id']}/statements"
    )
    return Rest(state["flink_rest"], **state["flink_key"]), prefix


def manifest_path() -> Path:
    return ROOT / ".state/statements.json"


def submit(state, config, path: Path) -> dict:
    rest, prefix = api(state, config)
    sql = path.read_text(encoding="utf-8").strip()
    digest = hashlib.sha256(sql.encode()).hexdigest()[:10]
    name = f"desafio-{path.stem.replace('_', '-')}-{digest}"
    manifest = json.loads(manifest_path().read_text()) if manifest_path().exists() else {}
    # O manifesto e salvo ANTES do POST: um timeout de rede ainda permite teardown seguro.
    if path.name in manifest and manifest[path.name] != name:
        raise ValueError(f"{path.name} mudou; remova os statements antigos antes de reaplicar")
    manifest[path.name] = name
    write_json(manifest_path(), manifest)
    try:
        existing = rest.request("GET", prefix + "/" + name)
    except ApiError as exc:
        if exc.status != 404:
            raise
        existing = rest.request(
            "POST",
            prefix,
            {
                "name": name,
                "organization_id": config["CONFLUENT_ORGANIZATION_ID"],
                "environment_id": state["environment_id"],
                "spec": {
                    "statement": sql,
                    "compute_pool_id": state["compute_pool_id"],
                    "principal": state["accounts"]["flink"],
                    "stopped": False,
                    "properties": {
                        "sql.current-catalog": state["environment_name"],
                        "sql.current-database": state["cluster_name"],
                        "sql.local-time-zone": "UTC",
                        "sql.tables.scan.idle-timeout": "5s",
                    },
                },
            },
        )
    deadline = time.monotonic() + int(config.get("TIMEOUT_SECONDS", 900))
    continuous = path.stem.startswith(("10_", "11_"))
    while time.monotonic() < deadline:
        phase = existing.get("status", {}).get("phase")
        if phase == "COMPLETED" or (continuous and phase == "RUNNING"):
            return {"name": name, "phase": phase, "file": path.name}
        if phase in ("FAILED", "STOPPED", "DELETED"):
            raise RuntimeError(f"Statement {name}: {phase}. Consulte exceptions no painel Flink")
        time.sleep(3)
        existing = rest.request("GET", prefix + "/" + name)
    raise TimeoutError(f"Statement {name} nao ficou pronto dentro do prazo")


def deploy(state, config) -> None:
    results = []
    for path in sorted((ROOT / "sql").glob("*.sql")):
        print(f"Flink: {path.name}")
        results.append(submit(state, config, path))
        write_json(
            ROOT / "evidencias/05-flink-statements.json",
            {"source": "flink-live", "statements": results},
        )


def stop(state, config) -> None:
    if not manifest_path().exists():
        return
    rest, prefix = api(state, config)
    manifest = json.loads(manifest_path().read_text())
    for name in reversed(list(manifest.values())):
        try:
            rest.request("DELETE", prefix + "/" + name)
        except ApiError as exc:
            if exc.status != 404:
                raise
    names = {item["name"] for item in rest.pages(prefix)}
    remaining = set(manifest.values()) & names
    if remaining:
        raise RuntimeError("Statements ainda presentes: " + ", ".join(sorted(remaining)))
    write_json(
        ROOT / "evidencias/09-flink-empty.json",
        {"source": "flink-live", "owned_statements_remaining": [], "result": "None found."},
    )
