"""Configuracao sem executar o conteudo de .env como codigo shell."""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent


def load_env(path: Path | None = None) -> dict[str, str]:
    values = {}
    path = path or ROOT / ".env"
    if path.exists():
        for number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError(f".env: linha {number} sem '='")
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if not key.isidentifier():
                raise ValueError(f".env: nome invalido na linha {number}")
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key] = value
    return values | dict(os.environ)


def require(config: dict, *keys: str) -> None:
    missing = [key for key in keys if not config.get(key)]
    if missing:
        raise ValueError("Preencha no .env: " + ", ".join(missing))


def database_coordinates(config: dict) -> dict[str, str]:
    require(config, "DATABASE_URL", "CDC_PASSWORD")
    parsed = urlsplit(config["DATABASE_URL"])
    if parsed.scheme not in ("postgresql", "postgres") or not parsed.hostname:
        raise ValueError("DATABASE_URL deve ser uma URL Postgres valida")
    if "-pooler" in parsed.hostname:
        raise ValueError("CDC exige endpoint direto do Neon, sem '-pooler'")
    if "EXEMPLO" in parsed.hostname or not parsed.path.strip("/"):
        raise ValueError("Substitua a DATABASE_URL de exemplo pelo banco do desafio")
    return {
        "database.hostname": parsed.hostname,
        "database.port": str(parsed.port or 5432),
        "database.dbname": parsed.path.strip("/"),
        "database.user": "cdc_user",
        "database.sslmode": config.get("DATABASE_SSLMODE", "require"),
    }


def write_json(path: Path, value) -> None:
    from datetime import date, datetime
    from decimal import Decimal

    def encode(item):
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, bytes):
            import base64

            return {"base64": base64.b64encode(item).decode()}
        raise TypeError(type(item).__name__)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=encode) + "\n", encoding="utf-8"
    )


def runtime() -> dict:
    path = ROOT / ".state/runtime.json"
    if not path.exists():
        raise ValueError("Execute setup --apply para gerar .state/runtime.json")
    return json.loads(path.read_text(encoding="utf-8"))
