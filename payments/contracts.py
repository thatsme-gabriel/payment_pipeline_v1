"""Contratos Avro de dominio; envelopes CDC reais pertencem ao Debezium."""

from __future__ import annotations

import copy
import json
from urllib.parse import quote

from .config import ROOT, write_json
from .http import Rest

MONEY = {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}
TIMESTAMP = {"type": "long", "logicalType": "timestamp-millis"}


def field(name: str, kind, *tags: str) -> dict:
    result = {"name": name, "type": copy.deepcopy(kind)}
    if tags:
        result["confluent:tags"] = list(tags)
    return result


def record(name: str, fields: list[dict]) -> dict:
    return {"type": "record", "name": name, "namespace": "com.bootcamp.payments", "fields": fields}


def definitions() -> dict[str, dict]:
    transaction_fields = [
        field("transaction_id", "long"),
        field("account_id", "long"),
        field("card_id", "long", "PCI"),
        field("merchant_id", "long"),
        field("customer_id", "long", "PII"),
        field("amount", MONEY),
        field("currency", "string"),
        field("account_status", "string"),
        field("occurred_at", TIMESTAMP),
    ]
    return {
        "customers": record(
            "Customer",
            [
                field("id", "long", "PII"),
                field("name", "string", "PII"),
                field("email", "string", "PII"),
                field("document", "string", "PII"),
            ],
        ),
        "accounts": record(
            "Account",
            [
                field("id", "long"),
                field("customer_id", "long", "PII"),
                field("status", "string"),
                field("balance", MONEY),
                field("updated_at", TIMESTAMP),
            ],
        ),
        "cards": record(
            "Card",
            [
                field("id", "long", "PCI"),
                field("account_id", "long"),
                field("token", "string", "PCI"),
                field("last4", "string", "PCI"),
                field("status", "string"),
            ],
        ),
        "merchants": record(
            "Merchant", [field("id", "long"), field("name", "string"), field("category", "string")]
        ),
        "transactions": record(
            "Transaction",
            [
                field("id", "long"),
                field("account_id", "long"),
                field("card_id", "long", "PCI"),
                field("merchant_id", "long"),
                field("amount", MONEY),
                field("currency", "string"),
                field("status", "string"),
                field("occurred_at", TIMESTAMP),
            ],
        ),
        "enriched-key": record("EnrichedKey", [field("transaction_id", "long")]),
        "enriched-value": record("EnrichedTransaction", transaction_fields),
        "fraud-key": record("FraudKey", [field("alert_id", "string")]),
        "fraud-value": record(
            "FraudAlert",
            [
                field("alert_id", "string"),
                field("rule_id", "string"),
                field("card_id", "long", "PCI"),
                field("first_transaction_id", "long"),
                field("second_transaction_id", "long"),
                field("last_transaction_id", "long"),
                field("first_at", TIMESTAMP),
                field("last_at", TIMESTAMP),
                field("transaction_count", "long"),
            ],
        ),
    }


def materialize() -> None:
    for name, schema in definitions().items():
        write_json(ROOT / "schemas" / f"{name}.avsc", schema)


def registry(state, identity="setup") -> Rest:
    return Rest(state["schema_rest"], **state["schema_keys"][identity])


def register(state) -> None:
    api = registry(state)
    for name, schema in definitions().items():
        if name.startswith("enriched-"):
            subject = "desafio-enriched-transactions-" + name.split("-", 1)[1]
        elif name.startswith("fraud-"):
            subject = "desafio.fraud.detected-" + name.split("-", 1)[1]
        else:
            subject = "desafio-contracts-" + name + "-value"
        uri = quote(subject, safe="")
        api.request("PUT", "/config/" + uri, {"compatibility": "BACKWARD"})
        api.request(
            "POST",
            "/subjects/" + uri + "/versions",
            {
                "schemaType": "AVRO",
                "schema": json.dumps(schema),
            },
        )


def variants() -> list[tuple[str, dict, bool]]:
    base = definitions()["transactions"]
    optional = copy.deepcopy(base)
    optional["fields"].append({"name": "channel", "type": "string", "default": "UNKNOWN"})
    required = copy.deepcopy(base)
    required["fields"].append({"name": "channel", "type": "string"})
    removed = copy.deepcopy(base)
    removed["fields"] = [f for f in removed["fields"] if f["name"] != "status"]
    return [
        ("add_field_with_default", optional, True),
        ("add_required_field_without_default", required, False),
        ("remove_required_field", removed, True),
    ]


def local_compatibility() -> list[dict]:
    from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType
    from avro.schema import parse

    writer = parse(json.dumps(definitions()["transactions"]))
    results = []
    for name, schema, expected in variants():
        reader = parse(json.dumps(schema))
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        actual = result.compatibility == SchemaCompatibilityType.compatible
        results.append({"case": name, "compatible": actual, "expected": expected})
    return results


def cloud_compatibility(state) -> list[dict]:
    api = registry(state)
    subject = "desafio-contracts-transactions-value"
    results = []
    for name, schema, expected in variants():
        result = api.request(
            "POST",
            f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
            {"schemaType": "AVRO", "schema": json.dumps(schema)},
        )
        actual = result["is_compatible"]
        results.append({"case": name, "expected": expected, "response": result})
        if actual != expected:
            raise ValueError(f"Compatibilidade inesperada no caso {name}")
    write_json(
        ROOT / "evidencias/03-compatibility.json",
        {
            "source": "schema-registry-live",
            "mode": "BACKWARD",
            "cases": results,
            "spec_correction": "Remover campo obrigatorio e compativel em BACKWARD; "
            "adicionar campo obrigatorio sem default e incompativel.",
        },
    )
    return results


def tag_cdc_schemas(state) -> None:
    api = registry(state)
    tags = {
        "**.name": ["PII"],
        "**.email": ["PII"],
        "**.document": ["PII"],
        "**.customer_id": ["PII"],
        "**.card_id": ["PCI"],
        "**.token": ["PCI"],
        "**.last4": ["PCI"],
    }
    observed = {}
    for table in ("customers", "accounts", "cards", "merchants", "transactions"):
        subject = f"desafio-cdc.public.{table}-value"
        latest = api.request("GET", f"/subjects/{subject}/versions/latest")
        validate_cdc_schema(json.loads(latest["schema"]), table)
        api.request("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        # Reusa o envelope exato do conector: nao registra um record simples em topico CDC.
        api.request(
            "POST",
            f"/subjects/{subject}/versions",
            {
                "schemaType": "AVRO",
                "schema": latest["schema"],
                "metadata": {"tags": tags},
            },
        )
        observed[subject] = api.request("GET", f"/subjects/{subject}/versions/latest")
    write_json(
        ROOT / "evidencias/04-cdc-schemas.json",
        {"source": "schema-registry-live", "subjects": observed},
    )


def validate_cdc_schema(schema: dict, table: str) -> None:
    name = schema.get("name", "")
    full_name = name if "." in name else schema.get("namespace", "") + "." + name
    if not full_name.startswith("com.bootcamp.payments."):
        raise ValueError(f"Namespace CDC inesperado para {table}")
    if not {"before", "after", "op"}.issubset({f["name"] for f in schema["fields"]}):
        raise ValueError(f"Envelope Debezium incompleto para {table}")
    money_field = {"accounts": "balance", "transactions": "amount"}.get(table)
    if money_field:
        found = []

        def visit(node):
            if isinstance(node, list):
                for item in node:
                    visit(item)
            elif isinstance(node, dict):
                if node.get("name") == money_field and "type" in node:
                    kind = node["type"]
                    choices = kind if isinstance(kind, list) else [kind]
                    found.extend(c for c in choices if isinstance(c, dict))
                for item in node.values():
                    if isinstance(item, (dict, list)):
                        visit(item)

        visit(schema)
        if not any(
            c.get("type") == "bytes"
            and c.get("logicalType") == "decimal"
            and c.get("precision") == 15
            and c.get("scale") == 2
            for c in found
        ):
            raise ValueError(f"{table}.{money_field} nao foi capturado como decimal(15,2)")
