"""Referencia local da regra. Nao executa Kafka, Debezium nem Flink."""

from __future__ import annotations

import hashlib
import io
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

from fastavro import schemaless_reader, schemaless_writer

from .config import ROOT, write_json
from .consumer import AlertStore
from .contracts import definitions
from .seed import BASE_TIME, dataset


def velocity(rows: list[dict]) -> list[dict]:
    """Referencia para T{3,}? / SKIP PAST LAST ROW em dados ordenaveis em memoria."""
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["card_id"]].append(row)
    alerts = []
    for card_id, events in grouped.items():
        events.sort(key=lambda r: (r["occurred_at"], r["transaction_id"]))
        window = []
        for event in events:
            if event["amount"] <= 0:
                window = []
                continue
            window = [
                r for r in window if event["occurred_at"] - r["occurred_at"] < timedelta(seconds=60)
            ]
            window.append(event)
            if len(window) == 3:
                first, second, last = window
                identity = (
                    f"RULE_VELOCITY:{card_id}:{first['transaction_id']}:{last['transaction_id']}"
                )
                alerts.append(
                    {
                        "alert_id": hashlib.sha256(identity.encode()).hexdigest(),
                        "rule_id": "RULE_VELOCITY",
                        "card_id": card_id,
                        "first_transaction_id": first["transaction_id"],
                        "second_transaction_id": second["transaction_id"],
                        "last_transaction_id": last["transaction_id"],
                        "first_at": first["occurred_at"],
                        "last_at": last["occurred_at"],
                        "transaction_count": 3,
                    }
                )
                window = []
    return alerts


def run() -> dict:
    data = dataset()
    accounts = {row["id"]: row for row in data["accounts"]}
    enriched = [
        {
            "transaction_id": row["id"],
            "account_id": row["account_id"],
            "card_id": row["card_id"],
            "merchant_id": row["merchant_id"],
            "customer_id": accounts[row["account_id"]]["customer_id"],
            "amount": row["amount"],
            "currency": row["currency"],
            "account_status": accounts[row["account_id"]]["status"],
            "occurred_at": row["occurred_at"],
        }
        for row in data["transactions"]
    ]
    sample = [
        dict(
            enriched[0],
            transaction_id=3_000_000 + i,
            amount=Decimal("19.90"),
            occurred_at=BASE_TIME + timedelta(days=3, seconds=i * 10),
        )
        for i in range(3)
    ]
    enriched.extend(sample)
    schema = definitions()["enriched-value"]
    decoded = []
    for row in enriched:
        stream = io.BytesIO()
        schemaless_writer(stream, schema, row)
        stream.seek(0)
        decoded.append(schemaless_reader(stream, schema))
    alerts = velocity(decoded)
    store = AlertStore(ROOT / "output/local-demo.sqlite3")
    try:
        before = store.connection.execute("SELECT count(*) FROM alerts").fetchone()[0]
        for alert in alerts:
            store.save(alert, "local-reference", 0, 0)
            store.save(alert, "local-reference", 0, 0)
        after = store.connection.execute("SELECT count(*) FROM alerts").fetchone()[0]
    finally:
        store.close()
    result = {
        "mode": "local-reference-only",
        "cloud_validated": False,
        "enriched_rows": len(enriched),
        "alerts": alerts,
        "new_stored_alerts": after - before,
        "total_stored_alerts": after,
        "note": "Referencia Python com roundtrip Avro e SQLite. "
        "Nao e evidencia de execucao Kafka/Debezium/Flink.",
    }
    write_json(ROOT / "output/local-demo.json", result)
    return result
