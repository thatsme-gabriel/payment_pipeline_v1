"""Persistencia idempotente: transacao SQLite primeiro, commit Kafka depois."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from .config import ROOT


class AlertStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS alerts ("
            "alert_id TEXT PRIMARY KEY, payload TEXT NOT NULL, topic TEXT NOT NULL, "
            "partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL)"
        )

    def save(self, value: dict, topic: str, partition: int, offset: int) -> bool:
        if not isinstance(value.get("alert_id"), str) or not value["alert_id"]:
            raise ValueError("Alerta sem alert_id valido; offset nao sera confirmado")
        with self.connection:
            result = self.connection.execute(
                "INSERT INTO alerts VALUES (?,?,?,?,?) ON CONFLICT(alert_id) DO NOTHING",
                (value["alert_id"], json.dumps(value, default=str), topic, partition, offset),
            )
        return result.rowcount == 1

    def close(self):
        self.connection.close()


def kafka_config(state, group: str) -> dict:
    if not group.startswith("desafio-"):
        raise ValueError("O grupo deve comecar com desafio- para corresponder a ACL")
    credentials = state["kafka_keys"]["consumer"]
    return {
        "bootstrap.servers": state["bootstrap"].removeprefix("SASL_SSL://"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": credentials["key"],
        "sasl.password": credentials["secret"],
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "isolation.level": "read_committed",
        "session.timeout.ms": 45000,
    }


def kafka_client(state, group: str):
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    credentials = state["schema_keys"]["consumer"]
    schema_registry = SchemaRegistryClient(
        {
            "url": state["schema_rest"],
            "basic.auth.user.info": credentials["key"] + ":" + credentials["secret"],
        }
    )
    return Consumer(kafka_config(state, group)), AvroDeserializer(schema_registry)


def decode(deserializer, message, part="value"):
    from confluent_kafka.serialization import MessageField, SerializationContext

    raw = message.value() if part == "value" else message.key()
    if raw is None:
        return None
    field = MessageField.VALUE if part == "value" else MessageField.KEY
    return deserializer(raw, SerializationContext(message.topic(), field))


def process_message(client, deserializer, store: AlertStore, message) -> bool:
    value = decode(deserializer, message)
    if value is None:
        client.commit(message=message, asynchronous=False)
        return False
    saved = store.save(value, message.topic(), message.partition(), message.offset())
    # Se este commit falhar, a releitura encontra o mesmo alert_id sem repetir o efeito.
    client.commit(message=message, asynchronous=False)
    return saved


def consume(state, group="desafio-alerts", seconds: int | None = None) -> dict:
    client, deserializer = kafka_client(state, group)
    store = AlertStore(ROOT / "data/alerts.sqlite3")
    deadline = time.monotonic() + seconds if seconds is not None else float("inf")
    result = {"read": 0, "stored": 0, "duplicates_or_tombstones": 0}
    client.subscribe(["desafio.fraud.detected"])
    try:
        while time.monotonic() < deadline:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Kafka retornou erro; confira conectividade e ACLs")
            result["read"] += 1
            saved = process_message(client, deserializer, store, message)
            result["stored"] += int(saved)
            result["duplicates_or_tombstones"] += int(not saved)
            print(
                f"Alerta lido: novos={result['stored']} repetidos="
                f"{result['duplicates_or_tombstones']}"
            )
    finally:
        client.close()
        store.close()
    return result
