"""Evidencias exclusivamente de servicos reais, sem resultados simulados."""

from __future__ import annotations

import secrets
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode

from .config import ROOT, write_json
from .consumer import decode, kafka_client
from .database import cdc_changes, fraud_changes
from .http import ApiError, Rest


def cloud(config) -> Rest:
    return Rest(
        "https://api.confluent.cloud",
        config["CONFLUENT_CLOUD_API_KEY"],
        config["CONFLUENT_CLOUD_API_SECRET"],
    )


def connector_prefix(state) -> str:
    return (
        f"/connect/v1/environments/{state['environment_id']}"
        f"/clusters/{state['cluster_id']}/connectors"
    )


def wait_cdc(state, config) -> None:
    from .contracts import registry

    rest = cloud(config)
    schema_api = registry(state)
    deadline = time.monotonic() + int(config.get("TIMEOUT_SECONDS", 900))
    while time.monotonic() < deadline:
        status = rest.request("GET", connector_prefix(state) + "/desafio-cdc/status")
        tasks = status.get("tasks", [])
        if any(t.get("state") == "FAILED" for t in tasks):
            raise RuntimeError("Tarefa CDC falhou. Consulte o painel do conector")
        if (
            status.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t.get("state") == "RUNNING" for t in tasks)
        ):
            try:
                for table in ("customers", "accounts", "cards", "merchants", "transactions"):
                    schema_api.request(
                        "GET", f"/subjects/desafio-cdc.public.{table}-value/versions/latest"
                    )
                return
            except ApiError as exc:
                if exc.status != 404:
                    raise
        time.sleep(5)
    raise TimeoutError("CDC nao ficou pronto ou snapshot ainda nao publicou os schemas")


def latest_reader(state, topic: str):
    from confluent_kafka import TopicPartition

    client, deserializer = kafka_client(state, "desafio-evidence-" + secrets.token_hex(4))
    try:
        metadata = client.list_topics(topic=topic, timeout=30)
        assignments = []
        for partition in metadata.topics[topic].partitions:
            _, high = client.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
            assignments.append(TopicPartition(topic, partition, high))
        client.assign(assignments)
        return client, deserializer
    except Exception:
        client.close()
        raise


def message_record(message, value, key) -> dict:
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": key,
        "value": value,
        "timestamp_ms": message.timestamp()[1],
    }


def capture_cdc(state, config, timeout=180) -> None:
    topic = "desafio-cdc.public.merchants"
    client, deserializer = latest_reader(state, topic)
    identifier = 2_000_000_000 + secrets.randbelow(1_000_000_000)
    records = []
    try:
        cdc_changes(config, identifier)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Falha Kafka durante captura CDC")
            key = decode(deserializer, message, "key")
            if not key or key.get("id") != identifier:
                continue
            value = decode(deserializer, message)
            records.append(message_record(message, value, key))
            if value is None:
                validate_envelopes(records)
                write_json(
                    ROOT / "evidencias/01-envelopes-cdc.json",
                    {"source": "kafka-live", "status": "complete", "records": records},
                )
                return
        raise TimeoutError("Nao chegaram os tres envelopes CDC e o tombstone")
    finally:
        client.close()


def validate_envelopes(records: list[dict]) -> None:
    if [r["value"]["op"] if r["value"] else "tombstone" for r in records] != [
        "c",
        "u",
        "d",
        "tombstone",
    ]:
        raise ValueError("Esperados INSERT, UPDATE, DELETE e tombstone, nesta ordem")
    created, updated, deleted, tombstone = [r["value"] for r in records]
    if created["before"] is not None or created["after"] is None:
        raise ValueError("INSERT invalido")
    if not updated["before"] or not updated["after"]:
        raise ValueError("UPDATE sem estados completos; confira REPLICA IDENTITY FULL")
    if deleted["after"] is not None or not deleted["before"] or tombstone is not None:
        raise ValueError("DELETE invalido")
    if len({(r["topic"], r["partition"]) for r in records}) != 1:
        raise ValueError("Eventos da mesma chave deveriam estar na mesma particao")
    if [r["offset"] for r in records] != sorted(r["offset"] for r in records):
        raise ValueError("Offsets fora de ordem")


def capture_fraud(state, config, timeout=240) -> None:
    client, deserializer = latest_reader(state, "desafio.fraud.detected")
    try:
        rows = fraud_changes(config)
        expected_ids = {row["id"] for row in rows}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Falha Kafka durante captura de fraude")
            value = decode(deserializer, message)
            if (
                value
                and {
                    value.get("first_transaction_id"),
                    value.get("second_transaction_id"),
                    value.get("last_transaction_id"),
                }
                == expected_ids
            ):
                write_json(
                    ROOT / "evidencias/02-fraud-alert.json",
                    {
                        "source": "kafka-live",
                        "inputs": rows,
                        "alert": message_record(
                            message, value, decode(deserializer, message, "key")
                        ),
                    },
                )
                return
        raise TimeoutError("Alerta nao chegou. Confira statements, watermarks e temporal join")
    finally:
        client.close()


def security(state, config) -> None:
    rest = Rest(state["kafka_rest"], **state["kafka_keys"]["setup"])
    principals = set(state["accounts"].values())
    acls = rest.pages(f"/kafka/v3/clusters/{state['cluster_id']}/acls")
    accounts = cloud(config).pages("/iam/v2/service-accounts")
    write_json(
        ROOT / "evidencias/06-security.json",
        {
            "source": "confluent-live",
            "accounts": [a for a in accounts if a["id"] in principals],
            "acls": [a for a in acls if a.get("principal", "").removeprefix("User:") in principals],
            "roles": [
                role
                for principal in sorted(principals)
                for role in cloud(config).pages(
                    "/iam/v2/role-bindings?" + urlencode({"principal": "User:" + principal})
                )
            ],
        },
    )


def metrics(state, config) -> None:
    rest = Rest(
        "https://api.telemetry.confluent.cloud",
        config.get("METRICS_API_KEY") or config["CONFLUENT_CLOUD_API_KEY"],
        config.get("METRICS_API_SECRET") or config["CONFLUENT_CLOUD_API_SECRET"],
    )
    end = datetime.now(UTC).replace(microsecond=0)
    start = end - timedelta(hours=1)
    interval = (
        start.isoformat().replace("+00:00", "Z") + "/" + end.isoformat().replace("+00:00", "Z")
    )
    responses = {}
    for name, group_by in (
        ("received_bytes", []),
        ("consumer_lag_offsets", ["metric.consumer_group_id"]),
    ):
        query = {
            "aggregations": [
                {
                    "metric": "io.confluent.kafka.server/" + name,
                    "agg": "SUM" if name == "received_bytes" else "MAX",
                }
            ],
            "filter": {"field": "resource.kafka.id", "op": "EQ", "value": state["cluster_id"]},
            "granularity": "PT1M",
            "intervals": [interval],
            "limit": 1000,
        }
        if group_by:
            query["group_by"] = group_by
        response = rest.request("POST", "/v2/metrics/cloud/query", query)
        responses[name] = {"query": query, "response": response}
    write_json(
        ROOT / "evidencias/07-metrics.json",
        {
            "source": "metrics-api-live",
            "samples": responses,
            "summary": summarize_metrics(responses),
        },
    )


def summarize_metrics(responses) -> dict:
    received = [row["value"] for row in responses["received_bytes"]["response"].get("data", [])]
    groups = {}
    for row in responses["consumer_lag_offsets"]["response"].get("data", []):
        group = row.get("metric.consumer_group_id", "unknown")
        groups[group] = max(groups.get(group, 0), row["value"])
    return {
        "peak_received_bytes_per_minute": max(received) if received else None,
        "peak_lag_offsets_by_group": groups,
        "note": "Sem amostras significa indisponivel, nao zero.",
    }


def billing(state, config) -> None:
    today = datetime.now(UTC).date()
    start = config.get("BILLING_START_DATE") or (today - timedelta(days=3)).isoformat()
    end = config.get("BILLING_END_DATE") or (today + timedelta(days=1)).isoformat()
    rows = cloud(config).pages(
        "/billing/v1/costs?" + urlencode({"start_date": start, "end_date": end})
    )
    selected = [
        row
        for row in rows
        if ((row.get("resource") or {}).get("environment") or {}).get("id")
        == state["environment_id"]
    ]
    total = sum((Decimal(str(row["amount"])) for row in selected), Decimal("0"))
    write_json(
        ROOT / "evidencias/08-costs.json",
        {
            "source": "billing-api-live",
            "start_inclusive": start,
            "end_exclusive": end,
            "environment_id": state["environment_id"],
            "items": selected,
            "reported_amount": total if selected else None,
            "status": "reported-not-final" if selected else "not-yet-reported",
            "note": "Billing pode levar 72h; ausencia de lancamentos nao comprova custo zero. "
            "Custos Neon nao estao incluidos. Recolha novamente apos teardown.",
        },
    )
