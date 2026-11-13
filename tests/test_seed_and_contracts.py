import copy
import io
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer

from payments.contracts import MONEY, definitions, local_compatibility
from payments.local_demo import velocity
from payments.seed import BASE_TIME, COUNTS, dataset


def test_seed_has_exact_counts_and_repeatable_values():
    first = dataset(42)
    assert first == dataset(42)
    assert first != dataset(43)
    assert {table: len(rows) for table, rows in first.items()} == COUNTS


def test_foreign_keys_and_card_account_relationship_are_consistent():
    data = dataset()
    accounts = {r["id"]: r for r in data["accounts"]}
    cards = {r["id"]: r for r in data["cards"]}
    for row in data["transactions"]:
        assert row["account_id"] in accounts
        assert cards[row["card_id"]]["account_id"] == row["account_id"]
        assert 1 <= row["merchant_id"] <= 60
        assert isinstance(row["amount"], Decimal)
        assert row["amount"] > 0
    assert all(r["email"].endswith("@example.invalid") for r in data["customers"])
    assert all(r["token"].startswith("tok_test_") for r in data["cards"])


def test_seed_does_not_contain_velocity_fraud():
    rows = [dict(r, transaction_id=r["id"]) for r in dataset()["transactions"]]
    assert velocity(rows) == []
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["card_id"]].append(row["occurred_at"])
    assert all(times[1] - times[0] > timedelta(seconds=60) for times in grouped.values())


@pytest.mark.parametrize("amount", ["0.01", "19.90", "9999999999999.99", "-123.45"])
def test_avro_money_round_trip_preserves_every_cent(amount):
    stream = io.BytesIO()
    schema = {"type": "record", "name": "MoneyTest", "fields": [{"name": "amount", "type": MONEY}]}
    schemaless_writer(stream, schema, {"amount": Decimal(amount)})
    stream.seek(0)
    assert schemaless_reader(stream, schema)["amount"] == Decimal(amount)


def test_money_rejects_precision_overflow():
    with pytest.raises(ValueError):
        schemaless_writer(io.BytesIO(), MONEY, Decimal("10000000000000.00"))


def test_all_schemas_parse_with_namespace_and_sensitive_tags():
    schemas = definitions()
    for schema in schemas.values():
        parse_schema(copy.deepcopy(schema))
        assert schema["namespace"] == "com.bootcamp.payments"
    customer = {f["name"]: f for f in schemas["customers"]["fields"]}
    assert "PII" in customer["email"]["confluent:tags"]
    card = {f["name"]: f for f in schemas["cards"]["fields"]}
    assert "PCI" in card["token"]["confluent:tags"]


def test_real_avro_resolution_exposes_the_spec_backward_error():
    result = {row["case"]: row["compatible"] for row in local_compatibility()}
    assert result == {
        "add_field_with_default": True,
        "add_required_field_without_default": False,
        "remove_required_field": True,
    }


def row(identifier, second, card=1):
    return {
        "transaction_id": identifier,
        "card_id": card,
        "amount": Decimal("1.00"),
        "occurred_at": BASE_TIME + timedelta(seconds=second),
    }


def test_velocity_groups_cards_and_emits_stable_ids_for_replay():
    events = [row(1, 0), row(2, 20), row(3, 40), row(4, 45, card=2)]
    alerts = velocity(events)
    assert len(alerts) == 1
    assert alerts[0]["transaction_count"] == 3
    assert alerts[0]["second_transaction_id"] == 2
    assert alerts == velocity(list(reversed(events)))


def test_velocity_excludes_sixty_seconds_and_skips_past_a_match():
    assert velocity([row(1, 0), row(2, 30), row(3, 60)]) == []
    alerts = velocity([row(i, i) for i in range(1, 8)])
    assert [a["first_transaction_id"] for a in alerts] == [1, 4]


def test_live_cdc_validation_rejects_an_unwrapped_record_and_lossy_money():
    from payments.contracts import validate_cdc_schema

    envelope = {
        "type": "record",
        "name": "Envelope",
        "namespace": "com.bootcamp.payments.public.transactions",
        "fields": [
            {
                "name": "before",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Row",
                        "fields": [{"name": "amount", "type": MONEY}],
                    },
                ],
            },
            {"name": "after", "type": ["null", "Row"]},
            {"name": "op", "type": "string"},
        ],
    }
    validate_cdc_schema(envelope, "transactions")
    lossy = copy.deepcopy(envelope)
    lossy["fields"][0]["type"][1]["fields"][0]["type"] = "double"
    with pytest.raises(ValueError, match="decimal"):
        validate_cdc_schema(lossy, "transactions")
    with pytest.raises(ValueError, match="Envelope"):
        validate_cdc_schema(definitions()["transactions"], "transactions")
