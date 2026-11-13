import sqlite3
from unittest.mock import Mock

import pytest

from payments.consumer import AlertStore, kafka_config, process_message


def message():
    result = Mock()
    result.topic.return_value = "desafio.fraud.detected"
    result.partition.return_value = 0
    result.offset.return_value = 10
    return result


def test_failure_after_database_commit_does_not_duplicate_on_replay(tmp_path, monkeypatch):
    monkeypatch.setattr("payments.consumer.decode", lambda *args: {"alert_id": "same-alert"})
    store = AlertStore(tmp_path / "alerts.sqlite3")
    client = Mock()
    client.commit.side_effect = RuntimeError("Kafka unavailable after SQLite commit")
    with pytest.raises(RuntimeError):
        process_message(client, None, store, message())
    store.close()
    # Reinicio do processo apos a falha.
    store = AlertStore(tmp_path / "alerts.sqlite3")
    client.commit.side_effect = None
    assert process_message(client, None, store, message()) is False
    assert store.connection.execute("SELECT count(*) FROM alerts").fetchone()[0] == 1
    assert client.commit.call_args.kwargs["asynchronous"] is False
    store.close()


def test_database_failure_does_not_acknowledge_the_kafka_message(tmp_path, monkeypatch):
    monkeypatch.setattr("payments.consumer.decode", lambda *args: {"alert_id": "new-alert"})
    store = Mock()
    store.save.side_effect = sqlite3.OperationalError("disk full")
    client = Mock()
    with pytest.raises(sqlite3.OperationalError):
        process_message(client, None, store, message())
    client.commit.assert_not_called()


def test_invalid_alert_is_not_acknowledged(tmp_path, monkeypatch):
    monkeypatch.setattr("payments.consumer.decode", lambda *args: {"rule_id": "missing-id"})
    store = AlertStore(tmp_path / "alerts.sqlite3")
    client = Mock()
    with pytest.raises(ValueError):
        process_message(client, None, store, message())
    client.commit.assert_not_called()
    store.close()


def test_tombstone_can_be_acknowledged_without_business_effect():
    from payments.consumer import decode

    sample = Mock()
    sample.value.return_value = None
    deserializer = Mock()
    assert decode(deserializer, sample) is None
    deserializer.assert_not_called()


def test_consumer_enforces_commits_and_read_committed():
    state = {
        "bootstrap": "SASL_SSL://example.invalid:9092",
        "kafka_keys": {"consumer": {"key": "id", "secret": "secret"}},
    }
    config = kafka_config(state, "desafio-test")
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["isolation.level"] == "read_committed"
    with pytest.raises(ValueError):
        kafka_config(state, "outside-allowed-prefix")
