import json
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from payments.config import database_coordinates, load_env
from payments.evidence import summarize_metrics, validate_envelopes
from payments.http import ApiError, Rest
from payments.provision import scrub, setup, teardown


def test_dotenv_is_parsed_as_data_and_environment_takes_precedence(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nVALUE='$(never-run)'\nKEPT=file\n", encoding="utf-8")
    monkeypatch.setenv("KEPT", "process")
    result = load_env(env)
    assert result["VALUE"] == "$(never-run)"
    assert result["KEPT"] == "process"


def test_cdc_refuses_pooled_neon_endpoint():
    config = {
        "DATABASE_URL": "postgres://example-user:example-password@ep-test-pooler.example.invalid/db",
        "CDC_PASSWORD": "test-only",
    }
    with pytest.raises(ValueError, match="direto"):
        database_coordinates(config)


def test_setup_and_teardown_dry_runs_do_not_contact_services(capsys):
    setup({})
    teardown({})
    assert "sem conexoes externas" in capsys.readouterr().out


def test_secrets_are_removed_from_errors():
    config = {
        "CDC_PASSWORD": "secret-cdc",
        "CONFLUENT_CLOUD_API_SECRET": "secret-cloud",
        "DATABASE_URL": "postgres://example-user:encoded%40password@example.invalid/db",
    }
    output = scrub("secret-cdc secret-cloud encoded%40password encoded@password", config)
    assert output == "[REDACTED] [REDACTED] [REDACTED] [REDACTED]"


def envelopes():
    values = [
        {"op": "c", "before": None, "after": {"id": 1, "name": "old"}},
        {"op": "u", "before": {"id": 1, "name": "old"}, "after": {"id": 1, "name": "new"}},
        {"op": "d", "before": {"id": 1, "name": "new"}, "after": None},
        None,
    ]
    return [
        {"value": value, "topic": "merchants", "partition": 0, "offset": i}
        for i, value in enumerate(values)
    ]


def test_cdc_proof_requires_update_before_and_tombstone():
    records = envelopes()
    validate_envelopes(records)
    records[1]["value"]["before"] = None
    with pytest.raises(ValueError, match="UPDATE"):
        validate_envelopes(records)
    with pytest.raises(ValueError):
        validate_envelopes(envelopes()[:-1])


def test_missing_metrics_are_not_reported_as_zero():
    responses = {
        name: {"response": {"data": []}} for name in ("received_bytes", "consumer_lag_offsets")
    }
    assert summarize_metrics(responses)["peak_received_bytes_per_minute"] is None


def test_http_does_not_retry_non_idempotent_create_or_leak_body(monkeypatch):
    request = Mock(side_effect=HTTPError("https://api.invalid", 503, "password-secret", {}, None))
    monkeypatch.setattr("payments.http.urlopen", request)
    with pytest.raises(ApiError) as error:
        Rest("https://api.invalid", "key", "secret").request("POST", "/resources", {"secret": "s"})
    assert request.call_count == 1
    assert "password-secret" not in str(error.value)


def test_http_retries_transient_read_failure(monkeypatch):
    response = Mock()
    response.read.return_value = b'{"data": []}'
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=None)
    monkeypatch.setattr("payments.http.time.sleep", lambda _: None)
    call = Mock(side_effect=[HTTPError("https://api.invalid", 429, "limit", {}, None), response])
    monkeypatch.setattr("payments.http.urlopen", call)
    assert Rest("https://api.invalid", "key", "secret").request("GET", "/resources") == {"data": []}
    assert call.call_count == 2


def test_pagination_refuses_sending_credentials_to_another_domain(monkeypatch):
    rest = Rest("https://api.invalid", "key", "secret")
    monkeypatch.setattr(
        rest,
        "request",
        lambda *args: {"data": [], "metadata": {"next": "https://elsewhere.invalid/steal"}},
    )
    with pytest.raises(ValueError, match="dominio"):
        rest.pages("/resources")


def test_json_connector_contains_no_embedded_secrets():
    from payments.config import ROOT

    config = json.loads((ROOT / "connectors/cdc.json").read_text())
    assert "database.password" not in config
    assert "kafka.api.secret" not in config
    assert config["after.state.only"] == "false"
    assert config["heartbeat.interval.ms"] == "60000"


def test_teardown_resumes_database_cleanup_without_reusing_deleted_keys(tmp_path, monkeypatch):
    from payments import database, provision
    from payments.config import write_json

    state = {"environment_id": "env-test", "cluster_id": "lkc-test"}
    write_json(tmp_path / ".state/runtime.json", state)
    write_json(
        tmp_path / ".state/teardown-progress.json",
        {"cloud_cleanup_done": True, "resources_destroyed": True},
    )
    monkeypatch.setattr(provision, "ROOT", tmp_path)
    monkeypatch.setattr(provision, "runtime", lambda: state)
    monkeypatch.setattr(provision, "terraform_binary", lambda config: "terraform")
    external = Mock(side_effect=AssertionError("Nao deve chamar a nuvem novamente"))
    monkeypatch.setattr(provision, "terraform", external)
    cleanup = Mock(side_effect=[RuntimeError("Banco temporariamente indisponivel"), None])
    monkeypatch.setattr(database, "cleanup_replication", cleanup)
    config = {
        key: "test"
        for key in (
            "CONFLUENT_CLOUD_API_KEY",
            "CONFLUENT_CLOUD_API_SECRET",
            "CONFLUENT_ORGANIZATION_ID",
            "DATABASE_URL",
            "CDC_PASSWORD",
        )
    }
    with pytest.raises(RuntimeError, match="Banco"):
        teardown(config, apply=True)
    assert (tmp_path / ".state/teardown-progress.json").exists()
    teardown(config, apply=True)
    external.assert_not_called()
    assert not (tmp_path / ".state/runtime.json").exists()
    assert not (tmp_path / ".state/teardown-progress.json").exists()
