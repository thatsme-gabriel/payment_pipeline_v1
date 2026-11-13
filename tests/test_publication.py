"""Publication checks: credential-free templates and Git's actual ignore behavior."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def test_public_environment_template_has_no_credentials():
    values = {}
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    credential_fields = {
        "CONFLUENT_CLOUD_API_KEY",
        "CONFLUENT_CLOUD_API_SECRET",
        "CONFLUENT_ORGANIZATION_ID",
        "DATABASE_URL",
        "CDC_PASSWORD",
        "METRICS_API_KEY",
        "METRICS_API_SECRET",
    }
    assert credential_fields <= values.keys()
    assert all(values[key] == "" for key in credential_fields)


def test_git_excludes_private_artifacts_and_keeps_public_sources(tmp_path):
    git = shutil.which("git")
    if not git:
        pytest.skip("Git is required to verify ignore rules")
    # Isolated repository: never initialize or stage the real project.
    folder = tmp_path / "publication-check"
    folder.mkdir()
    (folder / ".gitignore").write_text((ROOT / ".gitignore").read_text(), encoding="utf-8")
    empty_template = tmp_path / "empty-template"
    empty_template.mkdir()
    empty_excludes = tmp_path / "empty-excludes"
    empty_excludes.touch()
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    command = [
        git,
        "-c",
        f"core.excludesFile={empty_excludes}",
        "-c",
        f"init.templateDir={empty_template}",
        "-C",
        str(folder),
    ]
    subprocess.run([*command, "init", "--quiet"], env=env, check=True, capture_output=True)
    private = {
        ".env",
        ".env.production",
        ".env.backup",
        "config/.env",
        ".state/runtime.json",
        ".venv/pyvenv.cfg",
        ".tools/download.zip",
        "payments/__pycache__/config.pyc",
        "data/alerts.sqlite3",
        "output/local-demo.json",
        "output/screenshot.png",
        "tmp/metrics-query.json",
        "debug.log",
        "backup.sqlite3-wal",
        "infra/.terraform/providers/plugin.exe",
        "infra/terraform.tfstate",
        "infra/terraform.tfstate.backup",
        "terraform.tfstate",
        "saved.tfplan",
        "saved.tfplan.json",
        "terraform.tfvars",
        "secrets.auto.tfvars.json",
        "backend.tfbackend",
        "override.tf",
        "local_override.tf.json",
        ".terraformrc",
        "terraform.rc",
        "certs/private.key",
        "certs/client.pem",
        "evidencias/01-envelopes-cdc.json",
        "evidencias/export.csv",
        "evidencias/console.png",
        "evidencias/notes.txt",
        "evidencias/nested/export.csv",
    }
    public = {
        ".env.example",
        ".gitignore",
        ".gitattributes",
        "README.md",
        "IMPLEMENTACAO.md",
        "docs/OPERACAO.md",
        "docs/mapa-mental.png",
        "evidencias/README.md",
        "infra/main.tf",
        "infra/.terraform.lock.hcl",
        "connectors/cdc.json",
        "database/001_schema.sql",
        "schemas/transactions.avsc",
        "payments/config.py",
        "tests/test_publication.py",
        "requirements.lock.txt",
    }
    result = subprocess.run(
        [*command, "check-ignore", "--no-index", "--stdin", "-z"],
        input="\0".join(sorted(private | public)) + "\0",
        env=env,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode in (0, 1), "Git ignore check failed"
    ignored = set(result.stdout.strip("\0").split("\0"))
    assert private <= ignored, f"Private artifacts not ignored: {sorted(private - ignored)}"
    assert not public & ignored, f"Public sources unexpectedly ignored: {sorted(public & ignored)}"
