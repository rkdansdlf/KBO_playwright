"""Secret-name coverage and evidence redaction regression tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from tools.agent_harness.evidence import EvidenceStore
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import project_root

SECRET_SUFFIXES = ("_PASSWORD", "_SECRET", "_TOKEN", "_API_KEY", "_DSN", "_DB_URL")
SECRET_EXACT_NAMES = {"DATABASE_URL", "KBO_USER_ID", "KBO_USER_PWD", "SLACK_WEBHOOK_URL"}


def _env_example_names() -> set[str]:
    names: set[str] = set()
    for raw_line in (project_root() / "env.example").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        names.add(line.split("=", 1)[0].strip())
    return names


def _secret_like_names() -> set[str]:
    return {name for name in _env_example_names() if name in SECRET_EXACT_NAMES or name.endswith(SECRET_SUFFIXES)}


def test_env_example_secret_names_are_registered_for_redaction() -> None:
    policy = PermissionPolicy.load()
    expected = _secret_like_names()

    assert expected
    assert expected <= set(policy.redact_env)
    assert "BEGA_PROD_PASSWORD" in policy.redact_env


@pytest.mark.parametrize("secret_name", sorted(PermissionPolicy.load().redact_env))
def test_configured_secret_is_removed_from_child_env_and_masked(
    secret_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = PermissionPolicy.load()
    secret_value = f"test-secret-value-for-{secret_name}"
    monkeypatch.setenv(secret_name, secret_value)

    assert secret_name not in policy.sanitize_environment({secret_name: secret_value}, "harness")
    assert secret_value not in policy.redact(secret_value)
    assert f"[REDACTED:{secret_name}]" in policy.redact(secret_value)


def test_evidence_writers_redact_configured_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    policy = replace(PermissionPolicy.load(), root=root)
    store = EvidenceStore.create(root / "artifacts" / "agent-harness", policy)
    secret_value = "bega-production-secret-value"
    monkeypatch.setenv("BEGA_PROD_PASSWORD", secret_value)
    payload = {"service": "bega", "value": secret_value}

    store.write_json("task.json", payload)
    store.append_jsonl("commands.jsonl", payload)
    store.write_text("report.md", f"secret={secret_value}")

    for name in ("task.json", "commands.jsonl", "report.md"):
        content = (store.root / name).read_text(encoding="utf-8")
        assert secret_value not in content
        assert "[REDACTED:BEGA_PROD_PASSWORD]" in content
