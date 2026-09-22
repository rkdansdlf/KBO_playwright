"""Tests for Harness file, network, and redaction policy."""

from __future__ import annotations

from tools.agent_harness.permissions import PermissionPolicy


def test_secret_paths_are_denied() -> None:
    policy = PermissionPolicy.load()

    assert policy.can_read(".env") is False
    assert policy.can_read("server.pem") is False
    assert policy.can_read("Wallet_PROD/cwallet.sso") is False
    assert policy.can_read("wallet/cwallet.sso") is False
    assert policy.can_read("data/kbo.db") is False
    assert policy.can_read("src/crawlers/base.py") is True


def test_writes_are_limited_to_artifact_and_plan_paths() -> None:
    policy = PermissionPolicy.load()

    assert policy.can_write("artifacts/agent-harness/run-1/plan.json") is True
    assert policy.can_write("Docs/plans/refactor.md") is True
    assert policy.can_write("src/crawlers/base.py") is False


def test_only_fresh_research_adapter_has_network_access() -> None:
    policy = PermissionPolicy.load()

    assert policy.can_use_network("last30days") is True
    assert policy.can_use_network("graphify") is False


def test_redact_masks_configured_environment_values(monkeypatch) -> None:
    policy = PermissionPolicy.load()
    monkeypatch.setenv("DATABASE_URL", "oracle://secret")

    assert policy.redact("connect oracle://secret now") == "connect [REDACTED:DATABASE_URL] now"
