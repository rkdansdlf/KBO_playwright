"""PR2 security tests: command allowlist, env sanitization, runner boundary."""

from __future__ import annotations

import sys

import pytest

from tools.agent_harness.command_runner import CommandRunner
from tools.agent_harness.dto import PermissionDecision
from tools.agent_harness.exceptions import PermissionDeniedError
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import project_root


def _policy() -> PermissionPolicy:
    return PermissionPolicy.load()


def test_graphify_env_read_denied_but_network_denied() -> None:
    policy = _policy()

    assert policy.can_read(".env") is False
    assert policy.check_network("graphify").decision == PermissionDecision.DENY
    assert policy.check_network("last30days").decision == PermissionDecision.ALLOW


def test_external_skill_cannot_write_src() -> None:
    policy = _policy()

    assert policy.check_write("src/crawlers/base.py", "graphify").decision == PermissionDecision.DENY
    assert policy.check_write("artifacts/agent-harness/x/plan.json", "graphify").allowed is True


def test_authorize_command_allows_verification_argv() -> None:
    policy = _policy()

    assert policy.authorize_command([sys.executable, "-m", "pytest", "-q"], "harness").allowed is True
    assert (
        policy.authorize_command([sys.executable, "-m", "src.cli.crawler_selector_gate", "--json"], "harness").allowed
        is True
    )
    assert policy.authorize_command(["ruff", "check", "src/"], "harness").allowed is True
    assert policy.authorize_command(["git", "rev-parse", "HEAD"], "harness").allowed is True


def test_authorize_command_denies_unknown_and_shell() -> None:
    policy = _policy()

    assert policy.authorize_command(["evil_script.py"], "graphify").allowed is False
    assert policy.authorize_command([sys.executable, "evil_script.py"], "graphify").allowed is False
    assert policy.authorize_command([sys.executable, "-m", "unknown.module"], "graphify").allowed is False
    assert policy.authorize_command(["pytest", "x; rm -rf /"], "graphify").allowed is False
    assert policy.authorize_command(["ruff", "check", "--shell"], "graphify").allowed is False
    assert policy.authorize_command([], "graphify").allowed is False


def test_sanitize_environment_strips_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy()
    monkeypatch.setenv("DATABASE_URL", "oracle://secret")

    clean = policy.sanitize_environment({"DATABASE_URL": "oracle://secret", "PATH": "/bin"}, "graphify")

    assert "DATABASE_URL" not in clean
    assert clean["PATH"] == "/bin"


def test_redact_chains_env_masking_and_secret_patterns(monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy()
    monkeypatch.setenv("DATABASE_URL", "oracle://secret")

    assert "[REDACTED:DATABASE_URL]" in policy.redact("connect oracle://secret now")


def test_runner_executes_allowlisted_command() -> None:
    policy = _policy()
    runner = CommandRunner(permissions=policy, root=project_root())

    result = runner.run([sys.executable, "-m", "tools.agent_harness", "doctor", "--json"], skill_id="harness")

    assert result.exit_code == 0
    assert "reference_only" in result.stdout or "skills" in result.stdout


def test_runner_refuses_denied_command_without_execution() -> None:
    policy = _policy()
    runner = CommandRunner(permissions=policy, root=project_root())

    with pytest.raises(PermissionDeniedError):
        runner.run([sys.executable, "-m", "unknown.module"], skill_id="graphify")


def test_runner_sanitizes_env_for_child(monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy()
    monkeypatch.setenv("DATABASE_URL", "oracle://secret")
    runner = CommandRunner(permissions=policy, root=project_root())

    result = runner.run(
        [sys.executable, "-m", "tools.agent_harness", "doctor", "--json"],
        skill_id="harness",
        env={"DATABASE_URL": "oracle://secret", "PATH": "/usr/bin:/bin"},
    )

    assert result.exit_code == 0
    assert "oracle://secret" not in result.stdout
    assert "oracle://secret" not in result.stderr
