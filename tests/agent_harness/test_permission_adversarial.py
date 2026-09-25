"""Adversarial regression tests for the Harness permission boundary."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from tools.agent_harness.cli import _refresh_context
from tools.agent_harness.command_runner import CommandRunner
from tools.agent_harness.dto import PermissionDecision
from tools.agent_harness.evidence import EvidenceStore
from tools.agent_harness.exceptions import HarnessConfigError, PermissionDeniedError
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import HarnessRegistry, project_root
from tools.agent_harness.verifier import GatePolicy, ProjectVerifier


def _policy_for_root(root: Path) -> PermissionPolicy:
    return replace(PermissionPolicy.load(), root=root)


def test_relative_traversal_and_absolute_outside_paths_are_denied(tmp_path: Path) -> None:
    policy = PermissionPolicy.load()
    outside = tmp_path / "outside.json"

    assert policy.can_read("../.env") is False
    assert policy.can_read("src/../.env") is False
    assert policy.can_read("invalid\x00path") is False
    assert policy.can_read(str(outside)) is False
    assert policy.can_write(str(outside)) is False
    assert policy.check_read("../.env", "graphify").decision == PermissionDecision.DENY


def test_absolute_paths_inside_repository_are_normalized() -> None:
    policy = PermissionPolicy.load()
    source = project_root() / "src" / "crawlers" / "base.py"
    artifact = project_root() / "artifacts" / "agent-harness" / "run-1" / "plan.json"

    assert policy.can_read(source) is True
    assert policy.can_write(artifact) is True


def test_hardlinked_file_alias_is_denied(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("private", encoding="utf-8")
    linked = root / "linked.txt"
    linked.hardlink_to(outside)
    policy = replace(PermissionPolicy.load(), root=root)

    assert policy.can_read(linked) is False


def test_symlink_escape_is_denied_for_read_write_and_cwd(tmp_path: Path) -> None:
    policy = PermissionPolicy.load()
    link = project_root() / "artifacts" / "agent-harness" / f"p15-read-{tmp_path.name}"
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(tmp_path, target_is_directory=True)
    runner = CommandRunner(permissions=policy, root=project_root())
    try:
        assert policy.can_read(link / ".env") is False
        assert policy.can_write(link / "escape.json") is False
        with pytest.raises(PermissionDeniedError):
            runner.run(
                [sys.executable, "-m", "pytest", "tests/agent_harness", "-q"],
                skill_id="harness",
                cwd=link,
            )
    finally:
        link.unlink(missing_ok=True)


def test_evidence_store_rejects_traversal_and_symlinked_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    artifacts_root = root / "artifacts" / "agent-harness"
    policy = _policy_for_root(root)
    store = EvidenceStore.create(artifacts_root, policy)
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / "owned.txt"
    outside_file.write_text("unchanged", encoding="utf-8")
    linked_run = artifacts_root / "linked-run"
    linked_run.symlink_to(outside, target_is_directory=True)
    (store.root / "escape.json.tmp").symlink_to(outside_file)

    try:
        store.write_json("escape.json", {"escaped": True})
        assert json.loads((store.root / "escape.json").read_text(encoding="utf-8"))["escaped"] is True
        with pytest.raises(PermissionDeniedError):
            store.write_json("../escape.json", {"escaped": True})
        with pytest.raises(PermissionDeniedError):
            EvidenceStore.open(artifacts_root, linked_run.name, policy)
        with pytest.raises(PermissionDeniedError):
            EvidenceStore.create(linked_run, policy)
    finally:
        linked_run.unlink(missing_ok=True)
    assert outside_file.read_text(encoding="utf-8") == "unchanged"


def test_shared_context_ignores_stale_temp_symlink(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    shared = root / "artifacts" / "agent-harness" / "shared"
    shared.mkdir(parents=True)
    outside = tmp_path / "outside.txt"
    outside.write_text("unchanged", encoding="utf-8")
    (shared / "context.json.tmp").symlink_to(outside)
    registry = replace(HarnessRegistry.load(), root=root)
    permissions = replace(PermissionPolicy.load(), root=root)

    payload = _refresh_context(registry, permissions)

    assert payload["path"] == "artifacts/agent-harness/shared/context.json"
    assert outside.read_text(encoding="utf-8") == "unchanged"


@pytest.mark.parametrize(
    "token",
    ["x;calc", "x|calc", "x&calc", "x>out", "x<in", "line\nnext", "line\rnext", "${HOME}", "`id`"],
)
def test_shell_metacharacters_are_denied(token: str) -> None:
    policy = PermissionPolicy.load()

    assert policy.authorize_command(["pytest", token], "harness").allowed is False


def test_python_inline_and_executable_path_spoofing_are_denied(tmp_path: Path) -> None:
    policy = PermissionPolicy.load()
    fake_python = tmp_path / "python3"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_ruff = tmp_path / "ruff"
    fake_ruff.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")

    assert policy.authorize_command([sys.executable, "-c", "print(1)"], "harness").allowed is False
    assert policy.authorize_command([str(fake_python), "-m", "pytest"], "harness").allowed is False
    assert policy.authorize_command([str(fake_ruff), "check", "src"], "harness").allowed is False
    assert policy.authorize_command(["./ruff", "check", "src"], "harness").allowed is False


def test_execution_control_environment_is_removed() -> None:
    policy = PermissionPolicy.load()
    clean = policy.sanitize_environment(
        {
            "PATH": "/tmp/fake",
            "PYTHONPATH": "/tmp/evil",
            "PythonPath": "/tmp/evil",
            "PYTHONHOME": "/tmp/evil",
            "PYTHONPYCACHEPREFIX": "/tmp/cache",
            "PYTEST_ADDOPTS": "--collect-only",
            "Pytest_Plugins": "/tmp/plugin",
            "GIT_DIR": "/tmp/other-repo",
            "GIT_EXTERNAL_DIFF": "/tmp/evil-diff",
            "KBO_USER_ID": "kbo-user",
            "KBO_USER_PWD": "kbo-password",
            "BASH_ENV": "/tmp/evil.sh",
            "LD_PRELOAD": "/tmp/evil.so",
            "Ld_Preload": "/tmp/evil.so",
            "DYLD_INSERT_LIBRARIES": "/tmp/evil.dylib",
        },
        "harness",
    )

    assert clean == {"PATH": "/tmp/fake"}


def test_runner_uses_trusted_path_and_ignores_caller_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_ruff = fake_bin / "ruff"
    fake_ruff.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
    fake_ruff.chmod(0o755)
    observed_argv: list[str] = []
    observed_env: dict[str, str] = {}

    def _fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed_argv.extend(argv)
        observed_env.update(kwargs["env"])
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr("tools.agent_harness.command_runner.subprocess.run", _fake_run)
    monkeypatch.setenv("PATH", str(fake_bin))
    runner = CommandRunner(permissions=PermissionPolicy.load(), root=project_root())

    result = runner.run(["ruff", "--version"], skill_id="harness", env={"PATH": str(fake_bin)})

    assert result.exit_code == 0
    assert Path(observed_argv[0]).name == "ruff"
    assert Path(observed_argv[0]).resolve() != fake_ruff.resolve()
    assert observed_env["PATH"] != str(fake_bin)


def test_trusted_root_symlink_cannot_redirect_outside(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted_bin = tmp_path / "trusted-bin"
    trusted_bin.mkdir()
    outside = tmp_path / "outside-ruff"
    outside.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    outside.chmod(0o755)
    linked = trusted_bin / "ruff"
    linked.symlink_to(outside)
    monkeypatch.setattr(
        CommandRunner,
        "_trusted_executable_roots",
        classmethod(lambda cls: (trusted_bin,)),
    )
    runner = CommandRunner(permissions=PermissionPolicy.load(), root=project_root())

    with pytest.raises(PermissionDeniedError):
        runner.run(["ruff", "--version"], skill_id="harness")


def test_windows_executable_suffix_is_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[str] = []

    def _fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.extend(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    policy = PermissionPolicy.load()
    root = project_root()
    runner = CommandRunner(permissions=policy, root=root)
    monkeypatch.setattr("tools.agent_harness.command_runner.IS_WINDOWS", True)
    monkeypatch.setattr(
        "tools.agent_harness.command_runner.shutil.which",
        lambda _program, *, path: "/trusted/ruff.exe",
    )
    monkeypatch.setattr(
        CommandRunner,
        "_trusted_executable_targets",
        classmethod(lambda cls: (Path("/trusted"),)),
    )
    monkeypatch.setattr("tools.agent_harness.command_runner.subprocess.run", _fake_run)

    result = runner.run(["ruff", "--version"], skill_id="harness")

    assert result.exit_code == 0
    assert observed[0].endswith("ruff.exe")


def test_runner_denied_command_never_reaches_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def _fake_run(*_args: object, **_kwargs: object) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr("tools.agent_harness.command_runner.subprocess.run", _fake_run)
    runner = CommandRunner(permissions=PermissionPolicy.load(), root=project_root())

    with pytest.raises(PermissionDeniedError):
        runner.run(["/tmp/ruff", "check", "src"], skill_id="harness")

    assert called is False


def test_verifier_without_command_runner_fails_closed(tmp_path: Path) -> None:
    verifier = ProjectVerifier(
        root=tmp_path,
        levels={},
        profiles={"test": GatePolicy(gates=("doctor",))},
    )

    with pytest.raises(HarnessConfigError):
        verifier.verify("test")
