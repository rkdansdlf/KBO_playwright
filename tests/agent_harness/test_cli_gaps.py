"""CLI gap closure tests: file-signal flags and enforced verify boundary."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from tools.agent_harness.cli import _verify_run, main
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.verifier import CommandResult

if TYPE_CHECKING:
    import pytest


def test_route_flag_files_beat_prompt(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        main(
            [
                "route",
                "이거 좀 정리해줘",
                "--changed-files",
                "src/crawlers/game_boxscore_crawler.py",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)

    assert payload["profile"] == "crawler-bug"
    assert "file signal" in payload["reason"]


def test_plan_and_run_flags_propagate_files(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        main(
            [
                "plan",
                "정리해줘",
                "--changed-files",
                "src/rag/engine.py",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["profile"] == "analytics"

    assert (
        main(
            [
                "run",
                "정리해줘",
                "--changed-files",
                "src/rag/engine.py",
                "--json",
            ]
        )
        == 0
    )
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    try:
        task = json.loads((Path("artifacts") / "agent-harness" / run_id / "task.json").read_text())
        assert task["profile"] == "analytics"
    finally:
        shutil.rmtree(Path("artifacts") / "agent-harness" / run_id, ignore_errors=True)


def _new_research_run(capsys: pytest.CaptureFixture[str], task: str = "level probe") -> str:
    assert main(["run", task, "--profile", "research", "--json"]) == 0
    return str(json.loads(capsys.readouterr().out)["run_id"])


def _remove_run(run_id: str) -> None:
    shutil.rmtree(Path("artifacts") / "agent-harness" / run_id, ignore_errors=True)


def test_verify_level_none_passes_without_commands(capsys: pytest.CaptureFixture[str]) -> None:
    run_id = _new_research_run(capsys)
    try:
        assert main(["verify", run_id, "--level", "none", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)

        assert payload["passed"] is True
        assert payload["profile"] == "level:none"
        assert payload["commands"] == []
    finally:
        _remove_run(run_id)


def _fake_success_run(self: object, argv: object, **kwargs: object) -> CommandResult:
    _ = (self, kwargs)
    assert isinstance(argv, (list, tuple))
    return CommandResult(argv=tuple(argv), exit_code=0, duration_ms=0.0, stdout="", stderr="")


def test_verify_level_composition_without_execution(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compose quick/standard plans through a fake runner (real pytest would self-recurse)."""
    from tools.agent_harness.command_runner import CommandRunner

    recorded: list[tuple[str, ...]] = []

    def _record(self: object, argv: object, **kwargs: object) -> CommandResult:
        result = _fake_success_run(self, argv, **kwargs)
        recorded.append(result.argv)
        return result

    monkeypatch.setattr(CommandRunner, "run", _record)
    run_id = _new_research_run(capsys)
    recorded.clear()
    try:
        assert main(["verify", run_id, "--level", "quick", "--json"]) == 0
        quick_payload = json.loads(capsys.readouterr().out)
        assert quick_payload["passed"] is True
        assert len(recorded) == len(quick_payload["commands"]) == 2

        recorded.clear()
        assert (
            main(
                [
                    "verify",
                    run_id,
                    "--level",
                    "standard",
                    "--changed-files",
                    "src/crawlers/x.py",
                    "--json",
                ]
            )
            == 0
        )
        standard_payload = json.loads(capsys.readouterr().out)
        assert standard_payload["passed"] is True
        assert any("crawler_selector_gate" in token for argv in recorded for token in argv)
        assert any("tests/monitoring/test_crawler_selector_gate.py" in token for argv in recorded for token in argv)
        _ = standard_payload
    finally:
        _remove_run(run_id)


def test_verify_level_denied_exits_two(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = _new_research_run(capsys)
    try:
        locked = PermissionPolicy(
            deny_read=(),
            restricted_read=(),
            write_default=False,
            allowed_write=("artifacts/agent-harness/**",),
            network_default=False,
            network_skills=(),
            redact_env=(),
            allowed_executables=(),
            python_modules=(),
            forbidden_flags=("--shell",),
        )
        monkeypatch.setattr(PermissionPolicy, "load", classmethod(lambda cls, root=None: locked))
        assert main(["verify", run_id, "--level", "quick"]) == 2
        capsys.readouterr()
    finally:
        _remove_run(run_id)


def test_verify_denied_returns_error_payload(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "gap denial probe", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    try:
        locked = PermissionPolicy(
            deny_read=(),
            restricted_read=(),
            write_default=False,
            allowed_write=("artifacts/agent-harness/**",),
            network_default=False,
            network_skills=(),
            redact_env=(),
            allowed_executables=(),
            python_modules=(),
            forbidden_flags=("--shell",),
        )
        payload = _verify_run(HarnessRegistry.load(), locked, run_id)

        assert payload["passed"] is False
        assert payload.get("error") is not None

        assert main(["verify", run_id, "--json"]) == 0
        capsys.readouterr()
    finally:
        shutil.rmtree(Path("artifacts") / "agent-harness" / run_id, ignore_errors=True)


def test_verify_denied_cli_exit_code(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    assert main(["run", "gap exit probe", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    try:
        locked = PermissionPolicy(
            deny_read=(),
            restricted_read=(),
            write_default=False,
            allowed_write=("artifacts/agent-harness/**",),
            network_default=False,
            network_skills=(),
            redact_env=(),
            allowed_executables=(),
            python_modules=(),
            forbidden_flags=("--shell",),
        )
        monkeypatch.setattr(PermissionPolicy, "load", classmethod(lambda cls, root=None: locked))
        assert main(["verify", run_id]) == 2
        capsys.readouterr()
    finally:
        shutil.rmtree(Path("artifacts") / "agent-harness" / run_id, ignore_errors=True)
