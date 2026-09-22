"""CLI gap closure tests: file-signal flags and enforced verify boundary."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from tools.agent_harness.cli import _verify_run, main
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import HarnessRegistry

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
