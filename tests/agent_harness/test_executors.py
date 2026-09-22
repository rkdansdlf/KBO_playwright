"""PR4 tests: honest executors, route CLI, and enriched evidence."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from tools.agent_harness.cli import main
from tools.agent_harness.dto import SkillTransport
from tools.agent_harness.executors import (
    SkillExecutionStatus,
    execute_route,
    executor_for,
)
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter

if TYPE_CHECKING:
    import pytest


def test_native_and_cli_are_not_executed() -> None:
    registry = HarnessRegistry.load()

    native = executor_for(registry.get_skill_definition("superpowers")).execute(
        registry.get_skill_definition("superpowers"), "workflow"
    )
    cli = executor_for(registry.get_skill_definition("graphify")).execute(
        registry.get_skill_definition("graphify"), "context"
    )
    policy = executor_for(registry.get_skill_definition("i-have-adhd")).execute(
        registry.get_skill_definition("i-have-adhd"), "output"
    )

    assert native.status == SkillExecutionStatus.HOST_EXECUTION_REQUIRED
    assert cli.status == SkillExecutionStatus.HOST_EXECUTION_REQUIRED
    assert policy.status == SkillExecutionStatus.EXECUTED
    assert native.transport == SkillTransport.NATIVE
    assert cli.transport == SkillTransport.CLI


def test_execute_route_covers_all_stages() -> None:
    registry = HarnessRegistry.load()
    decision = TaskRouter(registry).route("boxscore crawler timeout")

    results = execute_route(decision, registry)
    stages = [result.stage for result in results]

    assert stages.count("context") == len(decision.context)
    assert stages.count("workflow") == len(decision.workflow)
    assert stages.count("guard") == len(decision.guards)
    assert stages[-1] == "output"
    assert all(result.to_dict()["status"] for result in results)


def test_route_cli_shows_classification(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["route", "boxscore crawler timeout", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["profile"] == "crawler-bug"
    assert payload["context"] == ["graphify"]
    assert payload["verification"] == "crawler"


def test_run_records_executor_outcomes(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "executor smoke", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = Path("artifacts") / "agent-harness" / run_id
    try:
        lines = (artifact_dir / "skill-trace.jsonl").read_text(encoding="utf-8").splitlines()
        assert any("host_execution_required" in line or "executed" in line for line in lines)
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)
