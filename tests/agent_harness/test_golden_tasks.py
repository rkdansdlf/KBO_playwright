"""Operational replay tests for real KBO development and security tasks."""

from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from tools.agent_harness.artifact_contract import validate_artifact_bundle
from tools.agent_harness.cli import main
from tools.agent_harness.context_builder import ContextBuilder
from tools.agent_harness.dto import GoldenTask, GoldenTaskDeviation
from tools.agent_harness.exceptions import HarnessConfigError
from tools.agent_harness.golden_tasks import (
    GoldenTaskReplay,
    load_golden_tasks,
    replay_task,
    validate_golden_routes,
    validate_golden_tasks,
)
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.runner import HarnessRunner
from tools.agent_harness.verifier import ProjectVerifier

REGISTRY = HarnessRegistry.load()
DATASET = load_golden_tasks(REGISTRY.golden_tasks_path)
KNOWN_DEVIATIONS = {deviation.task_id for deviation in DATASET.known_deviations}
DEVIATIONS = {deviation.task_id: deviation for deviation in DATASET.known_deviations}
COLLISION_TASK_ID = "crawler-boxscore-timeout"


def _replay(task: GoldenTask) -> GoldenTaskReplay:
    return replay_task(
        task,
        REGISTRY,
        PermissionPolicy.load(),
        ProjectVerifier.load(),
        deviation=DEVIATIONS.get(task.task_id),
    )


def _task(task_id: str) -> GoldenTask:
    return next(task for task in DATASET.tasks if task.task_id == task_id)


def _synthetic_deviation(task: GoldenTask, *, pin: str | None = None) -> GoldenTaskDeviation:
    """Declare a deviation pinning the real route, or a third profile when `pin` is given.

    The shipped dataset is collision-free, so the declared-deviation contract is exercised
    with an in-memory dataset instead of live data.
    """
    decision = TaskRouter(REGISTRY).route_request(task.request)
    return GoldenTaskDeviation(
        task_id=task.task_id,
        reason="synthetic deviation used to exercise the declared-collision contract",
        actual_profile=pin or decision.profile,
        actual_skills=(*decision.context, *decision.workflow, *decision.guards, decision.output),
        actual_verification=decision.verification,
    )


def _mis_expected_task(task: GoldenTask) -> GoldenTask:
    """Return the task with a deliberately wrong expected profile so a deviation activates."""
    return replace(task, expected=replace(task.expected, profile="analytics"), risk="router-collision")


def test_golden_task_dataset_contract() -> None:
    assert DATASET.schema_version == "1"
    assert len(DATASET.tasks) == 28
    assert len({task.task_id for task in DATASET.tasks}) == 28
    assert {task.round for task in DATASET.tasks} == {1, 2, 3}
    assert sum(task.category == "security" for task in DATASET.tasks) == 5
    assert sum(task.category == "ambiguous" for task in DATASET.tasks) == 4
    assert not KNOWN_DEVIATIONS
    assert all(task.risk != "router-collision" for task in DATASET.tasks)
    assert validate_golden_routes(DATASET, REGISTRY) == ()
    assert REGISTRY.validate() == []


def test_replay_cli_round_one(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["replay", "--round", "1", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["execution"] == "read_only_replay"
    assert payload["round"] == 1
    assert payload["task_count"] == 10
    assert payload["suite_ok"] is True
    assert payload["declared_deviations"] == []

    assert main(["replay", "--round", "3", "--json"]) == 0
    round_three = json.loads(capsys.readouterr().out)
    assert round_three["task_count"] == 9
    assert round_three["declared_deviations"] == []


def test_replay_cli_rejects_empty_selection(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["replay", "--task-id", "does-not-exist", "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "error"
    assert "No golden tasks" in payload["error"]


def test_main_fails_closed_on_malformed_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(cls: object) -> None:
        _ = cls
        raise TypeError("malformed policy")

    monkeypatch.setattr(HarnessRegistry, "load", classmethod(_raise))

    assert main(["route", "crawler timeout"]) == 2


def test_verify_malformed_verifier_policy_returns_exit_two(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert main(["run", "malformed verifier probe", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = Path("artifacts") / "agent-harness" / run_id

    def _raise(cls: object, root: object = None) -> None:
        _ = (cls, root)
        raise TypeError("malformed verification policy")

    monkeypatch.setattr(ProjectVerifier, "load", classmethod(_raise))
    try:
        assert main(["verify", run_id, "--json"]) == 2
        assert "malformed verification policy" in json.loads(capsys.readouterr().out)["error"]
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)


def test_replay_cli_reports_configuration_error_as_exit_two(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail(_path: Path) -> None:
        raise HarnessConfigError("invalid golden task dataset")

    monkeypatch.setattr("tools.agent_harness.golden_tasks.load_golden_tasks", _fail)
    assert main(["replay", "--json"]) == 2
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "error"
    assert "invalid golden task dataset" in payload["error"]


def test_round_distribution_is_explicit() -> None:
    counts = {round_number: sum(task.round == round_number for task in DATASET.tasks) for round_number in (1, 2, 3)}

    assert counts == {1: 10, 2: 9, 3: 9}


@pytest.mark.parametrize("task", DATASET.tasks, ids=lambda task: task.task_id)
def test_operational_task_replay(task: GoldenTask) -> None:
    observation = _replay(task)

    assert observation.suite_ok is True
    assert observation.planned_checks == task.expected.checks
    assert observation.known_deviation is False
    assert observation.route_ok is True
    assert observation.permission_decision == task.expected.permission.decision
    assert observation.routed_permission_ok is True
    assert observation.actual_profile == task.expected.profile
    assert observation.actual_skills == task.expected.skills
    assert observation.actual_verification == task.expected.verification


def test_dataset_has_no_router_false_negatives() -> None:
    mismatches = {task.task_id for task in DATASET.tasks if _replay(task).actual_profile != task.expected.profile}

    assert mismatches == KNOWN_DEVIATIONS


def test_declared_deviation_authorizes_a_known_wrong_route() -> None:
    task = _mis_expected_task(_task(COLLISION_TASK_ID))
    deviation = _synthetic_deviation(task)

    observation = replay_task(
        task,
        REGISTRY,
        PermissionPolicy.load(),
        ProjectVerifier.load(),
        deviation=deviation,
    )

    assert observation.route_ok is False
    assert observation.known_deviation is True
    assert observation.suite_ok is True


def test_declared_deviation_drift_is_rejected() -> None:
    task = _mis_expected_task(_task(COLLISION_TASK_ID))
    drifted = _synthetic_deviation(task, pin="analytics")
    dataset = replace(
        DATASET,
        known_deviations=(drifted,),
        tasks=tuple(task if row.task_id == task.task_id else row for row in DATASET.tasks),
    )

    issues = validate_golden_routes(dataset, REGISTRY)
    observation = replay_task(
        task,
        REGISTRY,
        PermissionPolicy.load(),
        ProjectVerifier.load(),
        deviation=drifted,
    )

    assert any("declared deviation changed" in issue for issue in issues)
    assert observation.known_deviation is False
    assert observation.suite_ok is False


def test_route_deviation_does_not_mask_undeclared_permission_regression() -> None:
    task = _mis_expected_task(_task(COLLISION_TASK_ID))
    deviation = _synthetic_deviation(task)
    permission = replace(task.expected.permission, action="read", decision="allow", target=".env", skill_id="graphify")
    regressed = replace(task, expected=replace(task.expected, permission=permission))

    observation = replay_task(
        regressed,
        REGISTRY,
        PermissionPolicy.load(),
        ProjectVerifier.load(),
        deviation=deviation,
    )

    assert observation.known_deviation is True
    assert observation.permission_ok is False
    assert observation.suite_ok is False


def test_full_validation_gates_permission_and_verification_checks() -> None:
    env_task = next(task for task in DATASET.tasks if task.task_id == "security-env-read")
    report_task = next(task for task in DATASET.tasks if task.task_id == "feature-report-option")
    env_task = replace(
        env_task,
        expected=replace(env_task.expected, permission=replace(env_task.expected.permission, decision="allow")),
    )
    report_task = replace(report_task, expected=replace(report_task.expected, checks=("not-a-real-check",)))
    tasks = tuple(env_task if task.task_id == env_task.task_id else report_task for task in DATASET.tasks)
    dataset = replace(DATASET, tasks=tasks)

    issues = validate_golden_tasks(
        dataset,
        REGISTRY,
        PermissionPolicy.load(),
        ProjectVerifier.load(),
    )

    assert any("expected permission allow" in issue for issue in issues)
    assert any("expected checks" in issue for issue in issues)


def test_research_network_probe_targets_the_intended_skill() -> None:
    task = _task("research-playwright-latest")
    observation = _replay(task)

    assert observation.permission_skill_id == "last30days"
    assert observation.permission_decision == "allow"
    assert observation.routed_permission_skill_id == "last30days"
    assert observation.routed_permission_decision == "allow"
    assert observation.routed_permission_ok is True


def test_quick_verification_level_is_covered() -> None:
    assert any(task.expected.level == "quick" for task in DATASET.tasks)


def test_round_three_tasks_create_complete_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _context(self: object, decision: object = None) -> dict[str, object]:
        _ = (self, decision)
        return {
            "git_revision": "test-revision",
            "files": {},
            "context_engines": ["graphify"],
            "external_execution": "reference_only",
            "secrets_included": False,
        }

    monkeypatch.setattr(ContextBuilder, "build", _context)
    round_three = [task for task in DATASET.tasks if task.round == 3]
    for task in round_three:
        root = tmp_path / task.task_id
        root.mkdir()
        registry = replace(REGISTRY, root=root)
        permissions = replace(PermissionPolicy.load(), root=root)

        run = HarnessRunner(registry, permissions).run(
            task.request.prompt,
            profile=task.request.explicit_profile,
            changed_files=task.request.changed_files,
        )

        assert validate_artifact_bundle(run.artifact_dir).valid is True
        records = [
            json.loads(line)
            for line in (run.artifact_dir / "skill-trace.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        executions = [record for record in records if "skill_id" in record]
        assert any(record["status"] == "host_execution_required" for record in executions)
        assert any(record["status"] == "executed" for record in executions)
        assert all(
            record["status"] == "host_execution_required"
            for record in executions
            if record["transport"] in {"native", "cli", "mcp"}
        )
