"""Operational metrics aggregation and explicit metrics output tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.agent_harness.cli import main
from tools.agent_harness.golden_tasks import GoldenTaskReplay, load_golden_tasks, replay_dataset
from tools.agent_harness.metrics import METRICS_SCHEMA_VERSION, summarize_replay
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.verifier import ProjectVerifier

REGISTRY = HarnessRegistry.load()
DATASET = load_golden_tasks(REGISTRY.golden_tasks_path)


def _observations() -> tuple[GoldenTaskReplay, ...]:
    return replay_dataset(
        DATASET,
        REGISTRY,
        PermissionPolicy.load(),
        ProjectVerifier.load(),
    )


def test_replay_metrics_summarize_all_operational_tasks() -> None:
    report = summarize_replay(DATASET, _observations()).to_dict()
    overall = report["overall"]
    rounds = report["rounds"]

    assert report["schema_version"] == METRICS_SCHEMA_VERSION
    assert report["generated_at_utc"]
    assert overall["task_count"] == 28
    assert overall["route_ok_count"] == 28
    assert overall["route_accuracy"] == 1.0
    assert overall["suite_ok_count"] == 28
    assert overall["undeclared_failure_count"] == 0
    assert overall["declared_deviation_count"] == 0
    assert overall["permission_decisions"] == {"allow": 2, "deny": 5, "not_applicable": 21}
    assert overall["routed_permission_decisions"] == {"allow": 2, "deny": 5, "not_applicable": 21}
    assert {round_id: row["task_count"] for round_id, row in rounds.items()} == {"1": 10, "2": 9, "3": 9}
    assert report["verification"] == {"missing_checks": [], "extra_checks": []}
    assert report["artifact_completeness"]["status"] == "not_evaluated"
    assert report["executor"]["status"] == "not_evaluated"


def test_replay_metrics_include_optional_artifact_and_executor_results() -> None:
    round_three = [task.task_id for task in DATASET.tasks if task.round == 3]
    artifact_results = dict.fromkeys(round_three, True)
    artifact_results[round_three[0]] = False

    report = summarize_replay(
        DATASET,
        _observations(),
        artifact_results=artifact_results,
        executor_statuses={"executed": 3, "host_execution_required": 24},
    ).to_dict()

    assert report["artifact_completeness"] == {
        "status": "evaluated",
        "evaluated": 9,
        "passed": 8,
        "failed": [round_three[0]],
    }
    assert report["executor"] == {
        "status": "evaluated",
        "status_counts": {"executed": 3, "host_execution_required": 24},
    }


def test_replay_cli_writes_explicit_metrics_artifact(capsys: pytest.CaptureFixture[str]) -> None:
    target = Path("artifacts") / "agent-harness" / "replay" / "test-round-1-metrics.json"
    try:
        assert main(["replay", "--round", "1", "--metrics-out", str(target), "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        metrics = json.loads(target.read_text(encoding="utf-8"))

        assert payload["metrics_path"] == str(target)
        assert metrics["schema_version"] == METRICS_SCHEMA_VERSION
        assert metrics["overall"]["task_count"] == 10
        assert metrics["overall"]["undeclared_failure_count"] == 0
    finally:
        target.unlink(missing_ok=True)
        parent = target.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()


def test_replay_cli_denies_metrics_outside_artifact_policy(capsys: pytest.CaptureFixture[str]) -> None:
    target = Path("../p17-metrics-escape.json")

    assert main(["replay", "--round", "1", "--metrics-out", str(target), "--json"]) == 2
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "error"
    assert "denied metrics path" in payload["error"]
    assert not target.exists()
