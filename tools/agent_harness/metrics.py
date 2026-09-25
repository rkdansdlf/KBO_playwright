"""Aggregate secret-free metrics for operational Harness task replay."""

from __future__ import annotations

import json
import secrets
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from tools.agent_harness.exceptions import PermissionDeniedError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from tools.agent_harness.golden_tasks import GoldenTaskDataset, GoldenTaskReplay
    from tools.agent_harness.permissions import PermissionPolicy

METRICS_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class ReplayMetrics:
    """Hold a JSON-ready aggregate of one replay dataset."""

    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        """Return the metrics payload."""
        return self.payload

    def to_json(self) -> str:
        """Render deterministic, human-readable JSON."""
        return json.dumps(self.payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def _observation_metrics(observations: Sequence[GoldenTaskReplay]) -> dict[str, object]:
    task_count = len(observations)
    route_ok_count = sum(observation.route_ok for observation in observations)
    suite_ok_count = sum(observation.suite_ok for observation in observations)
    checks_ok_count = sum(observation.checks_ok for observation in observations)
    permission_counts = Counter(observation.permission_decision for observation in observations)
    routed_counts = Counter(observation.routed_permission_decision for observation in observations)
    planned_check_counts = Counter(check_id for observation in observations for check_id in observation.planned_checks)
    return {
        "task_count": task_count,
        "route_ok_count": route_ok_count,
        "route_accuracy": _rate(route_ok_count, task_count),
        "suite_ok_count": suite_ok_count,
        "suite_pass_rate": _rate(suite_ok_count, task_count),
        "checks_ok_count": checks_ok_count,
        "check_pass_rate": _rate(checks_ok_count, task_count),
        "declared_deviation_count": sum(observation.known_deviation for observation in observations),
        "undeclared_failure_count": sum(not observation.suite_ok for observation in observations),
        "permission_decisions": dict(sorted(permission_counts.items())),
        "routed_permission_decisions": dict(sorted(routed_counts.items())),
        "planned_check_counts": dict(sorted(planned_check_counts.items())),
    }


def _check_differences(
    dataset: GoldenTaskDataset,
    observations: Sequence[GoldenTaskReplay],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    expected_by_id = {task.task_id: set(task.expected.checks) for task in dataset.tasks}
    missing: list[dict[str, object]] = []
    extra: list[dict[str, object]] = []
    for observation in observations:
        expected = expected_by_id.get(observation.task_id, set())
        actual = set(observation.planned_checks)
        if expected - actual:
            missing.append({"task_id": observation.task_id, "checks": sorted(expected - actual)})
        if actual - expected:
            extra.append({"task_id": observation.task_id, "checks": sorted(actual - expected)})
    return missing, extra


def _artifact_metrics(artifact_results: Mapping[str, bool]) -> dict[str, object]:
    failed = sorted(task_id for task_id, passed in artifact_results.items() if not passed)
    return {
        "status": "evaluated" if artifact_results else "not_evaluated",
        "evaluated": len(artifact_results),
        "passed": sum(artifact_results.values()),
        "failed": failed,
    }


def _executor_metrics(executor_statuses: Mapping[str, int]) -> dict[str, object]:
    return {
        "status": "evaluated" if executor_statuses else "not_evaluated",
        "status_counts": dict(sorted(executor_statuses.items())),
    }


def summarize_replay(
    dataset: GoldenTaskDataset,
    observations: Sequence[GoldenTaskReplay],
    *,
    artifact_results: Mapping[str, bool] | None = None,
    executor_statuses: Mapping[str, int] | None = None,
) -> ReplayMetrics:
    """Aggregate route, permission, verification, artifact, and executor metrics."""
    rows = tuple(observations)
    missing_checks, extra_checks = _check_differences(dataset, rows)
    rounds = {
        str(round_number): _observation_metrics([row for row in rows if row.round == round_number])
        for round_number in sorted({row.round for row in rows})
    }
    payload: dict[str, object] = {
        "schema_version": METRICS_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "dataset": {
            "schema_version": dataset.schema_version,
            "task_count": len(dataset.tasks),
            "declared_deviation_count": len(dataset.known_deviations),
            "declared_deviations": [
                {
                    "task_id": deviation.task_id,
                    "reason": deviation.reason,
                    "actual_profile": deviation.actual_profile,
                }
                for deviation in dataset.known_deviations
            ],
        },
        "overall": _observation_metrics(rows),
        "rounds": rounds,
        "verification": {
            "missing_checks": missing_checks,
            "extra_checks": extra_checks,
        },
        "artifact_completeness": _artifact_metrics(artifact_results or {}),
        "executor": _executor_metrics(executor_statuses or {}),
    }
    return ReplayMetrics(payload=payload)


def write_metrics(path: Path, metrics: ReplayMetrics, permissions: PermissionPolicy) -> Path:
    """Atomically write metrics only to a policy-approved artifact path."""
    decision = permissions.check_write(path, "harness")
    if not decision.allowed:
        msg = f"Harness policy denied metrics path: {path}"
        raise PermissionDeniedError(msg)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    temporary = permissions.check_write(temp_path, "harness")
    if not temporary.allowed:
        msg = f"Harness policy denied metrics temporary path for: {path}"
        raise PermissionDeniedError(msg)
    with temp_path.open("x", encoding="utf-8") as stream:
        temp_path.chmod(0o600)
        stream.write(permissions.redact(metrics.to_json()))
    temp_path.replace(path)
    return path


__all__ = ["METRICS_SCHEMA_VERSION", "ReplayMetrics", "summarize_replay", "write_metrics"]
