"""Load and safely replay the operational KBO task dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import yaml

from tools.agent_harness.dto import (
    GoldenTask,
    GoldenTaskDataset,
    GoldenTaskDeviation,
    GoldenTaskExpectation,
    GoldenTaskPermission,
    TaskRequest,
)
from tools.agent_harness.exceptions import HarnessConfigError
from tools.agent_harness.registry import load_yaml_mapping

if TYPE_CHECKING:
    from pathlib import Path

    from tools.agent_harness.permissions import PermissionPolicy
    from tools.agent_harness.registry import HarnessRegistry
    from tools.agent_harness.verifier import ProjectVerifier

GOLDEN_TASK_SCHEMA_VERSION = "1"
MIN_GOLDEN_TASKS = 24
MAX_GOLDEN_TASKS = 30
VALID_ROUNDS = {1, 2, 3}
VALID_CATEGORIES = {
    "crawler-bug",
    "feature",
    "refactor",
    "architecture",
    "research",
    "analytics",
    "ambiguous",
    "security",
}
VALID_RISKS = {"normal", "ambiguous", "attack", "router-collision"}
VALID_PERMISSION_ACTIONS = {"none", "read", "write", "command", "network"}
VALID_PERMISSION_DECISIONS = {"allow", "deny", "require_approval", "not_applicable"}


@dataclass(frozen=True)
class GoldenTaskReplay:
    """Summarize one safe route, permission, and verification replay."""

    task_id: str
    round: int
    actual_profile: str
    actual_skills: tuple[str, ...]
    actual_verification: str
    route_ok: bool
    permission_decision: str
    permission_skill_id: str
    permission_ok: bool
    routed_permission_decision: str
    routed_permission_skill_id: str
    routed_permission_ok: bool
    planned_checks: tuple[str, ...]
    checks_ok: bool
    known_deviation: bool
    suite_ok: bool
    issues: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        """Serialize one replay observation for JSON output."""
        return {
            "task_id": self.task_id,
            "round": self.round,
            "actual_profile": self.actual_profile,
            "actual_skills": list(self.actual_skills),
            "actual_verification": self.actual_verification,
            "route_ok": self.route_ok,
            "permission_decision": self.permission_decision,
            "permission_skill_id": self.permission_skill_id,
            "permission_ok": self.permission_ok,
            "routed_permission_decision": self.routed_permission_decision,
            "routed_permission_skill_id": self.routed_permission_skill_id,
            "routed_permission_ok": self.routed_permission_ok,
            "planned_checks": list(self.planned_checks),
            "checks_ok": self.checks_ok,
            "known_deviation": self.known_deviation,
            "suite_ok": self.suite_ok,
            "issues": list(self.issues),
        }


def _mapping(value: object, label: str) -> dict[str, object]:
    """Require a YAML mapping with string keys."""
    if not isinstance(value, dict):
        msg = f"Expected mapping for {label}"
        raise HarnessConfigError(msg)
    return {str(key): item for key, item in value.items()}


def _string(value: object, label: str, *, allow_empty: bool = False) -> str:
    """Require one string value."""
    if not isinstance(value, str) or (not allow_empty and not value):
        qualifier = "string" if allow_empty else "non-empty string"
        msg = f"Expected {qualifier} for {label}"
        raise HarnessConfigError(msg)
    return value


def _strings(value: object, label: str) -> tuple[str, ...]:
    """Require one list of strings."""
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        msg = f"Expected string list for {label}"
        raise HarnessConfigError(msg)
    return tuple(value)


def _optional_string(value: object, label: str) -> str | None:
    """Require a string or null."""
    if value is None:
        return None
    return _string(value, label)


def _integer(value: object, label: str) -> int:
    """Require an integer value."""
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"Expected integer for {label}"
        raise HarnessConfigError(msg)
    return value


def _load_request(value: object, label: str) -> TaskRequest:
    """Parse one task request."""
    request = _mapping(value, label)
    dry_run = request.get("dry_run", True)
    if not isinstance(dry_run, bool):
        msg = f"Expected boolean for {label}.dry_run"
        raise HarnessConfigError(msg)
    return TaskRequest(
        prompt=_string(request.get("prompt"), f"{label}.prompt", allow_empty=True),
        changed_files=list(_strings(request.get("changed_files", []), f"{label}.changed_files")),
        explicit_profile=_optional_string(request.get("explicit_profile"), f"{label}.explicit_profile"),
        explicit_skills=list(_strings(request.get("explicit_skills", []), f"{label}.explicit_skills")),
        dry_run=dry_run,
    )


def _validate_permission_shape(
    label: str,
    action: str,
    decision: str,
    subject: tuple[str | None, str | None],
    argv: tuple[str, ...],
) -> None:
    target, skill_id = subject
    if action == "none" and decision != "not_applicable":
        msg = f"{label}: action none requires decision not_applicable"
        raise HarnessConfigError(msg)
    if action != "none" and decision == "not_applicable":
        msg = f"{label}: non-none action cannot use not_applicable"
        raise HarnessConfigError(msg)
    if action in {"read", "write"} and (target is None or skill_id is None or argv):
        msg = f"{label}: read/write permission requires target and skill_id without argv"
        raise HarnessConfigError(msg)
    if action == "command" and (len(argv) < 1 or skill_id is None or target is not None):
        msg = f"{label}: command permission requires argv and skill_id without target"
        raise HarnessConfigError(msg)
    if action == "network" and (skill_id is None or target is not None or argv):
        msg = f"{label}: network permission requires only skill_id"
        raise HarnessConfigError(msg)


def _load_permission(value: object, label: str) -> GoldenTaskPermission:
    """Parse one permission expectation."""
    permission = _mapping(value, label)
    action = _string(permission.get("action"), f"{label}.action")
    decision = _string(permission.get("decision"), f"{label}.decision")
    if action not in VALID_PERMISSION_ACTIONS:
        msg = f"Unknown permission action at {label}: {action}"
        raise HarnessConfigError(msg)
    if decision not in VALID_PERMISSION_DECISIONS:
        msg = f"Unknown permission decision at {label}: {decision}"
        raise HarnessConfigError(msg)
    target = _optional_string(permission.get("target"), f"{label}.target")
    skill_id = _optional_string(permission.get("skill_id"), f"{label}.skill_id")
    argv = _strings(permission.get("argv", []), f"{label}.argv")
    reason_contains = _optional_string(permission.get("reason_contains"), f"{label}.reason_contains")
    _validate_permission_shape(label, action, decision, (target, skill_id), argv)
    return GoldenTaskPermission(
        action=action,
        decision=decision,
        target=target,
        skill_id=skill_id,
        argv=argv,
        reason_contains=reason_contains,
    )


def _load_expectation(value: object, label: str) -> GoldenTaskExpectation:
    """Parse one task expectation."""
    expected = _mapping(value, label)
    # `checks` is compared against the plan resolved from `verification`, the axis that
    # actually executes. A second declaration of the same decision could only drift.
    return GoldenTaskExpectation(
        profile=_string(expected.get("profile"), f"{label}.profile"),
        skills=_strings(expected.get("skills"), f"{label}.skills"),
        verification=_string(expected.get("verification"), f"{label}.verification"),
        checks=_strings(expected.get("checks"), f"{label}.checks"),
        permission=_load_permission(expected.get("permission"), f"{label}.permission"),
    )


def _load_deviation(value: object, label: str) -> GoldenTaskDeviation:
    """Parse one known router deviation."""
    deviation = _mapping(value, label)
    allow_permission_mismatch = deviation.get("allow_permission_mismatch", False)
    if not isinstance(allow_permission_mismatch, bool):
        msg = f"Expected boolean for {label}.allow_permission_mismatch"
        raise HarnessConfigError(msg)
    return GoldenTaskDeviation(
        task_id=_string(deviation.get("task_id"), f"{label}.task_id"),
        reason=_string(deviation.get("reason"), f"{label}.reason"),
        actual_profile=_string(deviation.get("actual_profile"), f"{label}.actual_profile"),
        actual_skills=_strings(deviation.get("actual_skills"), f"{label}.actual_skills"),
        actual_verification=_string(deviation.get("actual_verification"), f"{label}.actual_verification"),
        allow_permission_mismatch=allow_permission_mismatch,
    )


def _load_task(value: object, label: str, identifiers: set[str]) -> GoldenTask:
    task = _mapping(value, label)
    task_id = _string(task.get("task_id"), f"{label}.task_id")
    if task_id in identifiers:
        msg = f"Duplicate golden task ID: {task_id}"
        raise HarnessConfigError(msg)
    identifiers.add(task_id)
    task_round = _integer(task.get("round"), f"{label}.round")
    if task_round not in VALID_ROUNDS:
        msg = f"Unknown golden task round at {label}: {task_round}"
        raise HarnessConfigError(msg)
    category = _string(task.get("category"), f"{label}.category")
    risk = _string(task.get("risk"), f"{label}.risk")
    if category not in VALID_CATEGORIES:
        msg = f"Unknown golden task category at {label}: {category}"
        raise HarnessConfigError(msg)
    if risk not in VALID_RISKS:
        msg = f"Unknown golden task risk at {label}: {risk}"
        raise HarnessConfigError(msg)
    return GoldenTask(
        task_id=task_id,
        title=_string(task.get("title"), f"{label}.title"),
        category=category,
        risk=risk,
        round=task_round,
        request=_load_request(task.get("request"), f"{label}.request"),
        expected=_load_expectation(task.get("expected"), f"{label}.expected"),
    )


def _validate_round_distribution(tasks: list[GoldenTask]) -> None:
    round_counts = {round_number: sum(task.round == round_number for task in tasks) for round_number in VALID_ROUNDS}
    if any(count == 0 for count in round_counts.values()):
        msg = f"Every operational round must contain at least one task: {round_counts}"
        raise HarnessConfigError(msg)


def _validate_deviation_bindings(tasks: list[GoldenTask], deviations: tuple[GoldenTaskDeviation, ...]) -> None:
    deviation_map = {deviation.task_id: deviation for deviation in deviations}
    for task in tasks:
        deviation = deviation_map.get(task.task_id)
        collision = deviation is not None
        if (task.risk == "router-collision") != collision:
            msg = f"{task.task_id}: risk router-collision must match known deviation membership"
            raise HarnessConfigError(msg)
        if (
            deviation is not None
            and deviation.allow_permission_mismatch
            and task.expected.permission.action != "network"
        ):
            msg = f"{task.task_id}: permission deviation allowance requires a network probe"
            raise HarnessConfigError(msg)


def load_golden_tasks(path: Path) -> GoldenTaskDataset:
    """Load and structurally validate the operational task dataset."""
    try:
        payload = load_yaml_mapping(path)
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        msg = f"Cannot load golden task dataset {path}: {exc}"
        raise HarnessConfigError(msg) from exc
    schema_version = _string(payload.get("schema_version"), "golden_tasks.schema_version")
    if schema_version != GOLDEN_TASK_SCHEMA_VERSION:
        msg = f"Unsupported golden task schema: {schema_version}"
        raise HarnessConfigError(msg)
    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list):
        msg = "Expected list for golden_tasks.tasks"
        raise HarnessConfigError(msg)
    if not MIN_GOLDEN_TASKS <= len(raw_tasks) <= MAX_GOLDEN_TASKS:
        msg = f"Golden task dataset must contain {MIN_GOLDEN_TASKS}-{MAX_GOLDEN_TASKS} tasks"
        raise HarnessConfigError(msg)
    tasks: list[GoldenTask] = []
    identifiers: set[str] = set()
    for index, raw_task in enumerate(raw_tasks):
        tasks.append(_load_task(raw_task, f"golden_tasks.tasks.{index}", identifiers))
    _validate_round_distribution(tasks)
    raw_deviations = payload.get("known_deviations", [])
    if not isinstance(raw_deviations, list):
        msg = "Expected list for golden_tasks.known_deviations"
        raise HarnessConfigError(msg)
    deviations = tuple(
        _load_deviation(raw, f"golden_tasks.known_deviations.{index}") for index, raw in enumerate(raw_deviations)
    )
    unknown_deviations = {deviation.task_id for deviation in deviations} - identifiers
    if unknown_deviations:
        msg = f"Known deviations reference unknown tasks: {', '.join(sorted(unknown_deviations))}"
        raise HarnessConfigError(msg)
    _validate_deviation_bindings(tasks, deviations)
    return GoldenTaskDataset(
        schema_version=schema_version,
        tasks=tuple(tasks),
        known_deviations=deviations,
    )


def _route_snapshot(
    registry: HarnessRegistry,
    request: TaskRequest,
) -> tuple[str, tuple[str, ...], str]:
    from tools.agent_harness.router import TaskRouter

    decision = TaskRouter(registry).route_request(request)
    skills = (*decision.context, *decision.workflow, *decision.guards, decision.output)
    return decision.profile, skills, decision.verification


def validate_golden_routes(dataset: GoldenTaskDataset, registry: HarnessRegistry) -> tuple[str, ...]:
    """Return unexpected route mismatches and malformed declared deviations."""
    deviations = {deviation.task_id: deviation for deviation in dataset.known_deviations}
    known_skills = set(registry.skills)
    issues: list[str] = []
    for task in dataset.tasks:
        if task.expected.profile not in registry.profiles:
            issues.append(f"{task.task_id}: unknown expected profile {task.expected.profile}")
        unknown_skills = set(task.expected.skills) - known_skills
        if unknown_skills:
            issues.append(f"{task.task_id}: unknown expected skills {', '.join(sorted(unknown_skills))}")
        actual_profile, actual_skills, actual_verification = _route_snapshot(registry, task.request)
        deviation = deviations.get(task.task_id)
        if deviation is not None:
            if deviation.actual_profile not in registry.profiles:
                issues.append(f"{task.task_id}: unknown declared actual profile {deviation.actual_profile}")
            unknown_actual = set(deviation.actual_skills) - known_skills
            if unknown_actual:
                issues.append(f"{task.task_id}: unknown declared actual skills {', '.join(sorted(unknown_actual))}")
            pinned = (deviation.actual_profile, deviation.actual_skills, deviation.actual_verification)
            expected = (task.expected.profile, task.expected.skills, task.expected.verification)
            if (actual_profile, actual_skills, actual_verification) == expected or (
                actual_profile,
                actual_skills,
                actual_verification,
            ) != pinned:
                issues.append(
                    f"{task.task_id}: declared deviation changed; expected actual {pinned}, "
                    f"got {(actual_profile, actual_skills, actual_verification)}"
                )
        else:
            matches = (
                actual_profile == task.expected.profile
                and actual_skills == task.expected.skills
                and actual_verification == task.expected.verification
            )
            if not matches:
                issues.append(
                    f"{task.task_id}: expected profile {task.expected.profile} / {task.expected.skills}, "
                    f"got {actual_profile} / {actual_skills}"
                )
    return tuple(issues)


def validate_golden_tasks(
    dataset: GoldenTaskDataset,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
    verifier: ProjectVerifier,
) -> tuple[str, ...]:
    """Return route, permission, and verification expectation issues."""
    issues = list(validate_golden_routes(dataset, registry))
    issues.extend(
        f"{task.task_id}: unknown expected verification {task.expected.verification}"
        for task in dataset.tasks
        if task.expected.verification not in verifier.profiles
    )
    issues.extend(
        f"{observation.task_id}: {'; '.join(observation.issues)}"
        for observation in replay_dataset(dataset, registry, permissions, verifier)
        if not observation.suite_ok
    )
    return tuple(issues)


def _permission_result(permission: GoldenTaskPermission, policy: PermissionPolicy) -> tuple[str, str]:
    if permission.action == "none":
        return "not_applicable", "no permission probe"
    skill_id = permission.skill_id or "harness"
    if permission.action == "read":
        result = policy.check_read(permission.target or "", skill_id)
    elif permission.action == "write":
        result = policy.check_write(permission.target or "", skill_id)
    elif permission.action == "command":
        result = policy.authorize_command(list(permission.argv), skill_id)
    else:
        result = policy.check_network(skill_id)
    return result.decision.value, result.reason


def _routed_permission_skill(
    permission: GoldenTaskPermission,
    permissions: PermissionPolicy,
    routed_skills: tuple[str, ...],
) -> str:
    if permission.action != "network":
        return permission.skill_id or "harness"
    network_skills = [skill_id for skill_id in routed_skills if permissions.can_use_network(skill_id)]
    return (network_skills or list(routed_skills))[0]


def replay_task(
    task: GoldenTask,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
    verifier: ProjectVerifier,
    *,
    deviation: GoldenTaskDeviation | None = None,
) -> GoldenTaskReplay:
    """Evaluate one task without executing external skills or project commands."""
    actual_profile, actual_skills, actual_verification = _route_snapshot(registry, task.request)
    route_ok = (
        actual_profile == task.expected.profile
        and actual_skills == task.expected.skills
        and actual_verification == task.expected.verification
    )
    deviation_matches = (
        deviation is not None
        and not route_ok
        and (actual_profile, actual_skills, actual_verification)
        == (deviation.actual_profile, deviation.actual_skills, deviation.actual_verification)
    )
    permission_skill_id = task.expected.permission.skill_id or "harness"
    permission_decision, permission_reason = _permission_result(task.expected.permission, permissions)
    expected_reason = task.expected.permission.reason_contains
    permission_ok = permission_decision == task.expected.permission.decision
    if expected_reason is not None and expected_reason not in permission_reason:
        permission_ok = False
    routed_skill_id = _routed_permission_skill(task.expected.permission, permissions, actual_skills)
    routed_permission = task.expected.permission
    if routed_skill_id != permission_skill_id:
        routed_permission = GoldenTaskPermission(
            action=task.expected.permission.action,
            decision=task.expected.permission.decision,
            target=task.expected.permission.target,
            skill_id=routed_skill_id,
            argv=task.expected.permission.argv,
            reason_contains=task.expected.permission.reason_contains,
        )
    routed_permission_decision, routed_permission_reason = _permission_result(routed_permission, permissions)
    routed_permission_ok = routed_permission_decision == task.expected.permission.decision
    if expected_reason is not None and expected_reason not in routed_permission_reason:
        routed_permission_ok = False
    # Derive the expected gates from the axis that actually executes. `plan.json` carries
    # `expected.verification`, so deriving from a level instead would validate a run that
    # never happens, and `crawler` would silently lose its selector-gate test.
    plan = verifier.build_profile_plan(task.expected.verification, task.request.changed_files)
    planned_checks = tuple(check.check_id for check in plan.checks)
    checks_ok = planned_checks == task.expected.checks
    issues: list[str] = []
    if not route_ok:
        issues.append(
            f"expected profile {task.expected.profile} / {task.expected.skills}, got {actual_profile} / {actual_skills}"
        )
    if not permission_ok:
        issues.append(
            f"expected permission {task.expected.permission.decision} / {expected_reason}, "
            f"got {permission_decision} / {permission_reason}"
        )
    if not routed_permission_ok and not (deviation is not None and deviation.allow_permission_mismatch):
        issues.append(
            f"routed permission {routed_skill_id} expected {task.expected.permission.decision}, "
            f"got {routed_permission_decision}"
        )
    if not checks_ok:
        issues.append(f"expected checks {task.expected.checks}, got {planned_checks}")
    if deviation is not None and not deviation_matches:
        issues.append("declared route deviation no longer matches its pinned actual route")
    route_dimension_ok = route_ok or deviation_matches
    deviation_dimension_ok = deviation is None or deviation_matches
    permission_dimension_ok = permission_ok and (
        routed_permission_ok or (deviation is not None and deviation.allow_permission_mismatch)
    )
    return GoldenTaskReplay(
        task_id=task.task_id,
        round=task.round,
        actual_profile=actual_profile,
        actual_skills=actual_skills,
        actual_verification=actual_verification,
        route_ok=route_ok,
        permission_decision=permission_decision,
        permission_skill_id=permission_skill_id,
        permission_ok=permission_ok,
        routed_permission_decision=routed_permission_decision,
        routed_permission_skill_id=routed_skill_id,
        routed_permission_ok=routed_permission_ok,
        planned_checks=planned_checks,
        checks_ok=checks_ok,
        known_deviation=deviation_matches,
        suite_ok=route_dimension_ok and deviation_dimension_ok and permission_dimension_ok and checks_ok,
        issues=tuple(issues),
    )


def replay_dataset(
    dataset: GoldenTaskDataset,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
    verifier: ProjectVerifier,
    *,
    round_number: int | None = None,
) -> tuple[GoldenTaskReplay, ...]:
    """Replay all tasks or one operational round without running project commands."""
    deviations = {deviation.task_id: deviation for deviation in dataset.known_deviations}
    tasks = [task for task in dataset.tasks if round_number is None or task.round == round_number]
    return tuple(
        replay_task(task, registry, permissions, verifier, deviation=deviations.get(task.task_id)) for task in tasks
    )


__all__ = [
    "GOLDEN_TASK_SCHEMA_VERSION",
    "MAX_GOLDEN_TASKS",
    "MIN_GOLDEN_TASKS",
    "GoldenTaskReplay",
    "load_golden_tasks",
    "replay_dataset",
    "replay_task",
    "validate_golden_routes",
    "validate_golden_tasks",
]
