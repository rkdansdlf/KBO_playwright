"""Load and evaluate the versioned Harness routing golden dataset."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from tools.agent_harness.dto import (
    GoldenRoutingCase,
    GoldenRoutingDataset,
    GoldenRoutingExpectation,
    TaskRequest,
)
from tools.agent_harness.exceptions import HarnessConfigError

if TYPE_CHECKING:
    from pathlib import Path

    from tools.agent_harness.registry import HarnessRegistry

GOLDEN_ROUTING_SCHEMA_VERSION = "1"
MIN_GOLDEN_ROUTING_CASES = 30
MAX_GOLDEN_ROUTING_CASES = 50


def _mapping(value: object, label: str) -> dict[str, object]:
    """Require a JSON object with string keys."""
    if not isinstance(value, dict):
        msg = f"Expected object for {label}"
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


def _load_request(value: object, label: str) -> TaskRequest:
    """Parse one typed routing request."""
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


def _load_expectation(value: object, label: str) -> GoldenRoutingExpectation:
    """Parse one typed route expectation."""
    expected = _mapping(value, label)
    return GoldenRoutingExpectation(
        profile=_string(expected.get("profile"), f"{label}.profile"),
        skills=_strings(expected.get("skills"), f"{label}.skills"),
        verification=_string(expected.get("verification"), f"{label}.verification"),
        reason=_string(expected.get("reason"), f"{label}.reason"),
    )


def load_golden_routing(path: Path) -> GoldenRoutingDataset:
    """Load and structurally validate a routing golden dataset."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        msg = f"Cannot load routing golden dataset {path}: {exc}"
        raise HarnessConfigError(msg) from exc
    root = _mapping(payload, "routing_golden")
    schema_version = _string(root.get("schema_version"), "routing_golden.schema_version")
    external_execution = _string(root.get("external_execution"), "routing_golden.external_execution")
    if schema_version != GOLDEN_ROUTING_SCHEMA_VERSION:
        msg = f"Unsupported routing golden schema: {schema_version}"
        raise HarnessConfigError(msg)
    raw_cases = root.get("cases")
    if not isinstance(raw_cases, list):
        msg = "Expected list for routing_golden.cases"
        raise HarnessConfigError(msg)
    if not MIN_GOLDEN_ROUTING_CASES <= len(raw_cases) <= MAX_GOLDEN_ROUTING_CASES:
        msg = f"Routing golden dataset must contain {MIN_GOLDEN_ROUTING_CASES}-{MAX_GOLDEN_ROUTING_CASES} cases"
        raise HarnessConfigError(msg)
    cases: list[GoldenRoutingCase] = []
    identifiers: set[str] = set()
    for index, raw_case in enumerate(raw_cases):
        label = f"routing_golden.cases.{index}"
        case = _mapping(raw_case, label)
        case_id = _string(case.get("id"), f"{label}.id")
        if case_id in identifiers:
            msg = f"Duplicate routing golden case ID: {case_id}"
            raise HarnessConfigError(msg)
        identifiers.add(case_id)
        cases.append(
            GoldenRoutingCase(
                case_id=case_id,
                request=_load_request(case.get("request"), f"{label}.request"),
                expected=_load_expectation(case.get("expected"), f"{label}.expected"),
            )
        )
    return GoldenRoutingDataset(
        schema_version=schema_version,
        external_execution=external_execution,
        cases=tuple(cases),
    )


def validate_golden_routing(
    dataset: GoldenRoutingDataset,
    registry: HarnessRegistry,
) -> tuple[str, ...]:
    """Return deterministic mismatches between golden cases and live routing."""
    from tools.agent_harness.router import TaskRouter

    issues: list[str] = []
    router = TaskRouter(registry)
    known_skills = set(registry.skills)
    if dataset.external_execution != "reference_only":
        issues.append("routing_golden.external_execution must remain reference_only")
    for case in dataset.cases:
        try:
            decision = router.route_request(case.request)
        except (TypeError, ValueError) as exc:
            issues.append(f"{case.case_id}: route failed: {exc}")
            continue
        actual_skills = (*decision.context, *decision.workflow, *decision.guards, decision.output)
        if decision.profile != case.expected.profile:
            issues.append(f"{case.case_id}: expected profile {case.expected.profile}, got {decision.profile}")
        if actual_skills != case.expected.skills:
            issues.append(f"{case.case_id}: expected skills {case.expected.skills}, got {actual_skills}")
        if decision.verification != case.expected.verification:
            issues.append(
                f"{case.case_id}: expected verification {case.expected.verification}, got {decision.verification}"
            )
        if decision.reason != case.expected.reason:
            issues.append(f"{case.case_id}: expected reason {case.expected.reason!r}, got {decision.reason!r}")
        if decision.external_execution != dataset.external_execution:
            issues.append(
                f"{case.case_id}: expected external execution {dataset.external_execution}, "
                f"got {decision.external_execution}"
            )
        unknown_skills = set(case.expected.skills) - known_skills
        if unknown_skills:
            issues.append(f"{case.case_id}: unknown skills: {', '.join(sorted(unknown_skills))}")
    return tuple(issues)


__all__ = [
    "GOLDEN_ROUTING_SCHEMA_VERSION",
    "MAX_GOLDEN_ROUTING_CASES",
    "MIN_GOLDEN_ROUTING_CASES",
    "load_golden_routing",
    "validate_golden_routing",
]
