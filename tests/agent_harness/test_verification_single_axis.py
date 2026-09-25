"""Single-axis contract for golden task verification expectations (P21).

A golden task used to declare its verification twice: `expected.verification` named the
profile the router selects, and `expected.level` named a level, and `expected.checks` was
derived from the *level*. Since the run that actually happens comes from the profile, that
made `checks_ok` validate a plan that never executes.

Fifteen of twenty-eight tasks were affected. The worst were the `security-*` attack probes,
which declared `checks: []` while the router sent them to the `project` profile, so the
dataset claimed "no gates run" for a task that runs pytest, Ruff, and doctor.

These tests pin the fix: one axis, derived from what executes, with no room for a second
declaration to drift.
"""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from tools.agent_harness.dto import GoldenTaskExpectation, TaskRequest
from tools.agent_harness.golden_tasks import load_golden_tasks
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.verifier import ProjectVerifier

ROOT = Path(__file__).resolve().parents[2]
DATASET_PATH = ROOT / ".agent-harness" / "golden_tasks.yaml"
REGISTRY = HarnessRegistry.load()
DATASET = load_golden_tasks(REGISTRY.golden_tasks_path)
VERIFIER = ProjectVerifier.load()
ROUTER = TaskRouter(REGISTRY)


def _resolved(task_id: str) -> tuple[str, ...]:
    task = next(t for t in DATASET.tasks if t.task_id == task_id)
    plan = VERIFIER.build_profile_plan(task.expected.verification, task.request.changed_files or ())
    return tuple(check.check_id for check in plan.checks)


def test_expectation_has_no_second_verification_axis() -> None:
    assert "level" not in {f.name for f in fields(GoldenTaskExpectation)}


def test_dataset_declares_no_level_key() -> None:
    raw = DATASET_PATH.read_text(encoding="utf-8")

    assert "\n      level:" not in raw


def test_declared_checks_match_the_executing_axis_for_every_task() -> None:
    """`checks` must describe the verification the router actually selects."""
    mismatches: list[str] = []
    for task in DATASET.tasks:
        plan = VERIFIER.build_profile_plan(task.expected.verification, task.request.changed_files or ())
        resolved = tuple(check.check_id for check in plan.checks)
        if resolved != task.expected.checks:
            mismatches.append(f"{task.task_id}: {list(task.expected.checks)} != {list(resolved)}")

    assert mismatches == []


def test_crawler_tasks_keep_the_selector_gate_test() -> None:
    """Regression for the exact bug P21 fixed.

    `crawler` inherits the `standard` level and then adds gates. Deriving the expectation
    from the level alone silently dropped `pytest-crawler-gate`, so a crawler task could
    look fully covered while its selector-gate test never ran.
    """
    crawler_tasks = [task for task in DATASET.tasks if task.expected.verification == "crawler"]

    assert crawler_tasks, "expected crawler tasks in the dataset"
    for task in crawler_tasks:
        assert "pytest-crawler-gate" in task.expected.checks, task.task_id
        assert "crawler-gate" in task.expected.checks, task.task_id


def test_no_task_claims_to_run_nothing_while_its_profile_runs_gates() -> None:
    """A task may not declare an empty check set if its profile resolves to gates."""
    offenders = [
        task.task_id
        for task in DATASET.tasks
        if not task.expected.checks and VERIFIER.build_profile_plan(task.expected.verification)
    ]

    assert offenders == []


def test_security_attack_tasks_declare_the_gates_they_will_run() -> None:
    """Intent change from P21, asserted so it cannot be reverted by accident.

    These tasks previously declared `level: none` with empty checks, implying "no gates".
    The router sends them to the `project` profile, so the truthful declaration is the
    project gate set. The golden replay is read-only, so this costs nothing at runtime.
    """
    security_tasks = [task for task in DATASET.tasks if task.category == "security"]

    assert security_tasks, "expected security tasks in the dataset"
    for task in security_tasks:
        routed = ROUTER.route_request(
            TaskRequest(
                prompt=task.request.prompt,
                changed_files=task.request.changed_files or (),
                explicit_profile=task.request.explicit_profile,
            ),
        )
        assert task.expected.verification == routed.verification, task.task_id
        assert task.expected.checks == tuple(
            check.check_id
            for check in VERIFIER.build_profile_plan(routed.verification, task.request.changed_files or ()).checks
        ), task.task_id


def test_research_tasks_declare_the_manifest_gate_they_actually_run() -> None:
    """`research` runs `doctor`, so an empty expectation for it was never truthful."""
    for task_id in ("research-site-change", "research-playwright-latest"):
        assert _resolved(task_id) == ("doctor",)
        assert next(t for t in DATASET.tasks if t.task_id == task_id).expected.checks == ("doctor",)
