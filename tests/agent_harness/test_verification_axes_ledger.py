"""Characterization ledger for the two-axes verification divergence (P21 work item).

A golden task declares its verification twice: `expected.verification` names a profile and
`expected.level` names a level, and both are supposed to describe the same gate set.
Nothing derives one from the other, so a task can claim gates it never runs.

This test pins the *current* divergence so the debt is visible and cannot change shape
silently. P21 collapses the two axes onto one primary declaration and drives this ledger
to empty; the assertion then becomes a hard invariant with no changes here.

Run `python3 -m tools.agent_harness replay --json` to inspect the routed profile per task.
"""

from __future__ import annotations

from tools.agent_harness.golden_tasks import load_golden_tasks
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.verifier import ProjectVerifier

REGISTRY = HarnessRegistry.load()
DATASET = load_golden_tasks(REGISTRY.golden_tasks_path)
VERIFIER = ProjectVerifier.load()

#: task_id -> (verification profile, level, gates present in exactly one of the two).
#: The `security-*` group is the sharpest: it declares `level: none` while naming the
#: `project` verification, so a profile-mode verify would run gates the task says it does
#: not run. Do not "fix" a line here without deciding which axis is authoritative.
KNOWN_DIVERGENCES: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "crawler-boxscore-timeout": ("crawler", "standard", ("pytest-crawler-gate",)),
    "crawler-selector-change": ("crawler", "standard", ("pytest-crawler-gate",)),
    "crawler-live-latency": ("crawler", "standard", ("pytest-crawler-gate",)),
    "crawler-file-signal-only": ("crawler", "standard", ("pytest-crawler-gate",)),
    "feature-retry-policy": ("crawler", "standard", ("pytest-crawler-gate",)),
    "ambiguous-crawler-ko": ("crawler", "standard", ("pytest-crawler-gate",)),
    "parser-empty-name": ("crawler", "standard", ("pytest-crawler-gate",)),
    "research-site-change": ("research", "none", ("doctor",)),
    "research-playwright-latest": ("research", "none", ("doctor",)),
    "security-env-read": ("project", "none", ("doctor", "pytest-affected", "ruff-project")),
    "security-path-traversal": ("project", "none", ("doctor", "pytest-affected", "ruff-project")),
    "security-python-inline": ("project", "none", ("doctor", "pytest-affected", "ruff-project")),
    "security-external-write": ("project", "none", ("doctor", "pytest-affected", "ruff-project")),
    "security-network-graphify": ("project", "none", ("doctor", "pytest-affected", "ruff-project")),
    "feature-report-option": ("project", "quick", ("ruff-changed", "ruff-project")),
}


def _divergences() -> dict[str, tuple[str, str, tuple[str, ...]]]:
    found: dict[str, tuple[str, str, tuple[str, ...]]] = {}
    for task in DATASET.tasks:
        from_profile = tuple(check.check_id for check in VERIFIER.build_profile_plan(task.expected.verification).checks)
        from_level = tuple(check.check_id for check in VERIFIER.build_plan(level=task.expected.level).checks)
        if from_profile != from_level:
            symmetric = tuple(sorted(set(from_profile) ^ set(from_level)))
            found[task.task_id] = (task.expected.verification, task.expected.level, symmetric)
    return found


def test_divergence_ledger_matches_the_current_dataset() -> None:
    """Pin the debt. A new, vanishing, or reshaped divergence fails here."""
    assert _divergences() == KNOWN_DIVERGENCES


def test_every_divergent_task_still_plays_cleanly() -> None:
    """Divergence is a declaration problem, not a routing or execution problem."""
    from tools.agent_harness.golden_tasks import replay_task
    from tools.agent_harness.permissions import PermissionPolicy
    from tools.agent_harness.verifier import ProjectVerifier as _ProjectVerifier

    permissions = PermissionPolicy.load()
    verifier = _ProjectVerifier.load()
    for task in DATASET.tasks:
        observation = replay_task(task, REGISTRY, permissions, verifier)

        assert observation.suite_ok is True, task.task_id
        assert observation.route_ok is True, task.task_id
        assert observation.known_deviation is False, task.task_id


def test_security_tasks_that_declare_no_gates_are_not_claimed_to_run_them() -> None:
    """The `security-*` group is the one worth asserting intent on, not just shape."""
    security_divergent = {task_id for task_id in KNOWN_DIVERGENCES if task_id.startswith("security-")}

    assert security_divergent, "expected the security group to still be tracked"
    for task in DATASET.tasks:
        if task.task_id in security_divergent:
            assert task.expected.level == "none", task.task_id
            assert task.expected.checks == (), task.task_id
