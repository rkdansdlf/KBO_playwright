"""Tests for verification profile resolution through the shared gate catalog."""

from __future__ import annotations

import sys

import pytest

from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.verifier import GATE_CATALOG, ProjectVerifier


def test_profile_commands_resolve_through_the_gate_catalog() -> None:
    verifier = ProjectVerifier.load()

    commands = verifier.commands_for("project")

    assert commands, "project profile must resolve to at least one gate"
    assert all(isinstance(command, tuple) for command in commands)
    assert commands[0][:3] == (sys.executable, "-m", "pytest")


def test_every_policy_gate_exists_in_the_catalog() -> None:
    """A gate id that the catalog cannot build must fail at load, not silently vanish."""
    verifier = ProjectVerifier.load()
    declared = {
        gate_id
        for policy in (*verifier.levels.values(), *verifier.profiles.values())
        for gate_id in (*policy.gates, *policy.always_gates, *(gate for gate, _ in policy.conditional_gates))
    }

    assert declared
    assert declared <= set(GATE_CATALOG)


def test_gate_argv_always_runs_an_allowlisted_module_form() -> None:
    """`python script.py` is denied by policy, so every gate must use `python -m <allowed>`."""
    verifier = ProjectVerifier.load()
    allowed = set(PermissionPolicy.load().python_modules)

    for level in sorted(verifier.levels):
        for check in verifier.build_plan(level=level).checks:
            assert check.argv[:2] == (sys.executable, "-m"), f"{level}/{check.check_id}"
            assert check.argv[2] in allowed, f"{level}/{check.check_id} runs a non-allowlisted module"


def test_verifier_rejects_unknown_profile() -> None:
    verifier = ProjectVerifier.load()

    with pytest.raises(ValueError, match="Unknown verification profile"):
        verifier.commands_for("missing")
