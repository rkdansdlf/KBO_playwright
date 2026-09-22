"""Tests for project verification command selection and execution."""

from __future__ import annotations

import sys

from tools.agent_harness.verifier import ProjectVerifier


def test_verifier_loads_fixed_argv_commands() -> None:
    verifier = ProjectVerifier.load()

    commands = verifier.commands_for("project")

    assert commands[0][:3] == (sys.executable, "-m", "pytest")
    assert all(isinstance(command, tuple) for command in commands)


def test_verifier_rejects_unknown_profile() -> None:
    verifier = ProjectVerifier.load()

    try:
        verifier.commands_for("missing")
    except ValueError as exc:
        assert "Unknown verification profile" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
