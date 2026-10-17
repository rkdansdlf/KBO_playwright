"""CLI smoke tests for the repository-local agent Harness."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from tools.agent_harness.cli import main

if TYPE_CHECKING:
    import pytest


def test_doctor_reports_activation_states(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["doctor", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PASS"
    assert payload["skill_count"] == 10
    activated = [skill for skill in payload["skills"] if skill["mode"] == "activated"]
    reference_only = [skill for skill in payload["skills"] if skill["mode"] == "reference_only"]
    # Seven license-cleared adapters are activated; the three gated ones
    # (caveman NOASSERTION, aas/scientific selected-asset review) stay locked.
    assert len(activated) == 7
    assert {skill["name"] for skill in reference_only} == {"caveman", "aas", "scientific"}


def test_plan_routes_crawler_task(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", "--profile", "crawler-bug", "boxscore timeout", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "crawler-bug"
    assert payload["verification"] == "crawler"


def test_doctor_strict_fails_on_license_warnings(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["doctor", "--strict", "--json"]) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PASS"
    assert payload["strict"] is True
    assert len(payload["warnings"]) > 0


def test_doctor_default_ignores_warnings_for_exit_code(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["doctor", "--json"]) == 0
    capsys.readouterr()
