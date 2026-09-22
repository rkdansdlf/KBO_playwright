"""CLI smoke tests for the repository-local agent Harness."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from tools.agent_harness.cli import main

if TYPE_CHECKING:
    import pytest


def test_doctor_reports_locked_reference_only_stack(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["doctor", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PASS"
    assert payload["skill_count"] == 10
    assert all(skill["mode"] == "reference_only" for skill in payload["skills"])


def test_plan_routes_crawler_task(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", "--profile", "crawler-bug", "boxscore timeout", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "crawler-bug"
    assert payload["verification"] == "crawler"
