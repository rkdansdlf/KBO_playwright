"""P0 baseline lock-in: full CLI lifecycle smoke plus RouteDecision contract."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from tools.agent_harness.cli import main
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import RouteDecision, TaskRouter

if TYPE_CHECKING:
    import pytest


def test_route_decision_contract_shape() -> None:
    decision = TaskRouter(HarnessRegistry.load()).route("boxscore crawler timeout")

    assert isinstance(decision, RouteDecision)
    assert decision.profile == "crawler-bug"
    assert decision.external_execution == "reference_only"
    payload = decision.to_dict()
    assert set(payload) == {
        "profile",
        "reason",
        "context",
        "workflow",
        "guards",
        "verification",
        "output",
        "external_execution",
    }


def test_full_lifecycle_smoke(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    _ = tmp_path
    # run creates a real artifact dir under artifacts/agent-harness; track and clean up.
    assert main(["run", "smoke baseline task", "--profile", "research", "--json"]) == 0
    run_payload = json.loads(capsys.readouterr().out)
    run_id = str(run_payload["run_id"])
    artifact_dir = Path("artifacts") / "agent-harness" / run_id
    try:
        assert artifact_dir.is_dir()
        for name in (
            "task.json",
            "plan.json",
            "context.json",
            "skill-trace.jsonl",
            "verification.json",
            "report.md",
        ):
            assert (artifact_dir / name).is_file(), name

        assert main(["verify", run_id, "--json"]) == 0
        verify_payload = json.loads(capsys.readouterr().out)
        assert verify_payload["run_id"] == run_id
        assert verify_payload["passed"] is True

        assert main(["report", run_id]) == 0
        report_out = capsys.readouterr().out
        assert run_id in report_out
        assert "reference_only" in report_out
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)


def test_evidence_schema_version_present(capsys: pytest.CaptureFixture[str]) -> None:
    from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION

    assert main(["run", "schema version probe", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = Path("artifacts") / "agent-harness" / run_id
    try:
        task = json.loads((artifact_dir / "task.json").read_text(encoding="utf-8"))
        plan = json.loads((artifact_dir / "plan.json").read_text(encoding="utf-8"))

        assert task["schema_version"] == EVIDENCE_SCHEMA_VERSION
        assert plan["schema_version"] == EVIDENCE_SCHEMA_VERSION
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)


def test_v1_evidence_without_version_still_verifies(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "v1 compat probe", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = Path("artifacts") / "agent-harness" / run_id
    try:
        for name in ("task.json", "plan.json"):
            path = artifact_dir / name
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload.pop("schema_version", None)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        assert main(["verify", run_id, "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["passed"] is True
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)


def test_plan_and_context_commands(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", "boxscore timeout", "--profile", "crawler-bug", "--json"]) == 0
    plan_payload = json.loads(capsys.readouterr().out)
    assert plan_payload["profile"] == "crawler-bug"
    assert plan_payload["verification"] == "crawler"

    assert main(["context", "refresh", "--json"]) == 0
    context_payload = json.loads(capsys.readouterr().out)
    assert context_payload["secrets_included"] is False
    assert context_payload["external_execution"] == "reference_only"
