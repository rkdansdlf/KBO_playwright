"""Contract tests for the dedicated Agent Harness CI workflow."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/agent_harness.yml"


def _workflow() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_agent_harness_workflow_is_a_separate_fast_gate() -> None:
    workflow = _workflow()

    assert "name: Agent Harness" in workflow
    assert 'python-version: "3.12"' in workflow
    assert "init-db: 'false'" in workflow
    assert "permissions:\n  contents: read" in workflow
    assert "timeout-minutes: 5" in workflow


def test_agent_harness_workflow_runs_all_harness_gates() -> None:
    workflow = _workflow()

    assert "ruff check --output-format=github tools/agent_harness tests/agent_harness" in workflow
    assert "ruff format --check tools/agent_harness tests/agent_harness" in workflow
    assert "pytest tests/agent_harness -q" in workflow
    assert "python3 -m tools.agent_harness doctor --json" in workflow


def test_agent_harness_workflow_replays_every_round_with_metrics() -> None:
    workflow = _workflow()

    for round_id in (1, 2, 3):
        assert (
            f"replay --round {round_id} --metrics-out artifacts/agent-harness/ci/round-{round_id}-metrics.json"
            in workflow
        )
    assert "actions/upload-artifact@v4" in workflow
    assert "path: artifacts/agent-harness/ci" in workflow


def test_agent_harness_workflow_stays_secret_free_and_offline() -> None:
    workflow = _workflow()

    assert "secrets." not in workflow
    assert "curl" not in workflow
    assert "wget" not in workflow
    assert "PLAYWRIGHT" not in workflow
