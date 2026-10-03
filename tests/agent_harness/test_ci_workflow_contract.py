"""Contract tests for the dedicated Agent Harness CI workflow."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/agent_harness.yml"

_UPLOAD_ARTIFACT_MAJOR = re.compile(r"uses:\s*actions/upload-artifact@v(\d+)")


def _workflow() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def _upload_artifact_major(workflow: str) -> int:
    """Return the highest major the workflow pins `actions/upload-artifact` to.

    A floor rather than a pin: Dependabot bumps this action often, and an exact
    `== "actions/upload-artifact@v4"` assertion made the bump PR fail the test
    meant to permit it.
    """
    majors = [int(major) for major in _UPLOAD_ARTIFACT_MAJOR.findall(workflow)]
    assert majors, "agent_harness.yml does not pin actions/upload-artifact to a major version"
    return max(majors)


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
    assert _upload_artifact_major(workflow) >= 4
    assert "path: artifacts/agent-harness/ci" in workflow


def test_agent_harness_workflow_stays_secret_free_and_offline() -> None:
    workflow = _workflow()

    assert "secrets." not in workflow
    assert "curl" not in workflow
    assert "wget" not in workflow
    assert "PLAYWRIGHT" not in workflow


def test_execution_gate_job_proves_the_run_verify_validate_path() -> None:
    """The read-only job cannot catch a regression in run/verify/evidence."""
    workflow = _workflow()

    assert "harness-execution-gate:" in workflow
    assert 'python3 -m tools.agent_harness run "harness execution path verification"' in workflow
    assert 'python3 -m tools.agent_harness verify "${{ steps.harness_run.outputs.run_id }}" --json' in workflow
    assert "--require-verified" in workflow
    # run_id must be threaded between steps: verify crashes on a missing run dir.
    assert 'echo "run_id=${RUN_ID}" | tee -a "$GITHUB_OUTPUT"' in workflow
    assert "steps.harness_run.outputs.run_id" in workflow


def test_execution_gate_stays_offline_and_uses_a_cheap_profile() -> None:
    """`feature` -> `project` gates cost ~7s; `refactor` -> `full` would run the whole suite."""
    workflow = _workflow()

    assert "--profile feature" in workflow
    assert "secrets." not in workflow
    assert "curl" not in workflow
    assert "wget" not in workflow
