"""Unit tests for src.ci.verifier."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.ci.dto import WorkflowTriggerType
from src.ci.verifier import WorkflowVerifier

if TYPE_CHECKING:
    from pathlib import Path


def test_parse_and_verify_valid_workflow(tmp_path: Path) -> None:
    wf_file = tmp_path / "valid_workflow.yml"
    wf_file.write_text("""
name: Test Valid Workflow
on:
  schedule:
    - cron: '0 18 * * *'
  workflow_dispatch:

concurrency:
  group: test-group
  cancel-in-progress: true

jobs:
  test_job:
    name: Test Job
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - name: Checkout
        uses: actions/checkout@v4
      - name: Run Script
        env:
          DB_URL: ${{ secrets.DATABASE_URL }}
        run: echo "ok"
""")

    verifier = WorkflowVerifier(project_root=tmp_path)
    meta, issues = verifier.verify_workflow(wf_file)

    assert meta.workflow_name == "Test Valid Workflow"
    assert WorkflowTriggerType.SCHEDULE in meta.triggers
    assert WorkflowTriggerType.WORKFLOW_DISPATCH in meta.triggers
    assert meta.cron_schedules == ["0 18 * * *"]
    assert meta.has_concurrency_guard is True
    assert len(meta.jobs) == 1
    assert meta.jobs[0].timeout_minutes == 15
    assert "DATABASE_URL" in meta.jobs[0].secret_keys

    # No errors or warnings
    assert not any(i.severity in {"ERROR", "WARN"} for i in issues)


def test_verify_workflow_detects_issues(tmp_path: Path) -> None:
    wf_file = tmp_path / "flawed_workflow.yml"
    wf_file.write_text("""
name: Flawed Workflow
on:
  schedule:
    - cron: 'invalid cron format'

jobs:
  no_timeout_job:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/nonexistent-action
""")

    verifier = WorkflowVerifier(project_root=tmp_path)
    _, issues = verifier.verify_workflow(wf_file)

    rules = [i.rule_name for i in issues]
    assert "INVALID_CRON_EXPRESSION" in rules
    assert "MISSING_JOB_TIMEOUT" in rules
    assert "UNKNOWN_COMPOSITE_ACTION" in rules


def test_verify_all_workflows_in_repo() -> None:
    verifier = WorkflowVerifier()
    report = verifier.verify_all_workflows()

    assert report.total_workflows >= 10
    assert report.total_jobs >= 10
    # Ensure there are no fatal syntax or broken composite action errors in the repo
    assert report.failed_workflows == 0
