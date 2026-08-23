"""Unit tests for src.ci.dto."""

from __future__ import annotations

from src.ci.dto import (
    WorkflowAuditIssue,
    WorkflowAuditReport,
    WorkflowJobMeta,
    WorkflowMeta,
    WorkflowTriggerType,
)


def test_workflow_trigger_type_values() -> None:
    assert WorkflowTriggerType.SCHEDULE == "schedule"
    assert WorkflowTriggerType.WORKFLOW_DISPATCH == "workflow_dispatch"
    assert WorkflowTriggerType.PUSH == "push"
    assert WorkflowTriggerType.PULL_REQUEST == "pull_request"


def test_workflow_job_meta_to_dict() -> None:
    meta = WorkflowJobMeta(
        job_id="finalize",
        name="Daily Finalize",
        runs_on="ubuntu-latest",
        timeout_minutes=30,
        uses_composite_action=True,
        composite_actions_used=["./.github/actions/kbo-job-setup"],
        env_keys=["DATABASE_URL"],
        secret_keys=["ORACLE_WALLET_B64"],
    )
    d = meta.to_dict()
    assert d["job_id"] == "finalize"
    assert d["timeout_minutes"] == 30
    assert d["uses_composite_action"] is True
    assert "./.github/actions/kbo-job-setup" in d["composite_actions_used"]


def test_workflow_meta_to_dict() -> None:
    wf = WorkflowMeta(
        file_path=".github/workflows/daily_kbo_sync.yml",
        workflow_name="Daily KBO Sync",
        triggers=[WorkflowTriggerType.SCHEDULE, WorkflowTriggerType.WORKFLOW_DISPATCH],
        cron_schedules=["0 18 * * *"],
        has_concurrency_guard=True,
    )
    d = wf.to_dict()
    assert d["workflow_name"] == "Daily KBO Sync"
    assert "schedule" in d["triggers"]
    assert "0 18 * * *" in d["cron_schedules"]
    assert d["has_concurrency_guard"] is True


def test_workflow_audit_report_to_dict() -> None:
    issue = WorkflowAuditIssue(
        severity="WARN",
        workflow_file="test.yml",
        rule_name="MISSING_TIMEOUT",
        message="Job missing timeout",
    )
    report = WorkflowAuditReport(
        total_workflows=10,
        total_jobs=25,
        passed_workflows=9,
        failed_workflows=1,
        issues=[issue],
    )
    d = report.to_dict()
    assert d["total_workflows"] == 10
    assert d["issues_count"] == 1
    assert d["issues"][0]["rule_name"] == "MISSING_TIMEOUT"
