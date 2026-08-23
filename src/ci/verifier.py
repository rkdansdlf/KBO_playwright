"""Static analysis and integrity verifier for GitHub Actions workflows."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

from src.ci.dto import (
    WorkflowAuditIssue,
    WorkflowAuditReport,
    WorkflowJobMeta,
    WorkflowMeta,
    WorkflowTriggerType,
)

logger = logging.getLogger(__name__)

CRON_FIELD_COUNT = 5
SECRET_PATTERN = re.compile(r"secrets\.([A-Za-z0-9_]+)")
KNOWN_COMPOSITE_ACTIONS = {
    "./.github/actions/kbo-job-setup",
    "./.github/actions/python-env",
    "./.github/actions/notify",
}


class WorkflowVerifier:
    """Audits GitHub Actions workflow definitions for safety, timeout guards, and contracts."""

    def __init__(self, project_root: Path | None = None) -> None:
        """Initialize the verifier with repository root path."""
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.workflows_dir = self.project_root / ".github" / "workflows"

    def _parse_trigger_type(self, trigger_key: object) -> WorkflowTriggerType:
        key_str = str(trigger_key).lower()
        if key_str == "schedule":
            return WorkflowTriggerType.SCHEDULE
        if key_str == "workflow_dispatch":
            return WorkflowTriggerType.WORKFLOW_DISPATCH
        if key_str == "push":
            return WorkflowTriggerType.PUSH
        if key_str in {"pull_request", "pull_request_target"}:
            return WorkflowTriggerType.PULL_REQUEST
        return WorkflowTriggerType.OTHER

    def _parse_triggers(self, on_block: object) -> tuple[list[WorkflowTriggerType], list[str]]:
        triggers: list[WorkflowTriggerType] = []
        cron_schedules: list[str] = []

        if isinstance(on_block, str):
            triggers.append(self._parse_trigger_type(on_block))
        elif isinstance(on_block, list):
            triggers.extend(self._parse_trigger_type(t) for t in on_block)
        elif isinstance(on_block, dict):
            for k, v in on_block.items():
                t_type = self._parse_trigger_type(k)
                triggers.append(t_type)
                if t_type == WorkflowTriggerType.SCHEDULE and isinstance(v, list):
                    cron_schedules.extend(str(item["cron"]) for item in v if isinstance(item, dict) and "cron" in item)
        return triggers, cron_schedules

    def _parse_job(self, j_id: str, j_data: dict[str, object]) -> WorkflowJobMeta:
        j_name = str(j_data.get("name", j_id))
        runs_on = str(j_data.get("runs-on", "ubuntu-latest"))
        timeout = j_data.get("timeout-minutes")

        steps = j_data.get("steps", [])
        composite_used: list[str] = []
        secrets_found: set[str] = set()
        env_keys: list[str] = list(j_data.get("env", {}).keys()) if isinstance(j_data.get("env"), dict) else []

        if isinstance(steps, list):
            for step in steps:
                if isinstance(step, dict):
                    uses = str(step.get("uses", ""))
                    if uses.startswith("./.github/actions/"):
                        composite_used.append(uses)
                    step_env = step.get("env", {})
                    if isinstance(step_env, dict):
                        env_keys.extend(step_env.keys())

        raw_job_text = yaml.dump(j_data)
        for match in SECRET_PATTERN.finditer(raw_job_text):
            secrets_found.add(match.group(1))

        return WorkflowJobMeta(
            job_id=j_id,
            name=j_name,
            runs_on=runs_on,
            timeout_minutes=int(str(timeout)) if timeout is not None else None,
            uses_composite_action=bool(composite_used),
            composite_actions_used=composite_used,
            env_keys=sorted(set(env_keys)),
            secret_keys=sorted(secrets_found),
        )

    def parse_workflow(self, file_path: Path) -> tuple[WorkflowMeta, list[WorkflowAuditIssue]]:
        """Parse a workflow YAML file into structured metadata and structural issues."""
        issues: list[WorkflowAuditIssue] = []
        file_name = file_path.name

        try:
            content = file_path.read_text(encoding="utf-8")
            data = yaml.safe_load(content) or {}
        except (yaml.YAMLError, OSError, ValueError) as exc:
            issues.append(
                WorkflowAuditIssue(
                    severity="ERROR",
                    workflow_file=file_name,
                    rule_name="YAML_SYNTAX_ERROR",
                    message=f"Failed to parse YAML: {exc}",
                )
            )
            return (
                WorkflowMeta(
                    file_path=str(file_path),
                    workflow_name=file_name,
                ),
                issues,
            )

        workflow_name = str(data.get("name", file_name))
        on_block = data.get("on") or data.get(True) or {}
        triggers, cron_schedules = self._parse_triggers(on_block)

        has_concurrency = "concurrency" in data
        jobs_block = data.get("jobs", {})
        jobs: list[WorkflowJobMeta] = []

        if isinstance(jobs_block, dict):
            for j_id, j_data in jobs_block.items():
                if isinstance(j_data, dict):
                    jobs.append(self._parse_job(j_id, j_data))

        meta = WorkflowMeta(
            file_path=str(file_path),
            workflow_name=workflow_name,
            triggers=triggers,
            cron_schedules=cron_schedules,
            jobs=jobs,
            has_concurrency_guard=has_concurrency,
        )
        return meta, issues

    def verify_workflow(self, file_path: Path) -> tuple[WorkflowMeta, list[WorkflowAuditIssue]]:
        """Run all verification rules on a single workflow file."""
        meta, issues = self.parse_workflow(file_path)
        file_name = file_path.name

        # Rule 1: Cron expression valid format
        for cron in meta.cron_schedules:
            parts = cron.strip().split()
            if len(parts) != CRON_FIELD_COUNT:
                issues.append(
                    WorkflowAuditIssue(
                        severity="ERROR",
                        workflow_file=file_name,
                        rule_name="INVALID_CRON_EXPRESSION",
                        message=f"Cron expression '{cron}' does not have 5 fields.",
                    )
                )

        # Rule 2: Job timeout validation
        for job in meta.jobs:
            if job.timeout_minutes is None:
                issues.append(
                    WorkflowAuditIssue(
                        severity="WARN",
                        workflow_file=file_name,
                        job_id=job.job_id,
                        rule_name="MISSING_JOB_TIMEOUT",
                        message=f"Job '{job.job_id}' does not specify 'timeout-minutes'.",
                    )
                )

            # Rule 3: Validate composite actions
            for action in job.composite_actions_used:
                if action not in KNOWN_COMPOSITE_ACTIONS:
                    action_path = self.project_root / action.lstrip("./")
                    if not action_path.exists():
                        issues.append(
                            WorkflowAuditIssue(
                                severity="ERROR",
                                workflow_file=file_name,
                                job_id=job.job_id,
                                rule_name="UNKNOWN_COMPOSITE_ACTION",
                                message=f"Composite action path '{action}' not found in repository.",
                            )
                        )

        # Rule 4: Scheduled workflows with multi-jobs should prefer concurrency guard
        if WorkflowTriggerType.SCHEDULE in meta.triggers and len(meta.jobs) > 1 and not meta.has_concurrency_guard:
            issues.append(
                WorkflowAuditIssue(
                    severity="INFO",
                    workflow_file=file_name,
                    rule_name="RECOMMEND_CONCURRENCY_GUARD",
                    message="Scheduled multi-job workflow may benefit from concurrency cancellation.",
                )
            )

        return meta, issues

    def verify_all_workflows(self, target_dir: Path | None = None) -> WorkflowAuditReport:
        """Verify all workflow files in the specified or default directory."""
        dir_path = target_dir or self.workflows_dir
        all_issues: list[WorkflowAuditIssue] = []
        total_workflows = 0
        total_jobs = 0
        failed_workflows = 0

        workflow_files = sorted(dir_path.glob("*.yml")) + sorted(dir_path.glob("*.yaml"))

        for f in workflow_files:
            total_workflows += 1
            meta, issues = self.verify_workflow(f)
            total_jobs += len(meta.jobs)
            if any(i.severity == "ERROR" for i in issues):
                failed_workflows += 1
            all_issues.extend(issues)

        passed_workflows = total_workflows - failed_workflows

        return WorkflowAuditReport(
            total_workflows=total_workflows,
            total_jobs=total_jobs,
            passed_workflows=passed_workflows,
            failed_workflows=failed_workflows,
            issues=all_issues,
        )
