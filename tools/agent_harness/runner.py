"""Create auditable Harness runs without executing external adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.context_builder import ContextBuilder
from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION
from tools.agent_harness.evidence import EvidenceStore
from tools.agent_harness.planner import build_plan
from tools.agent_harness.router import TaskRouter

if TYPE_CHECKING:
    from pathlib import Path

    from tools.agent_harness.executors import SkillExecutionResult
    from tools.agent_harness.permissions import PermissionPolicy
    from tools.agent_harness.registry import HarnessRegistry
    from tools.agent_harness.router import RouteDecision


@dataclass(frozen=True)
class HarnessRun:
    """Identify an initialized Harness evidence run."""

    run_id: str
    artifact_dir: Path
    profile: str


class HarnessRunner:
    """Route a task and persist its local execution handoff artifacts."""

    def __init__(self, registry: HarnessRegistry, permissions: PermissionPolicy) -> None:
        """Initialize the runner with validated configuration and policy."""
        self.registry = registry
        self.permissions = permissions

    def run(self, task: str, profile: str | None = None, changed_files: list[str] | tuple[str, ...] = ()) -> HarnessRun:
        """Create task, plan, context, trace, command, and report artifacts."""
        from tools.agent_harness.dto import TaskRequest

        request = TaskRequest(prompt=task, changed_files=list(changed_files), explicit_profile=profile)
        decision = TaskRouter(self.registry).route_request(request)
        plan = build_plan(task, decision)
        artifacts_root = self.registry.root / "artifacts" / "agent-harness"
        evidence = EvidenceStore.create(artifacts_root, self.permissions)
        context = ContextBuilder(self.registry, self.permissions).build(decision)

        evidence.write_json(
            "task.json",
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "run_id": evidence.run_id,
                "task": task,
                "profile": decision.profile,
                "changed_files": request.changed_files,
                "explicit_profile": request.explicit_profile,
            },
        )
        evidence.write_json("plan.json", {"run_id": evidence.run_id, **plan.to_dict()})
        evidence.write_json(
            "context.json",
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "run_id": evidence.run_id,
                **context,
            },
        )
        for stage in plan.stages:
            evidence.append_jsonl("skill-trace.jsonl", self._record(evidence.run_id, stage))
        for outcome in self.execute_stages(decision):
            evidence.append_jsonl("skill-trace.jsonl", self._record(evidence.run_id, outcome.to_dict()))
        evidence.write_text("commands.jsonl", "")
        evidence.write_json(
            "verification.json",
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "run_id": evidence.run_id,
                "profile": decision.verification,
                "status": "pending",
                "commands": [],
            },
        )
        evidence.write_text(
            "report.md",
            self._render_report(evidence.run_id, task, decision.profile, decision.verification),
        )
        from tools.agent_harness.artifact_contract import assert_artifact_contract, validate_execution_plan
        from tools.agent_harness.exceptions import ArtifactContractError

        assert_artifact_contract(evidence.root)
        plan_issues = validate_execution_plan(evidence.root, self.registry)
        if plan_issues:
            msg = "Harness execution plan contract failed: " + "; ".join(plan_issues)
            raise ArtifactContractError(msg)
        return HarnessRun(evidence.run_id, evidence.root, decision.profile)

    @staticmethod
    def _record(run_id: str, payload: dict[str, object]) -> dict[str, object]:
        """Add current schema and run identity to one JSONL record."""
        return {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "run_id": run_id,
            **payload,
        }

    def execute_stages(self, decision: RouteDecision) -> list[SkillExecutionResult]:
        """Record honest executor outcomes for a routing decision."""
        from tools.agent_harness.executors import execute_route

        return execute_route(decision, self.registry)

    @staticmethod
    def _render_report(run_id: str, task: str, profile: str, verification: str) -> str:
        return (
            "# Agent Harness Run\n\n"
            f"- Run: `{run_id}`\n"
            f"- Profile: `{profile}`\n"
            f"- Task: {task}\n"
            "- External adapters: `reference_only`\n"
            f"- Verification: `{verification}` (pending)\n"
        )


__all__ = ["HarnessRun", "HarnessRunner"]
