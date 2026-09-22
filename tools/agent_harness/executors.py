"""Skill transport executors for the agent Harness.

Executors never pretend to run skills they cannot execute. Native and CLI
skills require the host agent, so they are recorded as
``HOST_EXECUTION_REQUIRED``. Only policy skills are applied locally.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from tools.agent_harness.dto import SkillTransport

if TYPE_CHECKING:
    from tools.agent_harness.dto import SkillDefinition
    from tools.agent_harness.registry import HarnessRegistry
    from tools.agent_harness.router import RouteDecision


class SkillExecutionStatus(StrEnum):
    """Describe the honest outcome of one skill execution attempt."""

    EXECUTED = "executed"
    HOST_EXECUTION_REQUIRED = "host_execution_required"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class SkillExecutionResult:
    """Record the outcome of one routed skill."""

    skill_id: str
    stage: str
    transport: SkillTransport
    status: SkillExecutionStatus
    detail: str

    def to_dict(self) -> dict[str, object]:
        """Serialize the execution result to JSON-compatible values."""
        payload = asdict(self)
        payload["transport"] = self.transport.value
        payload["status"] = self.status.value
        return payload


class SkillExecutor(Protocol):
    """Execute one skill definition within a Harness stage."""

    def execute(self, skill: SkillDefinition, stage: str) -> SkillExecutionResult:
        """Execute a skill and return its honest outcome."""
        ...


class NativeSkillExecutor:
    """Request host-side execution for native skills without pretending."""

    def execute(self, skill: SkillDefinition, stage: str) -> SkillExecutionResult:
        """Record a host-execution request for a native skill."""
        return SkillExecutionResult(
            skill_id=skill.skill_id,
            stage=stage,
            transport=skill.transport,
            status=SkillExecutionStatus.HOST_EXECUTION_REQUIRED,
            detail=f"native skill requires host execution in stage {stage}",
        )


class CliSkillExecutor:
    """Request host-side CLI invocation without executing it directly."""

    def execute(self, skill: SkillDefinition, stage: str) -> SkillExecutionResult:
        """Record a host-execution request for a CLI skill."""
        return SkillExecutionResult(
            skill_id=skill.skill_id,
            stage=stage,
            transport=skill.transport,
            status=SkillExecutionStatus.HOST_EXECUTION_REQUIRED,
            detail=f"cli skill requires host invocation in stage {stage}",
        )


class McpSkillExecutor:
    """Request host-side MCP execution without pretending."""

    def execute(self, skill: SkillDefinition, stage: str) -> SkillExecutionResult:
        """Record a host-execution request for an MCP skill."""
        return SkillExecutionResult(
            skill_id=skill.skill_id,
            stage=stage,
            transport=skill.transport,
            status=SkillExecutionStatus.HOST_EXECUTION_REQUIRED,
            detail=f"mcp skill requires host execution in stage {stage}",
        )


class PolicyExecutor:
    """Apply output and context policy instructions locally."""

    def execute(self, skill: SkillDefinition, stage: str) -> SkillExecutionResult:
        """Apply a policy skill as a local output instruction."""
        return SkillExecutionResult(
            skill_id=skill.skill_id,
            stage=stage,
            transport=skill.transport,
            status=SkillExecutionStatus.EXECUTED,
            detail=f"policy applied as {stage} instruction",
        )


def executor_for(skill: SkillDefinition) -> SkillExecutor:
    """Return the executor matching a skill transport."""
    if skill.transport == SkillTransport.POLICY:
        return PolicyExecutor()
    if skill.transport == SkillTransport.CLI:
        return CliSkillExecutor()
    if skill.transport == SkillTransport.MCP:
        return McpSkillExecutor()
    return NativeSkillExecutor()


def execute_route(decision: RouteDecision, registry: HarnessRegistry) -> list[SkillExecutionResult]:
    """Record honest execution outcomes for every routed skill stage."""
    results: list[SkillExecutionResult] = []
    for stage, skill_ids in (
        ("context", decision.context),
        ("workflow", decision.workflow),
        ("guard", decision.guards),
    ):
        for skill_id in skill_ids:
            skill = registry.get_skill_definition(skill_id)
            results.append(executor_for(skill).execute(skill, stage))
    output_skill = registry.get_skill_definition(decision.output)
    results.append(executor_for(output_skill).execute(output_skill, "output"))
    return results


__all__ = [
    "CliSkillExecutor",
    "McpSkillExecutor",
    "NativeSkillExecutor",
    "PolicyExecutor",
    "SkillExecutionResult",
    "SkillExecutionStatus",
    "SkillExecutor",
    "execute_route",
    "executor_for",
]
