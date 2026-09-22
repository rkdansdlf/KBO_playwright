"""Build serializable Harness execution plans."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.agent_harness.router import RouteDecision


@dataclass(frozen=True)
class HarnessPlan:
    """Describe the selected stages for one development task."""

    task: str
    profile: str
    created_at: str
    stages: tuple[dict[str, object], ...]
    verification: str
    output: str
    external_execution: str

    def to_dict(self) -> dict[str, object]:
        """Serialize the plan to JSON-compatible values."""
        return asdict(self)


def build_plan(task: str, decision: RouteDecision) -> HarnessPlan:
    """Create an ordered plan without executing third-party adapters."""
    stages: list[dict[str, object]] = []
    for stage_name, skills in (
        ("context", decision.context),
        ("workflow", decision.workflow),
        ("guard", decision.guards),
    ):
        if skills:
            stages.append({"stage": stage_name, "skills": list(skills), "execution": "reference_only"})
    stages.append({"stage": "verification", "profile": decision.verification, "execution": "local"})
    stages.append({"stage": "output", "skills": [decision.output], "execution": "policy"})
    return HarnessPlan(
        task=task,
        profile=decision.profile,
        created_at=datetime.now(UTC).isoformat(),
        stages=tuple(stages),
        verification=decision.verification,
        output=decision.output,
        external_execution=decision.external_execution,
    )


__all__ = ["HarnessPlan", "build_plan"]
