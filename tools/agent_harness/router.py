"""Route development tasks to deterministic Harness profiles."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.agent_harness.dto import TaskRequest
    from tools.agent_harness.registry import HarnessRegistry


@dataclass(frozen=True)
class RouteDecision:
    """Record the selected profile and its ordered skill stages."""

    profile: str
    reason: str
    context: tuple[str, ...]
    workflow: tuple[str, ...]
    guards: tuple[str, ...]
    verification: str
    output: str
    external_execution: str = "reference_only"

    def to_dict(self) -> dict[str, object]:
        """Serialize the routing decision to JSON-compatible values."""
        return asdict(self)


class TaskRouter:
    """Classify task text using explicit, inspectable profile triggers."""

    def __init__(self, registry: HarnessRegistry) -> None:
        """Initialize the router from a validated registry."""
        self.registry = registry

    def route(self, task: str, profile: str | None = None) -> RouteDecision:
        """Select an explicit profile or the highest-scoring trigger match."""
        if profile is not None:
            if profile not in self.registry.profiles:
                msg = f"Unknown Harness profile: {profile}"
                raise ValueError(msg)
            selected = profile
            reason = "explicit profile"
        else:
            normalized = task.casefold()
            scores = {
                name: sum(trigger.casefold() in normalized for trigger in spec.triggers)
                for name, spec in self.registry.profiles.items()
            }
            best_score = max(scores.values(), default=0)
            if best_score == 0:
                selected = self.registry.default_profile
                reason = "default profile"
            else:
                selected = max(scores, key=lambda name: (scores[name], name == self.registry.default_profile))
                reason = f"matched {scores[selected]} profile trigger(s)"

        route = self.registry.routes[selected]
        return RouteDecision(
            profile=selected,
            reason=reason,
            context=route.context,
            workflow=route.workflow,
            guards=route.guards,
            verification=route.verification,
            output=route.output,
        )

    def route_request(self, request: TaskRequest) -> RouteDecision:
        """Route with explicit profile, changed files, prompt, then default priority."""
        if request.explicit_profile is not None:
            return self.route(request.prompt, request.explicit_profile)
        if request.changed_files:
            from tools.agent_harness.project_adapter import KBOProjectAdapter

            inferred = KBOProjectAdapter().infer_profile_from_files(request.changed_files)
            if inferred is not None and inferred in self.registry.profiles:
                decision = self.route(request.prompt, inferred)
                return replace(decision, reason=f"file signal matched {inferred}")
        return self.route(request.prompt)


__all__ = ["RouteDecision", "TaskRouter"]
