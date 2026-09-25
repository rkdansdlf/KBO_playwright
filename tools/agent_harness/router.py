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
            selected, reason = self._score_profiles(task)

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

    def _score_profiles(self, task: str) -> tuple[str, str]:
        """Rank profiles by strong intent markers, then capped domain trigger nouns."""
        precedence = self.registry.precedence
        normalized = task.casefold()
        scores: dict[str, int] = {}
        intent_counts: dict[str, int] = {}
        domain_counts: dict[str, int] = {}
        for name, spec in self.registry.profiles.items():
            domain_matches = sum(trigger.casefold() in normalized for trigger in spec.triggers)
            score, intent_matches = precedence.score(name, task, domain_matches)
            scores[name] = score
            intent_counts[name] = intent_matches
            domain_counts[name] = domain_matches

        best_score = max(scores.values(), default=0)
        if best_score == 0:
            return self.registry.default_profile, "default profile"

        selected = max(scores, key=lambda name: (scores[name], name == self.registry.default_profile))
        if intent_counts[selected]:
            reason = f"intent marker matched profile {selected}"
        else:
            reason = f"matched {domain_counts[selected]} domain trigger(s)"
        return selected, reason

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
