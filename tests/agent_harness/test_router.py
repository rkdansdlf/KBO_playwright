"""Tests for deterministic task routing."""

from __future__ import annotations

from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter


def test_router_selects_crawler_bug_profile() -> None:
    decision = TaskRouter(HarnessRegistry.load()).route("boxscore crawler timeout 수정")

    assert decision.profile == "crawler-bug"
    assert decision.context == ("graphify",)
    assert decision.verification == "crawler"


def test_refactor_is_only_default_route_with_both_graph_engines() -> None:
    router = TaskRouter(HarnessRegistry.load())

    decision = router.route("large scheduler architecture refactor")

    assert decision.profile == "refactor"
    assert decision.context == ("graphify", "understand-anything")
    assert decision.output == "i-have-adhd"
    assert "caveman" not in decision.context


def test_explicit_profile_overrides_keyword_routing() -> None:
    decision = TaskRouter(HarnessRegistry.load()).route("crawler timeout", profile="analytics")

    assert decision.profile == "analytics"
