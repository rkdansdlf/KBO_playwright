"""Tests for Harness configuration and lock validation."""

from __future__ import annotations

from tools.agent_harness.registry import HarnessRegistry


def test_registry_loads_locked_skill_stack() -> None:
    registry = HarnessRegistry.load()

    assert len(registry.skills) == 10
    assert set(registry.skills) == set(registry.locked_skills)
    assert registry.locked_skills["graphify"].license == "Apache-2.0"
    assert registry.locked_skills["caveman"].redistribution == "prohibited_pending_review"
    assert registry.locked_skills["scientific"].redistribution == "selected_asset_review_required"


def test_registry_validates_activation_gates() -> None:
    registry = HarnessRegistry.load()

    issues = registry.validate()

    assert issues == []
    # Activation is legal only where redistribution is allowed; the three
    # gated adapters must remain non-executable and unapplied.
    for adapter in registry.adapters.values():
        if adapter.executable or adapter.apply:
            assert registry.locked_skills[adapter.name].redistribution == "allowed"
    for name in ("caveman", "aas", "scientific"):
        assert not registry.adapters[name].executable
        assert not registry.adapters[name].apply
