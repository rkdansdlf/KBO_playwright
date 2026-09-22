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


def test_registry_validates_reference_only_adapters() -> None:
    registry = HarnessRegistry.load()

    issues = registry.validate()

    assert issues == []
    assert all(not adapter.executable for adapter in registry.adapters.values())
