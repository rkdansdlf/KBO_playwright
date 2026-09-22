"""PR1 contract tests: dto, exceptions, and registry typed parsing."""

from __future__ import annotations

import pytest

from tools.agent_harness.dto import (
    PermissionDecision,
    PermissionResult,
    SkillTransport,
    TaskRequest,
    VerificationLevel,
)
from tools.agent_harness.exceptions import HarnessError, UnknownSkillError
from tools.agent_harness.registry import HarnessRegistry


def test_task_request_defaults() -> None:
    request = TaskRequest(prompt="boxscore timeout")

    assert request.changed_files == []
    assert request.explicit_profile is None
    assert request.dry_run is True


def test_skill_transport_values() -> None:
    assert SkillTransport("cli") == SkillTransport.CLI
    assert SkillTransport("native") == SkillTransport.NATIVE
    assert SkillTransport("policy") == SkillTransport.POLICY
    with pytest.raises(ValueError):
        SkillTransport("shell")


def test_verification_and_permission_enums() -> None:
    assert VerificationLevel.FULL.value == "full"
    result = PermissionResult(decision=PermissionDecision.ALLOW, reason="ok")

    assert result.allowed is True
    denied = PermissionResult(decision=PermissionDecision.DENY, reason="no")
    assert denied.allowed is False


def test_registry_typed_definitions_match_adapters() -> None:
    registry = HarnessRegistry.load()

    graphify = registry.get_skill_definition("graphify")
    assert graphify.transport == SkillTransport.CLI
    assert "dependency-context" in graphify.capabilities
    assert graphify.source_repo is not None

    superpowers = registry.get_skill_definition("superpowers")
    assert superpowers.transport == SkillTransport.NATIVE

    adhd = registry.get_skill_definition("i-have-adhd")
    assert adhd.transport == SkillTransport.POLICY

    assert registry.validate() == []


def test_registry_capability_lookup_and_errors() -> None:
    registry = HarnessRegistry.load()

    matches = registry.find_by_capability("dependency-context")
    assert [skill.skill_id for skill in matches] == ["graphify"]

    assert registry.resolve_many(["graphify", "superpowers"])[0].skill_id == "graphify"

    with pytest.raises(UnknownSkillError):
        registry.get_skill_definition("missing-skill")
    with pytest.raises(HarnessError):
        registry.validate_lock("missing-skill")


def test_registry_health_and_lock() -> None:
    registry = HarnessRegistry.load()

    health = registry.skill_health("graphify")
    assert health.version_matches is True

    lock = registry.validate_lock("graphify")
    assert lock.pinned is True

    report = registry.typed_doctor()
    assert report.healthy is True
    assert len(report.skills) == 10
