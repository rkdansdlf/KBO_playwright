"""Load and validate the KBO agent Harness registry."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from tools.agent_harness.dto import (
        RegistryHealthReport,
        SkillDefinition,
        SkillHealth,
        SkillLockResult,
    )

REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")


def project_root() -> Path:
    """Return the repository root containing the Harness configuration."""
    return Path(__file__).resolve().parents[2]


def load_yaml_mapping(path: Path) -> dict[str, object]:
    """Load a YAML document and require a mapping root."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"Expected YAML mapping in {path}"
        raise TypeError(msg)
    return {str(key): value for key, value in payload.items()}


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        msg = f"Expected mapping for {label}"
        raise TypeError(msg)
    return {str(key): item for key, item in value.items()}


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        msg = f"Expected non-empty string for {label}"
        raise ValueError(msg)
    return value


def _strings(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        msg = f"Expected string list for {label}"
        raise ValueError(msg)
    return tuple(value)


@dataclass(frozen=True)
class SkillSpec:
    """Describe one skill role selected by the Harness."""

    name: str
    role: str
    mode: str


@dataclass(frozen=True)
class LockedSkill:
    """Pin one external skill source and its license metadata."""

    name: str
    repository: str
    revision: str
    license: str
    redistribution: str


@dataclass(frozen=True)
class AdapterSpec:
    """Describe the local, non-executing adapter for an external skill."""

    name: str
    source: str
    role: str
    executable: bool
    capabilities: tuple[str, ...]
    license_gate: str | None = None
    transport: str = "native"


@dataclass(frozen=True)
class ProfileSpec:
    """Describe one task classification profile."""

    name: str
    description: str
    triggers: tuple[str, ...]
    requires_network: bool


@dataclass(frozen=True)
class RouteSpec:
    """Describe the skill stages and verifier selected by a profile."""

    name: str
    context: tuple[str, ...]
    workflow: tuple[str, ...]
    guards: tuple[str, ...]
    verification: str
    output: str


@dataclass(frozen=True)
class HarnessRegistry:
    """Hold validated Harness, profile, adapter, stack, and lock metadata."""

    root: Path
    skills: dict[str, SkillSpec]
    locked_skills: dict[str, LockedSkill]
    adapters: dict[str, AdapterSpec]
    profiles: dict[str, ProfileSpec]
    routes: dict[str, RouteSpec]
    default_profile: str
    stack_skills: tuple[str, ...]
    stack_apply: bool

    @classmethod
    def load(cls, root: Path | None = None) -> HarnessRegistry:
        """Load the complete repository-local Harness registry."""
        repo_root = (root or project_root()).resolve()
        harness_dir = repo_root / ".agent-harness"
        harness = load_yaml_mapping(harness_dir / "harness.yaml")
        defaults = _mapping(harness.get("defaults"), "defaults")

        skill_rows = _mapping(harness.get("skills"), "skills")
        skills: dict[str, SkillSpec] = {}
        for name, raw in skill_rows.items():
            row = _mapping(raw, f"skills.{name}")
            skills[name] = SkillSpec(
                name=name,
                role=_string(row.get("role"), f"skills.{name}.role"),
                mode=_string(row.get("mode"), f"skills.{name}.mode"),
            )

        lock_payload = json.loads((repo_root / "harness.lock.json").read_text(encoding="utf-8"))
        lock_rows = _mapping(_mapping(lock_payload, "lock").get("skills"), "lock.skills")
        locked_skills: dict[str, LockedSkill] = {}
        for name, raw in lock_rows.items():
            row = _mapping(raw, f"lock.skills.{name}")
            locked_skills[name] = LockedSkill(
                name=name,
                repository=_string(row.get("repository"), f"lock.skills.{name}.repository"),
                revision=_string(row.get("revision"), f"lock.skills.{name}.revision"),
                license=_string(row.get("license"), f"lock.skills.{name}.license"),
                redistribution=_string(row.get("redistribution"), f"lock.skills.{name}.redistribution"),
            )

        adapters: dict[str, AdapterSpec] = {}
        for path in sorted((harness_dir / "adapters").glob("*.yaml")):
            row = load_yaml_mapping(path)
            name = _string(row.get("name"), f"{path}.name")
            capabilities = _strings(row.get("capabilities", []), f"{path}.capabilities")
            raw_transport = row.get("transport", "native")
            transport = str(raw_transport) if isinstance(raw_transport, str) else "native"
            adapters[name] = AdapterSpec(
                name=name,
                source=_string(row.get("source"), f"{path}.source"),
                role=_string(row.get("role"), f"{path}.role"),
                executable=row.get("executable") is True,
                capabilities=capabilities,
                license_gate=str(row["license_gate"]) if "license_gate" in row else None,
                transport=transport,
            )

        profiles: dict[str, ProfileSpec] = {}
        for path in sorted((harness_dir / "profiles").glob("*.yaml")):
            row = load_yaml_mapping(path)
            name = _string(row.get("name"), f"{path}.name")
            profiles[name] = ProfileSpec(
                name=name,
                description=_string(row.get("description"), f"{path}.description"),
                triggers=_strings(row.get("triggers", []), f"{path}.triggers"),
                requires_network=row.get("requires_network") is True,
            )

        route_rows = _mapping(harness.get("routes"), "routes")
        routes: dict[str, RouteSpec] = {}
        for name, raw in route_rows.items():
            row = _mapping(raw, f"routes.{name}")
            routes[name] = RouteSpec(
                name=name,
                context=_strings(row.get("context", []), f"routes.{name}.context"),
                workflow=_strings(row.get("workflow", []), f"routes.{name}.workflow"),
                guards=_strings(row.get("guards", []), f"routes.{name}.guards"),
                verification=_string(row.get("verification"), f"routes.{name}.verification"),
                output=_string(row.get("output"), f"routes.{name}.output"),
            )

        stack_payload = _mapping(
            json.loads((repo_root / "aas-stack.json").read_text(encoding="utf-8")),
            "stack",
        )
        return cls(
            root=repo_root,
            skills=skills,
            locked_skills=locked_skills,
            adapters=adapters,
            profiles=profiles,
            routes=routes,
            default_profile=_string(defaults.get("profile"), "defaults.profile"),
            stack_skills=_strings(stack_payload.get("skills"), "stack.skills"),
            stack_apply=stack_payload.get("apply") is True,
        )

    def validate(self) -> list[str]:
        """Return deterministic configuration issues without network access."""
        issues: list[str] = []
        issues.extend(self._validate_skill_sets())
        issues.extend(self._validate_locked_skills())
        issues.extend(self._validate_routes())
        return issues

    def _validate_skill_sets(self) -> list[str]:
        issues: list[str] = []
        skill_names = set(self.skills)
        for label, names in (
            ("lock", set(self.locked_skills)),
            ("adapters", set(self.adapters)),
            ("stack", set(self.stack_skills)),
        ):
            if names != skill_names:
                issues.append(f"{label} skill set differs from harness skill set")

        if set(self.profiles) != set(self.routes):
            issues.append("profile and route sets differ")
        if self.default_profile not in self.profiles:
            issues.append(f"unknown default profile: {self.default_profile}")
        if self.stack_apply:
            issues.append("AAS apply must remain disabled")
        return issues

    def _validate_locked_skills(self) -> list[str]:
        issues: list[str] = []
        for name, locked in self.locked_skills.items():
            if REVISION_PATTERN.fullmatch(locked.revision) is None:
                issues.append(f"{name} revision is not a full commit SHA")
            adapter = self.adapters.get(name)
            if adapter is None:
                continue
            if adapter.executable:
                issues.append(f"{name} external adapter must remain reference-only")
            if adapter.role != self.skills[name].role:
                issues.append(f"{name} adapter role differs from harness role")
            if locked.redistribution not in {"allowed", adapter.license_gate}:
                issues.append(f"{name} missing license gate")
        return issues

    def _validate_routes(self) -> list[str]:
        issues: list[str] = []
        known_skills = set(self.skills)
        for route in self.routes.values():
            selected = {*route.context, *route.workflow, *route.guards, route.output}
            unknown = selected - known_skills
            if unknown:
                issues.append(f"{route.name} references unknown skills: {', '.join(sorted(unknown))}")
        for name, adapter in self.adapters.items():
            if adapter.transport not in {"native", "cli", "mcp", "policy"}:
                issues.append(f"{name} has unknown transport: {adapter.transport}")
        return issues

    def get_skill_definition(self, skill_id: str) -> SkillDefinition:
        """Return the typed skill definition backed by adapter metadata."""
        from tools.agent_harness.dto import SkillDefinition, SkillTransport
        from tools.agent_harness.exceptions import UnknownSkillError

        if skill_id not in self.skills:
            msg = f"Unknown skill: {skill_id}"
            raise UnknownSkillError(msg)
        spec = self.skills[skill_id]
        adapter = self.adapters[skill_id]
        locked = self.locked_skills[skill_id]
        try:
            transport = SkillTransport(adapter.transport)
        except ValueError:
            transport = SkillTransport.NATIVE
        return SkillDefinition(
            skill_id=skill_id,
            role=spec.role,
            mode=spec.mode,
            transport=transport,
            capabilities=frozenset(adapter.capabilities),
            executable=adapter.source if transport == SkillTransport.CLI else None,
            network_required=skill_id == "last30days",
            write_required=False,
            source_repo=locked.repository,
            source_ref=locked.revision,
        )

    def list_enabled_definitions(self) -> list[SkillDefinition]:
        """Return typed definitions for all registered skills."""
        return [self.get_skill_definition(name) for name in sorted(self.skills)]

    def resolve_many(self, skill_ids: list[str]) -> list[SkillDefinition]:
        """Resolve skill identifiers to typed definitions in order."""
        return [self.get_skill_definition(skill_id) for skill_id in skill_ids]

    def find_by_capability(self, capability: str) -> list[SkillDefinition]:
        """Return skills exposing one capability string."""
        return [definition for definition in self.list_enabled_definitions() if capability in definition.capabilities]

    def skill_health(self, skill_id: str) -> SkillHealth:
        """Report availability and lock status for one skill."""
        import shutil

        from tools.agent_harness.dto import SkillHealth
        from tools.agent_harness.exceptions import UnknownSkillError

        if skill_id not in self.skills:
            msg = f"Unknown skill: {skill_id}"
            raise UnknownSkillError(msg)
        definition = self.get_skill_definition(skill_id)
        problems: list[str] = []
        locked = self.locked_skills[skill_id]
        version_matches = REVISION_PATTERN.fullmatch(locked.revision) is not None
        if not version_matches:
            problems.append("revision is not a full commit SHA")
        executable_found: bool | None = None
        if definition.transport.value == "cli" and definition.executable:
            executable_found = shutil.which(definition.executable) is not None
        return SkillHealth(
            skill_id=skill_id,
            available=not problems,
            version_matches=version_matches,
            executable_found=executable_found,
            problems=tuple(problems),
        )

    def validate_lock(self, skill_id: str) -> SkillLockResult:
        """Report the lock-pin status of one skill."""
        from tools.agent_harness.dto import SkillLockResult
        from tools.agent_harness.exceptions import UnknownSkillError

        if skill_id not in self.locked_skills:
            msg = f"Unknown skill: {skill_id}"
            raise UnknownSkillError(msg)
        locked = self.locked_skills[skill_id]
        pinned = REVISION_PATTERN.fullmatch(locked.revision) is not None
        problems = () if pinned else ("revision is not a full commit SHA",)
        return SkillLockResult(
            skill_id=skill_id,
            pinned=pinned,
            revision=locked.revision,
            problems=problems,
        )

    def typed_doctor(self) -> RegistryHealthReport:
        """Build a typed health report across all registered skills."""
        from tools.agent_harness.dto import RegistryHealthReport

        skills = [self.skill_health(name) for name in sorted(self.skills)]
        healthy = all(skill.available for skill in skills)
        return RegistryHealthReport(healthy=healthy, skills=skills, conflicts=[])


__all__ = [
    "AdapterSpec",
    "HarnessRegistry",
    "LockedSkill",
    "ProfileSpec",
    "RouteSpec",
    "SkillSpec",
    "load_yaml_mapping",
    "project_root",
]
