"""Typed data contracts for the agent Harness.

This module is additive: existing ``registry``/``router`` dataclasses keep
their current shape so P0 baseline tests stay green. New code should build
on these typed contracts instead of raw dict/YAML structures.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum


class SkillTransport(StrEnum):
    """Describe how a skill is executed by the host or Harness."""

    NATIVE = "native"
    CLI = "cli"
    MCP = "mcp"
    POLICY = "policy"


class VerificationLevel(StrEnum):
    """Describe the depth of project verification for a route."""

    NONE = "none"
    QUICK = "quick"
    STANDARD = "standard"
    FULL = "full"


class PermissionDecision(StrEnum):
    """Describe the outcome of one permission check."""

    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


EVIDENCE_SCHEMA_VERSION = "2"


@dataclass(frozen=True)
class TaskRequest:
    """Describe one Harness routing request with file signals."""

    prompt: str
    changed_files: list[str] = field(default_factory=list)
    explicit_profile: str | None = None
    explicit_skills: list[str] = field(default_factory=list)
    dry_run: bool = True


@dataclass(frozen=True)
class SkillDefinition:
    """Describe one skill with its transport and capability contract."""

    skill_id: str
    role: str
    mode: str
    transport: SkillTransport
    capabilities: frozenset[str]
    executable: str | None = None
    network_required: bool = False
    write_required: bool = False
    source_repo: str | None = None
    source_ref: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize the skill definition to JSON-compatible values."""
        payload = asdict(self)
        payload["transport"] = self.transport.value
        payload["capabilities"] = sorted(self.capabilities)
        return payload


@dataclass(frozen=True)
class PermissionResult:
    """Describe the outcome of one permission check."""

    decision: PermissionDecision
    reason: str
    matched_rule: str | None = None

    @property
    def allowed(self) -> bool:
        """Return whether the checked action may proceed."""
        return self.decision == PermissionDecision.ALLOW


@dataclass(frozen=True)
class SkillHealth:
    """Describe the availability of one skill."""

    skill_id: str
    available: bool
    version_matches: bool
    executable_found: bool | None
    problems: tuple[str, ...]


@dataclass(frozen=True)
class SkillConflict:
    """Describe one conflicting skill pair."""

    left: str
    right: str
    reason: str


@dataclass(frozen=True)
class SkillLockResult:
    """Describe the lock-pin status of one skill."""

    skill_id: str
    pinned: bool
    revision: str
    problems: tuple[str, ...]


@dataclass
class RegistryHealthReport:
    """Summarize the health of all registered skills."""

    healthy: bool
    skills: list[SkillHealth] = field(default_factory=list)
    conflicts: list[SkillConflict] = field(default_factory=list)


__all__ = [
    "EVIDENCE_SCHEMA_VERSION",
    "PermissionDecision",
    "PermissionResult",
    "RegistryHealthReport",
    "SkillConflict",
    "SkillDefinition",
    "SkillHealth",
    "SkillLockResult",
    "SkillTransport",
    "TaskRequest",
    "VerificationLevel",
]
