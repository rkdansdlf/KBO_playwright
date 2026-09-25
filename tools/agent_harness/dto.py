"""Typed data contracts for the agent Harness.

This module is additive: existing ``registry``/``router`` dataclasses keep
their current shape so P0 baseline tests stay green. New code should build
on these typed contracts instead of raw dict/YAML structures.

It also hosts ``gate_verdict``, the single pass/fail rule for verification attempts.
The rule is shared by the profile-mode runner, the level-mode runner, and the evidence
contract checker, which previously disagreed on whether running zero gates passed.
Putting it here avoids a cycle: this module has no internal imports, while every other
candidate is imported by at least one of those three call sites.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


def gate_verdict(*, declared_gate_ids: Iterable[str], exit_codes: Iterable[int]) -> bool:
    """Return whether a verification attempt may be reported as passed.

    Passing requires every declared gate to have run and exited zero. A policy that
    deliberately declares no gates (research routes run none) passes, but a policy that
    declared gates and ran fewer does not, so a skipped or misconfigured gate can never
    look green.

    Both the runner and the evidence contract checker call this, which is what keeps
    ``verify`` exit codes and ``validate`` from disagreeing.
    """
    declared = tuple(declared_gate_ids)
    observed = tuple(exit_codes)
    if len(observed) != len(declared):
        return False
    return all(code == 0 for code in observed)


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


@dataclass(frozen=True)
class GoldenRoutingExpectation:
    """Describe the stable route output for one golden request."""

    profile: str
    skills: tuple[str, ...]
    verification: str
    reason: str


@dataclass(frozen=True)
class GoldenRoutingCase:
    """Bind one representative request to its expected route output."""

    case_id: str
    request: TaskRequest
    expected: GoldenRoutingExpectation


@dataclass(frozen=True)
class GoldenRoutingDataset:
    """Describe a versioned collection of routing regression cases."""

    schema_version: str
    external_execution: str
    cases: tuple[GoldenRoutingCase, ...]


@dataclass(frozen=True)
class GoldenTaskPermission:
    """Describe the expected permission probe for one operational task."""

    action: str
    decision: str
    target: str | None = None
    skill_id: str | None = None
    argv: tuple[str, ...] = ()
    reason_contains: str | None = None


@dataclass(frozen=True)
class GoldenTaskExpectation:
    """Describe route, permission, and verification expectations for one task."""

    profile: str
    skills: tuple[str, ...]
    verification: str
    level: str
    checks: tuple[str, ...]
    permission: GoldenTaskPermission


@dataclass(frozen=True)
class GoldenTask:
    """Bind one real KBO task to its operational replay expectations."""

    task_id: str
    title: str
    category: str
    risk: str
    round: int
    request: TaskRequest
    expected: GoldenTaskExpectation


@dataclass(frozen=True)
class GoldenTaskDeviation:
    """Record an intentional current-routing deviation for a task."""

    task_id: str
    reason: str
    actual_profile: str
    actual_skills: tuple[str, ...]
    actual_verification: str
    allow_permission_mismatch: bool = False


@dataclass(frozen=True)
class GoldenTaskDataset:
    """Describe a versioned operational task replay dataset."""

    schema_version: str
    tasks: tuple[GoldenTask, ...]
    known_deviations: tuple[GoldenTaskDeviation, ...]


__all__ = [
    "EVIDENCE_SCHEMA_VERSION",
    "GoldenRoutingCase",
    "GoldenRoutingDataset",
    "GoldenRoutingExpectation",
    "GoldenTask",
    "GoldenTaskDataset",
    "GoldenTaskDeviation",
    "GoldenTaskExpectation",
    "GoldenTaskPermission",
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
