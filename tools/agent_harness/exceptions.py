"""Typed errors for the agent Harness.

Only conditions that abort a command are modelled here: invalid configuration, an unknown
skill, a denied permission, and a broken evidence contract. Skill health problems, routing
mistakes, and gate failures are *reported* through typed results and exit codes rather
than raised, so this module deliberately has no error class for them.
"""

from __future__ import annotations


class HarnessError(Exception):
    """Base error for Harness failures."""


class HarnessConfigError(HarnessError):
    """Raised when Harness configuration is invalid."""


class UnknownSkillError(HarnessError):
    """Raised when a skill identifier is not registered."""


class PermissionDeniedError(HarnessError):
    """Raised when a permission boundary denies an action."""


class ArtifactContractError(HarnessError):
    """Raised when a Harness evidence bundle violates its artifact contract."""


__all__ = [
    "ArtifactContractError",
    "HarnessConfigError",
    "HarnessError",
    "PermissionDeniedError",
    "UnknownSkillError",
]
