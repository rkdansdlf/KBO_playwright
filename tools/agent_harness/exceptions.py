"""Typed errors for the agent Harness."""

from __future__ import annotations


class HarnessError(Exception):
    """Base error for Harness failures."""


class HarnessConfigError(HarnessError):
    """Raised when Harness configuration is invalid."""


class UnknownSkillError(HarnessError):
    """Raised when a skill identifier is not registered."""


class SkillUnavailableError(HarnessError):
    """Raised when a skill is disabled or fails its health check."""


class RouteValidationError(HarnessError):
    """Raised when a routing decision fails validation."""


class PermissionDeniedError(HarnessError):
    """Raised when a permission boundary denies an action."""


class ArtifactContractError(HarnessError):
    """Raised when a Harness evidence bundle violates its artifact contract."""


class VerificationFailedError(HarnessError):
    """Raised when project verification fails for a Harness run."""


__all__ = [
    "ArtifactContractError",
    "HarnessConfigError",
    "HarnessError",
    "PermissionDeniedError",
    "RouteValidationError",
    "SkillUnavailableError",
    "UnknownSkillError",
    "VerificationFailedError",
]
