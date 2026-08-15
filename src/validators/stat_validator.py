"""Core Stat Validation Framework for KBO Data Pipeline.

Defines severity levels, validation results, and the central StatValidator engine.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class ValidationSeverity(StrEnum):
    """Validation issue severity."""

    ERROR = "ERROR"  # Blocks persistence or marks record as invalid
    WARNING = "WARNING"  # Persisted, but logs audit warning
    INFO = "INFO"  # Informational audit record


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Structured result of a single validation rule check."""

    validator: str
    rule_id: str
    entity_type: str  # 'batting', 'pitching', 'game'
    field_name: str
    expected: Any
    actual: Any
    severity: ValidationSeverity
    source: str = "kbo_official"
    entity_id: str | int | None = None
    game_id: str | None = None
    message: str = ""
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_blocking(self) -> bool:
        """Return True if this validation failure must block saving."""
        return self.severity == ValidationSeverity.ERROR


# Type signature for validation rule check functions
# Signature: (data: Mapping[str, Any], context: dict[str, Any] | None) -> list[ValidationResult]
RuleFunction = Callable[[dict[str, Any], dict[str, Any] | None], list[ValidationResult]]


class StatValidator:
    """Central statistical validator executing rules for batting, pitching, and games."""

    def __init__(self) -> None:
        """Initialize empty rule collections for batting, pitching, and games."""
        self._batting_rules: list[RuleFunction] = []
        self._pitching_rules: list[RuleFunction] = []
        self._game_rules: list[RuleFunction] = []

    def register_batting_rule(self, rule_fn: RuleFunction) -> None:
        """Register a validation rule for batting statistics."""
        self._batting_rules.append(rule_fn)

    def register_pitching_rule(self, rule_fn: RuleFunction) -> None:
        """Register a validation rule for pitching statistics."""
        self._pitching_rules.append(rule_fn)

    def register_game_rule(self, rule_fn: RuleFunction) -> None:
        """Register a validation rule for game-level statistics."""
        self._game_rules.append(rule_fn)

    def validate_batting(
        self,
        record: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResult]:
        """Run all registered batting validation rules against a single batting record."""
        results: list[ValidationResult] = []
        for rule in self._batting_rules:
            results.extend(rule(record, context))
        return results

    def validate_pitching(
        self,
        record: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResult]:
        """Run all registered pitching validation rules against a single pitching record."""
        results: list[ValidationResult] = []
        for rule in self._pitching_rules:
            results.extend(rule(record, context))
        return results

    def validate_game(
        self,
        game_payload: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResult]:
        """Run all registered game validation rules against a parsed game payload."""
        results: list[ValidationResult] = []
        for rule in self._game_rules:
            results.extend(rule(game_payload, context))
        return results


__all__ = ["StatValidator", "ValidationResult", "ValidationSeverity"]
