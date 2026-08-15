"""Validation rules package for KBO stat validators."""

from __future__ import annotations

from src.validators.rules.batting_rules import ALL_BATTING_RULES
from src.validators.rules.pitching_rules import ALL_PITCHING_RULES
from src.validators.stat_validator import StatValidator


def create_default_stat_validator() -> StatValidator:
    """Instantiate and configure StatValidator with all standard batting and pitching rules."""
    validator = StatValidator()
    for rule in ALL_BATTING_RULES:
        validator.register_batting_rule(rule)
    for rule in ALL_PITCHING_RULES:
        validator.register_pitching_rule(rule)
    return validator


__all__ = ["ALL_BATTING_RULES", "ALL_PITCHING_RULES", "create_default_stat_validator"]
