"""Test validator exports from src/validators/__init__.py."""

from __future__ import annotations

import src.validators as v


def test_validators_package_exports() -> None:
    """Verify that all core validators and data structures are exported."""
    expected_symbols = [
        "QualityGate",
        "QualityRegressionReport",
        "QualityRegressionResult",
        "run_regression_pack",
        "validate_standings_integrity",
        "PBPStateMachineReport",
        "validate_pbp_state_machine",
        "reconcile_pbp_with_boxscore",
        "validate_game_data",
        "StatValidator",
        "ValidationResult",
        "ValidationSeverity",
        "create_default_stat_validator",
    ]
    for sym in expected_symbols:
        assert hasattr(v, sym), f"src.validators missing expected export '{sym}'"
        assert sym in v.__all__, f"src.validators.__all__ missing '{sym}'"
