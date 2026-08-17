"""Backward-compatibility shim for audit_completeness_2009_2025.

Delegates directly to scripts.maintenance.audit_completeness.
"""

from __future__ import annotations

from scripts.maintenance.audit_completeness import (  # noqa: F401
    DEFAULT_END_YEAR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_START_YEAR,
    MISSING_PARENT_TOLERANCE,
    PBP_LIMITATION_COVERAGE_THRESHOLD,
    PBP_TABLES,
    QUALITY_CATEGORIES,
    REMEDIATION_COMMANDS,
    TEAM_CODE_NULL_ALERT_RATE,
    _count_by_dimension,
    _execute,
    _expected_games_per_team,
    _remediation_for,
    check_missing_parent_games,
    check_player_game_vs_lineup,
    check_season_aggregates,
    check_team_code_null_rate,
    main,
    render_markdown,
    run_completeness_audit,
    run_coverage_audit,
    run_quality_gate_audit,
    run_regression_audit,
)

if __name__ == "__main__":
    raise SystemExit(main())
