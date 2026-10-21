"""Tests for scripts.maintenance.audit_schedule_and_game_integrity."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from scripts.maintenance.audit_schedule_and_game_integrity import (
    AuditReport,
    MonthSummary,
    audit_monthly_counts_and_status,
    audit_team_code_mappings,
    audit_game_and_alias_duplicates,
    format_markdown_report,
)
from src.models.game import Game, GameIdAlias


def test_audit_monthly_counts_and_status_preserves_cancelled():
    mock_session = MagicMock()
    g1 = Game(
        game_id="20250401LGSS0",
        game_date=date(2025, 4, 1),
        game_status="COMPLETED",
        home_score=5,
        away_score=3,
    )
    g2 = Game(
        game_id="20250402LGSS0",
        game_date=date(2025, 4, 2),
        game_status="CANCELLED",
        home_score=None,
        away_score=None,
    )
    g3 = Game(
        game_id="20250403LGSS0",
        game_date=date(2025, 4, 3),
        game_status="CANCELLED",
        home_score=10,  # Invalid: cancelled game has completed score
        away_score=2,
    )
    mock_session.scalars.return_value.all.return_value = [g1, g2, g3]

    summaries, anomalies, cancelled_count, suspended_count, invalid_cancelled = audit_monthly_counts_and_status(
        mock_session, 2025
    )

    assert len(summaries) == 1
    assert summaries[0].year == 2025
    assert summaries[0].month == 4
    assert summaries[0].total_games == 3
    assert summaries[0].completed == 1
    assert summaries[0].cancelled == 2
    assert cancelled_count == 2
    assert suspended_count == 0
    assert invalid_cancelled == ["20250403LGSS0"]


def test_audit_team_code_mappings_detects_legacy_tokens():
    mock_session = MagicMock()
    g1 = Game(
        game_id="20260401LGSS0",
        game_date=date(2026, 4, 1),
        home_team="LG",
        away_team="SS",
    )
    g2 = Game(
        game_id="20260402OBSS0",
        game_date=date(2026, 4, 2),
        home_team="OB",  # Legacy technical token: in 2026 expected to be DB
        away_team="SS",
    )
    mock_session.scalars.return_value.all.return_value = [g1, g2]

    mismatches = audit_team_code_mappings(mock_session, 2026)
    assert len(mismatches) == 1
    assert mismatches[0]["game_id"] == "20260402OBSS0"
    assert mismatches[0]["stored"] == "OB"
    assert mismatches[0]["expected"] == "DB"


def test_audit_game_and_alias_duplicates():
    mock_session = MagicMock()
    # Duplicate fixtures: two games on the same date with the same teams and same DH suffix
    g1 = Game(game_id="20250401LGSS0", game_date=date(2025, 4, 1), home_team="LG", away_team="SS")
    g2 = Game(game_id="20250401LGSS0_ALT0", game_date=date(2025, 4, 1), home_team="LG", away_team="SS")

    alias1 = GameIdAlias(alias_game_id="alias_self", canonical_game_id="alias_self")
    alias2 = GameIdAlias(alias_game_id="alias_dangling", canonical_game_id="nonexistent_game")

    mock_session.scalars.side_effect = [
        MagicMock(all=MagicMock(return_value=[g1, g2])),
        MagicMock(all=MagicMock(return_value=[alias1, alias2])),
    ]

    duplicates, dangling, self_ref = audit_game_and_alias_duplicates(mock_session, 2025)
    assert len(duplicates) == 1
    assert "20250401LGSS0" in duplicates[0]["game_ids"]
    assert "20250401LGSS0_ALT0" in duplicates[0]["game_ids"]
    assert self_ref == ["alias_self"]


def test_format_markdown_report():
    report = AuditReport(
        target_year=2025,
        total_games_inspected=144,
        monthly_summaries=[
            {
                "year": 2025,
                "month": 4,
                "total_games": 144,
                "completed": 140,
                "scheduled": 0,
                "cancelled": 4,
                "suspended": 0,
                "postponed": 0,
                "other": 0,
            }
        ],
        cancelled_games_count=4,
        ok=True,
    )
    md = format_markdown_report(report)
    assert "# P2 Data Integrity & Cross-Validation Audit Report" in md
    assert "2025" in md
    assert "Total Games Inspected**: `144`" in md
    assert "PASSED" in md
