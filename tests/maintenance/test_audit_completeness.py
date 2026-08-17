"""Tests for the multi-layer audit_completeness module."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from scripts.maintenance.audit_completeness import (
    check_key_duplicates,
    check_player_game_vs_lineup,
)


class TestAuditCompletenessEnhancedChecks:
    def test_check_key_duplicates_detects_batting_duplicate(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE player_game_batting (game_id TEXT, player_id INTEGER)",
                ),
            )
            conn.execute(
                text(
                    "CREATE TABLE player_game_pitching (game_id TEXT, player_id INTEGER)",
                ),
            )
            # Insert duplicate batting row
            conn.execute(
                text(
                    "INSERT INTO player_game_batting VALUES ('20240501LGHH0', 101), ('20240501LGHH0', 101)",
                ),
            )
            findings = check_key_duplicates(conn, 2024, 2024)
            assert len(findings) == 1
            assert findings[0]["dimension"] == "duplicate_key:player_game_batting"
            assert findings[0]["count"] == 1

    def test_check_player_game_vs_lineup_ignores_pinch_runner(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE game_lineups (game_id TEXT, player_id INTEGER, standard_position TEXT, batting_order INTEGER, is_starter INTEGER)",
                ),
            )
            conn.execute(
                text(
                    "CREATE TABLE player_game_batting (game_id TEXT, player_id INTEGER)",
                ),
            )
            conn.execute(
                text(
                    "CREATE TABLE player_game_pitching (game_id TEXT, player_id INTEGER)",
                ),
            )

            # Insert starter (needs batting) and PR (does not need batting)
            conn.execute(
                text(
                    "INSERT INTO game_lineups VALUES "
                    "('20240501LGHH0', 101, 'CF', 1, 1), "
                    "('20240501LGHH0', 102, 'PR', 1, 0)",
                ),
            )
            # 101 is in batting, 102 is NOT in batting
            conn.execute(
                text(
                    "INSERT INTO player_game_batting VALUES ('20240501LGHH0', 101)",
                ),
            )

            findings = check_player_game_vs_lineup(conn, 2024, 2024)
            # Should have 0 missing since PR (102) is exempt!
            assert len(findings) == 0
