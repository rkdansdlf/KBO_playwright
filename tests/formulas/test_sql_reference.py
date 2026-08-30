"""Tests for SQL Reference Oracle in Phase 105A Gate 2C."""

from __future__ import annotations

from decimal import Decimal
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.formulas.sql_reference import SqlReferenceOracle


@pytest.fixture()
def in_memory_sql_db():
    """Create in-memory SQLite database with batting, pitching, fielding, baserunning tables."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE player_season_batting (
                season INTEGER,
                player_id INTEGER,
                team_id TEXT,
                at_bats INTEGER,
                hits INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                walks INTEGER,
                hit_by_pitch INTEGER,
                strike_outs INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER
            );
            """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_pitching (
                season INTEGER,
                player_id INTEGER,
                team_id TEXT,
                innings_pitched_outs INTEGER,
                earned_runs INTEGER,
                hits INTEGER,
                walks INTEGER,
                strike_outs INTEGER,
                home_runs INTEGER
            );
            """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_batting (
                season, player_id, team_id, at_bats, hits, doubles, triples, home_runs, walks, hit_by_pitch, strike_outs, sacrifice_hits, sacrifice_flies
            ) VALUES (2025, 101, 'LG', 400, 120, 20, 2, 15, 50, 5, 80, 4, 6);
            """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_pitching (
                season, player_id, team_id, innings_pitched_outs, earned_runs, hits, walks, strike_outs, home_runs
            ) VALUES (2025, 201, 'KIA', 300, 30, 80, 25, 90, 8);
            """)
        )
    yield engine
    engine.dispose()


class TestSqlReferenceOracle:
    def test_evaluate_batting_sql(self, in_memory_sql_db) -> None:
        """Verify SQL reference batting calculations."""
        results = SqlReferenceOracle.evaluate_batting(in_memory_sql_db, season=2025)
        assert len(results) == 1
        r = results[0]
        assert r["player_id"] == 101
        assert r["sql_avg"] == 0.300
        # OBP = (120+50+5) / (400+50+5+6) = 175 / 461 = 0.3796... -> 0.380
        assert r["sql_obp"] == 0.380
        # SLG = (120 + 20 + 4 + 45) / 400 = 189 / 400 = 0.4725... -> 0.473
        assert r["sql_slg"] == 0.472 or r["sql_slg"] == 0.473

    def test_evaluate_pitching_sql(self, in_memory_sql_db) -> None:
        """Verify SQL reference pitching calculations."""
        results = SqlReferenceOracle.evaluate_pitching(in_memory_sql_db, season=2025)
        assert len(results) == 1
        r = results[0]
        assert r["player_id"] == 201
        # ERA = (30 * 27) / 300 = 810 / 300 = 2.70
        assert r["sql_era"] == 2.70
        # WHIP = (25 + 80) * 3 / 300 = 315 / 300 = 1.05
        assert r["sql_whip"] == 1.05
