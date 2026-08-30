"""Tests for SQL Reference Oracle in Phase 105A Gate 2D."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from src.formulas.sql_reference import SqlReferenceOracle


@pytest.fixture()
def in_memory_sql_db():
    """Create in-memory SQLite database with batting, pitching, fielding tables."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE player_season_batting (
                season INTEGER,
                player_id INTEGER,
                team_code TEXT,
                league TEXT,
                level TEXT,
                at_bats INTEGER,
                hits INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                walks INTEGER,
                intentional_walks INTEGER,
                hbp INTEGER,
                strikeouts INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                stolen_bases INTEGER,
                caught_stealing INTEGER,
                gdp INTEGER,
                plate_appearances INTEGER
            );
            """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_pitching (
                season INTEGER,
                player_id INTEGER,
                team_code TEXT,
                league TEXT,
                level TEXT,
                innings_outs INTEGER,
                earned_runs INTEGER,
                runs_allowed INTEGER,
                hits_allowed INTEGER,
                walks_allowed INTEGER,
                hit_batters INTEGER,
                strikeouts INTEGER,
                home_runs_allowed INTEGER,
                tbf INTEGER,
                sacrifice_flies_allowed INTEGER
            );
            """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_fielding (
                year INTEGER,
                player_id INTEGER,
                team_id TEXT,
                position_id INTEGER,
                putouts INTEGER,
                assists INTEGER,
                errors INTEGER,
                innings REAL,
                games INTEGER
            );
            """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_batting (
                season, player_id, team_code, league, level,
                at_bats, hits, doubles, triples, home_runs,
                walks, intentional_walks, hbp, strikeouts, sacrifice_hits, sacrifice_flies,
                stolen_bases, caught_stealing, gdp, plate_appearances
            ) VALUES (
                2025, 101, 'LG', 'REGULAR', '1군',
                400, 120, 20, 2, 15,
                50, 2, 5, 80, 4, 6,
                10, 2, 8, 465
            );
            """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_pitching (
                season, player_id, team_code, league, level,
                innings_outs, earned_runs, runs_allowed, hits_allowed,
                walks_allowed, hit_batters, strikeouts, home_runs_allowed,
                tbf, sacrifice_flies_allowed
            ) VALUES (
                2025, 201, 'KIA', 'REGULAR', '1군',
                300, 30, 35, 80,
                25, 3, 90, 8,
                400, 2
            );
            """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_fielding (
                year, player_id, team_id, position_id,
                putouts, assists, errors, innings, games
            ) VALUES (
                2025, 101, 'LG', 4,
                200, 300, 10, 150.0, 120
            );
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

    def test_evaluate_fielding_sql(self, in_memory_sql_db) -> None:
        """Verify SQL reference fielding calculations."""
        results = SqlReferenceOracle.evaluate_fielding(in_memory_sql_db, season=2025)
        assert len(results) == 1
        r = results[0]
        assert r["player_id"] == 101
        # FPCT = (200 + 300) / (200 + 300 + 10) = 500 / 510 = 0.98039... -> 0.980
        assert r["sql_fpct"] == 0.980
