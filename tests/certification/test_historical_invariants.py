"""Unit and integration tests for H01~H07 Historical Certification Invariant classes."""

from __future__ import annotations

from pathlib import Path
import tempfile

import pytest
from sqlalchemy import create_engine, text

from src.certification.context import CertificationContext
from src.certification.historical.invariants import (
    BattingInvariants,
    BoxscoreReconciliationInvariant,
    GameStateInvariant,
    PitchingInvariants,
    ReferentialIntegrityInvariant,
    ScheduleCoverageInvariant,
    SeasonTotalsReconciliationInvariant,
)


@pytest.fixture
def test_db_engine():
    """Create in-memory SQLite engine with KBO tables."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE game (
                game_id VARCHAR(50) PRIMARY KEY,
                season_id INT,
                game_date VARCHAR(10),
                home_team VARCHAR(10),
                away_team VARCHAR(10),
                home_score INT,
                away_score INT,
                winning_team VARCHAR(10),
                winning_score INT,
                game_status VARCHAR(20)
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE game_batting_stats (
                id INT PRIMARY KEY,
                game_id VARCHAR(50),
                player_id INT,
                player_name VARCHAR(50),
                team_side VARCHAR(10),
                team_code VARCHAR(10),
                plate_appearances INT,
                at_bats INT,
                runs INT,
                hits INT,
                doubles INT,
                triples INT,
                home_runs INT,
                walks INT,
                strikeouts INT
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE game_pitching_stats (
                id INT PRIMARY KEY,
                game_id VARCHAR(50),
                player_id INT,
                player_name VARCHAR(50),
                innings_pitched FLOAT,
                hits_allowed INT,
                runs_allowed INT,
                earned_runs INT,
                home_runs_allowed INT,
                walks_allowed INT,
                strikeouts INT
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_batting (
                player_id INT,
                season INT,
                hits INT,
                home_runs INT,
                PRIMARY KEY (player_id, season)
            )
        """)
        )
    return engine


def test_h01_schedule_coverage(test_db_engine) -> None:
    """Test H01 schedule coverage: detects home == away and unrecorded score."""
    with test_db_engine.begin() as conn:
        # Valid game
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20240401LGNC0', 2024, '2024-04-01', 'LG', 'NC', 5, 3, 'COMPLETED')
        """)
        )
        # Invalid game: home_team == away_team
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20240402LGLG0', 2024, '2024-04-02', 'LG', 'LG', 4, 2, 'COMPLETED')
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        inv = ScheduleCoverageInvariant()
        results = inv.evaluate_seasons(test_db_engine, [2024], context)

        assert len(results) == 1
        assert results[0].status == "FAIL"
        assert results[0].violation_count == 1


def test_h02_referential_integrity(test_db_engine) -> None:
    """Test H02 referential integrity: detects orphan batting/pitching stats rows."""
    with test_db_engine.begin() as conn:
        # Parent game
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20240403HHSS0', 2024, '2024-04-03', 'HH', 'SS', 3, 2, 'COMPLETED')
        """)
        )
        # Valid child stat
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, at_bats, hits, home_runs)
            VALUES (1, '20240403HHSS0', 101, '노시환', 4, 2, 1)
        """)
        )
        # Orphan child stat (Parent game missing)
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, at_bats, hits, home_runs)
            VALUES (2, '20240404HHSS0', 102, '문현빈', 3, 1, 0)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        inv = ReferentialIntegrityInvariant()
        results = inv.evaluate_seasons(test_db_engine, [2024], context)

        assert len(results) == 1
        assert results[0].status == "FAIL"
        assert results[0].violation_count == 1


def test_h04_batting_invariants(test_db_engine) -> None:
    """Test H04 batting invariants: impossible baseball math (Hits > AB, HR > Hits)."""
    with test_db_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20230501KTNC0', 2023, '2023-05-01', 'KT', 'NC', 6, 4, 'COMPLETED')
        """)
        )
        # Valid stat
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, plate_appearances, at_bats, runs, hits, doubles, triples, home_runs, walks, strikeouts)
            VALUES (11, '20230501KTNC0', 201, '박병호', 4, 4, 1, 2, 0, 0, 1, 0, 1)
        """)
        )
        # Invalid stat: Hits (5) > AB (4)
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, plate_appearances, at_bats, runs, hits, doubles, triples, home_runs, walks, strikeouts)
            VALUES (12, '20230501KTNC0', 202, '강백호', 4, 4, 1, 5, 0, 0, 0, 0, 0)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        inv = BattingInvariants()
        results = inv.evaluate_seasons(test_db_engine, [2023], context)

        assert len(results) == 1
        assert results[0].status == "FAIL"
        assert results[0].violation_count >= 1


def test_h05_pitching_invariants(test_db_engine) -> None:
    """Test H05 pitching invariants: impossible math (Earned Runs > Runs Allowed)."""
    with test_db_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20230502LGKIA0', 2023, '2023-05-02', 'LG', 'KIA', 2, 1, 'COMPLETED')
        """)
        )
        # Valid stat
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (id, game_id, player_id, player_name, innings_pitched, hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed, strikeouts)
            VALUES (21, '20230502LGKIA0', 301, '켈리', 6.0, 5, 1, 1, 0, 1, 7)
        """)
        )
        # Invalid stat: ER (4) > R (2)
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (id, game_id, player_id, player_name, innings_pitched, hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed, strikeouts)
            VALUES (22, '20230502LGKIA0', 302, '양현종', 5.0, 6, 2, 4, 1, 2, 4)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        inv = PitchingInvariants()
        results = inv.evaluate_seasons(test_db_engine, [2023], context)

        assert len(results) == 1
        assert results[0].status == "FAIL"
        assert results[0].violation_count >= 1


def test_h06_boxscore_reconciliation(test_db_engine) -> None:
    """Test H06 boxscore reconciliation: player runs sum must match team final score."""
    with test_db_engine.begin() as conn:
        # Use season 2019 (undeclared exception season) to test that mismatches trigger FAIL
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20190801SSLT0', 2019, '2019-08-01', 'SS', 'LT', 3, 2, 'COMPLETED')
        """)
        )
        # Home team player runs sum = 3 (Matches home_score 3)
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, team_side, runs)
            VALUES (41, '20190801SSLT0', 401, '구자욱', 'home', 1),
                   (42, '20190801SSLT0', 402, '피렐라', 'home', 2)
        """)
        )
        # Away team player runs sum = 1 (Mismatch with away_score 2)
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, team_side, runs)
            VALUES (43, '20190801SSLT0', 403, '전준우', 'away', 1)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        inv = BoxscoreReconciliationInvariant()
        results = inv.evaluate_seasons(test_db_engine, [2019], context)

        assert len(results) == 1
        assert results[0].status == "FAIL"
        assert results[0].violation_count == 1


def test_h07_season_totals_reconciliation(test_db_engine) -> None:
    """Test H07 season totals reconciliation: compares game totals with season totals."""
    with test_db_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20190901SKNC0', 2019, '2019-09-01', 'SK', 'NC', 4, 1, 'COMPLETED')
        """)
        )
        # Player game stats: 2 hits, 1 HR
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, hits, home_runs)
            VALUES (51, '20190901SKNC0', 501, '최정', 2, 1)
        """)
        )
        # Official season total has 2 hits, 1 HR
        conn.execute(
            text("""
            INSERT INTO player_season_batting (player_id, season, hits, home_runs)
            VALUES (501, 2019, 2, 1)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        inv = SeasonTotalsReconciliationInvariant()
        results = inv.evaluate_seasons(test_db_engine, [2019], context)

        assert len(results) == 1
        assert results[0].status in {"PASS", "NOT_COMPARABLE"}
        assert results[0].violation_count == 0
