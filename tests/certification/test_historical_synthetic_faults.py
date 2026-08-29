"""Synthetic Fault-Injection tests for Phase 102 Historical Data Certification Gate."""

from __future__ import annotations

from pathlib import Path
import tempfile
from unittest.mock import patch

from sqlalchemy import create_engine, text

from src.certification.context import CertificationContext
from src.certification.gates.historical import HistoricalCertificationGate
from src.certification.historical.runner import HistoricalCertificationRunner
from src.certification.models import GateStatus


def _create_base_schema(engine):
    """Bootstrap isolated database schema."""
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


def test_fault_injection_impossible_batting_hits_greater_than_ab() -> None:
    """Inject H > AB and verify historical runner flags blocking failure."""
    engine = create_engine("sqlite:///:memory:")
    _create_base_schema(engine)

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20240501LGHH0', 2024, '2024-05-01', 'LG', 'HH', 4, 3, 'COMPLETED')
        """)
        )
        # Fault: hits 5 on 4 AB
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, plate_appearances, at_bats, runs, hits, doubles, triples, home_runs)
            VALUES (1, '20240501LGHH0', 101, '오지환', 4, 4, 1, 5, 0, 0, 0)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        runner = HistoricalCertificationRunner()
        with patch.object(HistoricalCertificationRunner, "_resolve_engine", return_value=engine):
            report = runner.run_historical_audit(context, start_season=2024, end_season=2024)

            assert report.failed_seasons == 1
            assert report.total_violations >= 1
            assert report.overall_verdict == "NOT_CERTIFIED"
            assert report.seasons[0].layer_status["H04"] == "FAIL"


def test_fault_injection_impossible_pitching_er_greater_than_r() -> None:
    """Inject ER > R and verify historical runner flags blocking failure."""
    engine = create_engine("sqlite:///:memory:")
    _create_base_schema(engine)

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20240502SSNC0', 2024, '2024-05-02', 'SS', 'NC', 5, 2, 'COMPLETED')
        """)
        )
        # Fault: earned_runs (5) > runs_allowed (3)
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (id, game_id, player_id, player_name, innings_pitched, hits_allowed, runs_allowed, earned_runs, home_runs_allowed)
            VALUES (1, '20240502SSNC0', 201, '원태인', 6.0, 4, 3, 5, 1)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        runner = HistoricalCertificationRunner()
        with patch.object(HistoricalCertificationRunner, "_resolve_engine", return_value=engine):
            report = runner.run_historical_audit(context, start_season=2024, end_season=2024)

            assert report.failed_seasons == 1
            assert report.overall_verdict == "NOT_CERTIFIED"
            assert report.seasons[0].layer_status["H05"] == "FAIL"


def test_fault_injection_historical_certification_gate_blocks_release() -> None:
    """Verify that HistoricalCertificationGate produces GateStatus.FAIL on data contradictions."""
    engine = create_engine("sqlite:///:memory:")
    _create_base_schema(engine)

    with engine.begin() as conn:
        # Fault: completed game with NULL score
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20230501KIAOB0', 2023, '2023-05-01', 'KIA', 'OB', NULL, 3, 'COMPLETED')
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="production", artifact_dir=Path(tmpdir))
        gate = HistoricalCertificationGate()

        with patch.object(HistoricalCertificationRunner, "_resolve_engine", return_value=engine):
            result = gate.run(context)

            assert result.status == GateStatus.FAIL
            assert result.blocking is True
            assert result.metrics["failed_seasons"] >= 1
            assert "failed historical data certification" in result.message
