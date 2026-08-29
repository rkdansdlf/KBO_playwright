"""Unit tests for Phase 102 Historical Certification Framework (Manifest, Registry, Reporter, Runner)."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from unittest.mock import patch

from sqlalchemy import create_engine, text

from src.certification.context import CertificationContext
from src.certification.historical.exceptions import (
    DeclaredException,
    HistoricalExceptionRegistry,
)
from src.certification.historical.manifest import (
    CURRENT_ACTIVE_SEASON,
    SeasonManifestRegistry,
)
from src.certification.historical.models import (
    DataDisposition,
    SeasonStatus,
)
from src.certification.historical.reporter import HistoricalReporter
from src.certification.historical.runner import HistoricalCertificationRunner


def test_manifest_registry_all_45_seasons() -> None:
    """Verify that SeasonManifestRegistry defines contracts for all 45 seasons (1982~2026) with zero UNKNOWN."""
    manifests = SeasonManifestRegistry.list_all_seasons(1982, CURRENT_ACTIVE_SEASON)
    assert len(manifests) == 45

    # Check that UNKNOWN dispositions are zero across all 45 seasons
    unknown_pbp = sum(1 for m in manifests if m.pbp_disposition == DataDisposition.UNKNOWN)
    unknown_lineups = sum(1 for m in manifests if m.lineup_disposition == DataDisposition.UNKNOWN)
    assert unknown_pbp == 0
    assert unknown_lineups == 0

    # Check 1982 inaugural contract
    m_1982 = SeasonManifestRegistry.get_manifest(1982)
    assert m_1982.season == 1982
    assert m_1982.status == SeasonStatus.FINAL
    assert m_1982.pbp_disposition == DataDisposition.UNAVAILABLE
    assert m_1982.lineup_disposition == DataDisposition.CONDITIONAL
    assert m_1982.expected_games_min >= 200

    # Check 2008 digital era transition
    m_2008 = SeasonManifestRegistry.get_manifest(2008)
    assert m_2008.pbp_disposition == DataDisposition.UNAVAILABLE
    assert m_2008.lineup_disposition == DataDisposition.REQUIRED

    # Check 2015 modern 10-team era
    m_2015 = SeasonManifestRegistry.get_manifest(2015)
    assert m_2015.pbp_disposition == DataDisposition.REQUIRED
    assert m_2015.lineup_disposition == DataDisposition.REQUIRED

    # Check 2026 active season
    m_2026 = SeasonManifestRegistry.get_manifest(2026)
    assert m_2026.status == SeasonStatus.ACTIVE
    assert m_2026.pbp_disposition == DataDisposition.AS_OF_CUTOFF


def test_exception_registry_lookup() -> None:
    """Verify declared exceptions lookup, matching, and dynamic registration."""
    # Test registration of custom test exception
    test_exc = DeclaredException(
        exception_id="HIST-TEST-001",
        seasons=[1999],
        invariant_id="H01-TEST",
        disposition=DataDisposition.CONDITIONAL,
        reason="Test exception reason",
        evidence="Test source evidence",
    )
    HistoricalExceptionRegistry.register_exception(test_exc)

    exc = HistoricalExceptionRegistry.get_exception("H01-TEST", 1999)
    assert exc is not None
    assert isinstance(exc, DeclaredException)
    assert exc.disposition == DataDisposition.CONDITIONAL
    assert exc.exception_id == "HIST-TEST-001"

    # Non-existent exception returns None
    no_exc = HistoricalExceptionRegistry.get_exception("H04-BATTING-INVARIANTS", 2024)
    assert no_exc is None


def test_historical_reporter_ascii_and_json() -> None:
    """Verify historical scorecard ASCII matrix and JSON export formatting."""
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
                year INT,
                hits INT,
                home_runs INT,
                PRIMARY KEY (player_id, year)
            )
        """)
        )

        # Seed compliant records
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, winning_team, winning_score, game_status)
            VALUES ('19820327OBLG0', 1982, '1982-03-27', 'OB', 'LG', 5, 2, 'OB', 5, 'COMPLETED')
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (id, game_id, player_id, player_name, team_side, team_code, plate_appearances, at_bats, runs, hits, doubles, triples, home_runs, walks, strikeouts)
            VALUES (1, '19820327OBLG0', 10, '선수A', 'home', 'OB', 4, 4, 1, 2, 0, 0, 1, 0, 1)
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (id, game_id, player_id, player_name, innings_pitched, hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed, strikeouts)
            VALUES (1, '19820327OBLG0', 20, '투수A', 6.0, 3, 2, 2, 0, 1, 5)
        """)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        context = CertificationContext(target="local", artifact_dir=Path(tmpdir))
        runner = HistoricalCertificationRunner()

        with patch.object(HistoricalCertificationRunner, "_resolve_engine", return_value=engine):
            report = runner.run_historical_audit(context, start_season=1982, end_season=1983)

            assert report.total_seasons == 2
            assert report.start_season == 1982
            assert report.end_season == 1983
            assert report.overall_verdict in {"CERTIFIED", "CERTIFIED_WITH_EXCEPTIONS", "NOT_CERTIFIED"}

            # Test ASCII Matrix
            matrix = HistoricalReporter.render_ascii_matrix(report)
            assert "KBO HISTORICAL DATA CERTIFICATION MATRIX" in matrix
            assert "1982" in matrix
            assert "1983" in matrix

            # Test JSON Artifact Export
            out_json = Path(tmpdir) / "hist_report.json"
            saved = HistoricalReporter.save_json_report(report, out_json)
            assert saved.exists()

            with saved.open("r", encoding="utf-8") as f:
                data = json.load(f)
                assert data["schema_version"] == "1.0"
                assert data["contract"] == "historical-v1"
                assert len(data["seasons"]) == 2
