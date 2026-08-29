"""Tests for Lineage Fault Injection and Broken Graph Handling."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from src.lineage.engine import LineageEngine
from src.lineage.tracers.game_tracer import GameLineageTracer
from src.lineage.tracers.player_tracer import PlayerMetricTracer


def test_game_tracer_missing_game_raises_error() -> None:
    """Test GameLineageTracer raises ValueError on non-existent game ID."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                season_id INT,
                game_date TEXT,
                home_team TEXT,
                away_team TEXT,
                home_score INT,
                away_score INT,
                winning_team TEXT,
                winning_score INT,
                game_status TEXT,
                stadium TEXT
            );
        """)
        )

    tracer = GameLineageTracer(engine)
    with pytest.raises(ValueError, match="Game '99999999NONEXISTENT' not found"):
        tracer.trace("99999999NONEXISTENT")


def test_player_tracer_missing_player_raises_error() -> None:
    """Test PlayerMetricTracer raises ValueError on non-existent player."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE player_basic (player_id INT PRIMARY KEY, name TEXT);"))
        conn.execute(text("CREATE TABLE game_batting_stats (id INT PRIMARY KEY, player_id INT, player_name TEXT);"))

    tracer = PlayerMetricTracer(engine)
    with pytest.raises(ValueError, match="Player '알수없는외계인선수' not found"):
        tracer.trace("알수없는외계인선수", season=2024)


def test_audit_lineage_detects_broken_game_id() -> None:
    """Test audit_lineage flags game records with malformed IDs."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                game_status TEXT
            );
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY,
                game_id TEXT
            );
        """)
        )
        conn.execute(text("INSERT INTO game VALUES ('INVALID_SHORT_ID', 'COMPLETED');"))

    engine_inst = LineageEngine(engine)
    report = engine_inst.audit_lineage(full=True)
    assert report.total_population >= 1
    assert report.broken_lineage_count == 1
    assert report.is_compliant is False
    assert report.table_breakdowns["game"].broken_rows == 1
