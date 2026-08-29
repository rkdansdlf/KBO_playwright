"""Tests for GameLineageTracer and Game Provenance Graph Construction."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from src.lineage.tracers.game_tracer import GameLineageTracer


def test_game_lineage_tracer_in_memory_db() -> None:
    """Test GameLineageTracer on in-memory SQLite schema."""
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
        conn.execute(
            text("""
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT,
                player_id INT,
                player_name TEXT,
                team_side TEXT,
                runs INT,
                hits INT
            );
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE game_pitching_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT,
                player_id INT,
                player_name TEXT,
                team_side TEXT,
                innings_pitched TEXT,
                earned_runs INT
            );
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status, stadium)
            VALUES ('20240401LGNC0', 2024, '2024-04-01', 'NC', 'LG', 3, 5, 'COMPLETED', 'Changwon');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (game_id, player_id, player_name, team_side, runs, hits)
            VALUES ('20240401LGNC0', 101, 'Batter1', 'home', 1, 2),
                   ('20240401LGNC0', 102, 'Batter2', 'away', 2, 3);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (game_id, player_id, player_name, team_side, innings_pitched, earned_runs)
            VALUES ('20240401LGNC0', 201, 'Pitcher1', 'home', '6.0', 3);
        """)
        )

    tracer = GameLineageTracer(engine)
    report = tracer.trace("20240401LGNC0")

    assert report.game_id == "20240401LGNC0"
    assert report.home_team == "NC"
    assert report.away_team == "LG"
    assert report.home_score == 3
    assert report.away_score == 5
    assert report.stored_tables["game_batting_stats"] == 2
    assert report.stored_tables["game_pitching_stats"] == 1

    # Verify DAG nodes and edges
    graph = report.graph
    assert len(graph.nodes) >= 5
    assert "source:20240401LGNC0" in graph.nodes
    assert "crawl_run:20240401LGNC0" in graph.nodes
    assert "parser:20240401LGNC0" in graph.nodes
    assert "game:20240401LGNC0" in graph.nodes
    assert "batting_stats:20240401LGNC0" in graph.nodes
    assert len(graph.edges) >= 4


def test_game_lineage_tracer_shutout_remediation() -> None:
    """Test GameLineageTracer identifies shutout correction for 20210523LTOB0."""
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
        conn.execute(
            text("""
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT,
                player_id INT,
                player_name TEXT,
                team_side TEXT,
                runs INT,
                hits INT
            );
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE game_pitching_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT,
                player_id INT,
                player_name TEXT,
                team_side TEXT,
                innings_pitched TEXT,
                earned_runs INT
            );
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status)
            VALUES ('20210523LTOB0', 2021, '2021-05-23', 'DB', 'LT', 4, 0, 'COMPLETED');
        """)
        )

    tracer = GameLineageTracer(engine)
    report = tracer.trace("20210523LTOB0")

    assert len(report.corrections) == 1
    assert report.corrections[0].remediation_id == "REM-20210523LTOB0-ZERO-SCORE"
    assert report.corrections[0].corrected_value == 0
