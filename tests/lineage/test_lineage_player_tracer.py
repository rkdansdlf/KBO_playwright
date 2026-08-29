"""Tests for PlayerMetricTracer and Derivation Lineage."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from src.lineage.tracers.player_tracer import PlayerMetricTracer


def test_player_metric_tracer_in_memory_db() -> None:
    """Test PlayerMetricTracer aggregates contributing game hits and builds provenance graph."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE player_basic (
                player_id INT PRIMARY KEY,
                name TEXT
            );
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                season_id INT,
                game_date TEXT,
                home_team TEXT,
                away_team TEXT
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
                hits INT,
                home_runs INT,
                at_bats INT,
                runs INT,
                rbi INT,
                walks INT,
                strikeouts INT
            );
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_batting (
                player_id INT,
                season INT,
                hits INT,
                home_runs INT,
                at_bats INT,
                PRIMARY KEY (player_id, season)
            );
        """)
        )
        conn.execute(
            text("""
            INSERT INTO player_basic (player_id, name) VALUES (52622, '김도영');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team)
            VALUES ('20240401HTLG0', 2024, '2024-04-01', 'LG', 'HT'),
                   ('20240402HTLG0', 2024, '2024-04-02', 'LG', 'HT');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (game_id, player_id, player_name, hits, home_runs, at_bats)
            VALUES ('20240401HTLG0', 52622, '김도영', 2, 1, 4),
                   ('20240402HTLG0', 52622, '김도영', 3, 0, 5);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_batting (player_id, season, hits, home_runs, at_bats)
            VALUES (52622, 2024, 5, 1, 9);
        """)
        )

    tracer = PlayerMetricTracer(engine)
    report = tracer.trace("김도영", season=2024, metric="hits")

    assert report.player_id == 52622
    assert report.player_name == "김도영"
    assert report.season == 2024
    assert report.metric_name == "hits"
    assert report.metric_value == 5
    assert report.player_appeared_games == 2
    assert report.observed_contributing_rows == 2
    assert report.lineage_coverage == 1.0
    assert report.team_scheduled_games == 144
    assert len(report.contributing_rows_sample) == 2
    assert "SUM(game_batting_stats.hits)" in report.formula

    # Verify DAG
    graph = report.graph
    assert "metric:52622:2024:hits" in graph.nodes
    assert "aggregator:2024:52622" in graph.nodes
    assert "game_rows:52622:2024" in graph.nodes
    assert len(graph.edges) >= 3
