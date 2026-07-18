from __future__ import annotations

import json

from sqlalchemy import create_engine, text

from src.cli.historical_coverage_report import (
    build_historical_coverage_report,
    main,
    render_historical_coverage_report,
)


COVERAGE_TABLES = (
    "game_lineups",
    "game_batting_stats",
    "game_pitching_stats",
    "player_game_batting",
    "player_game_pitching",
    "game_events",
    "game_play_by_play",
)


def _create_coverage_schema(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                game_date DATE NOT NULL,
                game_status TEXT,
                season_id INTEGER
            )
            """,
        ),
    )
    conn.execute(
        text(
            """
            CREATE TABLE kbo_seasons (
                season_id INTEGER PRIMARY KEY,
                season_year INTEGER NOT NULL,
                league_type_code INTEGER NOT NULL,
                league_type_name TEXT NOT NULL
            )
            """,
        ),
    )
    for table in COVERAGE_TABLES:
        conn.execute(text(f"CREATE TABLE {table} (game_id TEXT NOT NULL)"))


def _insert_fixture_data(conn) -> None:
    conn.execute(
        text(
            """
            INSERT INTO kbo_seasons (season_id, season_year, league_type_code, league_type_name)
            VALUES (115, 2001, 0, '정규시즌'), (116, 2001, 1, '시범경기')
            """,
        ),
    )
    conn.execute(
        text(
            """
            INSERT INTO game (game_id, game_date, game_status, season_id)
            VALUES
                ('20010405LGSS0', '2001-04-05', 'COMPLETED', 115),
                ('20010406LGSS0', '2001-04-06', 'DRAW', 115),
                ('20010320LGSS0', '2001-03-20', 'CANCELLED', 116)
            """,
        ),
    )
    for table in COVERAGE_TABLES:
        conn.execute(text(f"INSERT INTO {table} (game_id) VALUES ('20010405LGSS0')"))


def test_build_report_groups_series_and_lists_missing_game_ids() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_coverage_schema(conn)
        _insert_fixture_data(conn)

        report = build_historical_coverage_report(conn, start_year=2001, end_year=2001)

    year_report = report["years"][0]
    assert year_report["parent_games"] == 3
    assert year_report["terminal_games"] == 2
    assert year_report["coverage"]["game_batting_stats"] == {
        "covered_games": 1,
        "target_games": 2,
        "coverage_pct": 50.0,
    }
    assert year_report["missing_game_ids"]["game_events"] == ["20010406LGSS0"]
    assert {(item["series"], item["status"]) for item in year_report["series"]} == {
        ("정규시즌", "COMPLETED"),
        ("정규시즌", "DRAW"),
        ("시범경기", "CANCELLED"),
    }


def test_render_report_includes_coverage_summary() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_coverage_schema(conn)
        _insert_fixture_data(conn)
        report = build_historical_coverage_report(conn, start_year=2001, end_year=2001)

    rendered = render_historical_coverage_report(report)

    assert "Historical coverage report: 2001-2001" in rendered
    assert "batting=1/2 (50.0%)" in rendered
    assert "events=1/2 (50.0%)" in rendered


def test_cli_emits_json_and_writes_artifact(tmp_path, capsys) -> None:
    db_path = tmp_path / "coverage.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        _create_coverage_schema(conn)
        _insert_fixture_data(conn)

    output_path = tmp_path / "coverage.json"
    result = main(
        [
            "--start-year",
            "2001",
            "--end-year",
            "2001",
            "--database-url",
            f"sqlite:///{db_path}",
            "--json",
            "--output",
            str(output_path),
        ],
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["years"][0]["terminal_games"] == 2
    assert json.loads(output_path.read_text(encoding="utf-8"))["start_year"] == 2001
