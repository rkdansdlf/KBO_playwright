from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.cli import quality_gate_check
from src.validators.quality_gate import run_quality_gate


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE kbo_seasons (
                    season_id INTEGER PRIMARY KEY,
                    season_year INTEGER NOT NULL,
                    league_type_code INTEGER NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY,
                    game_status TEXT,
                    season_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    game_id TEXT,
                    player_id INTEGER,
                    plate_appearances INTEGER,
                    hits INTEGER,
                    runs INTEGER,
                    home_runs INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_batting (
                    player_id INTEGER,
                    season INTEGER,
                    league TEXT,
                    plate_appearances INTEGER,
                    hits INTEGER,
                    runs INTEGER,
                    home_runs INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_pitching_stats (
                    game_id TEXT,
                    player_id INTEGER,
                    innings_outs INTEGER,
                    wins INTEGER,
                    strikeouts INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_pitching (
                    player_id INTEGER,
                    season INTEGER,
                    league TEXT,
                    innings_outs INTEGER,
                    innings_pitched FLOAT,
                    extra_stats JSON,
                    wins INTEGER,
                    strikeouts INTEGER
                )
                """
            )
        )

    return sessionmaker(bind=engine)()


def _insert_regular_season(session):
    session.execute(
        text(
            """
            INSERT INTO kbo_seasons (season_id, season_year, league_type_code)
            VALUES (202501, 2025, 0)
            """
        )
    )
    session.commit()


def test_statistical_quality_gate_passes_when_transactional_totals_are_within_cumulative():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id)
                VALUES ('G1', 'COMPLETED', 202501), ('G2', 'SCHEDULED', 202501)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, plate_appearances, hits, runs, home_runs)
                VALUES ('G1', 10, 4, 2, 1, 0), ('G2', 10, 99, 99, 99, 99)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, league, plate_appearances, hits, runs, home_runs)
                VALUES (10, 2025, 'REGULAR', 5, 2, 1, 0)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO game_pitching_stats
                    (game_id, player_id, innings_outs, wins, strikeouts)
                VALUES ('G1', 20, 6, 1, 4), ('G2', 20, 99, 9, 9)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_pitching
                    (player_id, season, league, innings_outs, innings_pitched, extra_stats, wins, strikeouts)
                VALUES (20, 2025, 'REGULAR', 7, NULL, NULL, 1, 4)
                """
            )
        )
        session.commit()

        result = run_quality_gate(session, 2025)

        assert result["ok"] is True
        assert result["batting"]["checked_players"] == 1
        assert result["pitching"]["checked_players"] == 1
    finally:
        session.close()


def test_statistical_quality_gate_reports_missing_cumulative_records():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id)
                VALUES ('G1', 'COMPLETED', 202501)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, plate_appearances, hits, runs, home_runs)
                VALUES ('G1', 99, 4, 2, 1, 0)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO game_pitching_stats
                    (game_id, player_id, innings_outs, wins, strikeouts)
                VALUES ('G1', 88, 6, 1, 4)
                """
            )
        )
        session.commit()

        result = run_quality_gate(session, 2025)

        assert result["ok"] is False
        assert result["batting"]["mismatches"][0]["issue"] == "Missing cumulative record"
        assert result["pitching"]["mismatches"][0]["issue"] == "Missing cumulative record"
    finally:
        session.close()


def test_statistical_quality_gate_returns_cli_safe_shape_when_regular_season_missing():
    session = _make_session()
    try:
        result = run_quality_gate(session, 2025)

        assert result["ok"] is False
        assert result["batting"]["checked_players"] == 0
        assert result["batting"]["mismatches"] == []
        assert "No Regular Season IDs" in result["batting"]["error"]
    finally:
        session.close()


def test_quality_gate_cli_prints_failed_error_results(monkeypatch, capsys):
    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    failed_category = {
        "season": 2025,
        "league": "REGULAR",
        "checked_players": 0,
        "mismatches": [],
        "ok": False,
        "error": "No Regular Season IDs found for 2025",
    }

    monkeypatch.setattr(quality_gate_check, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(
        quality_gate_check,
        "run_quality_gate",
        lambda _session, _year: {
            "batting": dict(failed_category),
            "pitching": dict(failed_category),
            "ok": False,
        },
    )

    exit_code = quality_gate_check.main(["--year", "2025"])

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "Statistical Quality Gate for 2025" in out
    assert "No Regular Season IDs found for 2025" in out
    assert "Overall Status: FAILURE" in out
