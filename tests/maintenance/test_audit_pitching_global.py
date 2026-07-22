from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.audit_pitching_global import audit_pitching_global


def _engine() -> object:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE kbo_seasons (season_id INTEGER PRIMARY KEY, season_year INTEGER)"))
        conn.execute(text("CREATE TABLE game (game_id TEXT PRIMARY KEY, season_id INTEGER)"))
        conn.execute(
            text(
                "CREATE TABLE player_game_pitching ("
                "game_id TEXT, player_id INTEGER, team_code TEXT, innings_outs INTEGER, "
                "hits_allowed INTEGER, runs_allowed INTEGER, earned_runs INTEGER, "
                "home_runs_allowed INTEGER, walks_allowed INTEGER, strikeouts INTEGER)"
            ),
        )
        conn.execute(
            text(
                "CREATE TABLE game_pitching_stats ("
                "game_id TEXT, player_id INTEGER, appearance_seq INTEGER, team_code TEXT, innings_outs INTEGER, "
                "hits_allowed INTEGER, runs_allowed INTEGER, earned_runs INTEGER, "
                "home_runs_allowed INTEGER, walks_allowed INTEGER, strikeouts INTEGER)"
            ),
        )
        conn.execute(
            text(
                "CREATE TABLE team_season_pitching ("
                "team_id TEXT, season INTEGER, league TEXT, innings_outs INTEGER, "
                "hits_allowed INTEGER, runs_allowed INTEGER, earned_runs INTEGER, "
                "home_runs_allowed INTEGER, walks_allowed INTEGER, strikeouts INTEGER)"
            ),
        )
    return engine


def _seed(engine: object, *, duplicate: bool = False) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO kbo_seasons VALUES (1, 2021)"))
        conn.execute(text("INSERT INTO game VALUES ('g1', 1), ('g2', 1)"))
        rows = [
            {
                "game_id": "g1",
                "player_id": 1,
                "team_code": "LG",
                "innings_outs": 3,
                "hits_allowed": 1,
                "runs_allowed": 1,
                "earned_runs": 1,
                "home_runs_allowed": 0,
                "walks_allowed": 0,
                "strikeouts": 2,
            },
            {
                "game_id": "g2",
                "player_id": 2,
                "team_code": "LG",
                "innings_outs": 6,
                "hits_allowed": 2,
                "runs_allowed": 1,
                "earned_runs": 1,
                "home_runs_allowed": 0,
                "walks_allowed": 1,
                "strikeouts": 4,
            },
        ]
        conn.execute(
            text(
                "INSERT INTO player_game_pitching VALUES (:game_id, :player_id, :team_code, :innings_outs, :hits_allowed, :runs_allowed, :earned_runs, :home_runs_allowed, :walks_allowed, :strikeouts)"
            ),
            rows,
        )
        if duplicate:
            conn.execute(
                text(
                    "INSERT INTO player_game_pitching VALUES (:game_id, :player_id, :team_code, :innings_outs, :hits_allowed, :runs_allowed, :earned_runs, :home_runs_allowed, :walks_allowed, :strikeouts)"
                ),
                rows[:1],
            )
        conn.execute(
            text(
                "INSERT INTO game_pitching_stats VALUES (:game_id, :player_id, 1, :team_code, :innings_outs, :hits_allowed, :runs_allowed, :earned_runs, :home_runs_allowed, :walks_allowed, :strikeouts)"
            ),
            rows,
        )
        conn.execute(text("INSERT INTO team_season_pitching VALUES ('LG', 2021, 'REGULAR', 9, 3, 2, 2, 0, 1, 6)"))


def test_audit_reconciles_game_and_team_sources() -> None:
    engine = _engine()
    _seed(engine)

    with engine.connect() as conn:
        report = audit_pitching_global(conn, 2021)

    assert report["classification"] == "reconciled"
    assert report["comparisons"]["player_game_vs_game_pitching"]["ok"] is True
    assert report["comparisons"]["player_game_vs_team_season"]["ok"] is True
    assert report["duplicates"] == {
        "player_game_same_game_player": 0,
        "game_pitching_same_game_player_appearance": 0,
    }


def test_audit_identifies_game_level_mismatch_and_duplicate() -> None:
    engine = _engine()
    _seed(engine, duplicate=True)

    with engine.begin() as conn:
        conn.execute(text("UPDATE game_pitching_stats SET earned_runs = 3 WHERE game_id = 'g1'"))

    with engine.connect() as conn:
        report = audit_pitching_global(conn, 2021)

    assert report["classification"] == "game_level_aggregation_mismatch"
    assert report["comparisons"]["player_game_vs_game_pitching"]["diff"]["earned_runs"] == -1
    assert report["duplicates"]["player_game_same_game_player"] == 1


def test_audit_identifies_incomplete_player_game_coverage() -> None:
    engine = _engine()
    _seed(engine)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM player_game_pitching WHERE game_id = 'g2'"))

    with engine.connect() as conn:
        report = audit_pitching_global(conn, 2021)

    assert report["classification"] == "player_game_source_incomplete"
    coverage = report["comparisons"]["player_game_vs_game_pitching"]["game_coverage"]
    assert coverage == {"player_game": 1, "game_pitching": 2, "complete": False}
