"""Tests for the read-only player identity investigation report."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.investigate_oci_player_identity import collect_identity_report


def _engine() -> object:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT, team TEXT, position TEXT, status TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE player_season_pitching ("
                "player_id INTEGER, season INTEGER, league TEXT, team_code TEXT, innings_outs INTEGER, "
                "innings_pitched REAL, era REAL, games INTEGER)"
            ),
        )
        conn.execute(
            text(
                "CREATE TABLE game_pitching_stats ("
                "game_id TEXT, player_id INTEGER, player_name TEXT, team_code TEXT, innings_outs INTEGER, "
                "innings_pitched REAL)"
            ),
        )
        conn.execute(text("CREATE TABLE team_daily_roster (player_id INTEGER, team_code TEXT)"))
    return engine


def test_collect_identity_report_compares_target_and_local_evidence() -> None:
    target_engine = _engine()
    local_engine = _engine()
    with target_engine.begin() as conn:
        conn.execute(text("INSERT INTO player_basic (player_id, name, status) VALUES (1352, '강경학', 'retired')"))
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(player_id, season, league, team_code, innings_outs, innings_pitched, era, games) "
                "VALUES (1352, 2021, 'REGULAR', 'KIA', 0, 0, 54.0, 1)"
            ),
        )
    with local_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO player_basic (player_id, name, team, position, status) VALUES (61700, '강경학', 'KIA', '내야수', 'active')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(player_id, season, league, team_code, innings_outs, innings_pitched, era, games) "
                "VALUES (61700, 2021, 'REGULAR', 'KIA', 2, 0.6666667, 54.0, 1)"
            ),
        )
        conn.execute(
            text(
                "INSERT INTO game_pitching_stats "
                "(game_id, player_id, player_name, team_code, innings_outs, innings_pitched) "
                "VALUES ('20210401HHKIA0', 61700, '강경학', 'HH', 2, 0.6666667)"
            ),
        )

    with target_engine.connect() as target_conn, local_engine.connect() as local_conn:
        report = collect_identity_report(target_conn, local_conn, player_ids=(1352,), year=2021)

    assert report["target_profiles"][0]["name"] == "강경학"
    assert report["local_profiles"][0]["player_id"] == 61700
    assert report["target_season_pitching"][0]["team_code"] == "KIA"
    assert report["local_game_pitching"][0]["team_code"] == "HH"
    assert report["target_roster_tables"] == ["team_daily_roster"]
