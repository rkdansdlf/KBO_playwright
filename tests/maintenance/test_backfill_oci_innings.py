"""Tests for the dry-run OCI innings backfill migration."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.backfill_oci_innings import backfill_oci_innings


def _engine() -> object:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(
            text(
                "CREATE TABLE player_season_pitching ("
                "id INTEGER PRIMARY KEY, player_id INTEGER, season INTEGER, league TEXT, level TEXT, "
                "team_code TEXT, innings_outs INTEGER, innings_pitched REAL, updated_at TEXT)"
            ),
        )
        conn.execute(
            text(
                "CREATE TABLE game_pitching_stats ("
                "game_id TEXT, player_id INTEGER, player_name TEXT, team_code TEXT, canonical_team_code TEXT, "
                "innings_outs INTEGER, innings_pitched REAL)"
            ),
        )
    return engine


def _seed(source_engine: object, target_engine: object) -> None:
    profiles = [{"id": 73, "name": "박관진"}, {"id": 1352, "name": "강경학"}]
    with source_engine.begin() as source_conn:
        source_conn.execute(text("INSERT INTO player_basic (player_id, name) VALUES (:id, :name)"), profiles)
    with target_engine.begin() as target_conn:
        target_conn.execute(text("INSERT INTO player_basic (player_id, name) VALUES (:id, :name)"), profiles)
    with source_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(id, player_id, season, league, level, team_code, innings_outs, innings_pitched, updated_at) "
                "VALUES (1, 73, 2021, 'REGULAR', 'KBO1', 'LG', 7, 2.1, 'source')"
            ),
        )
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(id, player_id, season, league, level, team_code, innings_outs, innings_pitched, updated_at) "
                "VALUES (2, 1352, 2021, 'REGULAR', 'KBO1', 'KIA', 5, 1.2, 'source')"
            ),
        )
    with target_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(id, player_id, season, league, level, team_code, innings_outs, innings_pitched, updated_at) "
                "VALUES (11, 73, 2021, 'REGULAR', 'KBO1', 'LG', 0, 0, 'target')"
            ),
        )
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(id, player_id, season, league, level, team_code, innings_outs, innings_pitched, updated_at) "
                "VALUES (12, 1352, 2021, 'REGULAR', 'KBO1', 'KIA', 5, 1.2, 'target')"
            ),
        )


def test_dry_run_plans_only_missing_innings_and_preserves_target() -> None:
    source_engine = _engine()
    target_engine = _engine()
    _seed(source_engine, target_engine)

    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        report = backfill_oci_innings(
            source_conn,
            target_conn,
            year=2021,
            player_ids=(73, 1352),
            apply=False,
        )

    assert report["summary"] == {"planned": 1, "already_correct": 1, "source_missing": 0, "conflict": 0, "applied": 0}
    assert report["changes"][0]["player_id"] == 73
    with target_engine.connect() as conn:
        row = conn.execute(
            text("SELECT innings_outs, innings_pitched FROM player_season_pitching WHERE player_id = 73"),
        ).one()
    assert tuple(row) == (0, 0.0)


def test_apply_updates_only_planned_rows() -> None:
    source_engine = _engine()
    target_engine = _engine()
    _seed(source_engine, target_engine)

    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        report = backfill_oci_innings(
            source_conn,
            target_conn,
            year=2021,
            player_ids=(73, 1352),
            apply=True,
        )

    assert report["summary"]["applied"] == 1
    with target_engine.connect() as conn:
        row = conn.execute(
            text("SELECT innings_outs, innings_pitched FROM player_season_pitching WHERE player_id = 73"),
        ).one()
    assert tuple(row) == (7, 2.1)


def test_conflicting_positive_target_is_not_overwritten() -> None:
    source_engine = _engine()
    target_engine = _engine()
    _seed(source_engine, target_engine)
    with target_engine.begin() as conn:
        conn.execute(
            text("UPDATE player_season_pitching SET innings_outs = 8 WHERE player_id = 73"),
        )

    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        report = backfill_oci_innings(
            source_conn,
            target_conn,
            year=2021,
            player_ids=(73,),
            apply=True,
        )

    assert report["summary"]["conflict"] == 1
    with target_engine.connect() as conn:
        assert (
            conn.execute(text("SELECT innings_outs FROM player_season_pitching WHERE player_id = 73")).scalar_one() == 8
        )


def test_falls_back_to_unique_local_game_identity_when_ids_differ() -> None:
    source_engine = _engine()
    target_engine = _engine()
    with source_engine.begin() as conn:
        conn.execute(text("INSERT INTO player_basic (player_id, name) VALUES (50397, '박관진')"))
        conn.execute(
            text(
                "INSERT INTO game_pitching_stats "
                "(game_id, player_id, player_name, team_code, canonical_team_code, innings_outs, innings_pitched) "
                "VALUES ('20210401LGKT0', 50397, '박관진', 'LG', NULL, 7, 2.1)"
            ),
        )
    with target_engine.begin() as conn:
        conn.execute(text("INSERT INTO player_basic (player_id, name) VALUES (73, '박관진')"))
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(id, player_id, season, league, level, team_code, innings_outs, innings_pitched, updated_at) "
                "VALUES (11, 73, 2021, 'REGULAR', 'KBO1', 'LG', 0, 0, 'target')"
            ),
        )

    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        report = backfill_oci_innings(
            source_conn,
            target_conn,
            year=2021,
            player_ids=(73,),
            apply=False,
        )

    assert report["summary"]["planned"] == 1
    assert report["changes"][0]["source_player_id"] == 50397
