from __future__ import annotations

import json

from sqlalchemy import create_engine, text

from scripts.maintenance.cleanup_legacy_player_season_sources import run_cleanup


def _make_database(path) -> str:
    url = f"sqlite:///{path}"
    engine = create_engine(url)
    with engine.begin() as connection:
        for table in ("player_season_batting", "player_season_pitching"):
            connection.execute(
                text(
                    f"""
                    CREATE TABLE {table} (
                        id INTEGER PRIMARY KEY,
                        player_id INTEGER,
                        season INTEGER,
                        league TEXT,
                        source TEXT,
                        team_code TEXT,
                        value INTEGER
                    )
                    """,
                ),
            )
            connection.execute(
                text(
                    f"""
                    INSERT INTO {table} (id, player_id, season, league, source, team_code, value)
                    VALUES
                        (1, 1, 2021, 'REGULAR', 'PROFILE', 'SSG', 1),
                        (2, 1, 2021, 'REGULAR', 'CRAWLER', 'SSG', 2),
                        (3, 2, 2021, 'REGULAR', 'AGGREGATED', 'LG', 3),
                        (4, 2, 2021, 'REGULAR', 'MANUAL_RECALC', 'LG', 4),
                        (5, 3, 2021, 'REGULAR', 'CRAWLER', 'KT', 5),
                        (6, 4, 2026, 'REGULAR', 'ROLLUP', 'LG', 6),
                        (7, 5, 2026, 'REGULAR', 'MANUAL_RECALC', 'KT', 7)
                    """,
                ),
            )
    return url


def test_dry_run_creates_backup_without_deleting(tmp_path) -> None:
    database_url = _make_database(tmp_path / "cleanup.db")
    backup_path = tmp_path / "backup.json"

    report = run_cleanup(database_url=database_url, backup_out=backup_path)

    assert report["apply"] is False
    assert report["before"] == {"player_season_batting": 2, "player_season_pitching": 2}
    assert report["deleted"] == {"player_season_batting": 0, "player_season_pitching": 0}
    assert report["after"] == report["before"]
    payload = json.loads(backup_path.read_text(encoding="utf-8"))
    assert len(payload["tables"]["player_season_batting"]) == 2


def test_apply_deletes_only_selected_sources_and_years(tmp_path) -> None:
    database_url = _make_database(tmp_path / "cleanup.db")
    backup_path = tmp_path / "backup.json"

    report = run_cleanup(
        database_url=database_url,
        years=(2021, 2026),
        apply=True,
        backup_out=backup_path,
    )

    assert report["deleted"] == {"player_season_batting": 2, "player_season_pitching": 2}
    assert report["after"] == {"player_season_batting": 0, "player_season_pitching": 0}

    engine = create_engine(database_url)
    with engine.connect() as connection:
        remaining = connection.execute(
            text("SELECT COUNT(*) FROM player_season_batting WHERE source = 'ROLLUP'"),
        ).scalar_one()
        manual_remaining = connection.execute(
            text("SELECT COUNT(*) FROM player_season_batting WHERE source = 'MANUAL_RECALC'"),
        ).scalar_one()
    assert remaining == 1
    assert manual_remaining == 2
