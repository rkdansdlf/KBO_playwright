from __future__ import annotations

import csv
from pathlib import Path

from sqlalchemy import create_engine, text

from scripts.maintenance.repair_duplicate_pseudo_players import repair_duplicate_pseudo_players


def _make_db(tmp_path: Path, *, conflict: bool = False) -> str:
    db_path = tmp_path / "pseudo_players.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE player_basic (
                    player_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    team TEXT,
                    uniform_no TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT NOT NULL,
                    player_id INTEGER,
                    appearance_seq INTEGER,
                    player_name TEXT,
                    team_code TEXT,
                    uniform_no TEXT,
                    hits INTEGER,
                    UNIQUE (game_id, player_id, appearance_seq)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_batting (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id INTEGER NOT NULL,
                    season INTEGER NOT NULL,
                    league TEXT NOT NULL DEFAULT 'REGULAR',
                    level TEXT NOT NULL DEFAULT 'KBO1',
                    team_code TEXT,
                    games INTEGER,
                    UNIQUE (player_id, season, league, level)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_movements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    movement_date TEXT NOT NULL,
                    section TEXT NOT NULL,
                    team_code TEXT NOT NULL,
                    canonical_team_id TEXT,
                    player_basic_id INTEGER,
                    player_name TEXT NOT NULL,
                    resolution_status TEXT DEFAULT 'resolved'
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT NOT NULL,
                    event_seq INTEGER NOT NULL,
                    batter_id INTEGER,
                    pitcher_id INTEGER,
                    description TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE matchup_bvp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batter_id INTEGER NOT NULL,
                    pitcher_id INTEGER NOT NULL,
                    plate_appearances INTEGER,
                    UNIQUE (batter_id, pitcher_id)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team, uniform_no)
                VALUES
                    (900001, '임시선수', NULL, NULL),
                    (900002, '임시선수', NULL, NULL)
                """
            )
        )
        if conflict:
            conn.execute(
                text(
                    """
                    INSERT INTO game_batting_stats
                        (game_id, player_id, appearance_seq, player_name, team_code, uniform_no, hits)
                    VALUES
                        ('20260401LGSS0', 900001, 1, '임시선수', 'LG', '17', 1),
                        ('20260401LGSS0', 900002, 1, '임시선수', 'LG', '17', 2)
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO game_batting_stats
                        (game_id, player_id, appearance_seq, player_name, team_code, uniform_no, hits)
                    VALUES
                        ('20260401LGSS0', 900001, 1, '임시선수', 'LG', '17', 1),
                        ('20260402LGSS0', 900002, 1, '임시선수', 'LG', '17', 1)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, league, level, team_code, games)
                    VALUES
                        (900001, 2026, 'REGULAR', 'KBO1', 'LG', 1),
                        (900002, 2026, 'REGULAR', 'KBO1', 'LG', 1)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO player_movements
                        (movement_date, section, team_code, canonical_team_id, player_basic_id, player_name)
                    VALUES
                        ('2026-04-03', '등록', 'LG', 'LG', 900002, '임시선수')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO game_events
                        (game_id, event_seq, batter_id, pitcher_id, description)
                    VALUES
                        ('20260402LGSS0', 1, 900002, 900001, '타석')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO matchup_bvp (batter_id, pitcher_id, plate_appearances)
                    VALUES (900002, 900001, 1)
                    """
                )
            )
    return f"sqlite:///{db_path}"


def _write_mergeable_worklist(path: Path, rows: list[dict[str, object]]) -> Path:
    fieldnames = [
        "name",
        "team_key",
        "uniform_no",
        "target_player_id",
        "source_player_ids",
        "player_ids",
        "reference_rows",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_conflict_worklist(path: Path, rows: list[dict[str, object]]) -> Path:
    fieldnames = ["table_name", "name", "target_player_id", "source_player_ids", "key", "reason"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_duplicate_pseudo_player_repair_dry_run_and_apply(tmp_path):
    db_url = _make_db(tmp_path)

    dry_run = repair_duplicate_pseudo_players(
        db_url=db_url,
        output_dir=tmp_path / "reports",
        apply=False,
    )

    assert dry_run["dry_run"] is True
    assert dry_run["mergeable_groups"] == 1
    assert dry_run["conflicts"] == 0

    applied = repair_duplicate_pseudo_players(
        db_url=db_url,
        output_dir=tmp_path / "reports",
        apply=True,
    )

    assert applied["deleted_player_basic_rows"] == 1

    engine = create_engine(db_url)
    with engine.connect() as conn:
        player_ids = conn.execute(text("SELECT player_id FROM player_basic")).fetchall()
        batting_ids = conn.execute(
            text("SELECT DISTINCT player_id FROM game_batting_stats")
        ).fetchall()
        season_count = conn.execute(text("SELECT COUNT(*) FROM player_season_batting")).scalar()
        movement_ids = conn.execute(text("SELECT DISTINCT player_basic_id FROM player_movements")).fetchall()
        event_batter_ids = conn.execute(text("SELECT DISTINCT batter_id FROM game_events")).fetchall()
        matchup_batter_ids = conn.execute(text("SELECT DISTINCT batter_id FROM matchup_bvp")).fetchall()

    assert player_ids == [(900001,)]
    assert batting_ids == [(900001,)]
    assert season_count == 1
    assert movement_ids == [(900001,)]
    assert event_batter_ids == [(900001,)]
    assert matchup_batter_ids == [(900001,)]


def test_duplicate_pseudo_player_repair_reports_payload_conflict(tmp_path):
    db_url = _make_db(tmp_path, conflict=True)

    dry_run = repair_duplicate_pseudo_players(
        db_url=db_url,
        output_dir=tmp_path / "reports",
        apply=False,
    )

    assert dry_run["mergeable_groups"] == 1
    assert dry_run["safe_mergeable_groups"] == 0
    assert dry_run["skipped_conflict_groups"] == 1
    assert dry_run["conflicts"] == 1

    applied = repair_duplicate_pseudo_players(
        db_url=db_url,
        output_dir=tmp_path / "reports",
        apply=True,
    )

    assert applied["deleted_player_basic_rows"] == 0
    assert applied["skipped_conflict_groups"] == 1

    engine = create_engine(db_url)
    with engine.connect() as conn:
        player_count = conn.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()
        batting_count = conn.execute(text("SELECT COUNT(*) FROM game_batting_stats")).scalar()

    assert player_count == 2
    assert batting_count == 2


def test_duplicate_pseudo_player_repair_applies_reviewed_worklist_and_reports_conflicts(tmp_path):
    db_url = _make_db(tmp_path, conflict=True)
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team, uniform_no)
                VALUES
                    (900101, '검토선수', 'SS', NULL),
                    (900102, '검토선수', 'SS', NULL)
                """
            )
        )

    worklist = _write_mergeable_worklist(
        tmp_path / "reviewed_mergeable.csv",
        [
            {
                "name": "임시선수",
                "team_key": "LG",
                "uniform_no": "17",
                "target_player_id": 900001,
                "source_player_ids": "900002",
                "player_ids": "900001,900002",
                "reference_rows": 2,
                "reason": "single_team_single_uniform_evidence",
            },
            {
                "name": "검토선수",
                "team_key": "SS",
                "uniform_no": "",
                "target_player_id": 900101,
                "source_player_ids": "900102",
                "player_ids": "900101,900102",
                "reference_rows": 0,
                "reason": "single_team_single_uniform_evidence",
            },
        ],
    )
    conflict_worklist = _write_conflict_worklist(
        tmp_path / "reviewed_conflicts.csv",
        [
            {
                "table_name": "game_batting_stats",
                "name": "임시선수",
                "target_player_id": 900001,
                "source_player_ids": "900002",
                "key": "20260401LGSS0|900001|1",
                "reason": "conflicting_duplicate_reference_payload",
            },
            {
                "table_name": "players",
                "name": "검토선수",
                "target_player_id": 900101,
                "source_player_ids": "900102",
                "key": "900101",
                "reason": "conflicting_duplicate_reference_payload",
            },
        ],
    )

    applied = repair_duplicate_pseudo_players(
        db_url=db_url,
        output_dir=tmp_path / "reports",
        apply=True,
        mergeable_worklist=worklist,
        conflict_worklist=conflict_worklist,
        conflict_policy="delete-source-duplicates",
    )

    assert applied["mergeable_groups"] == 2
    assert applied["safe_mergeable_groups"] == 2
    assert applied["conflict_groups"] == 2
    assert applied["conflicts"] == 2
    assert applied["deleted_conflicting_reference_rows"] == 1
    assert applied["deleted_player_basic_rows"] == 2

    with Path(applied["conflict_report_path"]).open(newline="", encoding="utf-8") as fh:
        conflict_rows = list(csv.DictReader(fh))

    assert conflict_rows == [
        {
            "table_name": "game_batting_stats",
            "name": "임시선수",
            "target_player_id": "900001",
            "source_player_ids": "900002",
            "key": "20260401LGSS0|900001|1",
            "reason": "conflicting_duplicate_reference_payload",
        },
        {
            "table_name": "players",
            "name": "검토선수",
            "target_player_id": "900101",
            "source_player_ids": "900102",
            "key": "900101",
            "reason": "conflicting_duplicate_reference_payload",
        }
    ]

    with engine.connect() as conn:
        player_ids = conn.execute(text("SELECT player_id FROM player_basic ORDER BY player_id")).fetchall()
        batting_rows = conn.execute(
            text("SELECT player_id, hits FROM game_batting_stats ORDER BY id")
        ).fetchall()

    assert player_ids == [(900001,), (900101,)]
    assert batting_rows == [(900001, 1)]
