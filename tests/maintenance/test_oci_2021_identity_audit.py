"""Tests for the read-only OCI 2021 pitching identity audit."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from sqlalchemy import create_engine, text

from scripts.maintenance.oci_2021_identity_audit import (
    audit_identity,
    write_exact_override_candidates,
)


def _target_engine() -> object:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text("CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT, team TEXT, position TEXT)")
        )
        conn.execute(
            text(
                "CREATE TABLE player_season_pitching ("
                "player_id INTEGER, season INTEGER, league TEXT, level TEXT, team_code TEXT, "
                "source TEXT, games INTEGER, innings_outs INTEGER, innings_pitched REAL, era REAL)"
            ),
        )
    return engine


def _local_engine() -> object:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text("CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY, name TEXT, team TEXT, position TEXT)")
        )
        conn.execute(
            text(
                "CREATE TABLE game_pitching_stats ("
                "game_id TEXT, player_id INTEGER, player_name TEXT, team_code TEXT, innings_outs INTEGER)"
            ),
        )
        conn.execute(
            text(
                "CREATE TABLE player_season_pitching ("
                "player_id INTEGER, season INTEGER, league TEXT, level TEXT, team_code TEXT, "
                "innings_outs INTEGER, innings_pitched REAL)"
            ),
        )
    return engine


def _seed(target_engine: object, local_engine: object) -> None:
    profiles = [
        (1, "김A", "LG", "투수"),
        (2, "김A", "LG", "투수"),
        (3, "김B", "KIA", "투수"),
        (4, "김B", "KIA", "투수"),
        (5, "김C", "DB", "투수"),
        (6, "김C", "DB", "투수"),
    ]
    with target_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO player_basic (player_id, name, team, position) VALUES (:id, :name, :team, :position)"),
            [{"id": row[0], "name": row[1], "team": row[2], "position": row[3]} for row in profiles],
        )
        conn.execute(
            text(
                "INSERT INTO player_season_pitching "
                "(player_id, season, league, level, team_code, source, games, innings_outs, innings_pitched, era) "
                "VALUES (:id, 2021, 'REGULAR', 'KBO1', :team, :source, :games, :outs, :innings, 3.0)"
            ),
            [
                {"id": 1, "team": "LG", "source": "PROFILE", "games": 3, "outs": 9, "innings": 3.0},
                {"id": 2, "team": "LG", "source": "MANUAL_RECALC", "games": 1, "outs": 3, "innings": 1.0},
                {"id": 3, "team": "KIA", "source": "PROFILE", "games": 2, "outs": 6, "innings": 2.0},
                {"id": 4, "team": "KIA", "source": "MANUAL_RECALC", "games": 2, "outs": 6, "innings": 2.0},
                {"id": 5, "team": "DB", "source": "PROFILE", "games": 2, "outs": 6, "innings": 2.0},
                {"id": 6, "team": "DB", "source": "MANUAL_RECALC", "games": 2, "outs": 6, "innings": 2.0},
            ],
        )
    with local_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO player_basic (player_id, name, team, position) VALUES (:id, :name, :team, :position)"),
            [{"id": row[0], "name": row[1], "team": row[2], "position": row[3]} for row in profiles],
        )
        conn.execute(
            text(
                "INSERT INTO game_pitching_stats (game_id, player_id, player_name, team_code, innings_outs) "
                "VALUES (:game_id, :player_id, :player_name, :team_code, :innings_outs)"
            ),
            [
                {
                    "game_id": "20210401LGKT0",
                    "player_id": 1,
                    "player_name": "김A",
                    "team_code": "LG",
                    "innings_outs": 9,
                },
                {
                    "game_id": "20210402KIAOB0",
                    "player_id": 3,
                    "player_name": "김B",
                    "team_code": "KIA",
                    "innings_outs": 3,
                },
                {
                    "game_id": "20210403KIAOB0",
                    "player_id": 4,
                    "player_name": "김B",
                    "team_code": "KIA",
                    "innings_outs": 3,
                },
            ],
        )


def test_audit_classifies_pilot_groups_from_local_game_evidence() -> None:
    target_engine = _target_engine()
    local_engine = _local_engine()
    _seed(target_engine, local_engine)

    with target_engine.connect() as target_conn, local_engine.connect() as local_conn:
        report = audit_identity(
            target_conn,
            local_conn,
            year=2021,
            teams=("LG", "KIA", "DB"),
        )

    assert report["read_only"] is True
    assert report["selected_teams"] == ["DB", "KIA", "LG"]
    classifications = {
        (group["team_code"], group["player_name"]): group["classification"] for group in report["groups"]
    }
    assert classifications == {
        ("LG", "김A"): "exact",
        ("KIA", "김B"): "ambiguous",
        ("DB", "김C"): "unresolved",
    }
    exact = next(group for group in report["groups"] if group["classification"] == "exact")
    assert exact["resolved_player_id"] == 1
    assert exact["evidence_source"] == "local_game_pitching_stats"


def test_exact_candidates_are_written_as_reviewable_csv(tmp_path: Path) -> None:
    report = {
        "groups": [
            {
                "classification": "exact",
                "team_code": "LG",
                "player_name": "김A",
                "resolved_player_id": 1,
                "reason": "unique local game evidence",
                "evidence_source": "local_game_pitching_stats",
            },
            {
                "classification": "ambiguous",
                "team_code": "KIA",
                "player_name": "김B",
                "resolved_player_id": None,
                "reason": "multiple candidates",
                "evidence_source": "local_game_pitching_stats",
            },
        ],
    }
    output = tmp_path / "exact.csv"

    count = write_exact_override_candidates(report, output)

    assert count == 1
    with output.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "source_table": "player_season_pitching",
            "year": "2021",
            "team_code": "LG",
            "player_name": "김A",
            "resolved_player_id": "1",
            "reason": "unique local game evidence",
            "evidence_source": "local_game_pitching_stats",
        },
    ]


def test_report_is_json_serializable() -> None:
    report = {
        "read_only": True,
        "groups": [],
    }
    assert json.loads(json.dumps(report, ensure_ascii=False))["read_only"] is True
