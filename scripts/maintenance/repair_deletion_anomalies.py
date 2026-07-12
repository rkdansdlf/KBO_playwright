#!/usr/bin/env python3
"""Repair deletion-anomaly and referential-integrity gaps in local SQLite.

Default mode is a dry-run. Use --apply to mutate data and --schema to rebuild
affected SQLite tables with declared FKs and game-child ON DELETE CASCADE.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_DB_PATH = Path("data/kbo_dev.db")
PLAYER_ROSTER_POSITIONS = {"투수", "포수", "내야수", "외야수", "선수"}
STAFF_ROSTER_POSITIONS = {"감독", "코치"}
TEAM_NAME_TO_CODE = {
    "KIA": "KIA",
    "기아": "KIA",
    "두산": "DB",
    "DB": "DB",
    "OB": "OB",
    "롯데": "LT",
    "LT": "LT",
    "삼성": "SS",
    "SS": "SS",
    "한화": "HH",
    "HH": "HH",
    "키움": "KH",
    "KH": "KH",
    "넥센": "NX",
    "NX": "NX",
    "우리": "WO",
    "WO": "WO",
    "SSG": "SSG",
    "SK": "SK",
    "LG": "LG",
    "KT": "KT",
    "kt": "KT",
    "NC": "NC",
    "현대": "HU",
    "HU": "HU",
    "HD": "HU",
    "해태": "HT",
    "HT": "HT",
    "쌍방울": "SL",
    "SL": "SL",
    "태평양": "TP",
    "TP": "TP",
    "청보": "CB",
    "CB": "CB",
    "삼미": "SM",
    "SM": "SM",
    "빙그레": "BE",
    "BE": "BE",
    "MBC": "MBC",
}


@dataclass
class Action:
    name: str
    row_count: int
    status: str


UNIQUE_CONSTRAINTS = {
    "players": [
        "UNIQUE (kbo_person_id)",
        "UNIQUE (player_basic_id)",
    ],
    "team_daily_roster": [
        "CONSTRAINT uq_team_daily_roster UNIQUE (roster_date, team_code, player_id)",
    ],
    "player_movements": [
        "CONSTRAINT uq_player_movement UNIQUE (movement_date, team_code, player_name, section)",
    ],
    "game_id_aliases": [
        "FOREIGN KEY(canonical_game_id) REFERENCES game (game_id) ON DELETE CASCADE",
    ],
    "game_metadata": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
    ],
    "game_inning_scores": [
        "CONSTRAINT uq_game_inning_team UNIQUE (game_id, team_side, inning)",
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
    ],
    "game_lineups": [
        "CONSTRAINT uq_game_lineup_entry UNIQUE (game_id, team_side, appearance_seq)",
    ],
    "game_batting_stats": [
        "CONSTRAINT uq_game_batting_player UNIQUE (game_id, player_id, appearance_seq)",
    ],
    "game_pitching_stats": [
        "CONSTRAINT uq_game_pitching_player UNIQUE (game_id, player_id, appearance_seq)",
    ],
    "game_events": [
        "CONSTRAINT uq_game_event_seq UNIQUE (game_id, event_seq)",
    ],
    "matchup_bvp": [
        "CONSTRAINT uq_matchup_bvp UNIQUE (batter_id, pitcher_id)",
    ],
    "matchup_batter_splits": [
        "CONSTRAINT uq_batter_split UNIQUE (player_id, season_year, split_type)",
    ],
    "matchup_pitcher_splits": [
        "CONSTRAINT uq_pitcher_split UNIQUE (player_id, season_year, split_type)",
    ],
    "matchup_batter_team_split": [
        "CONSTRAINT uq_batter_team_split UNIQUE (season_year, league_type_code, player_id, team_code, opponent_team_code)",
    ],
    "matchup_pitcher_team_split": [
        "CONSTRAINT uq_pitcher_team_split UNIQUE (season_year, league_type_code, player_id, team_code, opponent_team_code)",
    ],
    "matchup_batter_stadium_split": [
        "CONSTRAINT uq_batter_stadium_split UNIQUE (season_year, league_type_code, player_id, team_code, stadium_name)",
    ],
    "matchup_batter_vs_starter": [
        "CONSTRAINT uq_batter_vs_starter UNIQUE (season_year, league_type_code, player_id, pitcher_name)",
    ],
}


FK_CONSTRAINTS = {
    "players": [
        "FOREIGN KEY(player_basic_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "team_daily_roster": [
        "FOREIGN KEY(team_code) REFERENCES teams (team_id) ON DELETE RESTRICT",
        "FOREIGN KEY(player_basic_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "player_movements": [
        "FOREIGN KEY(canonical_team_id) REFERENCES teams (team_id) ON DELETE RESTRICT",
        "FOREIGN KEY(player_basic_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "game_lineups": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "game_batting_stats": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "game_pitching_stats": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "game_events": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
        "FOREIGN KEY(batter_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
        "FOREIGN KEY(pitcher_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "game_summary": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "game_play_by_play": [
        "FOREIGN KEY(game_id) REFERENCES game (game_id) ON DELETE CASCADE",
    ],
    "matchup_bvp": [
        "FOREIGN KEY(batter_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
        "FOREIGN KEY(pitcher_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "matchup_batter_splits": [
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "matchup_pitcher_splits": [
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "matchup_batter_team_split": [
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "matchup_pitcher_team_split": [
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "matchup_batter_stadium_split": [
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
    "matchup_batter_vs_starter": [
        "FOREIGN KEY(player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT",
    ],
}

REBUILD_TABLES = (
    "players",
    "team_daily_roster",
    "player_movements",
    "game_id_aliases",
    "game_metadata",
    "game_inning_scores",
    "game_lineups",
    "game_batting_stats",
    "game_pitching_stats",
    "game_events",
    "game_summary",
    "game_play_by_play",
    "matchup_bvp",
    "matchup_batter_splits",
    "matchup_pitcher_splits",
    "matchup_batter_team_split",
    "matchup_pitcher_team_split",
    "matchup_batter_stadium_split",
    "matchup_batter_vs_starter",
)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _add_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    definition: str,
    actions: list[Action],
    apply: bool,
) -> None:
    if not _table_exists(conn, table_name) or column_name in _columns(conn, table_name):
        return
    if apply:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
    actions.append(Action(f"add_column {table_name}.{column_name}", 1, "applied" if apply else "dry_run"))


def _rowcount(result: sqlite3.Cursor) -> int:
    return int(result.rowcount or 0)


def _execute_counted(conn: sqlite3.Connection, sql: str, params: tuple = (), *, apply: bool) -> int:
    if not apply:
        count_sql = f"SELECT COUNT(*) FROM ({sql.removeprefix('SELECT').strip()})"
        return int(conn.execute(count_sql, params).fetchone()[0])
    return _rowcount(conn.execute(sql, params))


def _backup(db_path: Path) -> Path:
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{db_path.stem}_before_deletion_anomaly_repair_{stamp}{db_path.suffix}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _normalize_name(raw_name: str | None) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", str(raw_name or "")).strip()


def _extract_position(raw_name: str | None) -> str | None:
    match = re.search(r"\(([^)]*)\)\s*$", str(raw_name or "").strip())
    return match.group(1).strip() if match else None


def _unique_profile_mirror_player_id(conn: sqlite3.Connection, candidate_ids: set[int]) -> int | None:
    if not candidate_ids or not _table_exists(conn, "players") or "player_basic_id" not in _columns(conn, "players"):
        return None
    placeholders = ",".join("?" for _ in candidate_ids)
    rows = conn.execute(
        f"""
        SELECT DISTINCT player_basic_id
        FROM players
        WHERE player_basic_id IN ({placeholders})
        """,
        tuple(sorted(candidate_ids)),
    ).fetchall()
    mirrored_ids = {int(row[0]) for row in rows if row[0] is not None}
    return next(iter(mirrored_ids)) if len(mirrored_ids) == 1 else None


def _unique_roster_player_id(
    conn: sqlite3.Connection,
    player_name: str,
    team_id: str | None,
    season: int | None,
    candidate_ids: set[int],
) -> int | None:
    required = {"team_code", "player_name", "roster_date", "person_type", "player_basic_id"}
    if not player_name or not team_id or not season or not _table_exists(conn, "team_daily_roster"):
        return None
    if not required <= _columns(conn, "team_daily_roster"):
        return None
    rows = conn.execute(
        """
        SELECT DISTINCT player_basic_id
        FROM team_daily_roster
        WHERE team_code = ?
          AND player_name = ?
          AND substr(roster_date, 1, 4) = ?
          AND person_type = 'player'
          AND player_basic_id IS NOT NULL
        """,
        (team_id, player_name, str(season)),
    ).fetchall()
    roster_ids = {int(row[0]) for row in rows if row[0] is not None}
    if candidate_ids:
        roster_ids &= candidate_ids
    return next(iter(roster_ids)) if len(roster_ids) == 1 else None


def _unique_franchise_season_player_id(
    conn: sqlite3.Connection,
    candidate_ids: set[int],
    team_id: str | None,
    season: int | None,
) -> int | None:
    if not candidate_ids or not team_id or not season:
        return None
    team_row = conn.execute(
        "SELECT franchise_id FROM teams WHERE team_id = ?",
        (team_id,),
    ).fetchone()
    if not team_row or team_row[0] is None:
        return None
    team_ids = [
        str(row[0])
        for row in conn.execute(
            "SELECT team_id FROM teams WHERE franchise_id = ?",
            (team_row[0],),
        ).fetchall()
        if row[0]
    ]
    if not team_ids:
        return None

    player_placeholders = ",".join("?" for _ in candidate_ids)
    team_placeholders = ",".join("?" for _ in team_ids)
    season_ids: set[int] = set()
    for table_name in ("player_season_batting", "player_season_pitching"):
        if not _table_exists(conn, table_name):
            continue
        rows = conn.execute(
            f"""
            SELECT DISTINCT player_id
            FROM {table_name}
            WHERE player_id IN ({player_placeholders})
              AND season IN (?, ?)
              AND team_code IN ({team_placeholders})
            """,
            (*sorted(candidate_ids), season - 1, season, *team_ids),
        ).fetchall()
        season_ids.update(int(row[0]) for row in rows if row[0] is not None)
    return next(iter(season_ids)) if len(season_ids) == 1 else None


def _resolve_by_position(rows: list, position: str) -> int | None:
    position_ids = {int(row[0]) for row in rows if row[2] == position}
    return next(iter(position_ids)) if len(position_ids) == 1 else None


def _resolve_by_team_context(conn: sqlite3.Connection, rows: list, team_id: str) -> int | None:
    team_row = conn.execute("SELECT team_short_name, team_name FROM teams WHERE team_id = ?", (team_id,)).fetchone()
    terms = [team_id]
    if team_row:
        terms.extend(t for t in team_row if t)
    contextual = {int(row[0]) for row in rows if any(term and term in str(row[1] or "") for term in terms)}
    return next(iter(contextual)) if len(contextual) == 1 else None


def _resolve_by_season_team(conn: sqlite3.Connection, ids: set[int], season: int, team_id: str) -> int | None:
    season_ids: set[int] = set()
    for table_name in ("player_season_batting", "player_season_pitching"):
        if _table_exists(conn, table_name):
            season_rows = conn.execute(
                f"SELECT player_id FROM {table_name} WHERE player_id IN ({','.join('?' for _ in ids)}) AND season = ? AND team_code = ?",
                (*sorted(ids), season, team_id),
            ).fetchall()
            season_ids.update(int(row[0]) for row in season_rows)
    return next(iter(season_ids)) if len(season_ids) == 1 else None


def _try_resolvers(
    conn: sqlite3.Connection,
    rows: list,
    ids: set[int],
    team_id: str | None,
    season: int | None,
    position: str | None,
    roster_player_id: int | None,
) -> int | None:
    resolvers: list = []
    if position:
        resolvers.append(lambda: _resolve_by_position(rows, position))
    resolvers.append(
        lambda: _unique_profile_mirror_player_id(
            conn, {int(row[0]) for row in rows if row[2] == position} if position else ids
        )
    )
    if roster_player_id:
        resolvers.append(lambda: roster_player_id)
    resolvers.append(lambda: _unique_franchise_season_player_id(conn, ids, team_id, season))
    if team_id:
        resolvers.append(lambda: _resolve_by_team_context(conn, rows, team_id))
        if season:
            resolvers.append(lambda: _resolve_by_season_team(conn, ids, season, team_id))
    for resolver in resolvers:
        result = resolver()
        if result:
            return result
    return None


def _unique_player_id_by_name(
    conn: sqlite3.Connection,
    player_name: str,
    team_id: str | None = None,
    season: int | None = None,
    position: str | None = None,
) -> int | None:
    rows = conn.execute("SELECT player_id, team, position FROM player_basic WHERE name = ?", (player_name,)).fetchall()
    ids = {int(row[0]) for row in rows}
    if len(ids) == 1:
        return next(iter(ids))
    roster_player_id = _unique_roster_player_id(conn, player_name, team_id, season, ids)
    if not ids:
        return roster_player_id
    return _try_resolvers(conn, rows, ids, team_id, season, position, roster_player_id)


def _unique_team_by_player_history(conn: sqlite3.Connection, player_name: str, season: int | None) -> str | None:
    player_name = _normalize_name(player_name)
    if not player_name:
        return None
    player_ids = [
        int(row[0])
        for row in conn.execute("SELECT player_id FROM player_basic WHERE name = ?", (player_name,)).fetchall()
    ]
    if not player_ids:
        return None
    teams: set[str] = set()
    placeholders = ",".join("?" for _ in player_ids)
    for table_name in ("player_season_batting", "player_season_pitching"):
        if not _table_exists(conn, table_name):
            continue
        params: list[object] = [*player_ids]
        season_clause = ""
        if season:
            season_clause = "AND season <= ?"
            params.append(season)
        rows = conn.execute(
            f"""
            SELECT DISTINCT team_code FROM {table_name}
            WHERE player_id IN ({placeholders})
              {season_clause}
              AND team_code IS NOT NULL
              AND team_code <> ''
            """,
            tuple(params),
        ).fetchall()
        teams.update(str(row[0]) for row in rows)
    if len(teams) == 1:
        team_id = next(iter(teams))
        exists = conn.execute("SELECT 1 FROM teams WHERE team_id = ?", (team_id,)).fetchone()
        return team_id if exists else None
    return None


def _ensure_columns(conn: sqlite3.Connection, actions: list[Action], apply: bool) -> None:
    _add_column(conn, "players", "player_basic_id", "INTEGER", actions, apply)
    _add_column(conn, "team_daily_roster", "player_basic_id", "INTEGER", actions, apply)
    _add_column(conn, "team_daily_roster", "person_type", "VARCHAR(16) DEFAULT 'player'", actions, apply)
    _add_column(conn, "player_movements", "canonical_team_id", "VARCHAR(10)", actions, apply)
    _add_column(conn, "player_movements", "player_basic_id", "INTEGER", actions, apply)
    _add_column(conn, "player_movements", "resolution_status", "VARCHAR(24) DEFAULT 'unresolved'", actions, apply)


def _repair_players(conn: sqlite3.Connection, actions: list[Action], apply: bool) -> None:
    if not {"player_basic_id", "kbo_person_id"} <= _columns(conn, "players"):
        return
    sql = """
        UPDATE players
        SET player_basic_id = CAST(kbo_person_id AS INTEGER)
        WHERE kbo_person_id <> ''
          AND kbo_person_id NOT GLOB '*[^0-9]*'
          AND EXISTS (
              SELECT 1 FROM player_basic
              WHERE player_basic.player_id = CAST(players.kbo_person_id AS INTEGER)
          )
          AND (player_basic_id IS NULL OR player_basic_id <> CAST(kbo_person_id AS INTEGER))
    """
    if apply:
        count = _rowcount(conn.execute(sql))
    else:
        count = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM players
                WHERE kbo_person_id <> ''
                  AND kbo_person_id NOT GLOB '*[^0-9]*'
                  AND EXISTS (
                      SELECT 1 FROM player_basic
                      WHERE player_basic.player_id = CAST(players.kbo_person_id AS INTEGER)
                  )
                  AND (player_basic_id IS NULL OR player_basic_id <> CAST(kbo_person_id AS INTEGER))
                """,
            ).fetchone()[0],
        )
    actions.append(Action("backfill players.player_basic_id", count, "applied" if apply else "dry_run"))


def _repair_roster(conn: sqlite3.Connection, actions: list[Action], apply: bool) -> None:
    if not _table_exists(conn, "team_daily_roster"):
        return
    artifact_count = int(conn.execute("SELECT COUNT(*) FROM team_daily_roster WHERE position = '포지션'").fetchone()[0])
    if apply and artifact_count:
        conn.execute(
            """
            UPDATE team_daily_roster
            SET position = COALESCE(
                (
                    SELECT NULLIF(player_basic.position, '')
                    FROM player_basic
                    WHERE player_basic.player_id = team_daily_roster.player_id
                      AND player_basic.position IN ('투수', '포수', '내야수', '외야수')
                ),
                CASE
                    WHEN EXISTS (SELECT 1 FROM player_basic WHERE player_basic.player_id = team_daily_roster.player_id)
                    THEN '선수'
                    ELSE '코치'
                END
            )
            WHERE position = '포지션'
            """,
        )
    actions.append(
        Action(
            "normalize team_daily_roster parser artifact positions",
            artifact_count,
            "applied" if apply else "dry_run",
        ),
    )

    if not {"person_type", "player_basic_id", "position", "player_id"} <= _columns(conn, "team_daily_roster"):
        return
    player_marks = ",".join("?" for _ in PLAYER_ROSTER_POSITIONS)
    staff_marks = ",".join("?" for _ in STAFF_ROSTER_POSITIONS)
    if apply:
        player_count = _rowcount(
            conn.execute(
                f"UPDATE team_daily_roster SET person_type = 'player' WHERE position IN ({player_marks})",
                tuple(PLAYER_ROSTER_POSITIONS),
            ),
        )
        staff_count = _rowcount(
            conn.execute(
                f"UPDATE team_daily_roster SET person_type = 'staff', player_basic_id = NULL WHERE position IN ({staff_marks})",
                tuple(STAFF_ROSTER_POSITIONS),
            ),
        )
        unknown_count = _rowcount(
            conn.execute(
                f"""
                UPDATE team_daily_roster
                SET person_type = 'unknown', player_basic_id = NULL
                WHERE position NOT IN ({player_marks},{staff_marks})
                """,
                (*PLAYER_ROSTER_POSITIONS, *STAFF_ROSTER_POSITIONS),
            ),
        )
        link_count = _rowcount(
            conn.execute(
                """
                UPDATE team_daily_roster
                SET player_basic_id = player_id
                WHERE person_type = 'player'
                  AND EXISTS (SELECT 1 FROM player_basic WHERE player_basic.player_id = team_daily_roster.player_id)
                """,
            ),
        )
    else:
        player_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM team_daily_roster WHERE position IN ({player_marks})",
                tuple(PLAYER_ROSTER_POSITIONS),
            ).fetchone()[0],
        )
        staff_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM team_daily_roster WHERE position IN ({staff_marks})",
                tuple(STAFF_ROSTER_POSITIONS),
            ).fetchone()[0],
        )
        unknown_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM team_daily_roster WHERE position NOT IN ({player_marks},{staff_marks})",
                (*PLAYER_ROSTER_POSITIONS, *STAFF_ROSTER_POSITIONS),
            ).fetchone()[0],
        )
        link_count = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM team_daily_roster
                WHERE position IN ('투수','포수','내야수','외야수')
                  AND EXISTS (SELECT 1 FROM player_basic WHERE player_basic.player_id = team_daily_roster.player_id)
                """,
            ).fetchone()[0],
        )
    actions.extend(
        [
            Action("classify roster player rows", player_count, "applied" if apply else "dry_run"),
            Action("classify roster staff rows", staff_count, "applied" if apply else "dry_run"),
            Action("classify roster unknown rows", unknown_count, "applied" if apply else "dry_run"),
            Action("backfill roster player_basic_id", link_count, "applied" if apply else "dry_run"),
        ],
    )


def _repair_game_player_refs(
    conn: sqlite3.Connection,
    table_name: str,
    id_column: str,
    name_column: str,
    actions: list[Action],
    apply: bool,
) -> None:
    if not {id_column, name_column} <= _columns(conn, table_name):
        return
    rows = conn.execute(
        f"""
        SELECT rowid, {id_column}, {name_column}
        FROM {table_name} AS t
        WHERE t.{id_column} IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM player_basic AS p WHERE p.player_id = t.{id_column})
        """,
    ).fetchall()
    remapped = 0
    nulled = 0
    if apply:
        for rowid, _bad_id, raw_name in rows:
            resolved_id = _unique_player_id_by_name(
                conn,
                _normalize_name(raw_name),
                position=_extract_position(raw_name),
            )
            if resolved_id:
                conn.execute(f"UPDATE {table_name} SET {id_column} = ? WHERE rowid = ?", (resolved_id, rowid))
                remapped += 1
            else:
                conn.execute(f"UPDATE {table_name} SET {id_column} = NULL WHERE rowid = ?", (rowid,))
                nulled += 1
    actions.append(
        Action(
            f"repair {table_name}.{id_column} remap",
            remapped if apply else len(rows),
            "applied" if apply else "dry_run",
        ),
    )
    actions.append(
        Action(
            f"repair {table_name}.{id_column} null unresolved",
            nulled if apply else len(rows),
            "applied" if apply else "dry_run",
        ),
    )


def _delete_invalid_matchups(conn: sqlite3.Connection, actions: list[Action], apply: bool) -> None:
    specs = (
        ("matchup_bvp", "batter_id"),
        ("matchup_bvp", "pitcher_id"),
        ("matchup_batter_splits", "player_id"),
        ("matchup_pitcher_splits", "player_id"),
        ("matchup_batter_team_split", "player_id"),
        ("matchup_pitcher_team_split", "player_id"),
        ("matchup_batter_stadium_split", "player_id"),
        ("matchup_batter_vs_starter", "player_id"),
    )
    for table_name, column in specs:
        if column not in _columns(conn, table_name):
            continue
        count = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM {table_name} AS t
                WHERE NOT EXISTS (SELECT 1 FROM player_basic AS p WHERE p.player_id = t.{column})
                """,
            ).fetchone()[0],
        )
        if apply and count:
            conn.execute(
                f"""
                DELETE FROM {table_name}
                WHERE NOT EXISTS (SELECT 1 FROM player_basic AS p WHERE p.player_id = {table_name}.{column})
                """,
            )
        actions.append(Action(f"delete invalid {table_name}.{column}", count, "applied" if apply else "dry_run"))


def _repair_movements(conn: sqlite3.Connection, actions: list[Action], apply: bool) -> None:
    required = {
        "id",
        "movement_date",
        "team_code",
        "player_name",
        "canonical_team_id",
        "player_basic_id",
        "resolution_status",
    }
    if not required <= _columns(conn, "player_movements"):
        return
    rows = conn.execute("SELECT id, movement_date, team_code, player_name FROM player_movements").fetchall()
    team_resolved = 0
    player_resolved = 0
    unresolved_player = 0
    unresolved_team = 0
    team_ids = {row[0] for row in conn.execute("SELECT team_id FROM teams").fetchall()}
    for movement_id, movement_date, raw_team, raw_name in rows:
        canonical_team: str | None = TEAM_NAME_TO_CODE.get(str(raw_team or "").strip(), str(raw_team or "").strip())
        if canonical_team not in team_ids:
            canonical_team = None
        year = int(str(movement_date)[:4]) if movement_date else None
        if canonical_team is None:
            canonical_team = _unique_team_by_player_history(conn, raw_name, year)
        if canonical_team:
            team_resolved += 1
        else:
            unresolved_team += 1
        player_id = (
            _unique_player_id_by_name(
                conn,
                _normalize_name(raw_name),
                canonical_team,
                year,
                _extract_position(raw_name),
            )
            if canonical_team
            else None
        )
        if player_id:
            player_resolved += 1
            status = "resolved"
        elif canonical_team:
            unresolved_player += 1
            status = "unresolved_player"
        else:
            status = "unresolved_team"
        if apply:
            conn.execute(
                """
                UPDATE player_movements
                SET canonical_team_id = ?, player_basic_id = ?, resolution_status = ?
                WHERE id = ?
                """,
                (canonical_team, player_id, status, movement_id),
            )
    actions.extend(
        [
            Action("resolve player_movements canonical_team_id", team_resolved, "applied" if apply else "dry_run"),
            Action("resolve player_movements player_basic_id", player_resolved, "applied" if apply else "dry_run"),
            Action("mark player_movements unresolved_player", unresolved_player, "applied" if apply else "dry_run"),
            Action("mark player_movements unresolved_team", unresolved_team, "applied" if apply else "dry_run"),
        ],
    )


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _column_def(row: sqlite3.Row) -> str:
    name = _quote(str(row["name"]))
    col_type = str(row["type"] or "")
    pieces = [name]
    if col_type:
        pieces.append(col_type)
    if row["pk"]:
        pieces.append("PRIMARY KEY")
    if row["notnull"] and not row["pk"]:
        pieces.append("NOT NULL")
    if row["dflt_value"] is not None:
        pieces.append(f"DEFAULT {row['dflt_value']}")
    return " ".join(pieces)


def _rebuild_table(conn: sqlite3.Connection, table_name: str, actions: list[Action], apply: bool) -> None:
    if not _table_exists(conn, table_name):
        return
    if not apply:
        actions.append(Action(f"rebuild {table_name}", 1, "dry_run"))
        return
    temp_name = f"{table_name}_integrity_new"
    conn.execute(f"DROP TABLE IF EXISTS {_quote(temp_name)}")
    columns = conn.execute(f"PRAGMA table_info({_quote(table_name)})").fetchall()
    column_defs = [_column_def(row) for row in columns]
    constraints = [*UNIQUE_CONSTRAINTS.get(table_name, ()), *FK_CONSTRAINTS.get(table_name, ())]
    create_sql = f"CREATE TABLE {_quote(temp_name)} (\n    " + ",\n    ".join([*column_defs, *constraints]) + "\n)"
    index_sql = [
        row[0]
        for row in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
            (table_name,),
        ).fetchall()
    ]
    conn.execute(create_sql)
    column_list = ", ".join(_quote(str(row["name"])) for row in columns)
    conn.execute(f"INSERT INTO {_quote(temp_name)} ({column_list}) SELECT {column_list} FROM {_quote(table_name)}")
    conn.execute(f"DROP TABLE {_quote(table_name)}")
    conn.execute(f"ALTER TABLE {_quote(temp_name)} RENAME TO {_quote(table_name)}")
    for sql in index_sql:
        conn.execute(sql)
    actions.append(Action(f"rebuild {table_name}", 1, "applied"))


def _repair_data(conn: sqlite3.Connection, apply: bool) -> list[Action]:
    actions: list[Action] = []
    _ensure_columns(conn, actions, apply)
    _repair_players(conn, actions, apply)
    _repair_roster(conn, actions, apply)
    _repair_game_player_refs(conn, "game_events", "batter_id", "batter_name", actions, apply)
    _repair_game_player_refs(conn, "game_events", "pitcher_id", "pitcher_name", actions, apply)
    _repair_game_player_refs(conn, "game_summary", "player_id", "player_name", actions, apply)
    _delete_invalid_matchups(conn, actions, apply)
    _repair_movements(conn, actions, apply)
    return actions


def _apply_schema(conn: sqlite3.Connection, apply: bool) -> list[Action]:
    actions: list[Action] = []
    for table_name in REBUILD_TABLES:
        _rebuild_table(conn, table_name, actions, apply)
    return actions


def repair(db_path: Path, *, apply: bool, schema: bool) -> list[Action]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if apply:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("BEGIN IMMEDIATE")
        actions = _repair_data(conn, apply)
        if schema:
            actions.extend(_apply_schema(conn, apply))
        if apply:
            conn.commit()
            conn.execute("PRAGMA foreign_keys = ON")
        return actions
    except (sqlite3.Error, RuntimeError, ValueError, TypeError, OSError):
        if apply:
            conn.rollback()
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair deletion anomaly integrity gaps in local SQLite.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--apply", action="store_true", help="Apply repairs. Default is dry-run.")
    parser.add_argument("--schema", action="store_true", help="Rebuild affected tables with declared FKs.")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup creation when applying.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if args.apply and not args.no_backup:
        backup_path = _backup(db_path)
        print(f"Backup created: {backup_path}")
    actions = repair(db_path, apply=args.apply, schema=args.schema)
    for action in actions:
        print(f"[{action.status}] {action.name}: rows={action.row_count}")


if __name__ == "__main__":
    main()
