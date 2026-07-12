#!/usr/bin/env python3
"""Conservatively resolve NULL player_id values in per-game tables.

The resolver only applies a group when the evidence narrows to exactly one
existing player_basic row. It never auto-registers placeholder players.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import csv
import os
import shutil
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL, SessionLocal

DEFAULT_OVERRIDES_CSV = PROJECT_ROOT / "data/player_id_overrides.csv"
DEFAULT_ROW_OVERRIDES_CSV = PROJECT_ROOT / "data/player_id_row_overrides.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data"
DEFAULT_TABLES = ("game_batting_stats", "game_pitching_stats", "game_lineups")
CANONICAL_TEAM_CODES = {
    "OB": "DB",
    "SK": "SSG",
    "WO": "KH",
    "NX": "KH",
    "HT": "KIA",
}


def _default_years() -> tuple[int, ...]:
    return tuple(range(2001, datetime.now().year + 1))


DEFAULT_YEARS = _default_years()


@dataclass(frozen=True)
class OverrideEntry:
    source_table: str
    year: int
    team_code: str
    player_name: str
    resolved_player_id: int
    reason: str = ""
    evidence_source: str = ""


@dataclass(frozen=True)
class RowOverrideEntry:
    source_table: str
    game_id: str
    appearance_seq: int
    team_code: str
    player_name: str
    resolved_player_id: int
    reason: str = ""
    evidence_source: str = ""


def canonical_team_code(team_code: str | None) -> str:
    return CANONICAL_TEAM_CODES.get(team_code or "", team_code or "")


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(db_url: str, output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(db_url)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_null_player_id_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup_path)
    return backup_path


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _table_columns(session, table_name: str) -> set[str]:
    if session.bind.dialect.name == "sqlite":
        return {row[1] for row in session.execute(text(f"PRAGMA table_info({table_name})")).fetchall()}
    rows = session.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            """,
        ),
        {"table_name": table_name},
    ).fetchall()
    return {str(row[0]) for row in rows}


def _load_alias_map(path: Path | None = None) -> dict[str, str]:
    alias_path = path or PROJECT_ROOT / "data/player_name_aliases.csv"
    if not alias_path.exists():
        return {}
    aliases: dict[str, str] = {}
    with alias_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            old = str(row.get("old_name") or "").strip()
            new = str(row.get("new_name") or "").strip()
            if old and new:
                aliases[old] = new
    return aliases


def load_overrides(path: Path) -> dict[tuple[str, int, str, str], OverrideEntry]:
    if not path.exists():
        return {}
    overrides: dict[tuple[str, int, str, str], OverrideEntry] = {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            table = str(row.get("source_table") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            team_code = str(row.get("team_code") or "").strip()
            raw_year = str(row.get("year") or "").strip()
            raw_player_id = str(row.get("resolved_player_id") or "").strip()
            if not table or not player_name or not team_code or not raw_year or not raw_player_id:
                continue
            try:
                year = int(raw_year)
                player_id = int(raw_player_id)
            except ValueError:
                continue
            entry = OverrideEntry(
                source_table=table,
                year=year,
                team_code=team_code,
                player_name=player_name,
                resolved_player_id=player_id,
                reason=str(row.get("reason") or "").strip(),
                evidence_source=str(row.get("evidence_source") or "").strip(),
            )
            overrides[(table, year, team_code, player_name)] = entry
    return overrides


def load_row_overrides(path: Path) -> dict[tuple[str, str, int, str, str], RowOverrideEntry]:
    if not path.exists():
        return {}
    overrides: dict[tuple[str, str, int, str, str], RowOverrideEntry] = {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            table = str(row.get("source_table") or "").strip()
            game_id = str(row.get("game_id") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            team_code = str(row.get("team_code") or "").strip()
            raw_seq = str(row.get("appearance_seq") or "").strip()
            raw_player_id = str(row.get("resolved_player_id") or "").strip()
            if not table or not game_id or not player_name or not team_code or not raw_seq or not raw_player_id:
                continue
            try:
                appearance_seq = int(raw_seq)
                player_id = int(raw_player_id)
            except ValueError:
                continue
            entry = RowOverrideEntry(
                source_table=table,
                game_id=game_id,
                appearance_seq=appearance_seq,
                team_code=team_code,
                player_name=player_name,
                resolved_player_id=player_id,
                reason=str(row.get("reason") or "").strip(),
                evidence_source=str(row.get("evidence_source") or "").strip(),
            )
            overrides[(table, game_id, appearance_seq, team_code, player_name)] = entry
    return overrides


def is_group_resolvable(candidate_ids: list[int]) -> bool:
    return len(set(candidate_ids)) == 1


def fetch_group_uniform_nos(
    session,
    *,
    table_name: str,
    year: int,
    team_code: str,
    player_name: str,
) -> list[str]:
    columns = _table_columns(session, table_name)
    if "uniform_no" not in columns or table_name == "game_lineups":
        return []
    rows = session.execute(
        text(
            f"""
            SELECT DISTINCT uniform_no
            FROM {table_name}
            WHERE player_id IS NULL
              AND substr(game_id, 1, 4) = :year
              AND COALESCE(team_code, '') = :team_code
              AND player_name = :player_name
              AND uniform_no IS NOT NULL
              AND trim(uniform_no) != ''
            """,
        ),
        {"year": str(year), "team_code": team_code, "player_name": player_name},
    ).fetchall()
    return sorted({str(row[0]).strip() for row in rows if str(row[0]).strip()})


def _existing_player_id(session, player_id: int) -> bool:
    return bool(
        session.execute(
            text("SELECT 1 FROM player_basic WHERE player_id = :player_id LIMIT 1"),
            {"player_id": player_id},
        ).first(),
    )


def _lookup_group_override(
    overrides: dict[tuple[str, int, str, str], OverrideEntry],
    *,
    table_name: str,
    season: int,
    team_code: str,
    player_name: str,
) -> OverrideEntry | None:
    canonical_team = canonical_team_code(team_code)
    for candidate_team in (team_code, canonical_team):
        override = overrides.get((table_name, int(season), candidate_team, player_name))
        if override:
            return override
    return None


def _existing_non_null_player_ids_for_group(
    session,
    *,
    table_name: str,
    year: int,
    team_code: str,
    player_name: str,
) -> list[int]:
    rows = session.execute(
        text(
            f"""
            SELECT DISTINCT player_id
            FROM {table_name}
            WHERE player_id IS NOT NULL
              AND substr(game_id, 1, 4) = :year
              AND COALESCE(team_code, '') = :team_code
              AND player_name = :player_name
            """,
        ),
        {"year": str(year), "team_code": team_code, "player_name": player_name},
    ).fetchall()
    return sorted({int(row[0]) for row in rows if row[0] is not None})


def _candidate_ids_from_season_table(
    session,
    *,
    season_table: str,
    season: int,
    team_code: str | None,
    player_name: str,
) -> list[int]:
    team_filter = "AND ps.team_code = :team_code" if team_code else ""
    params: dict[str, Any] = {"season": int(season), "player_name": player_name}
    if team_code:
        params["team_code"] = team_code
    rows = session.execute(
        text(
            f"""
            SELECT DISTINCT pb.player_id
            FROM {season_table} ps
            JOIN player_basic pb ON pb.player_id = ps.player_id
            WHERE ps.season = :season
              AND pb.name = :player_name
              {team_filter}
            """,
        ),
        params,
    ).fetchall()
    return sorted({int(row[0]) for row in rows})


def _filter_by_uniform(session, candidate_ids: list[int], uniform_nos: list[str]) -> list[int]:
    if len(candidate_ids) <= 1 or not uniform_nos:
        return candidate_ids
    stmt = (
        text(
            """
            SELECT player_id
            FROM player_basic
            WHERE player_id IN :candidate_ids
              AND CAST(uniform_no AS TEXT) IN :uniform_nos
            """,
        )
        .bindparams(bindparam("candidate_ids", expanding=True))
        .bindparams(bindparam("uniform_nos", expanding=True))
    )
    rows = session.execute(
        stmt,
        {
            "candidate_ids": candidate_ids,
            "uniform_nos": [str(no) for no in uniform_nos],
        },
    ).fetchall()
    return sorted({int(row[0]) for row in rows})


def _unique_player_basic_exact_name(session, player_name: str) -> list[int]:
    rows = session.execute(
        text(
            """
            SELECT player_id
            FROM player_basic
            WHERE name = :player_name
            """,
        ),
        {"player_name": player_name},
    ).fetchall()
    return sorted({int(row[0]) for row in rows})


def _resolve_via_override(session, override, existing_group_ids) -> dict[str, Any] | None:
    if not override:
        return None
    if not _existing_player_id(session, override.resolved_player_id):
        return {
            "candidate_ids": [],
            "resolution_method": "",
            "resolution_reason": "override_player_id_not_found_in_player_basic",
        }
    existing_set = set(existing_group_ids)
    if existing_set and existing_set != {int(override.resolved_player_id)}:
        return {
            "candidate_ids": [],
            "resolution_method": "",
            "resolution_reason": "override_conflicts_existing_group_ids",
        }
    return {
        "candidate_ids": [override.resolved_player_id],
        "resolution_method": "override_exact_group",
        "resolution_reason": override.reason,
    }


def _resolve_via_existing_group(existing_group_ids) -> dict[str, Any] | None:
    if is_group_resolvable(existing_group_ids):
        return {
            "candidate_ids": existing_group_ids,
            "resolution_method": "existing_game_group",
            "resolution_reason": "single_existing_non_null_group_player_id",
        }
    return None


def _resolve_via_season_preferred(
    session, table_name: str, season: int, canonical_team: str, canonical_name: str, uniform_nos: list[str]
) -> dict[str, Any] | None:
    preferred = (
        ("player_season_pitching",)
        if table_name == "game_pitching_stats"
        else ("player_season_batting", "player_season_pitching")
    )
    for season_table in preferred:
        candidate_ids = _candidate_ids_from_season_table(
            session, season_table=season_table, season=season, team_code=canonical_team, player_name=canonical_name
        )
        uniform_filtered = _filter_by_uniform(session, candidate_ids, uniform_nos)
        if is_group_resolvable(uniform_filtered):
            method = "uniform_filter" if uniform_filtered != candidate_ids else "season_team_name"
            return {"candidate_ids": uniform_filtered, "resolution_method": method, "resolution_reason": season_table}
        if candidate_ids and uniform_nos and not uniform_filtered:
            return {"candidate_ids": [], "resolution_method": "", "resolution_reason": "uniform_filter_no_match"}
    return None


def _resolve_via_season_without_team(session, canonical_name: str, uniform_nos: list[str]) -> dict[str, Any] | None:
    season_candidates: set[int] = set()
    for season_table in ("player_season_batting", "player_season_pitching"):
        season_candidates.update(
            _candidate_ids_from_season_table(
                session, season_table=season_table, season=None, team_code=None, player_name=canonical_name
            )
        )
    sorted_ids = sorted(season_candidates)
    uniform_filtered = _filter_by_uniform(session, sorted_ids, uniform_nos)
    if is_group_resolvable(uniform_filtered):
        method = "uniform_filter" if uniform_filtered != sorted_ids else "season_name_unique"
        return {
            "candidate_ids": uniform_filtered,
            "resolution_method": method,
            "resolution_reason": "season_without_team",
        }
    return (
        {"candidate_ids": sorted_ids, "resolution_method": "", "resolution_reason": "ambiguous_or_missing_candidate"}
        if sorted_ids
        else None
    )


def _resolve_via_exact_name(session, canonical_name: str) -> dict[str, Any] | None:
    candidates = _unique_player_basic_exact_name(session, canonical_name)
    if is_group_resolvable(candidates):
        return {
            "candidate_ids": candidates,
            "resolution_method": "unique_player_basic_exact_name",
            "resolution_reason": "single_exact_name_in_player_basic",
        }
    return None


def choose_candidate_ids(
    session,
    *,
    table_name: str,
    season: int,
    team_code: str,
    player_name: str,
    uniform_nos: list[str],
    alias_map: dict[str, str],
    overrides: dict[tuple[str, int, str, str], OverrideEntry],
) -> dict[str, Any]:
    canonical_team = canonical_team_code(team_code)
    existing_group_ids = _existing_non_null_player_ids_for_group(
        session, table_name=table_name, year=season, team_code=team_code, player_name=player_name
    )
    override = _lookup_group_override(
        overrides, table_name=table_name, season=season, team_code=team_code, player_name=player_name
    )
    canonical_name = alias_map.get(player_name, player_name)

    resolvers = [
        lambda: _resolve_via_override(session, override, existing_group_ids),
        lambda: _resolve_via_existing_group(existing_group_ids),
        lambda: _resolve_via_season_preferred(session, table_name, season, canonical_team, canonical_name, uniform_nos),
        lambda: _resolve_via_season_without_team(session, canonical_name, uniform_nos),
        lambda: _resolve_via_exact_name(session, canonical_name),
    ]
    for resolver in resolvers:
        result = resolver()
        if result is not None:
            return result
    return {"candidate_ids": [], "resolution_method": "", "resolution_reason": "ambiguous_or_missing_candidate"}


def update_null_player_ids_for_group(
    session,
    *,
    table_name: str,
    year: int,
    team_code: str,
    player_name: str,
    player_id: int,
    dry_run: bool,
) -> int:
    columns = _table_columns(session, table_name)
    conflict_guard = ""
    if table_name in DEFAULT_TABLES and "id" in columns:
        conflict_guard = f"""
          AND NOT EXISTS (
              SELECT 1
              FROM {table_name} existing
              WHERE existing.game_id = {table_name}.game_id
                AND existing.player_id = :player_id
                AND existing.id != {table_name}.id
          )
        """
    where_sql = f"""
        WHERE player_id IS NULL
          AND substr(game_id, 1, 4) = :year
          AND COALESCE(team_code, '') = :team_code
          AND player_name = :player_name
          {conflict_guard}
    """
    params = {
        "player_id": int(player_id),
        "year": str(year),
        "team_code": team_code,
        "player_name": player_name,
    }
    if dry_run:
        return int(session.execute(text(f"SELECT COUNT(*) FROM {table_name} {where_sql}"), params).scalar() or 0)
    result = session.execute(
        text(f"UPDATE {table_name} SET player_id = :player_id {where_sql}"),
        params,
    )
    return int(result.rowcount or 0)


def delete_duplicate_null_player_id_rows_for_group(
    session,
    *,
    table_name: str,
    year: int,
    team_code: str,
    player_name: str,
    player_id: int,
    dry_run: bool,
) -> int:
    columns = _table_columns(session, table_name)
    if table_name not in DEFAULT_TABLES or "id" not in columns:
        return 0
    where_sql = f"""
        WHERE player_id IS NULL
          AND substr(game_id, 1, 4) = :year
          AND COALESCE(team_code, '') = :team_code
          AND player_name = :player_name
          AND EXISTS (
              SELECT 1
              FROM {table_name} existing
              WHERE existing.game_id = {table_name}.game_id
                AND existing.player_id = :player_id
                AND existing.id != {table_name}.id
          )
    """
    params = {
        "player_id": int(player_id),
        "year": str(year),
        "team_code": team_code,
        "player_name": player_name,
    }
    if dry_run:
        return int(session.execute(text(f"SELECT COUNT(*) FROM {table_name} {where_sql}"), params).scalar() or 0)
    result = session.execute(text(f"DELETE FROM {table_name} {where_sql}"), params)
    return int(result.rowcount or 0)


def _row_override_where_sql(table_name: str, columns: set[str]) -> str:
    appearance_filter = "AND appearance_seq = :appearance_seq" if "appearance_seq" in columns else ""
    return f"""
        WHERE player_id IS NULL
          AND game_id = :game_id
          AND COALESCE(team_code, '') = :team_code
          AND player_name = :player_name
          {appearance_filter}
    """


def _row_override_params(entry: RowOverrideEntry) -> dict[str, Any]:
    return {
        "player_id": int(entry.resolved_player_id),
        "game_id": entry.game_id,
        "appearance_seq": int(entry.appearance_seq),
        "team_code": entry.team_code,
        "player_name": entry.player_name,
    }


def _row_override_duplicate_count(session, *, table_name: str, entry: RowOverrideEntry) -> int:
    columns = _table_columns(session, table_name)
    if "id" not in columns:
        return 0
    where_sql = _row_override_where_sql(table_name, columns)
    params = _row_override_params(entry)
    return int(
        session.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                {where_sql}
                  AND EXISTS (
                      SELECT 1
                      FROM {table_name} existing
                      WHERE existing.game_id = {table_name}.game_id
                        AND existing.player_id = :player_id
                        AND existing.id != {table_name}.id
                  )
                """,
            ),
            params,
        ).scalar()
        or 0,
    )


def apply_row_override(
    session,
    *,
    entry: RowOverrideEntry,
    dry_run: bool,
    delete_duplicates: bool,
) -> dict[str, Any]:
    columns = _table_columns(session, entry.source_table)
    if not columns:
        return {
            "updated_rows": 0,
            "duplicate_null_rows": 0,
            "resolution_reason": "table_not_found",
        }
    if not _existing_player_id(session, entry.resolved_player_id):
        return {
            "updated_rows": 0,
            "duplicate_null_rows": 0,
            "resolution_reason": "override_player_id_not_found_in_player_basic",
        }

    where_sql = _row_override_where_sql(entry.source_table, columns)
    params = _row_override_params(entry)
    duplicate_rows = _row_override_duplicate_count(session, table_name=entry.source_table, entry=entry)
    if duplicate_rows:
        if dry_run or not delete_duplicates:
            return {
                "updated_rows": 0,
                "duplicate_null_rows": duplicate_rows,
                "resolution_reason": "row_override_duplicate_existing_game_player",
            }
        deleted = session.execute(text(f"DELETE FROM {entry.source_table} {where_sql}"), params)
        return {
            "updated_rows": 0,
            "duplicate_null_rows": int(deleted.rowcount or 0),
            "resolution_reason": "row_override_duplicate_deleted",
        }

    if dry_run:
        updated = session.execute(text(f"SELECT COUNT(*) FROM {entry.source_table} {where_sql}"), params).scalar()
        return {
            "updated_rows": int(updated or 0),
            "duplicate_null_rows": 0,
            "resolution_reason": entry.reason,
        }

    result = session.execute(
        text(f"UPDATE {entry.source_table} SET player_id = :player_id {where_sql}"),
        params,
    )
    return {
        "updated_rows": int(result.rowcount or 0),
        "duplicate_null_rows": 0,
        "resolution_reason": entry.reason,
    }


def _collect_null_groups(session, *, tables: tuple[str, ...], years: tuple[int, ...]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    years_as_text = [str(year) for year in years]
    for table_name in tables:
        stmt = text(
            f"""
                SELECT
                    substr(game_id, 1, 4) AS year,
                    COALESCE(team_code, '') AS team_code,
                    player_name,
                    COUNT(*) AS row_count
                FROM {table_name}
                WHERE player_id IS NULL
                  AND substr(game_id, 1, 4) IN :years
                GROUP BY substr(game_id, 1, 4), COALESCE(team_code, ''), player_name
                ORDER BY year, team_code, player_name
                """,
        ).bindparams(bindparam("years", expanding=True))
        groups.extend(
            {
                "table_name": table_name,
                "year": int(row["year"]),
                "team_code": str(row["team_code"] or ""),
                "player_name": str(row["player_name"] or ""),
                "row_count": int(row["row_count"] or 0),
            }
            for row in session.execute(stmt, {"years": years_as_text}).mappings()
        )
    return groups


RESOLVED_FIELDNAMES = [
    "table_name",
    "year",
    "team_code",
    "raw_team_code",
    "canonical_team_code",
    "player_name",
    "game_id",
    "appearance_seq",
    "row_count",
    "uniform_nos",
    "candidate_ids",
    "resolution_method",
    "resolution_reason",
    "resolved_player_id",
    "updated_rows",
    "duplicate_null_rows",
]


def _process_row_override_entry(
    session, entry, *, apply, delete_duplicates, years, tables, total_updated, resolved_rows
):
    if entry.source_table not in tables:
        return total_updated
    try:
        entry_year = int(entry.game_id[:4])
    except ValueError:
        return total_updated
    if entry_year not in years:
        return total_updated
    result = apply_row_override(session, entry=entry, dry_run=not apply, delete_duplicates=delete_duplicates)
    updated = int(result["updated_rows"])
    duplicate_rows = int(result["duplicate_null_rows"])
    if updated or duplicate_rows:
        total_updated += updated
        resolved_rows.append(
            {
                "table_name": entry.source_table,
                "year": entry_year,
                "team_code": entry.team_code,
                "raw_team_code": entry.team_code,
                "canonical_team_code": canonical_team_code(entry.team_code),
                "player_name": entry.player_name,
                "game_id": entry.game_id,
                "appearance_seq": entry.appearance_seq,
                "row_count": 1,
                "uniform_nos": "",
                "candidate_ids": str(entry.resolved_player_id),
                "resolution_method": "row_override",
                "resolution_reason": result["resolution_reason"],
                "resolved_player_id": entry.resolved_player_id,
                "updated_rows": updated,
                "duplicate_null_rows": duplicate_rows,
            }
        )
    return total_updated


def _process_null_group(
    session, group, *, alias_map, overrides, apply, delete_duplicates, resolved_rows, unresolved_rows
) -> int:
    uniforms = fetch_group_uniform_nos(
        session,
        table_name=group["table_name"],
        year=group["year"],
        team_code=group["team_code"],
        player_name=group["player_name"],
    )
    result = choose_candidate_ids(
        session,
        table_name=group["table_name"],
        season=group["year"],
        team_code=group["team_code"],
        player_name=group["player_name"],
        uniform_nos=uniforms,
        alias_map=alias_map,
        overrides=overrides,
    )
    candidate_ids = [int(pid) for pid in result["candidate_ids"]]
    base_row = {
        **group,
        "raw_team_code": group["team_code"],
        "canonical_team_code": canonical_team_code(group["team_code"]),
        "game_id": "",
        "appearance_seq": "",
        "uniform_nos": ",".join(uniforms),
        "candidate_ids": ",".join(str(pid) for pid in candidate_ids),
        "resolution_method": result.get("resolution_method", ""),
        "resolution_reason": result.get("resolution_reason", ""),
    }
    if is_group_resolvable(candidate_ids):
        duplicate_rows = delete_duplicate_null_player_id_rows_for_group(
            session,
            table_name=group["table_name"],
            year=group["year"],
            team_code=group["team_code"],
            player_name=group["player_name"],
            player_id=candidate_ids[0],
            dry_run=not (apply and delete_duplicates),
        )
        updated = update_null_player_ids_for_group(
            session,
            table_name=group["table_name"],
            year=group["year"],
            team_code=group["team_code"],
            player_name=group["player_name"],
            player_id=candidate_ids[0],
            dry_run=not apply,
        )
        resolved_rows.append(
            {
                **base_row,
                "resolved_player_id": candidate_ids[0],
                "updated_rows": updated,
                "duplicate_null_rows": duplicate_rows,
            }
        )
        return updated
    unresolved_rows.append({**base_row, "resolved_player_id": "", "updated_rows": 0, "duplicate_null_rows": 0})
    return 0


def resolve_null_player_ids(
    *,
    years: tuple[int, ...],
    tables: tuple[str, ...],
    overrides_csv: Path,
    output_dir: Path,
    apply: bool,
    row_overrides_csv: Path | None = None,
    backup: bool = True,
    db_url: str | None = None,
    delete_duplicates: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    alias_map = _load_alias_map()
    overrides = load_overrides(overrides_csv)
    row_overrides = load_row_overrides(row_overrides_csv or DEFAULT_ROW_OVERRIDES_CSV)
    resolved_rows: list[dict[str, Any]] = []
    unresolved_rows: list[dict[str, Any]] = []
    total_updated = 0

    backup_path = None
    active_db_url = db_url or DATABASE_URL
    if apply and backup:
        backup_path = _backup_sqlite_database(active_db_url, output_dir)

    engine = None
    session_factory = SessionLocal
    if db_url:
        engine = create_engine(db_url)
        session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    try:
        with session_factory() as session:
            for entry in row_overrides.values():
                total_updated = _process_row_override_entry(
                    session,
                    entry,
                    apply=apply,
                    delete_duplicates=delete_duplicates,
                    years=years,
                    tables=tables,
                    total_updated=total_updated,
                    resolved_rows=resolved_rows,
                )
            for group in _collect_null_groups(session, tables=tables, years=years):
                total_updated += _process_null_group(
                    session,
                    group,
                    alias_map=alias_map,
                    overrides=overrides,
                    apply=apply,
                    delete_duplicates=delete_duplicates,
                    resolved_rows=resolved_rows,
                    unresolved_rows=unresolved_rows,
                )
            if apply:
                session.commit()
            else:
                session.rollback()
    finally:
        if engine is not None:
            engine.dispose()

    resolved_csv = output_dir / f"null_player_id_conservative_resolved_{stamp}.csv"
    unresolved_csv = output_dir / f"null_player_id_conservative_unresolved_{stamp}.csv"
    _write_csv(resolved_csv, resolved_rows, RESOLVED_FIELDNAMES)
    _write_csv(unresolved_csv, unresolved_rows, RESOLVED_FIELDNAMES)
    return {
        "dry_run": not apply,
        "resolved_groups": len(resolved_rows),
        "unresolved_groups": len(unresolved_rows),
        "updated_rows": total_updated,
        "duplicate_null_rows": sum(int(row["duplicate_null_rows"]) for row in resolved_rows),
        "resolved_csv": str(resolved_csv),
        "unresolved_csv": str(unresolved_csv),
        "backup_path": str(backup_path) if backup_path else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Conservatively fill NULL player_id values.")
    parser.add_argument("--oci", action="store_true", help="Use OCI_DB_URL instead of local DATABASE_URL.")
    parser.add_argument("--db-url", default=None, help="Explicit database URL. Overrides --oci and local DATABASE_URL.")
    parser.add_argument(
        "--years",
        default=",".join(str(year) for year in DEFAULT_YEARS),
        help="Comma-separated years to inspect.",
    )
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES), help="Comma-separated table names.")
    parser.add_argument("--overrides-csv", default=str(DEFAULT_OVERRIDES_CSV), help="Manual override CSV path.")
    parser.add_argument(
        "--row-overrides-csv",
        default=str(DEFAULT_ROW_OVERRIDES_CSV),
        help="Manual row-level override CSV path.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for report CSV files.")
    parser.add_argument("--apply", action="store_true", help="Persist updates. Default is dry-run only.")
    parser.add_argument(
        "--delete-duplicates",
        action="store_true",
        help="With --apply, delete NULL rows that duplicate an existing resolved row.",
    )
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    load_dotenv()
    args = parse_args()
    db_url = args.db_url or (os.getenv("OCI_DB_URL") if args.oci else None)
    if args.oci and not db_url:
        raise SystemExit("OCI_DB_URL is required with --oci")
    years = tuple(int(part.strip()) for part in args.years.split(",") if part.strip())
    tables = tuple(part.strip() for part in args.tables.split(",") if part.strip())
    result = resolve_null_player_ids(
        years=years,
        tables=tables,
        overrides_csv=Path(args.overrides_csv),
        row_overrides_csv=Path(args.row_overrides_csv),
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
        backup=not args.no_backup,
        db_url=db_url,
        delete_duplicates=bool(args.delete_duplicates),
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(
        "[%s] resolved_groups=%s unresolved_groups=%s updated_rows=%s duplicate_null_rows=%s",
        mode,
        result["resolved_groups"],
        result["unresolved_groups"],
        result["updated_rows"],
        result["duplicate_null_rows"],
    )
    if result["backup_path"]:
        logger.info(f"[BACKUP] {result['backup_path']}")
    logger.info(f"[REPORT] resolved={result['resolved_csv']}")
    logger.info(f"[REPORT] unresolved={result['unresolved_csv']}")


if __name__ == "__main__":
    main()
