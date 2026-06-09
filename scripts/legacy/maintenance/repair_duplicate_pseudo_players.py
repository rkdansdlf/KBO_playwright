#!/usr/bin/env python3
"""Conservatively merge duplicate temporary player_basic rows.

Temporary IDs are local profiles with player_id >= 900000. This tool only
merges rows when evidence narrows duplicate names to one team and at most one
uniform number. Ambiguous duplicate names are reported and left untouched.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, and_, create_engine, inspect, select

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL  # noqa: E402
from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment  # noqa: E402

PSEUDO_MIN_PLAYER_ID = 900000
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "repair_duplicate_pseudo_players"
TIMESTAMP_COLUMNS = {"created_at", "updated_at"}
CONFLICT_POLICY_SKIP_GROUP = "skip-group"
CONFLICT_POLICY_DELETE_SOURCE_DUPLICATES = "delete-source-duplicates"
CONFLICT_POLICIES = {
    CONFLICT_POLICY_SKIP_GROUP,
    CONFLICT_POLICY_DELETE_SOURCE_DUPLICATES,
}

MERGEABLE_FIELDNAMES = [
    "name",
    "team_key",
    "uniform_no",
    "target_player_id",
    "source_player_ids",
    "player_ids",
    "reference_rows",
    "reason",
]
UNRESOLVED_FIELDNAMES = [
    "player_id",
    "name",
    "team",
    "uniform_no",
    "evidence_teams",
    "evidence_uniforms",
    "reference_rows",
    "reason",
]
CONFLICT_FIELDNAMES = ["table_name", "name", "target_player_id", "source_player_ids", "key", "reason"]


@dataclass(frozen=True)
class ReferenceSpec:
    table_name: str
    player_column: str = "player_id"
    unique_columns: tuple[str, ...] | None = None
    team_column: str | None = None
    uniform_column: str | None = None
    season_column: str | None = None
    year_column: str | None = None
    game_id_column: str | None = None


REFERENCE_SPECS = (
    ReferenceSpec(
        "game_batting_stats",
        unique_columns=("game_id", "player_id", "appearance_seq"),
        team_column="team_code",
        uniform_column="uniform_no",
        game_id_column="game_id",
    ),
    ReferenceSpec(
        "game_pitching_stats",
        unique_columns=("game_id", "player_id", "appearance_seq"),
        team_column="team_code",
        uniform_column="uniform_no",
        game_id_column="game_id",
    ),
    ReferenceSpec(
        "game_lineups",
        team_column="team_code",
        uniform_column="uniform_no",
        game_id_column="game_id",
    ),
    ReferenceSpec("game_summary"),
    ReferenceSpec("game_events", player_column="batter_id"),
    ReferenceSpec("game_events", player_column="pitcher_id"),
    ReferenceSpec(
        "matchup_bvp",
        player_column="batter_id",
        unique_columns=("batter_id", "pitcher_id"),
    ),
    ReferenceSpec(
        "matchup_bvp",
        player_column="pitcher_id",
        unique_columns=("batter_id", "pitcher_id"),
    ),
    ReferenceSpec(
        "matchup_batter_splits",
        unique_columns=("player_id", "season_year", "split_type"),
        year_column="season_year",
    ),
    ReferenceSpec(
        "matchup_pitcher_splits",
        unique_columns=("player_id", "season_year", "split_type"),
        year_column="season_year",
    ),
    ReferenceSpec(
        "matchup_batter_team_split",
        unique_columns=("season_year", "league_type_code", "player_id", "team_code", "opponent_team_code"),
        team_column="team_code",
        year_column="season_year",
    ),
    ReferenceSpec(
        "matchup_pitcher_team_split",
        unique_columns=("season_year", "league_type_code", "player_id", "team_code", "opponent_team_code"),
        team_column="team_code",
        year_column="season_year",
    ),
    ReferenceSpec(
        "matchup_batter_stadium_split",
        unique_columns=("season_year", "league_type_code", "player_id", "team_code", "stadium_name"),
        team_column="team_code",
        year_column="season_year",
    ),
    ReferenceSpec(
        "matchup_batter_vs_starter",
        unique_columns=("season_year", "league_type_code", "player_id", "pitcher_name"),
        year_column="season_year",
    ),
    ReferenceSpec(
        "player_season_batting",
        unique_columns=("player_id", "season", "league", "level"),
        team_column="team_code",
        season_column="season",
    ),
    ReferenceSpec(
        "player_season_pitching",
        unique_columns=("player_id", "season", "league", "level"),
        team_column="team_code",
        season_column="season",
    ),
    ReferenceSpec(
        "player_season_fielding",
        unique_columns=("player_id", "team_id", "year", "position_id"),
        team_column="team_id",
        year_column="year",
    ),
    ReferenceSpec(
        "player_season_baserunning",
        unique_columns=("player_id", "team_id", "year"),
        team_column="team_id",
        year_column="year",
    ),
    ReferenceSpec(
        "players",
        player_column="player_basic_id",
        unique_columns=("player_basic_id",),
    ),
    ReferenceSpec(
        "team_daily_roster",
        player_column="player_basic_id",
        team_column="team_code",
        uniform_column="back_number",
    ),
    ReferenceSpec(
        "player_movements",
        player_column="player_basic_id",
        team_column="canonical_team_id",
    ),
)


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(db_url: str, output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(db_url)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_pseudo_player_merge_{stamp}"
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


def _parse_id_list(value: Any) -> list[int]:
    return [int(part.strip()) for part in str(value or "").split(",") if part.strip()]


def _load_mergeable_worklist(path: Path) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for line_number, row in enumerate(reader, start=2):
            try:
                target_id = int(str(row.get("target_player_id") or "").strip())
                source_ids = _parse_id_list(row.get("source_player_ids"))
                player_ids = _parse_id_list(row.get("player_ids"))
                reference_rows = int(str(row.get("reference_rows") or "0").strip() or "0")
            except ValueError as exc:
                raise RuntimeError(f"Invalid mergeable worklist row {line_number} in {path}") from exc

            if not source_ids:
                source_ids = [player_id for player_id in player_ids if player_id != target_id]
            if not source_ids:
                raise RuntimeError(f"Missing source_player_ids in mergeable worklist row {line_number} in {path}")

            ordered_player_ids = []
            for player_id in [target_id, *source_ids, *player_ids]:
                if player_id not in ordered_player_ids:
                    ordered_player_ids.append(player_id)

            groups.append(
                {
                    "name": row.get("name") or "",
                    "team_key": row.get("team_key") or "",
                    "uniform_no": row.get("uniform_no") or "",
                    "target_player_id": target_id,
                    "source_player_ids": source_ids,
                    "player_ids": ordered_player_ids,
                    "reference_rows": reference_rows,
                    "reason": row.get("reason") or "reviewed_mergeable_worklist",
                }
            )
    return groups


def _load_conflict_worklist(path: Path) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for line_number, row in enumerate(reader, start=2):
            try:
                target_id = int(str(row.get("target_player_id") or "").strip())
                source_ids = _parse_id_list(row.get("source_player_ids"))
            except ValueError as exc:
                raise RuntimeError(f"Invalid conflict worklist row {line_number} in {path}") from exc
            if not source_ids:
                raise RuntimeError(f"Missing source_player_ids in conflict worklist row {line_number} in {path}")
            conflicts.append(
                {
                    "table_name": row.get("table_name") or "",
                    "name": row.get("name") or "",
                    "target_player_id": target_id,
                    "source_player_ids": ",".join(str(player_id) for player_id in source_ids),
                    "key": row.get("key") or "",
                    "reason": row.get("reason") or "conflicting_duplicate_reference_payload",
                }
            )
    return conflicts


def _merge_conflict_reports(*conflict_sets: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for conflicts in conflict_sets:
        for conflict in conflicts:
            key = (
                str(conflict.get("table_name") or ""),
                str(conflict.get("target_player_id") or ""),
                str(conflict.get("source_player_ids") or ""),
                str(conflict.get("key") or ""),
                str(conflict.get("reason") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(conflict)
    return merged


def _table_exists(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _load_tables(conn) -> dict[str, Table]:
    inspector = inspect(conn)
    metadata = MetaData()
    table_names = ["player_basic", *[spec.table_name for spec in REFERENCE_SPECS]]
    tables: dict[str, Table] = {}
    for table_name in table_names:
        if _table_exists(inspector, table_name):
            tables[table_name] = Table(table_name, metadata, autoload_with=conn)
    return tables


def _available_reference_specs(tables: dict[str, Table]) -> list[ReferenceSpec]:
    specs: list[ReferenceSpec] = []
    for spec in REFERENCE_SPECS:
        table = tables.get(spec.table_name)
        if table is None or spec.player_column not in table.c:
            continue
        specs.append(spec)
    return specs


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_uniform(value: Any) -> str:
    return _clean_text(value)


def _normalize_team(value: Any, season_year: int | None = None) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    resolved = resolve_team_code(raw, season_year)
    if not resolved:
        resolved = team_code_from_game_id_segment(raw, season_year)
    return str(resolved or raw).strip().upper()


def _season_from_row(row: dict[str, Any], spec: ReferenceSpec) -> int | None:
    for column_name in (spec.season_column, spec.year_column):
        if column_name and row.get(column_name) not in (None, ""):
            try:
                return int(row[column_name])
            except (TypeError, ValueError):
                return None
    if spec.game_id_column:
        game_id = str(row.get(spec.game_id_column) or "")
        if len(game_id) >= 4 and game_id[:4].isdigit():
            return int(game_id[:4])
    return None


def _load_pseudo_players(conn, tables: dict[str, Table]) -> list[dict[str, Any]]:
    player_basic = tables["player_basic"]
    rows = conn.execute(select(player_basic).where(player_basic.c.player_id >= PSEUDO_MIN_PLAYER_ID)).mappings()
    return [dict(row) for row in rows]


def _load_usage_evidence(
    conn,
    tables: dict[str, Table],
    player_ids: list[int],
) -> dict[int, dict[str, Any]]:
    evidence = {int(player_id): {"teams": set(), "uniforms": set(), "reference_rows": 0} for player_id in player_ids}
    if not player_ids:
        return evidence

    for spec in _available_reference_specs(tables):
        table = tables[spec.table_name]
        rows = conn.execute(select(table).where(table.c[spec.player_column].in_(player_ids))).mappings()
        for raw_row in rows:
            row = dict(raw_row)
            player_id = row.get(spec.player_column)
            if player_id is None:
                continue
            player_id = int(player_id)
            target = evidence.setdefault(
                player_id,
                {"teams": set(), "uniforms": set(), "reference_rows": 0},
            )
            target["reference_rows"] += 1
            season_year = _season_from_row(row, spec)
            if spec.team_column and spec.team_column in table.c:
                team = _normalize_team(row.get(spec.team_column), season_year)
                if team:
                    target["teams"].add(team)
            if spec.uniform_column and spec.uniform_column in table.c:
                uniform = _normalize_uniform(row.get(spec.uniform_column))
                if uniform:
                    target["uniforms"].add(uniform)
    return evidence


def _player_profile_evidence(row: dict[str, Any]) -> tuple[str, str]:
    return _normalize_team(row.get("team")), _normalize_uniform(row.get("uniform_no"))


def collect_merge_plan(conn, tables: dict[str, Table]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pseudo_rows = _load_pseudo_players(conn, tables)
    duplicate_names = {
        name
        for name, count in Counter(_clean_text(row.get("name")) for row in pseudo_rows).items()
        if name and count > 1
    }
    evidence = _load_usage_evidence(
        conn,
        tables,
        [int(row["player_id"]) for row in pseudo_rows if row.get("player_id") is not None],
    )

    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    unresolved: list[dict[str, Any]] = []
    for row in pseudo_rows:
        player_id = int(row["player_id"])
        name = _clean_text(row.get("name"))
        if name not in duplicate_names:
            continue
        profile_team, profile_uniform = _player_profile_evidence(row)
        player_evidence = evidence.get(player_id, {"teams": set(), "uniforms": set(), "reference_rows": 0})
        teams = set(player_evidence["teams"])
        uniforms = set(player_evidence["uniforms"])
        if profile_team:
            teams.add(profile_team)
        if profile_uniform:
            uniforms.add(profile_uniform)

        base_report = {
            "player_id": player_id,
            "name": name,
            "team": row.get("team"),
            "uniform_no": row.get("uniform_no"),
            "evidence_teams": ",".join(sorted(teams)),
            "evidence_uniforms": ",".join(sorted(uniforms)),
            "reference_rows": int(player_evidence["reference_rows"]),
        }
        if len(teams) > 1:
            unresolved.append({**base_report, "reason": "conflicting_team_evidence"})
            continue
        if len(uniforms) > 1:
            unresolved.append({**base_report, "reason": "conflicting_uniform_evidence"})
            continue
        team = next(iter(teams), "")
        uniform = next(iter(uniforms), "")
        if not team:
            unresolved.append({**base_report, "reason": "missing_team_evidence"})
            continue
        buckets[(name, team)].append(
            {
                **row,
                "_team_key": team,
                "_uniform_key": uniform,
                "_reference_rows": int(player_evidence["reference_rows"]),
            }
        )

    groups: list[dict[str, Any]] = []
    for (name, team), rows in sorted(buckets.items()):
        if len(rows) < 2:
            continue
        uniforms = sorted({_clean_text(row.get("_uniform_key")) for row in rows if row.get("_uniform_key")})
        if len(uniforms) > 1:
            for row in rows:
                unresolved.append(
                    {
                        "player_id": row["player_id"],
                        "name": name,
                        "team": row.get("team"),
                        "uniform_no": row.get("uniform_no"),
                        "evidence_teams": team,
                        "evidence_uniforms": ",".join(uniforms),
                        "reference_rows": row.get("_reference_rows", 0),
                        "reason": "group_conflicting_uniform_evidence",
                    }
                )
            continue
        player_ids = sorted(int(row["player_id"]) for row in rows)
        target_id = player_ids[0]
        source_ids = player_ids[1:]
        groups.append(
            {
                "name": name,
                "team_key": team,
                "uniform_no": uniforms[0] if uniforms else "",
                "target_player_id": target_id,
                "source_player_ids": source_ids,
                "player_ids": player_ids,
                "reference_rows": sum(int(row.get("_reference_rows") or 0) for row in rows),
                "reason": "single_team_single_uniform_evidence",
            }
        )
    return groups, unresolved


def _normalized_compare_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    text = str(value).strip()
    if not text:
        return ""
    if text[0] in "[{":
        try:
            return json.dumps(json.loads(text), ensure_ascii=False, sort_keys=True)
        except json.JSONDecodeError:
            return text
    return text


def _comparison_payload(row: dict[str, Any], table: Table, player_column: str) -> dict[str, str]:
    ignored = {"id", player_column, *TIMESTAMP_COLUMNS}
    return {
        column.name: _normalized_compare_value(row.get(column.name))
        for column in table.columns
        if column.name not in ignored
    }


def _unique_key(
    row: dict[str, Any], unique_columns: tuple[str, ...], player_column: str, target_id: int
) -> tuple[str, ...]:
    values = []
    for column_name in unique_columns:
        value = target_id if column_name == player_column else row.get(column_name)
        values.append(_normalized_compare_value(value))
    return tuple(values)


def _where_row_identity(table: Table, row: dict[str, Any], fallback_columns: tuple[str, ...]):
    if "id" in table.c and row.get("id") is not None:
        return table.c.id == row["id"]
    clauses = []
    for column_name in fallback_columns:
        column = table.c[column_name]
        value = row.get(column_name)
        clauses.append(column.is_(None) if value is None else column == value)
    return and_(*clauses)


def _unique_columns_available(table: Table, spec: ReferenceSpec) -> tuple[str, ...] | None:
    if not spec.unique_columns:
        return None
    if all(column_name in table.c for column_name in spec.unique_columns):
        return spec.unique_columns
    return None


def detect_group_conflicts(
    conn,
    tables: dict[str, Table],
    group: dict[str, Any],
) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    target_id = int(group["target_player_id"])
    player_ids = [target_id, *[int(pid) for pid in group["source_player_ids"]]]
    for spec in _available_reference_specs(tables):
        table = tables[spec.table_name]
        unique_columns = _unique_columns_available(table, spec)
        if not unique_columns:
            continue
        rows = [
            dict(row)
            for row in conn.execute(select(table).where(table.c[spec.player_column].in_(player_ids))).mappings()
        ]
        rows_by_key: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            rows_by_key[_unique_key(row, unique_columns, spec.player_column, target_id)].append(row)
        for key, key_rows in rows_by_key.items():
            if len(key_rows) < 2:
                continue
            first_payload = _comparison_payload(key_rows[0], table, spec.player_column)
            if all(_comparison_payload(row, table, spec.player_column) == first_payload for row in key_rows[1:]):
                continue
            conflicts.append(
                {
                    "table_name": spec.table_name,
                    "name": group["name"],
                    "target_player_id": target_id,
                    "source_player_ids": ",".join(str(pid) for pid in group["source_player_ids"]),
                    "key": "|".join(key),
                    "reason": "conflicting_duplicate_reference_payload",
                }
            )
    return conflicts


def collect_conflicts(conn, tables: dict[str, Table], groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for group in groups:
        conflicts.extend(detect_group_conflicts(conn, tables, group))
    return conflicts


def _group_conflict_key(group: dict[str, Any]) -> tuple[int, tuple[int, ...]]:
    return (
        int(group["target_player_id"]),
        tuple(sorted(int(pid) for pid in group["source_player_ids"])),
    )


def _conflicted_group_keys(conflicts: list[dict[str, Any]]) -> set[tuple[int, tuple[int, ...]]]:
    keys: set[tuple[int, tuple[int, ...]]] = set()
    for conflict in conflicts:
        source_ids = tuple(
            sorted(
                int(part.strip()) for part in str(conflict.get("source_player_ids") or "").split(",") if part.strip()
            )
        )
        if not source_ids:
            continue
        keys.add((int(conflict["target_player_id"]), source_ids))
    return keys


def _apply_unique_reference_spec(
    conn,
    table: Table,
    spec: ReferenceSpec,
    *,
    target_id: int,
    source_ids: list[int],
    conflict_policy: str,
) -> tuple[int, int, int]:
    unique_columns = _unique_columns_available(table, spec)
    if not unique_columns:
        return 0, 0, 0

    all_ids = [target_id, *source_ids]
    rows = [dict(row) for row in conn.execute(select(table).where(table.c[spec.player_column].in_(all_ids))).mappings()]
    rows.sort(key=lambda row: (0 if int(row[spec.player_column]) == target_id else 1, row.get("id") or 0))
    seen: dict[tuple[str, ...], dict[str, Any]] = {}
    updated = 0
    deleted = 0
    deleted_conflicting = 0
    for row in rows:
        key = _unique_key(row, unique_columns, spec.player_column, target_id)
        existing = seen.get(key)
        if existing is not None:
            source_payload = _comparison_payload(row, table, spec.player_column)
            target_payload = _comparison_payload(existing, table, spec.player_column)
            if source_payload != target_payload:
                if conflict_policy == CONFLICT_POLICY_DELETE_SOURCE_DUPLICATES:
                    conn.execute(table.delete().where(_where_row_identity(table, row, unique_columns)))
                    deleted += 1
                    deleted_conflicting += 1
                    continue
                raise RuntimeError(f"conflicting duplicate row in {spec.table_name}: {'|'.join(key)}")
            conn.execute(table.delete().where(_where_row_identity(table, row, unique_columns)))
            deleted += 1
            continue
        seen[key] = row
        if int(row[spec.player_column]) != target_id:
            conn.execute(
                table.update()
                .where(_where_row_identity(table, row, unique_columns))
                .values(**{spec.player_column: target_id})
            )
            updated += 1
    return updated, deleted, deleted_conflicting


def _apply_direct_reference_spec(
    conn,
    table: Table,
    spec: ReferenceSpec,
    *,
    target_id: int,
    source_ids: list[int],
) -> int:
    result = conn.execute(
        table.update().where(table.c[spec.player_column].in_(source_ids)).values(**{spec.player_column: target_id})
    )
    return int(result.rowcount or 0)


def apply_group(
    conn,
    tables: dict[str, Table],
    group: dict[str, Any],
    *,
    conflict_policy: str,
) -> dict[str, int]:
    target_id = int(group["target_player_id"])
    source_ids = [int(pid) for pid in group["source_player_ids"]]
    stats = {
        "updated_rows": 0,
        "deleted_duplicate_rows": 0,
        "deleted_conflicting_reference_rows": 0,
        "deleted_player_basic_rows": 0,
    }

    for spec in _available_reference_specs(tables):
        table = tables[spec.table_name]
        if _unique_columns_available(table, spec):
            updated, deleted, deleted_conflicting = _apply_unique_reference_spec(
                conn,
                table,
                spec,
                target_id=target_id,
                source_ids=source_ids,
                conflict_policy=conflict_policy,
            )
            stats["updated_rows"] += updated
            stats["deleted_duplicate_rows"] += deleted
            stats["deleted_conflicting_reference_rows"] += deleted_conflicting
        else:
            stats["updated_rows"] += _apply_direct_reference_spec(
                conn,
                table,
                spec,
                target_id=target_id,
                source_ids=source_ids,
            )

    player_basic = tables["player_basic"]
    deleted = conn.execute(player_basic.delete().where(player_basic.c.player_id.in_(source_ids))).rowcount
    stats["deleted_player_basic_rows"] = int(deleted or 0)
    return stats


def repair_duplicate_pseudo_players(
    *,
    db_url: str,
    output_dir: Path,
    apply: bool,
    backup: bool = True,
    mergeable_worklist: Path | None = None,
    conflict_worklist: Path | None = None,
    conflict_policy: str = CONFLICT_POLICY_SKIP_GROUP,
) -> dict[str, Any]:
    if conflict_policy not in CONFLICT_POLICIES:
        raise RuntimeError(f"Unsupported conflict_policy: {conflict_policy}")

    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = None
    if apply and backup:
        backup_path = _backup_sqlite_database(db_url, output_dir)

    applied_stats = {
        "updated_rows": 0,
        "deleted_duplicate_rows": 0,
        "deleted_conflicting_reference_rows": 0,
        "deleted_player_basic_rows": 0,
    }
    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            tables = _load_tables(conn)
            if "player_basic" not in tables:
                raise RuntimeError("player_basic table not found")
            if mergeable_worklist is not None:
                groups = _load_mergeable_worklist(Path(mergeable_worklist))
                unresolved = []
            else:
                groups, unresolved = collect_merge_plan(conn, tables)
            detected_conflicts = collect_conflicts(conn, tables, groups)
            worklist_conflicts = _load_conflict_worklist(Path(conflict_worklist)) if conflict_worklist else []
            conflicts = _merge_conflict_reports(worklist_conflicts, detected_conflicts)
            mergeable = [
                {
                    **group,
                    "source_player_ids": ",".join(str(pid) for pid in group["source_player_ids"]),
                    "player_ids": ",".join(str(pid) for pid in group["player_ids"]),
                }
                for group in groups
            ]

            mergeable_report_path = output_dir / f"pseudo_player_mergeable_{stamp}.csv"
            unresolved_report_path = output_dir / f"pseudo_player_unresolved_{stamp}.csv"
            conflict_report_path = output_dir / f"pseudo_player_conflicts_{stamp}.csv"
            _write_csv(
                mergeable_report_path,
                mergeable,
                MERGEABLE_FIELDNAMES,
            )
            _write_csv(
                unresolved_report_path,
                unresolved,
                UNRESOLVED_FIELDNAMES,
            )
            _write_csv(
                conflict_report_path,
                conflicts,
                CONFLICT_FIELDNAMES,
            )

            conflicted_group_keys = _conflicted_group_keys(conflicts)
            if conflict_policy == CONFLICT_POLICY_DELETE_SOURCE_DUPLICATES:
                safe_groups = groups
                skipped_conflict_groups = 0
            else:
                safe_groups = [group for group in groups if _group_conflict_key(group) not in conflicted_group_keys]
                skipped_conflict_groups = len(conflicted_group_keys)
            if apply:
                for group in safe_groups:
                    group_stats = apply_group(conn, tables, group, conflict_policy=conflict_policy)
                    for key, value in group_stats.items():
                        applied_stats[key] += value

        return {
            "dry_run": not apply,
            "mergeable_groups": len(groups),
            "safe_mergeable_groups": len(safe_groups),
            "skipped_conflict_groups": skipped_conflict_groups,
            "conflict_groups": len(conflicted_group_keys),
            "unresolved_rows": len(unresolved),
            "conflicts": len(conflicts),
            "updated_rows": applied_stats["updated_rows"],
            "deleted_duplicate_rows": applied_stats["deleted_duplicate_rows"],
            "deleted_conflicting_reference_rows": applied_stats["deleted_conflicting_reference_rows"],
            "deleted_player_basic_rows": applied_stats["deleted_player_basic_rows"],
            "output_dir": str(output_dir),
            "mergeable_report_path": str(mergeable_report_path),
            "unresolved_report_path": str(unresolved_report_path),
            "conflict_report_path": str(conflict_report_path),
            "backup_path": str(backup_path) if backup_path else "",
            "mergeable_worklist": str(mergeable_worklist) if mergeable_worklist else "",
            "conflict_worklist": str(conflict_worklist) if conflict_worklist else "",
            "conflict_policy": conflict_policy,
        }
    finally:
        engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Conservatively merge duplicate temporary player_basic rows.")
    parser.add_argument("--oci", action="store_true", help="Use OCI_DB_URL instead of local DATABASE_URL.")
    parser.add_argument("--db-url", default=None, help="Explicit database URL. Overrides --oci and local DATABASE_URL.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for report CSV files.")
    parser.add_argument(
        "--mergeable-worklist",
        default=None,
        help="Reviewed pseudo_player_mergeable CSV to apply instead of auto-discovering duplicate groups.",
    )
    parser.add_argument(
        "--conflict-worklist",
        default=None,
        help="Reviewed pseudo_player_conflicts CSV to include in conflict reporting.",
    )
    parser.add_argument(
        "--conflict-policy",
        default=CONFLICT_POLICY_SKIP_GROUP,
        choices=sorted(CONFLICT_POLICIES),
        help=(
            "How to handle detected duplicate reference payload conflicts. "
            "skip-group leaves conflicting groups untouched. "
            "delete-source-duplicates keeps the target reference row and deletes source-side duplicate rows."
        ),
    )
    parser.add_argument("--apply", action="store_true", help="Persist safe merges. Default is dry-run only.")
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    db_url = args.db_url or (os.getenv("OCI_DB_URL") if args.oci else None) or DATABASE_URL
    if args.oci and not db_url:
        raise SystemExit("OCI_DB_URL is required with --oci")

    try:
        result = repair_duplicate_pseudo_players(
            db_url=db_url,
            output_dir=Path(args.output_dir),
            apply=bool(args.apply),
            backup=not args.no_backup,
            mergeable_worklist=Path(args.mergeable_worklist) if args.mergeable_worklist else None,
            conflict_worklist=Path(args.conflict_worklist) if args.conflict_worklist else None,
            conflict_policy=args.conflict_policy,
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] mergeable_groups={result['mergeable_groups']} "
        f"safe_mergeable_groups={result['safe_mergeable_groups']} "
        f"skipped_conflict_groups={result['skipped_conflict_groups']} "
        f"unresolved_rows={result['unresolved_rows']} conflicts={result['conflicts']} "
        f"updated_rows={result['updated_rows']} "
        f"deleted_duplicate_rows={result['deleted_duplicate_rows']} "
        f"deleted_conflicting_reference_rows={result['deleted_conflicting_reference_rows']} "
        f"deleted_player_basic_rows={result['deleted_player_basic_rows']}"
    )
    if result["backup_path"]:
        print(f"[BACKUP] {result['backup_path']}")
    print(f"[REPORT] {result['output_dir']}")


if __name__ == "__main__":
    main()
