#!/usr/bin/env python3
"""Align OCI-only temporary player ids to the verified local SQLite state.

This is intentionally conservative:
- only player_basic rows with player_id >= 900000 and absent from local are pruned;
- game child rows are retargeted from matching local rows when possible;
- local-absent generated game child rows are removed or nullable ids are cleared;
- derived season/matchup rows that still point at OCI-only temporary ids are deleted;
- player/player movement mirror links are cleared or set to the matching local value.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, and_, bindparam, create_engine, inspect, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL


PSEUDO_MIN_PLAYER_ID = 900000
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "prune_oci_pseudo_players_to_local"
_TABLE_EXISTS_CACHE: dict[tuple[int, str], bool] = {}
_COLUMNS_CACHE: dict[tuple[int, str], set[str]] = {}
_TABLE_CACHE: dict[tuple[int, str], Table | None] = {}


@dataclass(frozen=True)
class GameRefSpec:
    table_name: str
    player_column: str
    match_columns: tuple[str, ...]
    missing_action: str
    conflict_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeleteRefSpec:
    table_name: str
    player_columns: tuple[str, ...]


GAME_REF_SPECS = (
    GameRefSpec(
        "game_batting_stats",
        "player_id",
        ("game_id", "team_side", "team_code", "player_name", "appearance_seq"),
        "delete_row",
        ("game_id", "player_id", "appearance_seq"),
    ),
    GameRefSpec(
        "game_pitching_stats",
        "player_id",
        ("game_id", "team_side", "team_code", "player_name", "appearance_seq"),
        "delete_row",
        ("game_id", "player_id", "appearance_seq"),
    ),
    GameRefSpec(
        "game_lineups",
        "player_id",
        ("game_id", "team_side", "team_code", "player_name", "appearance_seq"),
        "delete_row",
    ),
    GameRefSpec(
        "game_summary",
        "player_id",
        ("game_id", "summary_type", "player_name", "detail_text"),
        "set_null",
    ),
    GameRefSpec(
        "game_events",
        "batter_id",
        ("game_id", "event_seq"),
        "set_null",
    ),
    GameRefSpec(
        "game_events",
        "pitcher_id",
        ("game_id", "event_seq"),
        "set_null",
    ),
)

DELETE_REF_SPECS = (
    DeleteRefSpec("player_season_batting", ("player_id",)),
    DeleteRefSpec("player_season_pitching", ("player_id",)),
    DeleteRefSpec("player_season_fielding", ("player_id",)),
    DeleteRefSpec("player_season_baserunning", ("player_id",)),
    DeleteRefSpec("matchup_bvp", ("batter_id", "pitcher_id")),
    DeleteRefSpec("matchup_batter_splits", ("player_id",)),
    DeleteRefSpec("matchup_pitcher_splits", ("player_id",)),
    DeleteRefSpec("matchup_batter_team_split", ("player_id",)),
    DeleteRefSpec("matchup_pitcher_team_split", ("player_id",)),
    DeleteRefSpec("matchup_batter_stadium_split", ("player_id",)),
    DeleteRefSpec("matchup_batter_vs_starter", ("player_id",)),
)

ALL_REFERENCE_SPECS = (
    *[(spec.table_name, column) for spec in GAME_REF_SPECS for column in (spec.player_column,)],
    *[(spec.table_name, column) for spec in DELETE_REF_SPECS for column in spec.player_columns],
    ("players", "player_basic_id"),
    ("team_daily_roster", "player_basic_id"),
    ("player_movements", "player_basic_id"),
)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _table_exists(conn, table_name: str) -> bool:
    key = (id(conn), table_name)
    if key not in _TABLE_EXISTS_CACHE:
        _TABLE_EXISTS_CACHE[key] = table_name in inspect(conn).get_table_names()
    return _TABLE_EXISTS_CACHE[key]


def _columns(conn, table_name: str) -> set[str]:
    key = (id(conn), table_name)
    if key not in _COLUMNS_CACHE:
        if not _table_exists(conn, table_name):
            _COLUMNS_CACHE[key] = set()
        else:
            _COLUMNS_CACHE[key] = {col["name"] for col in inspect(conn).get_columns(table_name)}
    return _COLUMNS_CACHE[key]


def _load_table(conn, table_name: str) -> Table | None:
    key = (id(conn), table_name)
    if key not in _TABLE_CACHE:
        if not _table_exists(conn, table_name):
            _TABLE_CACHE[key] = None
        else:
            metadata = MetaData()
            _TABLE_CACHE[key] = Table(table_name, metadata, autoload_with=conn)
    return _TABLE_CACHE[key]


def _pseudo_ids(conn) -> set[int]:
    rows = conn.execute(
        text("SELECT player_id FROM player_basic WHERE player_id >= :min_id"),
        {"min_id": PSEUDO_MIN_PLAYER_ID},
    ).fetchall()
    return {int(row[0]) for row in rows}


def _extra_id_params(extra_ids: set[int]) -> dict[str, list[int]]:
    return {"extra_ids": sorted(extra_ids)}


def _stmt_with_extra_ids(sql: str):
    return text(sql).bindparams(bindparam("extra_ids", expanding=True))


def _local_candidate_id(local_conn, spec: GameRefSpec, row: dict[str, Any]) -> int | None:
    table = _load_table(local_conn, spec.table_name)
    if table is None:
        return None
    clauses = []
    for column_name in spec.match_columns:
        value = row.get(column_name)
        column = table.c[column_name]
        clauses.append(column.is_(None) if value is None else column == value)
    stmt = select(table.c[spec.player_column]).where(and_(*clauses), table.c[spec.player_column].is_not(None))
    candidates = sorted({int(item[0]) for item in local_conn.execute(stmt).fetchall() if item[0] is not None})
    if len(candidates) != 1:
        return None
    return candidates[0]


def _remote_unique_conflict(conn, spec: GameRefSpec, row: dict[str, Any], candidate_id: int) -> bool:
    if not spec.conflict_columns:
        return False
    table = _load_table(conn, spec.table_name)
    if table is None or "id" not in table.c:
        return False
    clauses = []
    for column_name in spec.conflict_columns:
        value = candidate_id if column_name == spec.player_column else row.get(column_name)
        column = table.c[column_name]
        clauses.append(column.is_(None) if value is None else column == value)
    clauses.append(table.c.id != row["id"])
    return conn.execute(select(table.c.id).where(and_(*clauses)).limit(1)).first() is not None


def _update_or_delete_game_refs(local_conn, oci_conn, spec: GameRefSpec, extra_ids: set[int], apply: bool) -> list[dict[str, Any]]:
    if not extra_ids or not _table_exists(oci_conn, spec.table_name):
        return []
    required = {"id", spec.player_column, *spec.match_columns}
    if not required <= _columns(oci_conn, spec.table_name):
        return []

    rows = [
        dict(row)
        for row in oci_conn.execute(
            _stmt_with_extra_ids(
                f"""
                SELECT id, {spec.player_column}, {", ".join(spec.match_columns)}
                FROM {spec.table_name}
                WHERE {spec.player_column} IN :extra_ids
                """
            ),
            _extra_id_params(extra_ids),
        ).mappings()
    ]
    actions: list[dict[str, Any]] = []
    table = _load_table(oci_conn, spec.table_name)
    if table is None:
        return actions

    for row in rows:
        current_id = int(row[spec.player_column])
        candidate_id = _local_candidate_id(local_conn, spec, row)
        if candidate_id is not None:
            if _remote_unique_conflict(oci_conn, spec, row, candidate_id):
                action = "delete_conflicting_source_row"
                if apply:
                    oci_conn.execute(table.delete().where(table.c.id == row["id"]))
            else:
                action = "retarget_to_local"
                if apply:
                    oci_conn.execute(
                        table.update()
                        .where(table.c.id == row["id"])
                        .values(**{spec.player_column: candidate_id})
                    )
            resolved_id: int | str = candidate_id
        elif spec.missing_action == "set_null":
            action = "set_null_no_local_match"
            resolved_id = ""
            if apply:
                oci_conn.execute(
                    table.update()
                    .where(table.c.id == row["id"])
                    .values(**{spec.player_column: None})
                )
        else:
            action = "delete_no_local_match"
            resolved_id = ""
            if apply:
                oci_conn.execute(table.delete().where(table.c.id == row["id"]))
        actions.append(
            {
                "table_name": spec.table_name,
                "row_id": row["id"],
                "player_column": spec.player_column,
                "current_player_id": current_id,
                "resolved_player_id": resolved_id,
                "action": action,
                "match_key": "|".join(str(row.get(column) or "") for column in spec.match_columns),
            }
        )
    return actions


def _delete_derived_refs(oci_conn, spec: DeleteRefSpec, extra_ids: set[int], apply: bool) -> list[dict[str, Any]]:
    if not extra_ids or not _table_exists(oci_conn, spec.table_name):
        return []
    columns = _columns(oci_conn, spec.table_name)
    available_columns = [column for column in spec.player_columns if column in columns]
    if not available_columns:
        return []
    table = _load_table(oci_conn, spec.table_name)
    if table is None:
        return []

    clauses = [table.c[column].in_(sorted(extra_ids)) for column in available_columns]
    where_clause = clauses[0]
    for clause in clauses[1:]:
        where_clause = where_clause | clause
    count = int(oci_conn.execute(select(text("COUNT(*)")).select_from(table).where(where_clause)).scalar() or 0)
    if apply and count:
        oci_conn.execute(table.delete().where(where_clause))
    return [
        {
            "table_name": spec.table_name,
            "player_column": ",".join(available_columns),
            "action": "delete_derived_rows",
            "row_count": count,
        }
    ]


def _clear_players_refs(oci_conn, extra_ids: set[int], apply: bool) -> list[dict[str, Any]]:
    if not extra_ids or not _table_exists(oci_conn, "players") or "player_basic_id" not in _columns(oci_conn, "players"):
        return []
    table = _load_table(oci_conn, "players")
    if table is None:
        return []
    rows = [
        dict(row)
        for row in oci_conn.execute(
            select(table.c.id, table.c.player_basic_id).where(table.c.player_basic_id.in_(sorted(extra_ids)))
        ).mappings()
    ]
    if apply and rows:
        oci_conn.execute(
            table.update()
            .where(table.c.player_basic_id.in_(sorted(extra_ids)))
            .values(player_basic_id=None)
        )
    return [
        {
            "table_name": "players",
            "row_id": row["id"],
            "player_column": "player_basic_id",
            "current_player_id": row["player_basic_id"],
            "resolved_player_id": "",
            "action": "set_null_extra_local_absent",
            "match_key": "",
        }
        for row in rows
    ]


def _retarget_player_movements(local_conn, oci_conn, extra_ids: set[int], apply: bool) -> list[dict[str, Any]]:
    if not extra_ids or not _table_exists(oci_conn, "player_movements") or "player_basic_id" not in _columns(oci_conn, "player_movements"):
        return []
    remote = _load_table(oci_conn, "player_movements")
    local = _load_table(local_conn, "player_movements")
    if remote is None:
        return []
    match_columns = ("movement_date", "section", "team_code", "player_name")
    rows = [
        dict(row)
        for row in oci_conn.execute(
            select(remote).where(remote.c.player_basic_id.in_(sorted(extra_ids)))
        ).mappings()
    ]
    actions: list[dict[str, Any]] = []
    for row in rows:
        candidate_id = None
        if local is not None and set(match_columns) <= set(local.c):
            clauses = []
            for column_name in match_columns:
                value = row.get(column_name)
                column = local.c[column_name]
                clauses.append(column.is_(None) if value is None else column == value)
            found = local_conn.execute(select(local.c.player_basic_id).where(and_(*clauses)).limit(1)).first()
            if found is not None:
                candidate_id = found[0]
        if apply:
            oci_conn.execute(
                remote.update()
                .where(remote.c.id == row["id"])
                .values(player_basic_id=candidate_id)
            )
        actions.append(
            {
                "table_name": "player_movements",
                "row_id": row["id"],
                "player_column": "player_basic_id",
                "current_player_id": row["player_basic_id"],
                "resolved_player_id": candidate_id if candidate_id is not None else "",
                "action": "retarget_to_local_or_null",
                "match_key": "|".join(str(row.get(column) or "") for column in match_columns),
            }
        )
    return actions


def _insert_missing_local_pseudo(local_conn, oci_conn, missing_ids: set[int], apply: bool) -> int:
    if not missing_ids:
        return 0
    local_table = _load_table(local_conn, "player_basic")
    oci_table = _load_table(oci_conn, "player_basic")
    if local_table is None or oci_table is None:
        return 0
    columns = [column.name for column in oci_table.columns if column.name in local_table.c]
    rows = [
        dict(row)
        for row in local_conn.execute(
            select(*(local_table.c[column] for column in columns)).where(local_table.c.player_id.in_(sorted(missing_ids)))
        ).mappings()
    ]
    if apply and rows:
        stmt = pg_insert(oci_table).values(rows)
        update_columns = {
            column: getattr(stmt.excluded, column)
            for column in columns
            if column != "player_id"
        }
        stmt = stmt.on_conflict_do_update(index_elements=["player_id"], set_=update_columns)
        oci_conn.execute(stmt)
    return len(rows)


def _referenced_extra_ids(conn, extra_ids: set[int]) -> tuple[set[int], list[dict[str, Any]]]:
    referenced: set[int] = set()
    rows: list[dict[str, Any]] = []
    if not extra_ids:
        return referenced, rows
    for table_name, column_name in ALL_REFERENCE_SPECS:
        if not _table_exists(conn, table_name) or column_name not in _columns(conn, table_name):
            continue
        result = conn.execute(
            _stmt_with_extra_ids(
                f"SELECT {column_name} AS player_id, COUNT(*) AS row_count FROM {table_name} "
                f"WHERE {column_name} IN :extra_ids GROUP BY {column_name}"
            ),
            _extra_id_params(extra_ids),
        ).mappings()
        for row in result:
            player_id = int(row["player_id"])
            referenced.add(player_id)
            rows.append(
                {
                    "table_name": table_name,
                    "player_column": column_name,
                    "player_id": player_id,
                    "row_count": int(row["row_count"] or 0),
                }
            )
    return referenced, rows


def prune_oci_pseudo_players_to_local(
    *,
    oci_url: str,
    output_dir: Path,
    apply: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_engine = create_engine(DATABASE_URL)
    oci_engine = create_engine(oci_url)

    game_actions: list[dict[str, Any]] = []
    derived_actions: list[dict[str, Any]] = []
    link_actions: list[dict[str, Any]] = []
    remaining_ref_rows: list[dict[str, Any]] = []
    deleted_player_basic_rows = 0
    inserted_player_basic_rows = 0

    try:
        with local_engine.connect() as local_conn, oci_engine.begin() as oci_conn:
            local_ids = _pseudo_ids(local_conn)
            oci_ids = _pseudo_ids(oci_conn)
            extra_ids = oci_ids - local_ids
            missing_ids = local_ids - oci_ids

            inserted_player_basic_rows = _insert_missing_local_pseudo(local_conn, oci_conn, missing_ids, apply)

            for spec in GAME_REF_SPECS:
                game_actions.extend(_update_or_delete_game_refs(local_conn, oci_conn, spec, extra_ids, apply))
            for spec in DELETE_REF_SPECS:
                derived_actions.extend(_delete_derived_refs(oci_conn, spec, extra_ids, apply))
            link_actions.extend(_clear_players_refs(oci_conn, extra_ids, apply))
            link_actions.extend(_retarget_player_movements(local_conn, oci_conn, extra_ids, apply))

            referenced, remaining_ref_rows = _referenced_extra_ids(oci_conn, extra_ids)
            deletable_ids = sorted(extra_ids - referenced)
            deleted_player_basic_rows = len(deletable_ids)
            if apply and deletable_ids:
                oci_conn.execute(
                    _stmt_with_extra_ids("DELETE FROM player_basic WHERE player_id IN :extra_ids"),
                    {"extra_ids": deletable_ids},
                )

            final_oci_pseudo_count = len(oci_ids) + inserted_player_basic_rows - deleted_player_basic_rows
            if not apply:
                oci_conn.rollback()
    finally:
        local_engine.dispose()
        oci_engine.dispose()

    _write_csv(
        output_dir / f"pseudo_game_reference_actions_{stamp}.csv",
        game_actions,
        ["table_name", "row_id", "player_column", "current_player_id", "resolved_player_id", "action", "match_key"],
    )
    _write_csv(
        output_dir / f"pseudo_derived_reference_actions_{stamp}.csv",
        derived_actions,
        ["table_name", "player_column", "action", "row_count"],
    )
    _write_csv(
        output_dir / f"pseudo_link_reference_actions_{stamp}.csv",
        link_actions,
        ["table_name", "row_id", "player_column", "current_player_id", "resolved_player_id", "action", "match_key"],
    )
    _write_csv(
        output_dir / f"pseudo_remaining_references_{stamp}.csv",
        remaining_ref_rows,
        ["table_name", "player_column", "player_id", "row_count"],
    )

    return {
        "dry_run": not apply,
        "game_reference_actions": len(game_actions),
        "derived_reference_tables": len([row for row in derived_actions if int(row.get("row_count") or 0) > 0]),
        "derived_reference_rows": sum(int(row.get("row_count") or 0) for row in derived_actions),
        "link_reference_actions": len(link_actions),
        "inserted_player_basic_rows": inserted_player_basic_rows,
        "deleted_player_basic_rows": deleted_player_basic_rows,
        "remaining_reference_rows": sum(int(row.get("row_count") or 0) for row in remaining_ref_rows),
        "final_oci_pseudo_count": final_oci_pseudo_count,
        "output_dir": str(output_dir),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prune OCI-only pseudo player rows using local SQLite as source of truth.")
    parser.add_argument("--oci-url", default=None, help="OCI/Postgres URL. Defaults to OCI_DB_URL.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--apply", action="store_true", help="Persist repairs. Default is dry-run only.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    oci_url = args.oci_url or os.getenv("OCI_DB_URL")
    if not oci_url:
        raise SystemExit("OCI_DB_URL or --oci-url is required")
    result = prune_oci_pseudo_players_to_local(
        oci_url=oci_url,
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] game_reference_actions={result['game_reference_actions']} "
        f"derived_reference_tables={result['derived_reference_tables']} "
        f"derived_reference_rows={result['derived_reference_rows']} "
        f"link_reference_actions={result['link_reference_actions']} "
        f"inserted_player_basic_rows={result['inserted_player_basic_rows']} "
        f"deleted_player_basic_rows={result['deleted_player_basic_rows']} "
        f"remaining_reference_rows={result['remaining_reference_rows']} "
        f"final_oci_pseudo_count={result['final_oci_pseudo_count']}"
    )
    print(f"[REPORT] {result['output_dir']}")


if __name__ == "__main__":
    main()
