"""Stage local innings evidence for selected OCI season-pitching rows.

The command is dry-run by default. It matches source and target rows by the
full logical season key and only plans a change when local innings are
positive, target innings are missing, and no existing positive target value
conflicts with the source. ``--apply`` commits the planned updates through one
transaction; the JSON report records before/after values for rollback review.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import bindparam, inspect, text

from src.constants import KST
from src.db.engine import create_engine_for_url

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TABLE_NAME = "player_season_pitching"
DEFAULT_PLAYER_IDS = (73, 1352)
INNINGS_FIELDS = ("innings_outs", "innings_pitched")


def _columns(conn: Connection) -> set[str]:
    """Return target table columns."""
    return {str(column["name"]).lower() for column in inspect(conn).get_columns(TABLE_NAME)}


def _team_expression(alias: str, columns: set[str]) -> str:
    """Build a canonical-team expression for a source or target table."""
    if {"canonical_team_code", "team_code"}.issubset(columns):
        return f"COALESCE({alias}.canonical_team_code, {alias}.team_code)"
    if "team_code" in columns:
        return f"{alias}.team_code"
    return "NULL"


def _level_expression(alias: str, columns: set[str]) -> str:
    """Build the logical level expression across local and OCI schemas."""
    if "level" in columns:
        return f"COALESCE({alias}.\"level\", 'KBO1')"
    if "league_level" in columns:
        return f"COALESCE({alias}.league_level, 'KBO1')"
    return "'KBO1'"


def _as_int(value: object) -> int:
    """Convert a nullable database value to an integer."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_float(value: object) -> float:
    """Convert a nullable database value to a float."""
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _rows(
    conn: Connection,
    *,
    player_ids: tuple[int, ...],
    year: int,
    include_id: bool,
) -> list[dict[str, object]]:
    """Read regular-season source or target rows for selected players."""
    columns = _columns(conn)
    id_column = "s.id," if include_id else ""
    team_expression = _team_expression("s", columns)
    level_expression = _level_expression("s", columns)
    statement = text(
        f"""
            SELECT
                {id_column}
                s.player_id,
                s.season,
                COALESCE(s.league, 'REGULAR') AS league,
                {level_expression} AS "level",
                {team_expression} AS team_code,
                COALESCE(pb.name, '') AS player_name,
                COALESCE(s.innings_outs, 0) AS innings_outs,
                COALESCE(s.innings_pitched, 0) AS innings_pitched
            FROM {TABLE_NAME} s
            LEFT JOIN player_basic pb ON pb.player_id = s.player_id
            WHERE s.player_id IN :player_ids
              AND s.season = :year
              AND COALESCE(s.league, 'REGULAR') = 'REGULAR'
            """,
    ).bindparams(bindparam("player_ids", expanding=True))
    return [dict(row) for row in conn.execute(statement, {"player_ids": list(player_ids), "year": year}).mappings()]


def _game_source_rows(conn: Connection, *, year: int) -> list[dict[str, object]]:
    """Aggregate local game-level pitching evidence by source player and team."""
    if not inspect(conn).has_table("game_pitching_stats"):
        return []
    columns = {str(column["name"]).lower() for column in inspect(conn).get_columns("game_pitching_stats")}
    team_expression = _team_expression("g", columns)
    rows = conn.execute(
        text(
            f"""
            SELECT
                g.player_id,
                g.player_name,
                {team_expression} AS team_code,
                COALESCE(SUM(g.innings_outs), 0) AS innings_outs,
                COALESCE(SUM(g.innings_pitched), 0) AS innings_pitched,
                COUNT(*) AS game_rows,
                COUNT(DISTINCT g.game_id) AS games,
                'local_game_pitching_stats' AS evidence_source
            FROM game_pitching_stats g
            WHERE g.player_id IS NOT NULL
              AND SUBSTR(g.game_id, 1, 4) = :year
            GROUP BY g.player_id, g.player_name, {team_expression}
            """,
        ),
        {"year": str(year)},
    ).mappings()
    return [dict(row) for row in rows]


def _season_identity_rows(conn: Connection, *, year: int) -> list[dict[str, object]]:
    """Aggregate local season pitching evidence by exact profile name/team."""
    if not inspect(conn).has_table(TABLE_NAME) or not inspect(conn).has_table("player_basic"):
        return []
    columns = _columns(conn)
    team_expression = _team_expression("s", columns)
    rows = conn.execute(
        text(
            f"""
            SELECT
                s.player_id,
                pb.name AS player_name,
                {team_expression} AS team_code,
                COALESCE(SUM(s.innings_outs), 0) AS innings_outs,
                COALESCE(SUM(s.innings_pitched), 0) AS innings_pitched,
                'local_player_season_pitching_identity' AS evidence_source
            FROM {TABLE_NAME} s
            JOIN player_basic pb ON pb.player_id = s.player_id
            WHERE s.season = :year
              AND COALESCE(s.league, 'REGULAR') = 'REGULAR'
            GROUP BY s.player_id, pb.name, {team_expression}
            """,
        ),
        {"year": year},
    ).mappings()
    return [dict(row) for row in rows]


def _row_key(row: dict[str, object]) -> tuple[int, int, str, str, str | None]:
    """Return the logical season-stat key used for source/target matching."""
    team_code = row.get("team_code")
    return (
        _as_int(row["player_id"]),
        _as_int(row["season"]),
        str(row.get("league") or "REGULAR"),
        str(row.get("level") or "KBO1"),
        str(team_code) if team_code is not None else None,
    )


def _source_index(
    rows: list[dict[str, object]],
) -> tuple[dict[tuple[int, int, str, str, str | None], dict[str, object]], set[tuple[int, int, str, str, str | None]]]:
    """Index unique source rows and retain duplicate-key conflicts separately."""
    grouped: dict[tuple[int, int, str, str, str | None], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[_row_key(row)].append(row)
    unique = {key: members[0] for key, members in grouped.items() if len(members) == 1}
    conflicts = {key for key, members in grouped.items() if len(members) > 1}
    return unique, conflicts


def _positive_source(source: dict[str, object]) -> bool:
    """Check that source innings provide a positive basis for the backfill."""
    return _as_int(source["innings_outs"]) > 0 or _as_float(source["innings_pitched"]) > 0


def _identity_source_index(rows: list[dict[str, object]]) -> dict[tuple[str, str], list[dict[str, object]]]:
    """Index game evidence by exact name/team identity for legacy-ID recovery."""
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = (str(row.get("player_name") or ""), str(row.get("team_code") or ""))
        if key[0] and key[1]:
            grouped[key].append(row)
    return grouped


def _source_for_target(
    target: dict[str, object],
    source_index: dict[tuple[int, int, str, str, str | None], dict[str, object]],
    source_conflicts: set[tuple[int, int, str, str, str | None]],
    identity_index: dict[tuple[str, str], list[dict[str, object]]],
) -> tuple[dict[str, object] | None, bool, str]:
    """Resolve direct-key evidence, then exact local name/team game evidence."""
    key = _row_key(target)
    if key in source_conflicts:
        return None, True, "local_season_key"
    direct = source_index.get(key)
    if direct is not None and _positive_source(direct):
        return direct, False, "local_player_season_pitching"

    identity_key = (str(target.get("player_name") or ""), str(target.get("team_code") or ""))
    candidates = identity_index.get(identity_key, [])
    candidate_ids = {int(row["player_id"]) for row in candidates}
    if len(candidate_ids) == 1:
        return candidates[0], False, str(candidates[0].get("evidence_source") or "local_identity")
    if len(candidate_ids) > 1:
        return None, True, "local_game_identity"
    return None, False, "none"


def _plan_row(
    target: dict[str, object],
    source: dict[str, object] | None,
    source_conflict: bool,
    evidence_source: str,
) -> dict[str, object]:
    """Plan one target row without mutating it."""
    before = {field: target[field] for field in INNINGS_FIELDS}
    key = _row_key(target)
    base: dict[str, object] = {
        "target_id": target.get("id"),
        "player_id": key[0],
        "player_name": str(target.get("player_name") or ""),
        "season": key[1],
        "league": key[2],
        "level": key[3],
        "team_code": key[4],
        "before": before,
        "evidence_source": evidence_source,
    }
    if source is not None:
        base["source_player_id"] = source.get("player_id")
    if source_conflict:
        return {**base, "status": "conflict", "reason": "multiple_source_rows_for_logical_key"}
    if source is None or not _positive_source(source):
        return {**base, "status": "source_missing", "reason": "no_positive_local_innings_evidence"}

    source_values = {
        "innings_outs": _as_int(source["innings_outs"]),
        "innings_pitched": _as_float(source["innings_pitched"]),
    }
    target_values = {
        "innings_outs": _as_int(target["innings_outs"]),
        "innings_pitched": _as_float(target["innings_pitched"]),
    }
    conflicts = [
        field for field in INNINGS_FIELDS if target_values[field] > 0 and target_values[field] != source_values[field]
    ]
    if conflicts:
        return {**base, "status": "conflict", "reason": f"positive_target_differs:{','.join(conflicts)}"}

    after = {
        field: target_values[field] if target_values[field] > 0 else source_values[field] for field in INNINGS_FIELDS
    }
    status = "already_correct" if after == target_values else "planned"
    result = {**base, "status": status, "reason": "local_positive_innings_evidence", "after": after}
    if status == "planned":
        result["rollback"] = {
            "table": TABLE_NAME,
            "target_id": target.get("id"),
            "restore": before,
        }
    return result


def _summary(changes: list[dict[str, object]], applied: int) -> dict[str, int]:
    """Summarize migration planning and application outcomes."""
    return {
        "planned": sum(change["status"] == "planned" for change in changes),
        "already_correct": sum(change["status"] == "already_correct" for change in changes),
        "source_missing": sum(change["status"] == "source_missing" for change in changes),
        "conflict": sum(change["status"] == "conflict" for change in changes),
        "applied": applied,
    }


def _apply_change(conn: Connection, change: dict[str, object], columns: set[str]) -> None:
    """Apply one previously planned row by primary key."""
    set_clauses = ["innings_outs = :innings_outs", "innings_pitched = :innings_pitched"]
    if "updated_at" in columns:
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
    before = change["before"]
    after = change["after"]
    params = {
        "innings_outs": after["innings_outs"],
        "innings_pitched": after["innings_pitched"],
        "target_id": change["target_id"],
    }
    conn.execute(text(f"UPDATE {TABLE_NAME} SET {', '.join(set_clauses)} WHERE id = :target_id"), params)
    change["applied_before"] = before


def backfill_oci_innings(
    source_conn: Connection,
    target_conn: Connection,
    *,
    year: int,
    player_ids: tuple[int, ...],
    apply: bool = False,
) -> dict[str, object]:
    """Plan or apply innings updates using exact logical-key evidence."""
    if not player_ids:
        raise ValueError("at least one player id is required")
    columns = _columns(target_conn)
    if "id" not in columns:
        raise ValueError("target player_season_pitching.id is required for rollback-safe updates")
    source_rows = _rows(source_conn, player_ids=player_ids, year=year, include_id=False)
    target_rows = _rows(target_conn, player_ids=player_ids, year=year, include_id=True)
    source_index, source_conflicts = _source_index(source_rows)
    identity_index = _identity_source_index(
        [
            *_game_source_rows(source_conn, year=year),
            *_season_identity_rows(source_conn, year=year),
        ],
    )
    changes = [
        _plan_row(target, *_source_for_target(target, source_index, source_conflicts, identity_index))
        for target in target_rows
    ]
    applied = 0
    if apply:
        for change in changes:
            if change["status"] == "planned":
                _apply_change(target_conn, change, columns)
                applied += 1
    return {
        "read_only": not apply,
        "year": year,
        "player_ids": list(player_ids),
        "source_dialect": source_conn.dialect.name,
        "target_dialect": target_conn.dialect.name,
        "summary": _summary(changes, applied),
        "changes": changes,
    }


def _default_output(year: int) -> Path:
    """Return a timestamped migration report path."""
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "data/audit" / f"oci_{year}_innings_backfill_{stamp}.json"


def main(argv: list[str] | None = None) -> int:
    """Run the dry-run or transactional innings backfill CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2021)
    parser.add_argument("--player-id", action="append", type=int, dest="player_ids", default=[])
    parser.add_argument("--source-url", default=None, help="Local source URL; defaults to DATABASE_URL")
    parser.add_argument(
        "--target-url", default=None, help="OCI target URL; defaults to OCI_DB_URL or TARGET_DATABASE_URL"
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--apply", action="store_true", help="Commit planned updates; default is dry-run")
    args = parser.parse_args(argv)

    source_url = args.source_url or os.getenv("DATABASE_URL")
    target_url = args.target_url or os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not source_url:
        parser.error("source URL is required via --source-url or DATABASE_URL")
    if not target_url:
        parser.error("target URL is required via --target-url, OCI_DB_URL, or TARGET_DATABASE_URL")

    player_ids = tuple(sorted(set(args.player_ids or DEFAULT_PLAYER_IDS)))
    source_engine = create_engine_for_url(source_url)
    target_engine = create_engine_for_url(target_url)
    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        report = backfill_oci_innings(
            source_conn,
            target_conn,
            year=args.year,
            player_ids=player_ids,
            apply=args.apply,
        )

    output = args.output or _default_output(args.year)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"mode={mode}")
    print(f"innings_backfill_report={output}")
    print(json.dumps(report["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
