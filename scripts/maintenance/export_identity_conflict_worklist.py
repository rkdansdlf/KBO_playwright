#!/usr/bin/env python3
"""Export row-level worklists for same-name game-stat identity conflicts.

This tool is intentionally read-only. It does not choose a replacement
``player_id`` unless the row already carries durable evidence; current game
stat rows generally do not, so most rows are marked for source review.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.maintenance.classify_game_stat_duplicates import (  # noqa: E402
    DEFAULT_DB_PATH,
    DUPLICATE_CONFIGS,
    _classify_group,
    _columns,
    _duplicate_groups,
    _group_rows,
    _table_exists,
)

DEFAULT_OUTPUT_DIR = Path("data/identity_conflict_worklists")


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})
            count += 1
    return count


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    return column_name in _columns(conn, table_name) if _table_exists(conn, table_name) else False


def _game_date(conn: sqlite3.Connection, game_id: str) -> str:
    if not (_table_exists(conn, "game") and _column_exists(conn, "game", "game_date")):
        return ""
    row = conn.execute("SELECT game_date FROM game WHERE game_id = ?", (game_id,)).fetchone()
    return str(row["game_date"]) if row and row["game_date"] is not None else ""


def _player_profile(conn: sqlite3.Connection, player_id: int) -> dict[str, Any]:
    if not _table_exists(conn, "player_basic"):
        return {"player_id": player_id}

    columns = set(_columns(conn, "player_basic"))
    wanted = [
        column
        for column in (
            "player_id",
            "name",
            "birth_date",
            "birth_date_date",
            "debut_year",
            "team",
            "position",
            "status",
            "staff_role",
        )
        if column in columns
    ]
    if not wanted:
        return {"player_id": player_id}

    row = conn.execute(
        f"SELECT {', '.join(wanted)} FROM player_basic WHERE player_id = ?",
        (player_id,),
    ).fetchone()
    return dict(row) if row else {"player_id": player_id}


def _candidate_profiles(conn: sqlite3.Connection, candidate_ids: str) -> str:
    parts: list[str] = []
    for raw_id in str(candidate_ids or "").split(","):
        if not raw_id:
            continue
        profile = _player_profile(conn, int(raw_id))
        name = profile.get("name") or ""
        birth_date = profile.get("birth_date_date") or profile.get("birth_date") or ""
        debut_year = profile.get("debut_year") or ""
        team = profile.get("team") or ""
        position = profile.get("position") or ""
        parts.append(f"{raw_id}:{name}:{birth_date}:debut={debut_year}:team={team}:pos={position}")
    return " | ".join(parts)


def _row_identity_summary(row: dict[str, Any]) -> str:
    return (
        f"order={row.get('batting_order') or ''};"
        f"seq={row.get('appearance_seq') or ''};"
        f"pos={row.get('standard_position') or row.get('position') or ''};"
        f"starter={row.get('is_starter') if 'is_starter' in row else row.get('is_starting', '')};"
        f"uniform={row.get('uniform_no') or ''}"
    )


def _suggestion_for_row(row: dict[str, Any], candidate_ids: str) -> dict[str, Any]:
    candidates = [int(part) for part in str(candidate_ids or "").split(",") if part]
    current_player_id = row.get("player_id")
    if len(candidates) < 2:
        return {
            "suggestion_status": "not_identity_conflict",
            "suggested_player_id": "",
            "needs_source_review": 0,
            "suggestion_reason": "Fewer than two season-level candidates were found.",
        }

    if current_player_id not in candidates:
        return {
            "suggestion_status": "source_review",
            "suggested_player_id": "",
            "needs_source_review": 1,
            "suggestion_reason": "Current player_id is outside the season-level candidate set.",
        }

    missing_candidates = [candidate for candidate in candidates if candidate != current_player_id]
    return {
        "suggestion_status": "source_review",
        "suggested_player_id": "",
        "needs_source_review": 1,
        "suggestion_reason": (
            "Same-name candidates share the game/team context; source lineup or boxscore "
            f"must identify whether this row stays on {current_player_id} or moves to "
            f"{','.join(str(candidate) for candidate in missing_candidates)}."
        ),
    }


def export_identity_conflict_worklist(
    *,
    db_path: Path,
    output_dir: Path,
    player_name: str | None = None,
    team_code: str | None = None,
    table_name: str | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")

    group_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    try:
        for config in DUPLICATE_CONFIGS:
            if table_name and config.table_name != table_name:
                continue
            if not _table_exists(conn, config.table_name):
                continue

            columns = _columns(conn, config.table_name)
            for duplicate_group in _duplicate_groups(conn, config):
                rows = _group_rows(
                    conn,
                    table_name=config.table_name,
                    group_columns=config.group_columns,
                    group=duplicate_group,
                )
                classified = _classify_group(conn, config=config, rows=rows, columns=columns)
                if classified["classification"] != "identity_conflict":
                    continue
                if player_name and player_name not in str(classified.get("player_names") or "").split(","):
                    continue
                if team_code and team_code not in str(classified.get("team_codes") or "").split(","):
                    continue

                first = rows[0]
                group_key = "|".join(
                    f"{column}={classified.get(column, first.get(column, ''))}" for column in config.group_columns
                )
                candidate_profiles = _candidate_profiles(conn, str(classified["candidate_ids"]))
                game_date = _game_date(conn, str(first.get("game_id") or ""))
                source_review_rows = 0

                for row in rows:
                    suggestion = _suggestion_for_row(row, str(classified["candidate_ids"]))
                    source_review_rows += int(suggestion["needs_source_review"])
                    profile = _player_profile(conn, int(row["player_id"]))
                    detail_rows.append(
                        {
                            "table_name": config.table_name,
                            "group_key": group_key,
                            "game_date": game_date,
                            "candidate_ids": classified["candidate_ids"],
                            "candidate_profiles": candidate_profiles,
                            "row_identity_summary": _row_identity_summary(row),
                            "current_player_birth_date": profile.get("birth_date_date")
                            or profile.get("birth_date")
                            or "",
                            "current_player_debut_year": profile.get("debut_year") or "",
                            **suggestion,
                            **row,
                        }
                    )

                group_rows.append(
                    {
                        **classified,
                        "group_key": group_key,
                        "game_date": game_date,
                        "candidate_profiles": candidate_profiles,
                        "source_review_rows": source_review_rows,
                    }
                )
    finally:
        conn.close()

    manifest_rows_by_game: dict[str, dict[str, Any]] = {}
    for row in group_rows:
        game_id = str(row.get("game_id") or "")
        if not game_id:
            continue
        manifest = manifest_rows_by_game.setdefault(
            game_id,
            {
                "game_id": game_id,
                "game_date": row.get("game_date") or "",
                "tables": set(),
                "player_names": set(),
                "team_codes": set(),
                "group_count": 0,
                "row_count": 0,
                "source_review_rows": 0,
            },
        )
        manifest["tables"].add(row.get("table_name") or "")
        for value in str(row.get("player_names") or "").split(","):
            if value:
                manifest["player_names"].add(value)
        for value in str(row.get("team_codes") or "").split(","):
            if value:
                manifest["team_codes"].add(value)
        manifest["group_count"] += 1
        manifest["row_count"] += int(row.get("row_count") or 0)
        manifest["source_review_rows"] += int(row.get("source_review_rows") or 0)

    manifest_rows = [
        {
            **row,
            "tables": ",".join(sorted(row["tables"])),
            "player_names": ",".join(sorted(row["player_names"])),
            "team_codes": ",".join(sorted(row["team_codes"])),
        }
        for row in sorted(manifest_rows_by_game.values(), key=lambda item: item["game_id"])
    ]

    groups_csv = output_dir / f"identity_conflict_groups_{stamp}.csv"
    rows_csv = output_dir / f"identity_conflict_rows_{stamp}.csv"
    manifest_csv = output_dir / f"identity_conflict_game_manifest_{stamp}.csv"
    group_fieldnames = [
        "table_name",
        "group_key",
        "game_date",
        "game_id",
        "player_id",
        "team_code",
        "row_count",
        "row_ids",
        "classification",
        "candidate_ids",
        "candidate_profiles",
        "player_names",
        "team_codes",
        "positions",
        "source_review_rows",
    ]
    detail_fieldnames = [
        "table_name",
        "group_key",
        "game_date",
        "id",
        "game_id",
        "team_side",
        "team_code",
        "player_id",
        "player_name",
        "candidate_ids",
        "candidate_profiles",
        "suggestion_status",
        "suggested_player_id",
        "needs_source_review",
        "suggestion_reason",
        "row_identity_summary",
        "current_player_birth_date",
        "current_player_debut_year",
        *sorted(
            {key for row in detail_rows for key in row}
            - {
                "table_name",
                "group_key",
                "game_date",
                "id",
                "game_id",
                "team_side",
                "team_code",
                "player_id",
                "player_name",
                "candidate_ids",
                "candidate_profiles",
                "suggestion_status",
                "suggested_player_id",
                "needs_source_review",
                "suggestion_reason",
                "row_identity_summary",
                "current_player_birth_date",
                "current_player_debut_year",
            }
        ),
    ]
    _write_csv(groups_csv, group_rows, group_fieldnames)
    _write_csv(rows_csv, detail_rows, detail_fieldnames or ["table_name"])
    _write_csv(
        manifest_csv,
        manifest_rows,
        [
            "game_id",
            "game_date",
            "tables",
            "player_names",
            "team_codes",
            "group_count",
            "row_count",
            "source_review_rows",
        ],
    )

    summary: dict[str, int] = {}
    for row in group_rows:
        key = f"{row['table_name']}:{row['player_names']}:{row['team_codes']}"
        summary[key] = summary.get(key, 0) + 1

    return {
        "groups": len(group_rows),
        "rows": len(detail_rows),
        "source_review_rows": sum(int(row["needs_source_review"]) for row in detail_rows),
        "groups_csv": str(groups_csv),
        "rows_csv": str(rows_csv),
        "manifest_csv": str(manifest_csv),
        "summary": summary,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export same-name identity-conflict duplicate worklists.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="CSV report directory.")
    parser.add_argument("--player-name", default=None, help="Optional exact Korean player name filter.")
    parser.add_argument("--team-code", default=None, help="Optional exact team code filter.")
    parser.add_argument(
        "--table",
        choices=[config.table_name for config in DUPLICATE_CONFIGS],
        default=None,
        help="Optional table filter.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = export_identity_conflict_worklist(
        db_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        player_name=args.player_name,
        team_code=args.team_code,
        table_name=args.table,
    )
    print(f"[REPORT] groups={result['groups']} rows={result['rows']} source_review_rows={result['source_review_rows']}")
    for key in sorted(result["summary"]):
        print(f"  {key}={result['summary'][key]}")
    print(f"[REPORT] groups_csv={result['groups_csv']}")
    print(f"[REPORT] rows_csv={result['rows_csv']}")
    print(f"[REPORT] manifest_csv={result['manifest_csv']}")


if __name__ == "__main__":
    main()
