#!/usr/bin/env python3
"""Repair reference-data integrity issues that are safe to normalize locally."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import csv
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_DB_URL = "sqlite:///./data/kbo_dev.db"
TEAM_CODE_REPAIRS = {"HD": "HU"}
TEAM_CODE_COLUMNS = (
    ("game", "home_team"),
    ("game", "away_team"),
    ("game", "winning_team"),
    ("game_inning_scores", "team_code"),
    ("game_lineups", "team_code"),
    ("game_batting_stats", "team_code"),
    ("game_pitching_stats", "team_code"),
    ("player_season_batting", "team_code"),
    ("player_season_pitching", "team_code"),
    ("player_season_fielding", "team_id"),
    ("player_season_baserunning", "team_id"),
    ("team_season_batting", "team_id"),
    ("team_season_pitching", "team_id"),
)

CODE_VARIANTS = {
    "SSG": ("SK", "SSG"),
    "SK": ("SK", "SSG"),
    "OB": ("DB", "OB"),
    "DB": ("DB", "OB"),
    "DO": ("DB", "OB"),
    "KIA": ("KIA", "HT"),
    "HT": ("KIA", "HT"),
    "NX": ("NX", "WO", "KH"),
    "WO": ("NX", "WO", "KH"),
    "KH": ("NX", "WO", "KH"),
    "HD": ("HU",),
    "HU": ("HU",),
    "SS": ("SS",),
    "LG": ("LG",),
    "LT": ("LT",),
    "HH": ("HH",),
    "KT": ("KT",),
    "NC": ("NC",),
    "MBC": ("MBC", "LG"),
    "BE": ("BE", "HH"),
    "SM": ("SM",),
    "CB": ("CB",),
    "TP": ("TP",),
    "SL": ("SL",),
}
MALFORMED_HOME_TOKENS = {
    "SGIA": ("KIA", "HT"),
    "GIA": ("KIA", "HT"),
    "SGT": ("KT",),
    "GT": ("KT",),
}
TEAM_TOKENS = tuple(sorted(CODE_VARIANTS, key=lambda code: (-len(code), code)))


@dataclass
class RepairAction:
    action_type: str
    table_name: str
    source_id: str
    target_id: str | None
    status: str
    reason: str
    row_count: int = 0


def _table_exists(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _columns(inspector, table_name: str) -> set[str]:
    if not _table_exists(inspector, table_name):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def _write_csv(path: Path, rows: list[RepairAction]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["action_type", "table_name", "source_id", "target_id", "status", "reason", "row_count"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _rowcount(result) -> int:
    return int(result.rowcount or 0) if result.rowcount is not None else 0


def repair_team_codes(conn, inspector, *, apply: bool) -> list[RepairAction]:
    actions: list[RepairAction] = []
    for table_name, column_name in TEAM_CODE_COLUMNS:
        if column_name not in _columns(inspector, table_name):
            continue
        for source_code, target_code in TEAM_CODE_REPAIRS.items():
            count = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = :source_code"),
                    {"source_code": source_code},
                ).scalar()
                or 0
            )
            if not count:
                continue
            if apply:
                result = conn.execute(
                    text(f"UPDATE {table_name} SET {column_name} = :target_code WHERE {column_name} = :source_code"),
                    {"target_code": target_code, "source_code": source_code},
                )
                count = _rowcount(result)
            actions.append(
                RepairAction(
                    action_type="team_code",
                    table_name=table_name,
                    source_id=f"{column_name}={source_code}",
                    target_id=target_code,
                    status="applied" if apply else "dry_run",
                    reason="normalize_hyundai_hd_to_hu",
                    row_count=count,
                )
            )
    return actions


def _parse_game_id(game_id: str) -> tuple[str, str, str] | None:
    match = re.match(r"^(\d{8})([A-Z]+)(\d)$", str(game_id or "").strip().upper())
    if not match:
        return None
    return match.group(1), match.group(2), match.group(3)


def _variant_candidates(game_id: str, existing_game_ids: set[str]) -> list[str]:
    parsed = _parse_game_id(game_id)
    if not parsed:
        return []
    date_part, team_part, dh = parsed
    candidates: set[str] = set()
    for away_token in TEAM_TOKENS:
        if not team_part.startswith(away_token):
            continue
        home_token = team_part[len(away_token) :]
        home_variants = CODE_VARIANTS.get(home_token) or MALFORMED_HOME_TOKENS.get(home_token)
        if not home_variants:
            continue
        for away_variant in CODE_VARIANTS[away_token]:
            for home_variant in home_variants:
                candidate = f"{date_part}{away_variant}{home_variant}{dh}"
                if candidate in existing_game_ids:
                    candidates.add(candidate)
    return sorted(candidates)


def _parsed_team_codes(game_id: str) -> tuple[str, str, str] | None:
    parsed = _parse_game_id(game_id)
    if not parsed:
        return None
    date_part, team_part, _dh = parsed
    for away_token in TEAM_TOKENS:
        if not team_part.startswith(away_token):
            continue
        home_token = team_part[len(away_token) :]
        home_variants = CODE_VARIANTS.get(home_token) or MALFORMED_HOME_TOKENS.get(home_token)
        if home_variants:
            return date_part, CODE_VARIANTS[away_token][0], home_variants[0]
    return None


def _ensure_alias_table(conn, inspector) -> None:
    if _table_exists(inspector, "game_id_aliases"):
        return
    conn.execute(
        text(
            """
            CREATE TABLE game_id_aliases (
                alias_game_id VARCHAR(20) PRIMARY KEY,
                canonical_game_id VARCHAR(20) NOT NULL,
                source VARCHAR(50),
                reason VARCHAR(120),
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
    )


def _record_alias(conn, alias_game_id: str, canonical_game_id: str) -> None:
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    conn.execute(
        text(
            """
            INSERT INTO game_id_aliases (alias_game_id, canonical_game_id, source, reason, created_at, updated_at)
            VALUES (:alias_game_id, :canonical_game_id, 'reference_repair', 'metadata_orphan_resolved', :now, :now)
            ON CONFLICT(alias_game_id) DO UPDATE SET
                canonical_game_id = excluded.canonical_game_id,
                source = excluded.source,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """
        ),
        {"alias_game_id": alias_game_id, "canonical_game_id": canonical_game_id, "now": now},
    )


def _is_blank(value: Any) -> bool:
    return value is None or value == "" or value == {} or value == []


def _merge_metadata(conn, source_game_id: str, target_game_id: str) -> str:
    source_row = (
        conn.execute(
            text("SELECT * FROM game_metadata WHERE game_id = :game_id"),
            {"game_id": source_game_id},
        )
        .mappings()
        .first()
    )
    if not source_row:
        return "source_missing"

    target_row = (
        conn.execute(
            text("SELECT * FROM game_metadata WHERE game_id = :game_id"),
            {"game_id": target_game_id},
        )
        .mappings()
        .first()
    )
    if not target_row:
        conn.execute(
            text("UPDATE game_metadata SET game_id = :target_id WHERE game_id = :source_id"),
            {"target_id": target_game_id, "source_id": source_game_id},
        )
        return "metadata_rekeyed"

    updates: dict[str, Any] = {}
    for key, value in dict(source_row).items():
        if key in {"game_id", "created_at", "updated_at"}:
            continue
        if _is_blank(target_row.get(key)) and not _is_blank(value):
            updates[key] = value
    if updates:
        assignments = ", ".join(f"{key} = :{key}" for key in updates)
        updates["game_id"] = target_game_id
        conn.execute(text(f"UPDATE game_metadata SET {assignments} WHERE game_id = :game_id"), updates)
    conn.execute(text("DELETE FROM game_metadata WHERE game_id = :game_id"), {"game_id": source_game_id})
    return "metadata_merged"


def _insert_cancelled_game(conn, inspector, game_id: str, game_date: str, away_team: str, home_team: str) -> None:
    game_columns = _columns(inspector, "game")
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    values: dict[str, Any] = {
        "game_id": game_id,
        "game_date": f"{game_date[:4]}-{game_date[4:6]}-{game_date[6:8]}",
        "away_team": away_team,
        "home_team": home_team,
        "game_status": "CANCELLED",
        "is_primary": 1,
        "created_at": now,
        "updated_at": now,
    }
    values = {key: value for key, value in values.items() if key in game_columns}
    columns = ", ".join(values)
    placeholders = ", ".join(f":{key}" for key in values)
    conn.execute(text(f"INSERT INTO game ({columns}) VALUES ({placeholders})"), values)


def repair_orphan_metadata(conn, inspector, *, apply: bool) -> list[RepairAction]:
    if not {"game", "game_metadata"} <= set(inspector.get_table_names()):
        return []
    if apply:
        _ensure_alias_table(conn, inspector)
        inspector = inspect(conn)

    existing_game_ids = {str(row[0]) for row in conn.execute(text("SELECT game_id FROM game")).fetchall()}
    orphan_ids = [
        str(row[0])
        for row in conn.execute(
            text(
                """
                SELECT m.game_id
                FROM game_metadata m
                LEFT JOIN game g ON m.game_id = g.game_id
                WHERE g.game_id IS NULL
                ORDER BY m.game_id
                """
            )
        ).fetchall()
    ]

    actions: list[RepairAction] = []
    for orphan_id in orphan_ids:
        candidates = _variant_candidates(orphan_id, existing_game_ids)
        if len(candidates) == 1:
            target_id = candidates[0]
            reason = "mapped_to_existing_game"
            if apply:
                reason = _merge_metadata(conn, orphan_id, target_id)
                _record_alias(conn, orphan_id, target_id)
            actions.append(
                RepairAction(
                    action_type="metadata",
                    table_name="game_metadata",
                    source_id=orphan_id,
                    target_id=target_id,
                    status="applied" if apply else "dry_run",
                    reason=reason,
                    row_count=1,
                )
            )
            continue
        if len(candidates) > 1:
            actions.append(
                RepairAction(
                    action_type="metadata",
                    table_name="game_metadata",
                    source_id=orphan_id,
                    target_id=",".join(candidates),
                    status="skipped",
                    reason="ambiguous_existing_game_candidates",
                    row_count=1,
                )
            )
            continue

        parsed = _parsed_team_codes(orphan_id)
        if not parsed:
            actions.append(
                RepairAction(
                    action_type="metadata",
                    table_name="game_metadata",
                    source_id=orphan_id,
                    target_id=None,
                    status="skipped",
                    reason="unparseable_game_id",
                    row_count=1,
                )
            )
            continue

        date_part, away_team, home_team = parsed
        if apply:
            _insert_cancelled_game(conn, inspector, orphan_id, date_part, away_team, home_team)
            existing_game_ids.add(orphan_id)
        actions.append(
            RepairAction(
                action_type="metadata",
                table_name="game_metadata",
                source_id=orphan_id,
                target_id=orphan_id,
                status="applied" if apply else "dry_run",
                reason="created_cancelled_parent_game",
                row_count=1,
            )
        )
    return actions


def run(*, db_url: str, apply: bool, only: str, output_dir: Path) -> dict[str, Any]:
    engine = create_engine(db_url)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_actions: list[RepairAction] = []

    with engine.begin() as conn:
        inspector = inspect(conn)
        if only in {"team-codes", "all"}:
            all_actions.extend(repair_team_codes(conn, inspector, apply=apply))
            inspector = inspect(conn)
        if only in {"metadata", "all"}:
            all_actions.extend(repair_orphan_metadata(conn, inspector, apply=apply))

    report_path = output_dir / f"reference_integrity_repair_{stamp}.csv"
    _write_csv(report_path, all_actions)
    applied = sum(1 for action in all_actions if action.status == "applied")
    skipped = sum(1 for action in all_actions if action.status == "skipped")
    dry_run = sum(1 for action in all_actions if action.status == "dry_run")
    return {
        "db_url": db_url,
        "apply": apply,
        "only": only,
        "actions": len(all_actions),
        "applied": applied,
        "skipped": skipped,
        "dry_run": dry_run,
        "report_csv": str(report_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair KBO reference integrity issues.")
    parser.add_argument(
        "--db-url", default=None, help="SQLAlchemy database URL. Defaults to DATABASE_URL or local SQLite."
    )
    parser.add_argument("--output-dir", default="data/reference_integrity_repair", help="CSV report output directory.")
    parser.add_argument("--apply", action="store_true", help="Apply repairs. Default is dry-run.")
    parser.add_argument(
        "--only", choices=("team-codes", "metadata", "all"), default="all", help="Repair subset to run."
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    db_url = args.db_url or os.getenv("DATABASE_URL") or os.getenv("SOURCE_DATABASE_URL") or DEFAULT_DB_URL
    result = run(db_url=db_url, apply=args.apply, only=args.only, output_dir=Path(args.output_dir))
    mode = "APPLY" if result["apply"] else "DRY-RUN"
    logger.info(
        "[%s] actions=%s applied=%s dry_run=%s skipped=%s",
        mode,
        result['actions'],
        result['applied'],
        result['dry_run'],
        result['skipped'],
    )
    logger.info(f"report_csv={result['report_csv']}")


if __name__ == "__main__":
    main()
