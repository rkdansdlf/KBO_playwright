#!/usr/bin/env python3
"""Conservatively resolve NULL player_id values in per-game tables.

The resolver only applies a group when the evidence narrows to exactly one
existing player_basic row. It never auto-registers placeholder players.
"""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL, SessionLocal


DEFAULT_OVERRIDES_CSV = PROJECT_ROOT / "data/player_id_overrides.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data"
DEFAULT_TABLES = ("game_batting_stats", "game_pitching_stats", "game_lineups")


@dataclass(frozen=True)
class OverrideEntry:
    source_table: str
    year: int
    team_code: str
    player_name: str
    resolved_player_id: int
    reason: str = ""
    evidence_source: str = ""


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(DATABASE_URL)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
            """
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
            """
        ),
        {"year": str(year), "team_code": team_code, "player_name": player_name},
    ).fetchall()
    return sorted({str(row[0]).strip() for row in rows if str(row[0]).strip()})


def _existing_player_id(session, player_id: int) -> bool:
    return bool(
        session.execute(
            text("SELECT 1 FROM player_basic WHERE player_id = :player_id LIMIT 1"),
            {"player_id": player_id},
        ).first()
    )


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
            """
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
            """
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
            """
        ),
        {"player_name": player_name},
    ).fetchall()
    return sorted({int(row[0]) for row in rows})


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
    override = overrides.get((table_name, int(season), team_code, player_name))
    if override:
        if _existing_player_id(session, override.resolved_player_id):
            return {
                "candidate_ids": [override.resolved_player_id],
                "resolution_method": "override_exact_group",
                "resolution_reason": override.reason,
            }
        return {
            "candidate_ids": [],
            "resolution_method": "",
            "resolution_reason": "override_player_id_not_found_in_player_basic",
        }

    canonical_name = alias_map.get(player_name, player_name)
    preferred_tables = (
        ("player_season_pitching",)
        if table_name == "game_pitching_stats"
        else ("player_season_batting", "player_season_pitching")
    )

    for season_table in preferred_tables:
        candidate_ids = _candidate_ids_from_season_table(
            session,
            season_table=season_table,
            season=season,
            team_code=team_code,
            player_name=canonical_name,
        )
        uniform_filtered = _filter_by_uniform(session, candidate_ids, uniform_nos)
        if is_group_resolvable(uniform_filtered):
            method = "uniform_filter" if uniform_filtered != candidate_ids else "season_team_name"
            return {
                "candidate_ids": uniform_filtered,
                "resolution_method": method,
                "resolution_reason": season_table,
            }
        if candidate_ids and uniform_nos and not uniform_filtered:
            return {
                "candidate_ids": [],
                "resolution_method": "",
                "resolution_reason": "uniform_filter_no_match",
            }

    season_candidates: set[int] = set()
    for season_table in ("player_season_batting", "player_season_pitching"):
        season_candidates.update(
            _candidate_ids_from_season_table(
                session,
                season_table=season_table,
                season=season,
                team_code=None,
                player_name=canonical_name,
            )
        )
    candidate_ids = sorted(season_candidates)
    uniform_filtered = _filter_by_uniform(session, candidate_ids, uniform_nos)
    if is_group_resolvable(uniform_filtered):
        method = "uniform_filter" if uniform_filtered != candidate_ids else "season_name_unique"
        return {
            "candidate_ids": uniform_filtered,
            "resolution_method": method,
            "resolution_reason": "season_without_team",
        }

    exact_name_candidates = _unique_player_basic_exact_name(session, canonical_name)
    if is_group_resolvable(exact_name_candidates):
        return {
            "candidate_ids": exact_name_candidates,
            "resolution_method": "unique_player_basic_exact_name",
            "resolution_reason": "single_exact_name_in_player_basic",
        }

    return {
        "candidate_ids": candidate_ids,
        "resolution_method": "",
        "resolution_reason": "ambiguous_or_missing_candidate",
    }


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
    if table_name in {"game_batting_stats", "game_pitching_stats"} and "appearance_seq" in columns:
        conflict_guard = f"""
          AND NOT EXISTS (
              SELECT 1
              FROM {table_name} existing
              WHERE existing.game_id = {table_name}.game_id
                AND existing.appearance_seq = {table_name}.appearance_seq
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
        return int(
            session.execute(text(f"SELECT COUNT(*) FROM {table_name} {where_sql}"), params).scalar()
            or 0
        )
    result = session.execute(
        text(f"UPDATE {table_name} SET player_id = :player_id {where_sql}"),
        params,
    )
    return int(result.rowcount or 0)


def _collect_null_groups(session, *, tables: tuple[str, ...], years: tuple[int, ...]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    years_as_text = [str(year) for year in years]
    for table_name in tables:
        stmt = (
            text(
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
                """
            ).bindparams(bindparam("years", expanding=True))
        )
        for row in session.execute(stmt, {"years": years_as_text}).mappings():
            groups.append(
                {
                    "table_name": table_name,
                    "year": int(row["year"]),
                    "team_code": str(row["team_code"] or ""),
                    "player_name": str(row["player_name"] or ""),
                    "row_count": int(row["row_count"] or 0),
                }
            )
    return groups


def resolve_null_player_ids(
    *,
    years: tuple[int, ...],
    tables: tuple[str, ...],
    overrides_csv: Path,
    output_dir: Path,
    apply: bool,
    backup: bool = True,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    alias_map = _load_alias_map()
    overrides = load_overrides(overrides_csv)
    resolved_rows: list[dict[str, Any]] = []
    unresolved_rows: list[dict[str, Any]] = []

    backup_path = None
    if apply and backup:
        backup_path = _backup_sqlite_database(output_dir)

    with SessionLocal() as session:
        groups = _collect_null_groups(session, tables=tables, years=years)
        total_updated = 0
        for group in groups:
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
                "uniform_nos": ",".join(uniforms),
                "candidate_ids": ",".join(str(pid) for pid in candidate_ids),
                "resolution_method": result.get("resolution_method", ""),
                "resolution_reason": result.get("resolution_reason", ""),
            }
            if is_group_resolvable(candidate_ids):
                updated = update_null_player_ids_for_group(
                    session,
                    table_name=group["table_name"],
                    year=group["year"],
                    team_code=group["team_code"],
                    player_name=group["player_name"],
                    player_id=candidate_ids[0],
                    dry_run=not apply,
                )
                total_updated += updated
                resolved_rows.append({**base_row, "resolved_player_id": candidate_ids[0], "updated_rows": updated})
            else:
                unresolved_rows.append({**base_row, "resolved_player_id": "", "updated_rows": 0})
        if apply:
            session.commit()
        else:
            session.rollback()

    resolved_csv = output_dir / f"null_player_id_conservative_resolved_{stamp}.csv"
    unresolved_csv = output_dir / f"null_player_id_conservative_unresolved_{stamp}.csv"
    fieldnames = [
        "table_name",
        "year",
        "team_code",
        "player_name",
        "row_count",
        "uniform_nos",
        "candidate_ids",
        "resolution_method",
        "resolution_reason",
        "resolved_player_id",
        "updated_rows",
    ]
    _write_csv(resolved_csv, resolved_rows, fieldnames)
    _write_csv(unresolved_csv, unresolved_rows, fieldnames)
    return {
        "dry_run": not apply,
        "resolved_groups": len(resolved_rows),
        "unresolved_groups": len(unresolved_rows),
        "updated_rows": total_updated,
        "resolved_csv": str(resolved_csv),
        "unresolved_csv": str(unresolved_csv),
        "backup_path": str(backup_path) if backup_path else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Conservatively fill NULL player_id values.")
    parser.add_argument("--years", default="2024,2025", help="Comma-separated years to inspect.")
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES), help="Comma-separated table names.")
    parser.add_argument("--overrides-csv", default=str(DEFAULT_OVERRIDES_CSV), help="Manual override CSV path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for report CSV files.")
    parser.add_argument("--apply", action="store_true", help="Persist updates. Default is dry-run only.")
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = tuple(int(part.strip()) for part in args.years.split(",") if part.strip())
    tables = tuple(part.strip() for part in args.tables.split(",") if part.strip())
    result = resolve_null_player_ids(
        years=years,
        tables=tables,
        overrides_csv=Path(args.overrides_csv),
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
        backup=not args.no_backup,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] resolved_groups={result['resolved_groups']} "
        f"unresolved_groups={result['unresolved_groups']} updated_rows={result['updated_rows']}"
    )
    if result["backup_path"]:
        print(f"[BACKUP] {result['backup_path']}")
    print(f"[REPORT] resolved={result['resolved_csv']}")
    print(f"[REPORT] unresolved={result['unresolved_csv']}")


if __name__ == "__main__":
    main()
