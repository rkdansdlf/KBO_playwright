#!/usr/bin/env python3
"""Conservatively repair game rows where one player_id appears on both teams.

The repair only updates a row when same-name, same-season, same-team season
tables identify exactly one existing player_id for that row's team. Ambiguous
or missing candidates are reported and left unchanged.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


DEFAULT_DB_PATH = Path("data/kbo_dev.db")
DEFAULT_OUTPUT_DIR = Path("data/player_team_collision_worklists")


@dataclass(frozen=True)
class RepairCandidate:
    table_name: str
    row_id: int
    game_id: str
    season: int
    player_name: str
    team_code: str
    current_player_id: int
    resolved_player_id: int | None
    status: str
    candidate_ids: tuple[int, ...]
    reason: str = ""
    conflicting_row_id: int | None = None


@dataclass(frozen=True)
class CuratedResolution:
    table_name: str
    row_id: int
    game_id: str
    player_name: str
    team_code: str
    current_player_id: int
    resolved_player_id: int
    reason: str
    delete_conflicting_target: bool = False


CURATED_RESOLUTIONS: dict[tuple[str, int], CuratedResolution] = {
    (
        "game_batting_stats",
        439975,
    ): CuratedResolution(
        table_name="game_batting_stats",
        row_id=439975,
        game_id="20181009SSSK0",
        player_name="김재현",
        team_code="SK",
        current_player_id=64499,
        resolved_player_id=76869,
        reason="2018 SK 김재현은 player_id 76869이고 64499는 삼성 김재현이다. 같은 unique key의 target row는 PA가 0인 중복 row라 current row를 보존한다.",
        delete_conflicting_target=True,
    ),
    (
        "game_batting_stats",
        954326,
    ): CuratedResolution(
        table_name="game_batting_stats",
        row_id=954326,
        game_id="20210323WOSS0",
        player_name="김재현",
        team_code="SS",
        current_player_id=62332,
        resolved_player_id=64499,
        reason="2021 삼성 보류/방출 명단의 내야수 김재현은 삼성 2014-2020 시즌 이력이 있는 player_id 64499이고, 62332는 키움 포수 김재현이다.",
    ),
    (
        "game_lineups",
        629063,
    ): CuratedResolution(
        table_name="game_lineups",
        row_id=629063,
        game_id="20060420LGSK0",
        player_name="김재현",
        team_code="LG",
        current_player_id=94107,
        resolved_player_id=98776,
        reason="2006 LG 투수 김재현은 player_season_pitching FINAL_VERIFICATION 기준 player_id 98776이고, 94107은 SK/LG 타자 김재현이다.",
    ),
    (
        "game_lineups",
        947301,
    ): CuratedResolution(
        table_name="game_lineups",
        row_id=947301,
        game_id="20210323WOSS0",
        player_name="김재현",
        team_code="SS",
        current_player_id=62332,
        resolved_player_id=64499,
        reason="2021 삼성 보류/방출 명단의 내야수 김재현은 삼성 2014-2020 시즌 이력이 있는 player_id 64499이고, 62332는 키움 포수 김재현이다.",
    ),
}


def _backup_sqlite_database(db_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_player_team_collision_repair_{stamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is not None


def _candidate_tables(table_name: str) -> tuple[str, ...]:
    if table_name == "game_batting_stats":
        return ("player_season_batting",)
    if table_name == "game_pitching_stats":
        return ("player_season_pitching",)
    return ("player_season_batting", "player_season_pitching")


def _season_from_game_id(game_id: str) -> int | None:
    if len(game_id) >= 4 and game_id[:4].isdigit():
        return int(game_id[:4])
    return None


def _candidate_ids(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    season: int,
    team_code: str,
    player_name: str,
) -> tuple[int, ...]:
    candidates: set[int] = set()
    for season_table in _candidate_tables(table_name):
        if not _table_exists(conn, season_table):
            continue
        rows = conn.execute(
            f"""
            SELECT DISTINCT pb.player_id
            FROM {season_table} ps
            JOIN player_basic pb ON pb.player_id = ps.player_id
            WHERE ps.season = ?
              AND ps.team_code = ?
              AND pb.name = ?
            """,
            (season, team_code, player_name),
        ).fetchall()
        candidates.update(int(row[0]) for row in rows if row[0] is not None)
    return tuple(sorted(candidates))


def _conflicting_unique_target_row_id(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    row_id: int,
    game_id: str,
    resolved_player_id: int,
) -> int | None:
    if table_name not in {"game_batting_stats", "game_pitching_stats"}:
        return None
    row = conn.execute(
        f"SELECT appearance_seq FROM {table_name} WHERE id = ?",
        (row_id,),
    ).fetchone()
    if row is None:
        return None
    conflict = conn.execute(
        f"""
        SELECT id
        FROM {table_name}
        WHERE id <> ?
          AND game_id = ?
          AND player_id = ?
          AND appearance_seq = ?
        LIMIT 1
        """,
        (row_id, game_id, resolved_player_id, row["appearance_seq"]),
    ).fetchone()
    return int(conflict["id"]) if conflict is not None else None


def _target_unique_key_exists(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    row_id: int,
    game_id: str,
    resolved_player_id: int,
) -> bool:
    return (
        _conflicting_unique_target_row_id(
            conn,
            table_name=table_name,
            row_id=row_id,
            game_id=game_id,
            resolved_player_id=resolved_player_id,
        )
        is not None
    )


def _validated_curated_resolution(row: sqlite3.Row, table_name: str) -> CuratedResolution | None:
    resolution = CURATED_RESOLUTIONS.get((table_name, int(row["id"])))
    if resolution is None:
        return None
    if (
        resolution.table_name != table_name
        or resolution.game_id != str(row["game_id"])
        or resolution.player_name != str(row["player_name"] or "")
        or resolution.team_code != str(row["team_code"] or "")
        or resolution.current_player_id != int(row["player_id"])
    ):
        return None
    return resolution


def _collision_rows(conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
    if not _table_exists(conn, table_name):
        return []
    return conn.execute(
        f"""
        WITH collisions AS (
            SELECT game_id, player_id
            FROM {table_name}
            WHERE player_id IS NOT NULL
            GROUP BY game_id, player_id
            HAVING COUNT(DISTINCT COALESCE(team_side, '') || ':' || COALESCE(team_code, '')) > 1
        )
        SELECT t.id, t.game_id, t.player_id, t.player_name, t.team_code
        FROM {table_name} t
        JOIN collisions c
          ON c.game_id = t.game_id
         AND c.player_id = t.player_id
        ORDER BY t.game_id, t.player_id, t.id
        """
    ).fetchall()


def collect_repair_candidates(
    conn: sqlite3.Connection,
    *,
    use_curated_overrides: bool = False,
) -> list[RepairCandidate]:
    conn.row_factory = sqlite3.Row
    candidates: list[RepairCandidate] = []
    for table_name in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
        for row in _collision_rows(conn, table_name):
            season = _season_from_game_id(str(row["game_id"]))
            if season is None:
                candidates.append(
                    RepairCandidate(
                        table_name=table_name,
                        row_id=int(row["id"]),
                        game_id=str(row["game_id"]),
                        season=0,
                        player_name=str(row["player_name"] or ""),
                        team_code=str(row["team_code"] or ""),
                        current_player_id=int(row["player_id"]),
                        resolved_player_id=None,
                        status="missing_season",
                        candidate_ids=(),
                    )
                )
                continue

            curated = _validated_curated_resolution(row, table_name) if use_curated_overrides else None
            if curated is not None:
                conflicting_row_id = _conflicting_unique_target_row_id(
                    conn,
                    table_name=table_name,
                    row_id=int(row["id"]),
                    game_id=str(row["game_id"]),
                    resolved_player_id=curated.resolved_player_id,
                )
                if conflicting_row_id is not None and not curated.delete_conflicting_target:
                    status = "target_unique_key_exists"
                    resolved = None
                elif conflicting_row_id is not None:
                    status = "manual_merge_conflicting_target"
                    resolved = curated.resolved_player_id
                else:
                    status = "manual_repairable"
                    resolved = curated.resolved_player_id
                candidates.append(
                    RepairCandidate(
                        table_name=table_name,
                        row_id=int(row["id"]),
                        game_id=str(row["game_id"]),
                        season=season,
                        player_name=str(row["player_name"] or ""),
                        team_code=str(row["team_code"] or ""),
                        current_player_id=int(row["player_id"]),
                        resolved_player_id=resolved,
                        status=status,
                        candidate_ids=(curated.resolved_player_id,),
                        reason=curated.reason,
                        conflicting_row_id=conflicting_row_id,
                    )
                )
                continue

            ids = _candidate_ids(
                conn,
                table_name=table_name,
                season=season,
                team_code=str(row["team_code"] or ""),
                player_name=str(row["player_name"] or ""),
            )
            resolved = ids[0] if len(ids) == 1 else None
            if resolved is None:
                status = "missing_candidate" if not ids else "ambiguous_candidate"
            elif resolved == int(row["player_id"]):
                status = "already_correct"
            elif _target_unique_key_exists(
                conn,
                table_name=table_name,
                row_id=int(row["id"]),
                game_id=str(row["game_id"]),
                resolved_player_id=resolved,
            ):
                status = "target_unique_key_exists"
                resolved = None
            else:
                status = "repairable"

            candidates.append(
                RepairCandidate(
                    table_name=table_name,
                    row_id=int(row["id"]),
                    game_id=str(row["game_id"]),
                    season=season,
                    player_name=str(row["player_name"] or ""),
                    team_code=str(row["team_code"] or ""),
                    current_player_id=int(row["player_id"]),
                    resolved_player_id=resolved,
                    status=status,
                    candidate_ids=ids,
                )
            )
    return candidates


def _candidate_to_row(candidate: RepairCandidate) -> dict[str, Any]:
    return {
        "table_name": candidate.table_name,
        "row_id": candidate.row_id,
        "game_id": candidate.game_id,
        "season": candidate.season,
        "player_name": candidate.player_name,
        "team_code": candidate.team_code,
        "current_player_id": candidate.current_player_id,
        "resolved_player_id": candidate.resolved_player_id or "",
        "status": candidate.status,
        "candidate_ids": ",".join(str(pid) for pid in candidate.candidate_ids),
        "conflicting_row_id": candidate.conflicting_row_id or "",
        "reason": candidate.reason,
    }


def repair_player_team_collisions(
    *,
    db_path: Path,
    output_dir: Path,
    apply: bool,
    backup: bool = True,
    use_curated_overrides: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = _backup_sqlite_database(db_path, output_dir) if apply and backup else None

    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        candidates = collect_repair_candidates(conn, use_curated_overrides=use_curated_overrides)
        repairable_statuses = {"repairable", "manual_repairable", "manual_merge_conflicting_target"}
        repairable = [candidate for candidate in candidates if candidate.status in repairable_statuses]
        unresolved = [
            candidate
            for candidate in candidates
            if candidate.status
            in {"missing_season", "missing_candidate", "ambiguous_candidate", "target_unique_key_exists"}
        ]
        already_correct = [candidate for candidate in candidates if candidate.status == "already_correct"]

        fieldnames = [
            "table_name",
            "row_id",
            "game_id",
            "season",
            "player_name",
            "team_code",
            "current_player_id",
            "resolved_player_id",
            "status",
            "candidate_ids",
            "conflicting_row_id",
            "reason",
        ]
        repairable_csv = output_dir / f"player_team_collision_repairable_{stamp}.csv"
        unresolved_csv = output_dir / f"player_team_collision_unresolved_{stamp}.csv"
        already_correct_csv = output_dir / f"player_team_collision_already_correct_{stamp}.csv"
        _write_csv(repairable_csv, (_candidate_to_row(item) for item in repairable), fieldnames)
        _write_csv(unresolved_csv, (_candidate_to_row(item) for item in unresolved), fieldnames)
        _write_csv(already_correct_csv, (_candidate_to_row(item) for item in already_correct), fieldnames)

        updated_rows = 0
        deleted_conflicting_rows = 0
        if apply:
            for candidate in repairable:
                if candidate.status == "manual_merge_conflicting_target":
                    if candidate.conflicting_row_id is None:
                        raise RuntimeError(f"Missing conflicting_row_id for {candidate.table_name}:{candidate.row_id}")
                    delete_result = conn.execute(
                        f"DELETE FROM {candidate.table_name} WHERE id = ?",
                        (candidate.conflicting_row_id,),
                    )
                    deleted_conflicting_rows += int(delete_result.rowcount or 0)
                result = conn.execute(
                    f"UPDATE {candidate.table_name} SET player_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (candidate.resolved_player_id, candidate.row_id),
                )
                updated_rows += int(result.rowcount or 0)
            conn.commit()
        else:
            conn.rollback()

        return {
            "dry_run": not apply,
            "repairable_rows": len(repairable),
            "unresolved_rows": len(unresolved),
            "already_correct_rows": len(already_correct),
            "updated_rows": updated_rows,
            "deleted_conflicting_rows": deleted_conflicting_rows,
            "repairable_csv": str(repairable_csv),
            "unresolved_csv": str(unresolved_csv),
            "already_correct_csv": str(already_correct_csv),
            "backup_path": str(backup_path) if backup_path else "",
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair player/team collision rows conservatively.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="CSV report directory.")
    parser.add_argument("--apply", action="store_true", help="Persist repairable row updates.")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup before --apply.")
    parser.add_argument(
        "--use-curated-overrides",
        action="store_true",
        help="Apply explicit row-level resolutions for audited local collision debt.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = repair_player_team_collisions(
        db_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
        backup=not args.no_backup,
        use_curated_overrides=bool(args.use_curated_overrides),
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] repairable_rows={result['repairable_rows']} "
        f"unresolved_rows={result['unresolved_rows']} "
        f"already_correct_rows={result['already_correct_rows']} "
        f"updated_rows={result['updated_rows']} "
        f"deleted_conflicting_rows={result['deleted_conflicting_rows']}"
    )
    if result["backup_path"]:
        print(f"[BACKUP] {result['backup_path']}")
    print(f"[REPORT] repairable={result['repairable_csv']}")
    print(f"[REPORT] unresolved={result['unresolved_csv']}")
    print(f"[REPORT] already_correct={result['already_correct_csv']}")


if __name__ == "__main__":
    main()
