#!/usr/bin/env python3
"""Apply curated row-level repairs for NULL player_id game rows.

This is intentionally narrower than resolve_null_player_ids_conservative.py:
it handles audited rows where group-level resolution is unsafe because the same
team/year/name can refer to multiple real players.
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
DEFAULT_OUTPUT_DIR = Path("data/null_player_id_worklists")


@dataclass(frozen=True)
class CuratedNullResolution:
    table_name: str
    row_id: int
    game_id: str
    team_code: str
    player_name: str
    appearance_seq: int
    resolved_player_id: int
    reason: str


CURATED_NULL_RESOLUTIONS: tuple[CuratedNullResolution, ...] = (
    CuratedNullResolution(
        "game_batting_stats",
        1243613,
        "20110914OBLG0",
        "LG",
        "이병규",
        5,
        97109,
        "2011 LG batting order 3 CF is veteran OF Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1243617,
        "20110914OBLG0",
        "LG",
        "이병규",
        9,
        76100,
        "2011 LG batting order 5 DH is younger Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1263406,
        "20110914OBLG0",
        "LG",
        "이병규",
        5,
        97109,
        "Mirror lineup row for 2011 LG veteran OF Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1263410,
        "20110914OBLG0",
        "LG",
        "이병규",
        9,
        76100,
        "Mirror lineup row for 2011 LG younger Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1243686,
        "20120624LTLG0",
        "LG",
        "이병규",
        1,
        97109,
        "2012 LG leadoff LF is veteran Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1243696,
        "20120624LTLG0",
        "LG",
        "이병규",
        11,
        76100,
        "2012 LG pinch hitter at order 9 is younger Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1263479,
        "20120624LTLG0",
        "LG",
        "이병규",
        1,
        97109,
        "Mirror lineup row for 2012 LG veteran Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1263489,
        "20120624LTLG0",
        "LG",
        "이병규",
        11,
        76100,
        "Mirror lineup row for 2012 LG younger Lee Byung-kyu.",
    ),
    CuratedNullResolution(
        "game_pitching_stats",
        468198,
        "20180814LGHT0",
        "LG",
        "김태형",
        3,
        62918,
        "2018 LG game pitching rows and FINAL_VERIFICATION season pitching use player_id 62918.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1223966,
        "20190721EAWE0",
        "EA",
        "김상수",
        12,
        79402,
        "2019 All-Star East 2B Kim Sang-su is Samsung infielder player_id 79402.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1223972,
        "20190721EAWE0",
        "WE",
        "김현수",
        5,
        76290,
        "2019 All-Star West LF Kim Hyun-soo is LG outfielder player_id 76290.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1243183,
        "20190721EAWE0",
        "EA",
        "김상수",
        12,
        79402,
        "Mirror lineup row for 2019 All-Star East Kim Sang-su.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1243189,
        "20190721EAWE0",
        "WE",
        "김현수",
        5,
        76290,
        "Mirror lineup row for 2019 All-Star West Kim Hyun-soo.",
    ),
    CuratedNullResolution(
        "game_pitching_stats",
        437261,
        "20190721EAWE0",
        "EA",
        "김태훈",
        8,
        79847,
        "2019 All-Star East pitcher Kim Tae-hoon is SK pitcher player_id 79847.",
    ),
    CuratedNullResolution(
        "game_pitching_stats",
        437263,
        "20190721EAWE0",
        "WE",
        "윌슨",
        1,
        68135,
        "2019 All-Star West starter Wilson is LG pitcher Tyler Wilson player_id 68135.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1413674,
        "20260514NCLT0",
        "NC",
        "박시원",
        12,
        50996,
        "2026 NC lineup LF No.53 Park Si-won matches player_id 50996.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494784,
        "20260514NCLT0",
        "NC",
        "박시원",
        12,
        50996,
        "Mirror lineup row for 2026 NC Park Si-won No.53.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1413700,
        "20260514OBHT0",
        "DB",
        "김민석",
        5,
        53554,
        "2026 DB roster around game date maps Kim Min-seok to player_id 53554.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1413701,
        "20260514OBHT0",
        "DB",
        "박지훈",
        11,
        50204,
        "2026 DB roster around game date maps Park Ji-hoon to player_id 50204.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494803,
        "20260514OBHT0",
        "DB",
        "김민석",
        5,
        53554,
        "Mirror lineup row for 2026 DB Kim Min-seok.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494809,
        "20260514OBHT0",
        "DB",
        "박지훈",
        11,
        50204,
        "Mirror lineup row for 2026 DB Park Ji-hoon.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1413755,
        "20260514SKKT0",
        "KT",
        "최원준",
        1,
        66606,
        "2026 KT roster around game date maps Choi Won-jun to player_id 66606.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1413756,
        "20260514SKKT0",
        "KT",
        "김민혁",
        6,
        64004,
        "2026 KT roster around game date maps Kim Min-hyeok to player_id 64004.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494865,
        "20260514SKKT0",
        "KT",
        "최원준",
        1,
        66606,
        "Mirror lineup row for 2026 KT Choi Won-jun.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494870,
        "20260514SKKT0",
        "KT",
        "김민혁",
        6,
        64004,
        "Mirror lineup row for 2026 KT Kim Min-hyeok.",
    ),
    CuratedNullResolution(
        "game_batting_stats",
        1413647,
        "20260514SSLG0",
        "LG",
        "김성진",
        11,
        69105,
        "2026 LG roster around game date maps Kim Sung-jin to player_id 69105.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494770,
        "20260514SSLG0",
        "LG",
        "김성진",
        11,
        69105,
        "Mirror lineup row for 2026 LG Kim Sung-jin.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494940,
        "20260515HHKT0",
        "KT",
        "최원준",
        1,
        66606,
        "2026-05-15 KT daily roster maps Choi Won-jun to player_id 66606.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1495002,
        "20260515LTOB0",
        "DB",
        "박지훈",
        9,
        50204,
        "2026-05-15 DB daily roster maps Park Ji-hoon to player_id 50204.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1495070,
        "20260516HHKT0",
        "KT",
        "최원준",
        1,
        66606,
        "Surrounding 2026 KT daily roster maps Choi Won-jun to player_id 66606.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1495132,
        "20260516LTOB0",
        "DB",
        "박지훈",
        9,
        50204,
        "Surrounding 2026 DB daily roster maps Park Ji-hoon to player_id 50204.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1494888,
        "20260517HHKT0",
        "KT",
        "최원준",
        1,
        66606,
        "Surrounding 2026 KT daily roster maps Choi Won-jun to player_id 66606.",
    ),
    CuratedNullResolution(
        "game_lineups",
        1495042,
        "20260517LTOB0",
        "DB",
        "박지훈",
        9,
        50204,
        "Surrounding 2026 DB daily roster maps Park Ji-hoon to player_id 50204.",
    ),
)


def _backup_sqlite_database(db_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_null_player_id_curated_{stamp}"
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
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _conflicting_unique_target_row_id(
    conn: sqlite3.Connection,
    resolution: CuratedNullResolution,
) -> int | None:
    if resolution.table_name not in {"game_batting_stats", "game_pitching_stats"}:
        return None
    row = conn.execute(
        f"""
        SELECT id
        FROM {resolution.table_name}
        WHERE id <> ?
          AND game_id = ?
          AND player_id = ?
          AND appearance_seq = ?
        LIMIT 1
        """,
        (
            resolution.row_id,
            resolution.game_id,
            resolution.resolved_player_id,
            resolution.appearance_seq,
        ),
    ).fetchone()
    return int(row["id"]) if row is not None else None


def _validate_resolution(conn: sqlite3.Connection, resolution: CuratedNullResolution) -> dict[str, Any]:
    base = {
        "table_name": resolution.table_name,
        "row_id": resolution.row_id,
        "game_id": resolution.game_id,
        "team_code": resolution.team_code,
        "player_name": resolution.player_name,
        "appearance_seq": resolution.appearance_seq,
        "resolved_player_id": resolution.resolved_player_id,
        "status": "",
        "conflicting_row_id": "",
        "reason": resolution.reason,
    }
    if not _table_exists(conn, resolution.table_name):
        return {**base, "status": "missing_table"}
    row = conn.execute(
        f"""
        SELECT id, game_id, team_code, player_name, player_id, appearance_seq
        FROM {resolution.table_name}
        WHERE id = ?
        """,
        (resolution.row_id,),
    ).fetchone()
    if row is None:
        return {**base, "status": "missing_row"}
    if row["player_id"] is not None:
        status = "already_resolved" if int(row["player_id"]) == resolution.resolved_player_id else "row_changed"
        return {**base, "status": status}
    if (
        str(row["game_id"]) != resolution.game_id
        or str(row["team_code"] or "") != resolution.team_code
        or str(row["player_name"] or "") != resolution.player_name
        or int(row["appearance_seq"]) != resolution.appearance_seq
    ):
        return {**base, "status": "row_identity_mismatch"}
    if (
        conn.execute(
            "SELECT 1 FROM player_basic WHERE player_id = ? LIMIT 1",
            (resolution.resolved_player_id,),
        ).fetchone()
        is None
    ):
        return {**base, "status": "missing_player_basic"}
    conflicting_row_id = _conflicting_unique_target_row_id(conn, resolution)
    if conflicting_row_id is not None:
        return {
            **base,
            "status": "target_unique_key_exists",
            "conflicting_row_id": conflicting_row_id,
        }
    return {**base, "status": "repairable"}


def repair_null_player_ids_curated(
    *,
    db_path: Path,
    output_dir: Path,
    apply: bool,
    backup: bool = True,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = _backup_sqlite_database(db_path, output_dir) if apply and backup else None
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        rows = [_validate_resolution(conn, resolution) for resolution in CURATED_NULL_RESOLUTIONS]
        repairable = [row for row in rows if row["status"] == "repairable"]
        skipped = [row for row in rows if row["status"] != "repairable"]

        updated_rows = 0
        if apply:
            for row in repairable:
                result = conn.execute(
                    f"UPDATE {row['table_name']} SET player_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (row["resolved_player_id"], row["row_id"]),
                )
                updated_rows += int(result.rowcount or 0)
            conn.commit()
        else:
            conn.rollback()

        fieldnames = [
            "table_name",
            "row_id",
            "game_id",
            "team_code",
            "player_name",
            "appearance_seq",
            "resolved_player_id",
            "status",
            "conflicting_row_id",
            "reason",
        ]
        repairable_csv = output_dir / f"null_player_id_curated_repairable_{stamp}.csv"
        skipped_csv = output_dir / f"null_player_id_curated_skipped_{stamp}.csv"
        _write_csv(repairable_csv, repairable, fieldnames)
        _write_csv(skipped_csv, skipped, fieldnames)

        return {
            "dry_run": not apply,
            "repairable_rows": len(repairable),
            "skipped_rows": len(skipped),
            "updated_rows": updated_rows,
            "repairable_csv": str(repairable_csv),
            "skipped_csv": str(skipped_csv),
            "backup_path": str(backup_path) if backup_path else "",
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply curated row-level NULL player_id repairs.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="CSV report directory.")
    parser.add_argument("--apply", action="store_true", help="Persist repairable updates.")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup before --apply.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = repair_null_player_ids_curated(
        db_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
        backup=not args.no_backup,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] repairable_rows={result['repairable_rows']} "
        f"skipped_rows={result['skipped_rows']} updated_rows={result['updated_rows']}"
    )
    if result["backup_path"]:
        print(f"[BACKUP] {result['backup_path']}")
    print(f"[REPORT] repairable={result['repairable_csv']}")
    print(f"[REPORT] skipped={result['skipped_csv']}")


if __name__ == "__main__":
    main()
