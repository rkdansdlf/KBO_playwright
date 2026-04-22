from __future__ import annotations

import argparse
import asyncio
import csv
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.db.engine import DATABASE_URL, SessionLocal
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.utils.team_codes import normalize_kbo_game_id


ACTIONABLE_CLASSIFICATIONS = {"pending_recrawl", "past_scheduled_missing_detail", "dry_run_target"}


def _read_manifest(path: Path, limit: int | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = [
            {
                **row,
                "game_id": normalize_kbo_game_id(row.get("game_id", "")),
                "game_date": str(row.get("game_date") or row.get("game_id", "")[:8]).replace("-", ""),
            }
            for row in csv.DictReader(f)
            if row.get("game_id")
        ]
    return rows[:limit] if limit else rows


def _is_actionable_target(row: dict[str, str]) -> bool:
    classification = str(row.get("classification") or "").strip()
    return not classification or classification in ACTIONABLE_CLASSIFICATIONS


def _write_results(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["game_id", "game_date", "classification", "detail_saved", "relay_rows", "failure_reason"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(DATABASE_URL)
    if db_path is None or not db_path.exists():
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_path = output_dir / f"{db_path.name}.backup_before_2024_detail_backfill_{datetime.now():%Y%m%d_%H%M%S}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _game_has_required_details(game_id: str) -> bool:
    with SessionLocal() as session:
        batting_count = session.execute(
            text("SELECT COUNT(*) FROM game_batting_stats WHERE game_id = :game_id"),
            {"game_id": game_id},
        ).scalar()
        pitching_count = session.execute(
            text("SELECT COUNT(*) FROM game_pitching_stats WHERE game_id = :game_id"),
            {"game_id": game_id},
        ).scalar()
    return bool(batting_count and pitching_count)


async def run(args: argparse.Namespace) -> int:
    targets = _read_manifest(Path(args.manifest), args.limit)
    if not targets:
        print("[BACKFILL] No targets found.")
        return 0

    output_path = Path(args.output or f"data/repair_game_id_integrity/backfill_results_{datetime.now():%Y%m%d_%H%M%S}.csv")
    if not args.apply:
        _write_results(
            output_path,
            [
                {
                    "game_id": row["game_id"],
                    "game_date": row["game_date"],
                    "classification": row.get("classification") or "dry_run_target",
                    "detail_saved": 0,
                    "relay_rows": 0,
                    "failure_reason": "" if _is_actionable_target(row) else "non_actionable_manifest_row",
                }
                for row in targets
            ],
        )
        print(f"[DRY-RUN] {len(targets)} targets written to {output_path}")
        return 0

    skipped = []
    pending_targets = []
    for row in targets:
        if not _is_actionable_target(row):
            skipped.append(
                {
                    "game_id": row["game_id"],
                    "game_date": row["game_date"],
                    "classification": row.get("classification") or "non_actionable",
                    "detail_saved": 0,
                    "relay_rows": 0,
                    "failure_reason": "non_actionable_manifest_row",
                }
            )
        elif _game_has_required_details(row["game_id"]):
            skipped.append(
                {
                    "game_id": row["game_id"],
                    "game_date": row["game_date"],
                    "classification": "already_has_detail",
                    "detail_saved": 0,
                    "relay_rows": 0,
                    "failure_reason": "",
                }
            )
        else:
            pending_targets.append(row)

    backup_path = None if args.no_backup else _backup_sqlite_database(output_path.parent)
    if backup_path:
        print(f"[BACKUP] {backup_path}")
    if not pending_targets:
        _write_results(output_path, skipped)
        print(f"[BACKFILL] no pending targets. results written to {output_path}")
        return 0

    crawler = GameDetailCrawler(request_delay=args.delay)
    details = await crawler.crawl_games(
        [{"game_id": row["game_id"], "game_date": row["game_date"]} for row in pending_targets],
        concurrency=args.concurrency,
    )
    detail_by_id = {detail.get("game_id"): detail for detail in details if detail.get("game_id")}
    relay_crawler = RelayCrawler(request_delay=args.delay) if args.relay else None

    results = list(skipped)
    for target in pending_targets:
        game_id = target["game_id"]
        detail = detail_by_id.get(game_id)
        if not detail:
            results.append(
                {
                    "game_id": game_id,
                    "game_date": target["game_date"],
                    "classification": "crawl_failed",
                    "detail_saved": 0,
                    "relay_rows": 0,
                    "failure_reason": crawler.get_last_failure_reason(game_id) or "no_detail_payload",
                }
            )
            continue

        detail_saved = save_game_detail(detail)
        relay_rows = 0
        if detail_saved and relay_crawler:
            relay_data = await relay_crawler.crawl_game_relay(game_id)
            if relay_data and relay_data.get("events"):
                relay_rows = save_relay_data(game_id, relay_data["events"])
        results.append(
            {
                "game_id": game_id,
                "game_date": target["game_date"],
                "classification": "recrawl_saved" if detail_saved else "save_failed",
                "detail_saved": int(detail_saved),
                "relay_rows": relay_rows,
                "failure_reason": "",
            }
        )

    _write_results(output_path, results)
    print(f"[BACKFILL] results written to {output_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing game details from a repair manifest.")
    parser.add_argument("--manifest", required=True, help="CSV from repair_game_id_integrity.py")
    parser.add_argument("--output", help="Result CSV path")
    parser.add_argument("--apply", action="store_true", help="Actually crawl and save. Default writes dry-run targets only.")
    parser.add_argument("--relay", action="store_true", help="Also fetch relay data after saving detail.")
    parser.add_argument("--limit", type=int, help="Limit number of manifest rows")
    parser.add_argument("--delay", type=float, default=1.2)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    raise SystemExit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()
