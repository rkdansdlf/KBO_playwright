"""
Batch parser middleware: processes pending RawSourceSnapshot records.

For each pending snapshot:
  1. Re-fetches the raw content from the URL
  2. Verifies content hash for consistency
  3. Dispatches to the appropriate parser via registry
  4. Saves parsed data to the correct repository
  5. Marks parse_status as 'done' or 'failed'
"""

from __future__ import annotations

import argparse
import hashlib
import logging
from datetime import datetime
from urllib.parse import urlparse

import httpx
from sqlalchemy import select

from src.db.engine import SessionLocal
from src.models.source_registry import DataSource as DSModel
from src.parsers.registry import get_parser
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository
from src.repositories.roster_transaction_repository import RosterTransactionRepository
from src.repositories.source_registry_repository import RawSourceSnapshotRepository
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository
from src.repositories.team_event_repository import TeamEventRepository
from src.repositories.ticket_price_repository import TicketPriceRepository
from src.utils.http_client import DEFAULT_HEADERS as HEADERS
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)

PARSER_VERSION = "1.0"

DOMAIN_FLAT_REPOS = {
    "event": TeamEventRepository,
    "roster": RosterTransactionRepository,
    "ticket": TicketPriceRepository,
    "seat": StadiumSeatSectionRepository,
}


def _save_parsed(session, target_domain: str, parsed_data: list[dict]) -> int:
    if target_domain in DOMAIN_FLAT_REPOS:
        repo_class = DOMAIN_FLAT_REPOS[target_domain]
        repo = repo_class(session)
        count = 0
        for item in parsed_data:
            try:
                repo.save(item)
                count += 1
            except Exception:
                logger.exception(
                    "Save failed in domain=%s: %s", target_domain, item.get("title", item.get("player_name", ""))
                )
        return count

    if target_domain == "parking":
        lot_repo = ParkingLotRepository(session)
        fee_repo = ParkingFeeRuleRepository(session)
        count = 0
        for entry in parsed_data:
            try:
                lot = lot_repo.save(entry.get("lot", {}))
                count += 1
                for fee in entry.get("fee_rules", []):
                    fee_repo.save({"parking_lot_id": lot.id, **fee})
            except Exception:
                logger.exception("Parking save failed: %s", entry.get("lot", {}).get("name", ""))
        return count

    if target_domain == "food":
        vendor_repo = StadiumFoodVendorRepository(session)
        menu_repo = StadiumFoodMenuItemRepository(session)
        count = 0
        for entry in parsed_data:
            try:
                vendor = vendor_repo.save(entry.get("vendor", {}))
                count += 1
                for menu in entry.get("menus", []):
                    menu_repo.save({"vendor_id": vendor.id, **menu})
            except Exception:
                logger.exception("Food save failed: %s", entry.get("vendor", {}).get("vendor_name", ""))
        return count

    logger.warning("No repository for domain: %s", target_domain)
    return 0


def run_batch_parse(
    limit: int = 50, dry_run: bool = False, retry_failed: bool = True, retry_after_hours: int = 1
) -> dict[str, int]:
    stats: dict[str, int] = {"processed": 0, "done": 0, "failed": 0, "skipped": 0}

    with SessionLocal() as session:
        snap_repo = RawSourceSnapshotRepository(session)
        ds_repo = DataSourceRepository(session)

        pending = snap_repo.get_unparsed(limit=limit)
        if retry_failed:
            failed = snap_repo.get_failed_for_retry(retry_after_hours=retry_after_hours, limit=limit - len(pending))
            pending.extend(failed)

        if not pending:
            logger.info("[PARSE] No pending snapshots found.")
            return stats

        logger.info("[PARSE] Processing %d snapshots (dry_run=%s)...", len(pending), dry_run)

        for snapshot in pending:
            stats["processed"] += 1
            stmt = select(DSModel).where(DSModel.id == snapshot.data_source_id)
            ds = session.execute(stmt).scalar_one_or_none()

            if not ds:
                snap_repo.update_parse_status(snapshot.id, "failed", error="DataSource not found")
                stats["failed"] += 1
                continue

            parser = get_parser(ds.source_key)
            if not parser:
                snap_repo.update_parse_status(snapshot.id, "failed", error=f"No parser for source_key={ds.source_key}")
                stats["failed"] += 1
                continue

            url = snapshot.raw_html_or_json_path
            if not url:
                snap_repo.update_parse_status(snapshot.id, "failed", error="No URL in snapshot")
                stats["failed"] += 1
                continue

            try:
                host = urlparse(url).hostname or "koreabaseball.com"
                throttle.wait_sync(host)
                resp = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
                if resp.status_code != 200:
                    snap_repo.update_parse_status(snapshot.id, "failed", error=f"HTTP {resp.status_code}")
                    stats["failed"] += 1
                    continue

                actual_hash = hashlib.sha256(resp.text.encode()).hexdigest()
                if snapshot.content_hash and actual_hash != snapshot.content_hash:
                    snap_repo.update_parse_status(
                        snapshot.id, "failed", error="Content hash mismatch (page changed since crawl)"
                    )
                    stats["failed"] += 1
                    continue

                metadata = {
                    "url": url,
                    "fetched_at": snapshot.fetched_at.isoformat() if snapshot.fetched_at else "",
                    "season": datetime.now().year,
                    "cutoff_days": 60,
                }
                parsed = parser(resp.text, ds.source_key, metadata)

                if dry_run:
                    logger.info("[DRY-RUN] %s: %d items would be saved", ds.source_key, len(parsed))
                    stats["done"] += 1
                else:
                    saved = _save_parsed(session, ds.target_domain, parsed)
                    snap_repo.update_parse_status(snapshot.id, "done", parser_version=PARSER_VERSION)
                    session.commit()
                    logger.info("[PARSE] %s: %d items saved", ds.source_key, saved)
                    stats["done"] += 1

            except Exception as e:
                session.rollback()
                snap_repo.update_parse_status(snapshot.id, "failed", error=str(e))
                session.commit()
                logger.exception(f"Parse failed for snapshot {snapshot.id} ({ds.source_key})")
                stats["failed"] += 1

    logger.info("[PARSE] Done: %d, Failed: %d, Skipped: %d", stats['done'], stats['failed'], stats['skipped'])
    return stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch-parse pending raw snapshots")
    parser.add_argument("--limit", type=int, default=50, help="Max snapshots to process")
    parser.add_argument("--dry-run", action="store_true", help="Fetch + parse but do not save to repos")
    parser.add_argument("--no-retry", action="store_true", help="Skip retry of failed snapshots")
    parser.add_argument(
        "--retry-after-hours", type=int, default=1, help="Retry failed snapshots older than N hours (default: 1)"
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    run_batch_parse(
        limit=args.limit, dry_run=args.dry_run, retry_failed=not args.no_retry, retry_after_hours=args.retry_after_hours
    )


if __name__ == "__main__":
    main()
