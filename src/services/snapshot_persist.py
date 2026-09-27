"""Persistence of parsed snapshot records into their domain repositories.

Shared by the batch parser script and the guarded ``kbo snapshot replay
--persist`` CLI. Writes rely on the domain repositories' upsert semantics and
unique constraints, so re-persisting the same snapshot is idempotent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository
from src.repositories.roster_transaction_repository import RosterTransactionRepository
from src.repositories.source_registry_repository import (
    DataSourceRepository,
    RawSourceSnapshotRepository,
)
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository
from src.repositories.team_event_repository import TeamEventRepository
from src.repositories.ticket_price_repository import TicketPriceRepository
from src.services.snapshot_replay import SnapshotReplayError, parse_snapshot

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PERSIST_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)

DOMAIN_FLAT_REPOS: dict[str, type] = {
    "event": TeamEventRepository,
    "roster": RosterTransactionRepository,
    "ticket": TicketPriceRepository,
    "seat": StadiumSeatSectionRepository,
}


@dataclass(frozen=True)
class SnapshotPersistResult:
    """Outcome of persisting one snapshot's parsed records."""

    snapshot_id: int
    source_key: str | None
    target_domain: str | None
    saved: int
    success: bool
    error: str | None = None
    skipped: bool = False


def _save_flat(session: Session, domain: str, data: list[dict]) -> int:
    repo = cast("Any", DOMAIN_FLAT_REPOS[domain](session))
    count = 0
    for item in data:
        try:
            repo.save(item)
            count += 1
        except PERSIST_EXCEPTIONS:
            logger.exception("Save failed in domain=%s: %s", domain, item.get("title", item.get("player_name", "")))
    return count


def _save_parking(session: Session, data: list[dict]) -> int:
    lot_repo = ParkingLotRepository(session)
    fee_repo = ParkingFeeRuleRepository(session)
    count = 0
    for entry in data:
        try:
            lot = lot_repo.save(entry.get("lot", {}))
            count += 1
            for fee in entry.get("fee_rules", []):
                fee_repo.save({"parking_lot_id": lot.id, **fee})
        except PERSIST_EXCEPTIONS:
            logger.exception("Parking save failed: %s", entry.get("lot", {}).get("name", ""))
    return count


def _save_food(session: Session, data: list[dict]) -> int:
    vendor_repo = StadiumFoodVendorRepository(session)
    menu_repo = StadiumFoodMenuItemRepository(session)
    count = 0
    for entry in data:
        try:
            vendor = vendor_repo.save(entry.get("vendor", {}))
            count += 1
            for menu in entry.get("menus", []):
                menu_repo.save({"vendor_id": vendor.id, **menu})
        except PERSIST_EXCEPTIONS:
            logger.exception("Food save failed: %s", entry.get("vendor", {}).get("vendor_name", ""))
    return count


_DOMAIN_SAVERS: dict[str, Callable[[Session, list[dict]], int]] = {
    "parking": _save_parking,
    "food": _save_food,
}


def supported_domains() -> frozenset[str]:
    """Return the target domains that have a persistence saver."""
    return frozenset(DOMAIN_FLAT_REPOS) | frozenset(_DOMAIN_SAVERS)


def save_parsed(session: Session, target_domain: str, parsed_data: list[dict]) -> int:
    """Persist parsed records into the repository matching ``target_domain``."""
    if target_domain in DOMAIN_FLAT_REPOS:
        return _save_flat(session, target_domain, parsed_data)
    saver = _DOMAIN_SAVERS.get(target_domain)
    if saver:
        return saver(session, parsed_data)
    logger.warning("No repository for domain: %s", target_domain)
    return 0


def _target_domain(factory: Callable[[], Session], snapshot_id: int) -> str | None:
    with factory() as session:
        snapshot = RawSourceSnapshotRepository(session).get_by_id(snapshot_id)
        if snapshot is None:
            return None
        data_source = DataSourceRepository(session).get_by_id(snapshot.data_source_id)
        return data_source.target_domain if data_source is not None else None


def _mark_status(
    factory: Callable[[], Session],
    snapshot_id: int,
    status: str,
    *,
    parser_version: str | None = None,
    error_message: str | None = None,
) -> None:
    try:
        with factory() as session:
            RawSourceSnapshotRepository(session).update_parse_status(
                snapshot_id,
                status,
                parser_version=parser_version,
                error_message=error_message,
            )
            session.commit()
    except SQLAlchemyError:
        logger.exception("Failed to update parse status for snapshot %s", snapshot_id)


def persist_snapshot(
    snapshot_id: int,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> SnapshotPersistResult:
    """Re-parse a snapshot and persist its records into the domain tables."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    parsed = parse_snapshot(snapshot_id, session_factory=factory)
    target_domain = _target_domain(factory, snapshot_id)
    if target_domain is None or target_domain not in supported_domains():
        return SnapshotPersistResult(
            snapshot_id=snapshot_id,
            source_key=parsed.source_key,
            target_domain=target_domain,
            saved=0,
            success=False,
            error=f"unsupported target_domain={target_domain}",
            skipped=True,
        )
    if not parsed.success:
        _mark_status(factory, snapshot_id, "failed", error_message=parsed.error)
        return SnapshotPersistResult(
            snapshot_id=snapshot_id,
            source_key=parsed.source_key,
            target_domain=target_domain,
            saved=0,
            success=False,
            error=parsed.error,
        )

    try:
        with factory() as session:
            saved = save_parsed(session, target_domain, parsed.records)
            session.commit()
    except PERSIST_EXCEPTIONS as exc:
        logger.exception("Persist failed for snapshot %s", snapshot_id)
        _mark_status(factory, snapshot_id, "failed", error_message=str(exc))
        return SnapshotPersistResult(
            snapshot_id=snapshot_id,
            source_key=parsed.source_key,
            target_domain=target_domain,
            saved=0,
            success=False,
            error=str(exc),
        )

    _mark_status(factory, snapshot_id, "done", parser_version=parsed.parser_version)
    return SnapshotPersistResult(
        snapshot_id=snapshot_id,
        source_key=parsed.source_key,
        target_domain=target_domain,
        saved=saved,
        success=True,
    )


def persist_recent_snapshots(
    *,
    limit: int = 50,
    session_factory: Callable[[], Session] | None = None,
) -> list[SnapshotPersistResult]:
    """Persist the most recent snapshots, isolating per-snapshot failures."""
    factory: Callable[[], Session] = session_factory or SessionLocal
    with factory() as session:
        snapshot_ids = [snapshot.id for snapshot in RawSourceSnapshotRepository(session).get_recent(limit=limit)]

    results: list[SnapshotPersistResult] = []
    for snapshot_id in snapshot_ids:
        try:
            results.append(persist_snapshot(snapshot_id, session_factory=factory))
        except SnapshotReplayError as exc:
            logger.warning("Skipping snapshot %s persist: %s", snapshot_id, exc)
            results.append(
                SnapshotPersistResult(
                    snapshot_id=snapshot_id,
                    source_key=None,
                    target_domain=None,
                    saved=0,
                    success=False,
                    error=str(exc),
                ),
            )
    return results
