"""Backfill ``parking_fee_kinds`` from the parking raw-source snapshots.

The paid fee kinds (기본/추가/일일/행사/경기/무료) were parsed on every sweep
but never persisted -- ``_save_team`` and ``snapshot_persist._save_parking``
both dropped them because the only fee table was keyed by vehicle class. The
kind text is still recoverable: the raw page HTML lives in the snapshot
artifact, and ``PARKING_FEE_PATTERN`` extracts the same ``{label, amount}``
pairs the crawler used.

Read-only by default; use --apply to persist.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.crawlers.parking_crawler import PARKING_FEE_PATTERN, TEAM_PARKING_SOURCES
from src.db.engine import SessionLocal
from src.models.parking_lot import ParkingLot
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.parking_lot_repository import ParkingFeeKindRepository

logger = logging.getLogger(__name__)

#: source_key -> stadium_id, built from the crawler's own source table.
_SOURCE_STADIUM: dict[str, str] = {info["source_key"]: info["stadium_id"] for info in TEAM_PARKING_SOURCES.values()}


@dataclass(frozen=True)
class ParkingFeeBackfillReport:
    """Summary of the backfill run."""

    snapshots: int = 0
    kinds_found: int = 0
    lots_matched: int = 0
    unmatched_lots: int = 0
    applied: bool = False
    unresolved_source_keys: list[str] = field(default_factory=list)


def backfill(
    session: Session,
    *,
    apply: bool = False,
) -> ParkingFeeBackfillReport:
    """Replay parking fee text from raw snapshots into ``parking_fee_kinds``."""
    stmt = (
        select(RawSourceSnapshot, DataSource)
        .join(DataSource, RawSourceSnapshot.data_source_id == DataSource.id)
        .where(DataSource.source_key.in_(_SOURCE_STADIUM.keys()))
    )
    rows = session.execute(stmt).all()

    kinds_found = 0
    lots_matched = 0
    unmatched = 0
    unresolved_keys: set[str] = set()
    pending: list[tuple[ParkingLot, int, str, str | None]] = []

    for snapshot, source in rows:
        source_key = source.source_key
        if source_key not in _SOURCE_STADIUM:
            unresolved_keys.add(source_key)
            continue
        stadium_id = _SOURCE_STADIUM[source_key]
        lot = _find_lot(session, stadium_id, source_key)
        if lot is None:
            unmatched += 1
            continue
        text = _read_snapshot_text(snapshot)
        if text is None:
            logger.warning("[backfill] snapshot %s has no readable artifact", snapshot.id)
            continue
        matches = list(PARKING_FEE_PATTERN.finditer(text))
        if not matches:
            continue
        lots_matched += 1
        for match in matches:
            kinds_found += 1
            amount = int(match.group(2).replace(",", ""))
            pending.append((lot, amount, match.group(1), snapshot.source_url))

    if apply:
        kind_repo = ParkingFeeKindRepository(session)
        # Dedup by (lot, kind): repeated snapshots of the same page would
        # otherwise upsert-overwrite the same row repeatedly.
        seen: set[tuple[int, str]] = set()
        for lot, amount, kind, source_url in pending:
            key = (lot.id, kind)
            if key in seen:
                continue
            seen.add(key)
            kind_repo.save(
                {
                    "parking_lot_id": lot.id,
                    "fee_kind": kind,
                    "amount_krw": amount,
                    "source_url": source_url,
                },
            )
        session.commit()

    return ParkingFeeBackfillReport(
        snapshots=len(rows),
        kinds_found=kinds_found,
        lots_matched=lots_matched,
        unmatched_lots=unmatched,
        applied=apply,
        unresolved_source_keys=sorted(unresolved_keys),
    )


def _find_lot(session: Session, stadium_id: str, source_key: str) -> ParkingLot | None:
    """Locate the lot a parking snapshot describes.

    The crawler names this lot ``"{stadium_id} 주차장"``; prefer that exact name
    and fall back to another lot in the same stadium, because manual seeds may
    have named it by the stadium directly.
    """
    lots = list(session.scalars(select(ParkingLot).where(ParkingLot.stadium_id == stadium_id)).all())
    if not lots:
        logger.warning("[backfill] no lot for stadium_id=%s (source %s)", stadium_id, source_key)
        return None
    for lot in lots:
        if lot.name == f"{stadium_id} 주차장":
            return lot
    return lots[0]


def _read_snapshot_text(snapshot: RawSourceSnapshot) -> str | None:
    """Return the raw snapshot text with HTML tags stripped, or None."""
    path = getattr(snapshot, "raw_html_or_json_path", None)
    if not path:
        return None
    try:
        raw = Path(path).read_bytes()
    except (OSError, ValueError):
        return None
    return _strip_tags(raw.decode("utf-8", errors="replace"))


def _strip_tags(html: str) -> str:
    """Strip HTML tags and collapse whitespace, mirroring the crawler's ``.get_text`` step."""
    import re

    text = re.sub(r"<[^>]+>", " ", html)
    return " ".join(text.split())


def main() -> int:
    """Run the backfill."""
    parser = argparse.ArgumentParser(description="Backfill parking_fee_kinds from raw parking snapshots")
    parser.add_argument("--apply", action="store_true", help="Persist rows (default: dry-run)")
    args = parser.parse_args()

    with SessionLocal() as session:
        report = backfill(session, apply=args.apply)

    print(
        f"snapshots={report.snapshots} kinds_found={report.kinds_found} "
        f"lots_matched={report.lots_matched} unmatched_lots={report.unmatched_lots} "
        f"applied={report.applied}"
    )
    if report.unresolved_source_keys:
        print(f"unresolved_source_keys={report.unresolved_source_keys}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
