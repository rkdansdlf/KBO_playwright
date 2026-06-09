"""
Repository for StadiumTransitTime CRUD operations.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.stadium_transit_time import StadiumTransitTime

logger = logging.getLogger(__name__)


class TransitTimeRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    # ─────────────────────────────────────────────
    # Write
    # ─────────────────────────────────────────────

    def upsert(self, data: dict) -> tuple[StadiumTransitTime, bool]:
        """
        Insert or update a transit time measurement.
        Dedup key: (stadium_code, origin_label, transport_mode, measured_at).

        Returns (record, created: bool).
        """
        stmt = select(StadiumTransitTime).where(
            and_(
                StadiumTransitTime.stadium_code == data.get("stadium_code"),
                StadiumTransitTime.origin_label == data.get("origin_label"),
                StadiumTransitTime.transport_mode == data.get("transport_mode"),
                StadiumTransitTime.measured_at == data.get("measured_at"),
            )
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            mutable = {"duration_minutes", "distance_meters", "congestion_factor", "source_api", "raw_response"}
            for k, v in data.items():
                if k in mutable and v is not None:
                    setattr(existing, k, v)
            existing.updated_at = datetime.now(UTC).replace(tzinfo=None)
            return existing, False

        record = StadiumTransitTime(**data)
        self.session.add(record)
        return record, True

    def bulk_upsert(self, records: list[dict]) -> tuple[int, int]:
        """Upsert multiple records. Returns (created, updated)."""
        created = updated = 0
        for rec in records:
            try:
                _, is_new = self.upsert(rec)
                if is_new:
                    created += 1
                else:
                    updated += 1
            except SQLAlchemyError:
                logger.exception(
                    "TransitTime upsert failed: origin=%s mode=%s",
                    rec.get("origin_label"),
                    rec.get("transport_mode"),
                )
        return created, updated

    # ─────────────────────────────────────────────
    # Read
    # ─────────────────────────────────────────────

    def get_by_game_date(
        self,
        stadium_code: str,
        game_date: date,
        *,
        origin_label: str | None = None,
        transport_mode: str | None = None,
    ) -> list[StadiumTransitTime]:
        stmt = select(StadiumTransitTime).where(
            and_(
                StadiumTransitTime.stadium_code == stadium_code,
                StadiumTransitTime.game_date == game_date,
            )
        )
        if origin_label:
            stmt = stmt.where(StadiumTransitTime.origin_label == origin_label)
        if transport_mode:
            stmt = stmt.where(StadiumTransitTime.transport_mode == transport_mode)
        stmt = stmt.order_by(StadiumTransitTime.measured_at.asc())
        return list(self.session.execute(stmt).scalars().all())

    def get_latest(
        self,
        stadium_code: str,
        origin_label: str,
        transport_mode: str,
    ) -> StadiumTransitTime | None:
        stmt = (
            select(StadiumTransitTime)
            .where(
                and_(
                    StadiumTransitTime.stadium_code == stadium_code,
                    StadiumTransitTime.origin_label == origin_label,
                    StadiumTransitTime.transport_mode == transport_mode,
                )
            )
            .order_by(StadiumTransitTime.measured_at.desc())
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_avg_duration(
        self,
        stadium_code: str,
        origin_label: str,
        game_date: date,
        transport_mode: str | None = None,
    ) -> float | None:
        """Returns average measured duration for a given origin+game_date."""
        from sqlalchemy import func

        stmt = select(func.avg(StadiumTransitTime.duration_minutes)).where(
            and_(
                StadiumTransitTime.stadium_code == stadium_code,
                StadiumTransitTime.origin_label == origin_label,
                StadiumTransitTime.game_date == game_date,
            )
        )
        if transport_mode:
            stmt = stmt.where(StadiumTransitTime.transport_mode == transport_mode)
        result = self.session.execute(stmt).scalar_one_or_none()
        return float(result) if result is not None else None
