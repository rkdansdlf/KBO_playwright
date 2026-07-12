"""Repository for StadiumCongestion CRUD operations."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError

from src.models.stadium_congestion import StadiumCongestion

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class CongestionRepository:
    """CongestionRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    # ─────────────────────────────────────────────
    # Write
    # ─────────────────────────────────────────────

    def upsert(self, data: dict) -> tuple[StadiumCongestion, bool]:
        """Insert or update a congestion measurement.

        Dedup key: (stadium_code, location_label, measured_at).

        Returns (record, created: bool).

        Args:
            data: Data.
            data: Data.

        """
        stmt = select(StadiumCongestion).where(
            and_(
                StadiumCongestion.stadium_code == data.get("stadium_code"),
                StadiumCongestion.location_label == data.get("location_label"),
                StadiumCongestion.measured_at == data.get("measured_at"),
            ),
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            mutable = {"congestion_level", "congestion_index", "people_count", "source", "raw_data"}
            for k, v in data.items():
                if k in mutable and v is not None:
                    setattr(existing, k, v)
            existing.updated_at = datetime.now(UTC).replace(tzinfo=None)
            return existing, False

        record = StadiumCongestion(**data)
        self.session.add(record)
        return record, True

    def bulk_upsert(self, records: list[dict]) -> tuple[int, int]:
        """Upsert multiple records.

            Returns (created, updated).

        Args:
            records: Records.
            records: Records.

        """
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
                    "Congestion upsert failed: location=%s at=%s",
                    rec.get("location_label"),
                    rec.get("measured_at"),
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
        location_type: str | None = None,
        location_label: str | None = None,
    ) -> list[StadiumCongestion]:
        """Get by game date.

        Args:
            stadium_code: Stadium Code.
            game_date: Game Date.
            location_type: Location Type.
            location_label: Location Label.
            stadium_code: Stadium Code.
            game_date: Game Date.
            location_type: Location Type.
            location_label: Location Label.
            stadium_code: Stadium Code.
            game_date: Game Date.

        Returns:
            List of results.

        """
        stmt = select(StadiumCongestion).where(
            and_(
                StadiumCongestion.stadium_code == stadium_code,
                StadiumCongestion.game_date == game_date,
            ),
        )
        if location_type:
            stmt = stmt.where(StadiumCongestion.location_type == location_type)
        if location_label:
            stmt = stmt.where(StadiumCongestion.location_label == location_label)
        stmt = stmt.order_by(StadiumCongestion.measured_at.asc())
        return list(self.session.execute(stmt).scalars().all())

    def get_latest(
        self,
        stadium_code: str,
        location_label: str,
    ) -> StadiumCongestion | None:
        """Get latest.

        Args:
            stadium_code: Stadium Code.
            location_label: Location Label.
            stadium_code: Stadium Code.
            location_label: Location Label.
            stadium_code: Stadium Code.
            location_label: Location Label.

        Returns:
            The result of the operation.

        """
        stmt = (
            select(StadiumCongestion)
            .where(
                and_(
                    StadiumCongestion.stadium_code == stadium_code,
                    StadiumCongestion.location_label == location_label,
                ),
            )
            .order_by(StadiumCongestion.measured_at.desc())
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_peak_congestion(
        self,
        stadium_code: str,
        game_date: date,
    ) -> StadiumCongestion | None:
        """Return the highest congestion record for a given game date.

        Args:
            stadium_code: Stadium Code.
            game_date: Game Date.
            stadium_code: Stadium Code.
            game_date: Game Date.

        """
        from sqlalchemy import desc

        stmt = (
            select(StadiumCongestion)
            .where(
                and_(
                    StadiumCongestion.stadium_code == stadium_code,
                    StadiumCongestion.game_date == game_date,
                    StadiumCongestion.congestion_index.isnot(None),
                ),
            )
            .order_by(desc(StadiumCongestion.congestion_index))
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()
