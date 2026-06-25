"""Repository for StadiumOperationNotice CRUD operations.

Handles upsert logic based on (stadium_code, source_name, external_id)
or (stadium_code, source_name, title, published_at) as fallback.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.models.stadium_operation_notice import StadiumOperationNotice

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class OperationNoticeRepository:
    """OperationNoticeRepository class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    # ─────────────────────────────────────────────
    # Write
    # ─────────────────────────────────────────────

    def upsert(self, data: dict) -> tuple[StadiumOperationNotice, bool]:
        """Insert or update a notice.

        Returns (record, created: bool).
        """
        stadium_code = data.get("stadium_code", "")
        source_name = data.get("source_name", "")
        external_id = data.get("external_id")

        # Primary dedup: external_id
        existing: StadiumOperationNotice | None = None
        if external_id:
            stmt = select(StadiumOperationNotice).where(
                and_(
                    StadiumOperationNotice.stadium_code == stadium_code,
                    StadiumOperationNotice.source_name == source_name,
                    StadiumOperationNotice.external_id == external_id,
                ),
            )
            existing = self.session.execute(stmt).scalar_one_or_none()

        # Fallback dedup: title + published_at
        if existing is None:
            title = data.get("title", "")
            published_at = data.get("published_at")
            if title and published_at:
                stmt = select(StadiumOperationNotice).where(
                    and_(
                        StadiumOperationNotice.stadium_code == stadium_code,
                        StadiumOperationNotice.source_name == source_name,
                        StadiumOperationNotice.title == title,
                        StadiumOperationNotice.published_at == published_at,
                    ),
                )
                existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            # Update mutable fields
            mutable_fields = {"content", "is_urgent", "is_confirmed", "raw_snapshot", "game_date", "notice_type"}
            for key, value in data.items():
                if key in mutable_fields and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(UTC).replace(tzinfo=None)
            return existing, False

        record = StadiumOperationNotice(**data)
        self.session.add(record)
        return record, True

    def bulk_upsert(self, notices: list[dict]) -> tuple[int, int]:
        """Upsert multiple notices.

        Returns (created, updated) counts.
        """
        created = updated = 0
        unique_notices = {}
        for n in notices:
            ext_id = n.get("external_id")
            if ext_id:
                key = ("ext", n.get("stadium_code"), n.get("source_name"), ext_id)
            else:
                key = ("fallback", n.get("stadium_code"), n.get("source_name"), n.get("title"), n.get("published_at"))

            if key not in unique_notices:
                unique_notices[key] = n

        for notice in unique_notices.values():
            try:
                _, is_new = self.upsert(notice)
                if is_new:
                    created += 1
                else:
                    updated += 1
            except SQLAlchemyError:
                logger.exception("Notice upsert failed: %s", notice.get("title", "")[:60])
        return created, updated

    # ─────────────────────────────────────────────
    # Read
    # ─────────────────────────────────────────────

    def get_by_game_date(
        self,
        stadium_code: str,
        game_date: date,
        *,
        urgent_only: bool = False,
    ) -> list[StadiumOperationNotice]:
        """Gets by game date.

        Args:
            stadium_code: Stadium Code.
            game_date: Game Date.

        Returns:
            List of results.

        """
        stmt = select(StadiumOperationNotice).where(
            and_(
                StadiumOperationNotice.stadium_code == stadium_code,
                StadiumOperationNotice.game_date == game_date,
            ),
        )
        if urgent_only:
            stmt = stmt.where(StadiumOperationNotice.is_urgent.is_(True))
        stmt = stmt.order_by(StadiumOperationNotice.published_at.desc().nullslast())
        return list(self.session.execute(stmt).scalars().all())

    def get_recent(
        self,
        stadium_code: str,
        *,
        limit: int = 50,
        notice_type: str | None = None,
        source_name: str | None = None,
    ) -> list[StadiumOperationNotice]:
        """Gets recent.

        Args:
            stadium_code: Stadium Code.

        Returns:
            List of results.

        """
        stmt = select(StadiumOperationNotice).where(StadiumOperationNotice.stadium_code == stadium_code)
        if notice_type:
            stmt = stmt.where(StadiumOperationNotice.notice_type == notice_type)
        if source_name:
            stmt = stmt.where(StadiumOperationNotice.source_name == source_name)
        stmt = stmt.order_by(StadiumOperationNotice.published_at.desc().nullslast()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def get_urgent_today(self, stadium_code: str) -> list[StadiumOperationNotice]:
        """Gets urgent today.

        Args:
            stadium_code: Stadium Code.

        Returns:
            List of results.

        """
        today = datetime.now(KST).date()
        return self.get_by_game_date(stadium_code, today, urgent_only=True)

    def get_latest_external_id(self, stadium_code: str, source_name: str) -> str | None:
        """Returns the most recently published external_id for incremental crawling."""
        stmt = (
            select(StadiumOperationNotice.external_id)
            .where(
                and_(
                    StadiumOperationNotice.stadium_code == stadium_code,
                    StadiumOperationNotice.source_name == source_name,
                    StadiumOperationNotice.external_id.isnot(None),
                ),
            )
            .order_by(StadiumOperationNotice.published_at.desc().nullslast())
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()
