"""Repository for KBO Press Releases and Notices."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.kbo_press_release import KboPressRelease

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class KboPressReleaseRepository:
    """Repository for managing KBO press release records."""

    def __init__(self, session: Session) -> None:
        """Initialize repository with DB session.

        Args:
            session: DB Session.

        """
        self.session = session

    def save_press_release(self, data: dict[str, Any]) -> KboPressRelease:
        """Save or update a press release record.

        Args:
            data: Press release dictionary data.

        Returns:
            Saved ORM instance.

        """
        notice_id = data["notice_id"]
        stmt = select(KboPressRelease).where(KboPressRelease.notice_id == notice_id)
        existing = self.session.execute(stmt).scalar_one_or_none()

        pub_date = data["published_date"]
        if isinstance(pub_date, str):
            try:
                pub_date = datetime.strptime(pub_date, "%Y-%m-%d").date()  # noqa: DTZ007
            except ValueError:
                pub_date = date.today()  # noqa: DTZ011

        if existing:
            existing.title = data.get("title", existing.title)
            existing.category = data.get("category", existing.category)
            existing.source_url = data.get("source_url", existing.source_url)
            existing.content_summary = data.get("content_summary", existing.content_summary)
            return existing

        record = KboPressRelease(
            notice_id=notice_id,
            published_date=pub_date,
            category=data.get("category", "공시/공지"),
            title=data["title"],
            content_summary=data.get("content_summary"),
            source_url=data["source_url"],
            attachment_url=data.get("attachment_url"),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_recent_releases(self, limit: int = 20) -> list[KboPressRelease]:
        """Get recent press releases.

        Args:
            limit: Maximum count to return.

        Returns:
            List of press release records.

        """
        stmt = select(KboPressRelease).order_by(KboPressRelease.published_date.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())
