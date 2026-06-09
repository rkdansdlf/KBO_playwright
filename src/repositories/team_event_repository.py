"""
Repository for TeamEvent operations.
"""

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ..models.team_event import TeamEvent


class TeamEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> TeamEvent:
        source_url = data.get("source_url")
        title = data.get("title", "")
        if source_url:
            stmt = select(TeamEvent).where(TeamEvent.source_url == source_url).limit(1)
            existing = self.session.execute(stmt).scalar_one_or_none()
            if existing:
                for key, value in data.items():
                    if key not in ("source_url",) and value is not None:
                        setattr(existing, key, value)
                existing.last_seen_at = datetime.now(UTC).replace(tzinfo=None)
                return existing

        stmt = select(TeamEvent).where(
            TeamEvent.team_id == data.get("team_id"),
            TeamEvent.title == title,
        )
        existing = self.session.execute(stmt.limit(1)).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("team_id", "title") and value is not None:
                    setattr(existing, key, value)
            existing.last_seen_at = datetime.now(UTC).replace(tzinfo=None)
            return existing

        new_record = TeamEvent(**data)
        self.session.add(new_record)
        return new_record

    def get_by_team(self, team_id: str, limit: int = 50) -> list[TeamEvent]:
        stmt = (
            select(TeamEvent)
            .where(TeamEvent.team_id == team_id)
            .order_by(TeamEvent.published_at.desc().nullslast())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_upcoming(self, limit: int = 50) -> list[TeamEvent]:
        now = datetime.now(UTC).replace(tzinfo=None)
        stmt = (
            select(TeamEvent)
            .where(
                TeamEvent.status.in_(["scheduled", "open"]),
                TeamEvent.event_end_at > now,
            )
            .order_by(TeamEvent.event_start_at.asc().nullslast())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_game(self, game_id: str) -> list[TeamEvent]:
        stmt = select(TeamEvent).where(TeamEvent.game_id == game_id).order_by(TeamEvent.published_at.desc())
        return list(self.session.execute(stmt).scalars().all())

    def update_status(self, event_id: int, status: str) -> None:
        self.session.execute(update(TeamEvent).where(TeamEvent.id == event_id).values(status=status))
