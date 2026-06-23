"""
Repository for TicketOpenRule operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from ..models.ticket_open_rule import TicketOpenRule

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class TicketOpenRuleRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> TicketOpenRule:
        stmt = select(TicketOpenRule).where(
            TicketOpenRule.team_id == data["team_id"],
            TicketOpenRule.platform == data["platform"],
            TicketOpenRule.open_offset_days == data["open_offset_days"],
            TicketOpenRule.open_time == data["open_time"],
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("team_id", "platform", "open_offset_days", "open_time") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = TicketOpenRule(**data)
        self.session.add(new_record)
        return new_record

    def get_by_team(self, team_id: str) -> list[TicketOpenRule]:
        stmt = (
            select(TicketOpenRule)
            .where(TicketOpenRule.team_id == team_id)
            .order_by(TicketOpenRule.platform, TicketOpenRule.open_offset_days)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_all_active(self) -> list[TicketOpenRule]:
        stmt = select(TicketOpenRule).order_by(TicketOpenRule.team_id)
        return list(self.session.execute(stmt).scalars().all())

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count
