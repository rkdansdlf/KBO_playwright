from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models.injury import InjuryEntry


class InjuryRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_injury(self, data: dict) -> InjuryEntry:
        player_id = data.get("player_id")
        il_placement_date = data.get("il_placement_date")

        if player_id and il_placement_date:
            stmt = select(InjuryEntry).where(
                InjuryEntry.player_id == player_id,
                InjuryEntry.il_placement_date == il_placement_date,
            )
            existing = self.session.execute(stmt).scalar_one_or_none()
            if existing:
                for key, value in data.items():
                    if key not in ("player_id", "il_placement_date") and value is not None:
                        setattr(existing, key, value)
                return existing

        new_record = InjuryEntry(**data)
        self.session.add(new_record)
        return new_record

    def get_active_by_team(self, team_id: str) -> list[InjuryEntry]:
        stmt = (
            select(InjuryEntry)
            .where(
                InjuryEntry.team_id == team_id,
                InjuryEntry.status.in_(["ACTIVE", "15_IL", "60_IL"]),
            )
            .order_by(InjuryEntry.il_placement_date.desc())
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_all_active(self) -> list[InjuryEntry]:
        stmt = (
            select(InjuryEntry)
            .where(InjuryEntry.status.in_(["ACTIVE", "15_IL", "60_IL"]))
            .order_by(InjuryEntry.il_placement_date.desc())
        )
        return list(self.session.execute(stmt).scalars().all())

    def mark_returned(self, injury_id: int, return_date: date):
        record = self.session.get(InjuryEntry, injury_id)
        if record:
            record.status = "RETURNED"
            record.actual_return_date = return_date
