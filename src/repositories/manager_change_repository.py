from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models.manager_change import ManagerChange


class ManagerChangeRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_change(self, data: dict) -> ManagerChange:
        team_id = data["team_id"]
        season = data["season"]
        new_manager = data["new_manager"]

        stmt = select(ManagerChange).where(
            ManagerChange.team_id == team_id,
            ManagerChange.season == season,
            ManagerChange.new_manager == new_manager,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            for key, value in data.items():
                if key not in ("team_id", "season", "new_manager") and value is not None:
                    setattr(existing, key, value)
            return existing

        new_record = ManagerChange(**data)
        self.session.add(new_record)
        return new_record

    def get_by_team(self, team_id: str) -> list[ManagerChange]:
        stmt = (
            select(ManagerChange)
            .where(ManagerChange.team_id == team_id)
            .order_by(ManagerChange.change_date.desc().nullslast())
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_season(self, season: int) -> list[ManagerChange]:
        stmt = (
            select(ManagerChange)
            .where(ManagerChange.season == season)
            .order_by(ManagerChange.change_date.desc().nullslast())
        )
        return list(self.session.execute(stmt).scalars().all())
