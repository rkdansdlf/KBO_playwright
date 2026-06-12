from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models.foreign_player import ForeignPlayerChange


class ForeignPlayerRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_change(self, data: dict) -> ForeignPlayerChange:
        player_name = data["player_name"]
        team_id = data["team_id"]
        season = data["season"]
        change_type = data["change_type"]

        stmt = select(ForeignPlayerChange).where(
            ForeignPlayerChange.player_name == player_name,
            ForeignPlayerChange.team_id == team_id,
            ForeignPlayerChange.season == season,
            ForeignPlayerChange.change_type == change_type,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            for key, value in data.items():
                if key not in ("player_name", "team_id", "season", "change_type") and value is not None:
                    setattr(existing, key, value)
            return existing

        new_record = ForeignPlayerChange(**data)
        self.session.add(new_record)
        return new_record

    def get_by_team(self, team_id: str, season: int | None = None) -> list[ForeignPlayerChange]:
        stmt = select(ForeignPlayerChange).where(ForeignPlayerChange.team_id == team_id)
        if season:
            stmt = stmt.where(ForeignPlayerChange.season == season)
        stmt = stmt.order_by(ForeignPlayerChange.announcement_date.desc().nullslast())
        return list(self.session.execute(stmt).scalars().all())

    def get_by_season(self, season: int) -> list[ForeignPlayerChange]:
        stmt = (
            select(ForeignPlayerChange)
            .where(ForeignPlayerChange.season == season)
            .order_by(ForeignPlayerChange.announcement_date.desc().nullslast())
        )
        return list(self.session.execute(stmt).scalars().all())
