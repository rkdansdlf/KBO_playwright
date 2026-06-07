"""
Repository for RosterTransaction operations.
"""

from datetime import date

from sqlalchemy import inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.player import PlayerBasic
from ..models.roster_transaction import RosterTransaction


class RosterTransactionRepository:
    def __init__(self, session: Session):
        self.session = session

    def save(self, data: dict) -> RosterTransaction:
        data = dict(data)
        if not self._player_basic_exists(data.get("player_id")):
            data["player_id"] = None

        dedupe_key = data["dedupe_key"]
        stmt = select(RosterTransaction).where(RosterTransaction.dedupe_key == dedupe_key)
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key != "dedupe_key" and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = RosterTransaction(**data)
        self.session.add(new_record)
        return new_record

    def _player_basic_exists(self, player_id: int | None) -> bool:
        if player_id is None:
            return True
        try:
            tables = set(inspect(self.session.get_bind()).get_table_names())
        except SQLAlchemyError:
            return True
        if PlayerBasic.__tablename__ not in tables:
            return True
        return (
            self.session.execute(
                select(PlayerBasic.player_id).where(PlayerBasic.player_id == player_id)
            ).scalar_one_or_none()
            is not None
        )

    def get_by_team_date(self, team_id: str, transaction_date: date) -> list[RosterTransaction]:
        stmt = (
            select(RosterTransaction)
            .where(
                RosterTransaction.team_id == team_id,
                RosterTransaction.transaction_date == transaction_date,
            )
            .order_by(RosterTransaction.player_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_date(self, transaction_date: date) -> list[RosterTransaction]:
        stmt = (
            select(RosterTransaction)
            .where(RosterTransaction.transaction_date == transaction_date)
            .order_by(RosterTransaction.team_id, RosterTransaction.player_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_player(self, player_id: int, limit: int = 50) -> list[RosterTransaction]:
        stmt = (
            select(RosterTransaction)
            .where(RosterTransaction.player_id == player_id)
            .order_by(RosterTransaction.transaction_date.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_recent_by_team(self, team_id: str, days: int = 7) -> list[RosterTransaction]:
        from datetime import timedelta

        since = date.today() - timedelta(days=days)
        stmt = (
            select(RosterTransaction)
            .where(
                RosterTransaction.team_id == team_id,
                RosterTransaction.transaction_date >= since,
            )
            .order_by(RosterTransaction.transaction_date.desc())
        )
        return list(self.session.execute(stmt).scalars().all())

    def exists(self, dedupe_key: str) -> bool:
        stmt = select(RosterTransaction).where(RosterTransaction.dedupe_key == dedupe_key)
        return self.session.execute(stmt).scalar_one_or_none() is not None

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count
