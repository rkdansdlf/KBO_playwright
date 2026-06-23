from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from ..models.broadcast import GameBroadcast

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class BroadcastRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_broadcast(self, data: dict) -> GameBroadcast:
        game_id = data["game_id"]
        broadcaster = data["broadcaster"]

        stmt = select(GameBroadcast).where(
            GameBroadcast.game_id == game_id,
            GameBroadcast.broadcaster == broadcaster,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            for key, value in data.items():
                if key not in ("game_id", "broadcaster") and value is not None:
                    setattr(existing, key, value)
            return existing

        new_record = GameBroadcast(**data)
        self.session.add(new_record)
        return new_record

    def get_by_game(self, game_id: str) -> list[GameBroadcast]:
        stmt = select(GameBroadcast).where(GameBroadcast.game_id == game_id)
        return list(self.session.execute(stmt).scalars().all())

    def delete_by_game(self, game_id: str) -> None:
        stmt = delete(GameBroadcast).where(GameBroadcast.game_id == game_id)
        self.session.execute(stmt)
