"""broadcast repository 리포지토리."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from src.models.broadcast import GameBroadcast

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class BroadcastRepository:
    """BroadcastRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    def save_broadcast(self, data: dict) -> GameBroadcast:
        """Save broadcast.

        Args:
            data: Data.
            data: Data.
            data: Data.

        Returns:
            GameBroadcast instance.

        """
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
        """Get by game.

        Args:
            game_id: Game ID.
            game_id: Game ID.
            game_id: Game ID.

        Returns:
            List of results.

        """
        stmt = select(GameBroadcast).where(GameBroadcast.game_id == game_id)

        return list(self.session.execute(stmt).scalars().all())

    def delete_by_game(self, game_id: str) -> None:
        """Delete by game.

        Args:
            game_id: Game ID.
            game_id: Game ID.
            game_id: Game ID.

        """
        stmt = delete(GameBroadcast).where(GameBroadcast.game_id == game_id)

        self.session.execute(stmt)
