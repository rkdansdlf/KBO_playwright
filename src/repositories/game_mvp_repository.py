"""game mvp repository 리포지토리."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from src.models.game_mvp import GameMvp

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class GameMvpRepository:
    """GameMvpRepository class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    def save_mvp(self, data: dict) -> GameMvp:
        """
        Saves mvp.

        Args:
            data: Data.

        Returns:
            GameMvp instance.

        """
        game_id = data["game_id"]
        mvp_type = data.get("mvp_type", "GAME")
        player_name = data["player_name"]

        stmt = select(GameMvp).where(
            GameMvp.game_id == game_id,
            GameMvp.mvp_type == mvp_type,
            GameMvp.player_name == player_name,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            for key, value in data.items():
                if key not in ("game_id", "mvp_type", "player_name") and value is not None:
                    setattr(existing, key, value)
            return existing

        new_record = GameMvp(**data)
        self.session.add(new_record)
        return new_record

    def get_by_game(self, game_id: str) -> list[GameMvp]:
        """
        Gets by game.

        Args:
            game_id: Game ID.

        Returns:
            List of results.

        """
        stmt = select(GameMvp).where(GameMvp.game_id == game_id)
        return list(self.session.execute(stmt).scalars().all())

    def delete_by_game(self, game_id: str) -> None:
        """
        Deletes by game.

        Args:
            game_id: Game ID.

        """
        stmt = delete(GameMvp).where(GameMvp.game_id == game_id)
        self.session.execute(stmt)
