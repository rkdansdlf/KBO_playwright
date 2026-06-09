from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..models.game_mvp import GameMvp


class GameMvpRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_mvp(self, data: dict) -> GameMvp:
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
        stmt = select(GameMvp).where(GameMvp.game_id == game_id)
        return list(self.session.execute(stmt).scalars().all())

    def delete_by_game(self, game_id: str) -> None:
        stmt = delete(GameMvp).where(GameMvp.game_id == game_id)
        self.session.execute(stmt)
