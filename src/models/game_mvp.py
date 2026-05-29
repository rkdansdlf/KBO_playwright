from __future__ import annotations

from sqlalchemy import ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class GameMvp(Base, TimestampMixin):
    __tablename__ = "game_mvps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    player_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True, index=True
    )
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="MVP player name")
    team_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="MVP team code")
    mvp_type: Mapped[str] = mapped_column(String(20), nullable=False, default="GAME", comment="GAME / WEEKLY / MONTHLY")
    reason: Mapped[str | None] = mapped_column(Text, nullable=True, comment="MVP selection reason / highlights")
    award_source: Mapped[str] = mapped_column(
        String(20), nullable=False, default="NAVER", comment="Source: KBO_POSTGAME / NAVER / MEDIA"
    )

    __table_args__ = (
        UniqueConstraint("game_id", "mvp_type", "player_name", name="uq_game_mvp"),
        Index("idx_mvp_game", "game_id"),
        Index("idx_mvp_player", "player_id"),
    )

    def __repr__(self) -> str:
        return f"<GameMvp(game={self.game_id}, player='{self.player_name}', type='{self.mvp_type}')>"
