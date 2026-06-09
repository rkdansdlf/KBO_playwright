from __future__ import annotations

from sqlalchemy import JSON, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class GameBroadcast(Base, TimestampMixin):
    __tablename__ = "game_broadcasts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    broadcaster: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Broadcaster name (e.g. MBC, SBS, KBS, SPOTV, CPBC)",
    )
    channel_name: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="TV channel name")
    streaming_platform: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Streaming platform (e.g. Tving, Naver, Wavve)",
    )
    casters: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, comment="Caster/commentator info {caster, commentator}",
    )
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="KBO", comment="Data source (KBO/NAVER)")

    __table_args__ = (
        UniqueConstraint("game_id", "broadcaster", name="uq_game_broadcast"),
        Index("idx_broadcast_game", "game_id"),
    )

    def __repr__(self) -> str:
        return f"<GameBroadcast(game={self.game_id}, broadcaster='{self.broadcaster}')>"
