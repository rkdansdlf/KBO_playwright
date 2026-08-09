"""ORM model for KBO Rookie Draft history."""

from __future__ import annotations

from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class PlayerDraftHistory(Base, TimestampMixin):
    """KBO Rookie Draft History Entry."""

    __tablename__ = "player_draft_histories"

    __table_args__ = (
        UniqueConstraint("season", "draft_type", "round_num", "pick_seq", name="uq_player_draft_history"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    draft_type: Mapped[str] = mapped_column(String(20), nullable=False, default="2차", comment="1차, 2차, 얼리 등")

    round_num: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    pick_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=1, comment="전체 지명 순번")
    team_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    player_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    player_id: Mapped[str | None] = mapped_column(String(50), nullable=True)

    position: Mapped[str | None] = mapped_column(String(20), nullable=True)
    school: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="출신학교")
    sign_fee: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="계약금")

    def __repr__(self) -> str:
        """Return representation string."""
        return (
            f"<PlayerDraftHistory(season={self.season}, pick={self.pick_seq}, "
            f"team={self.team_code}, player={self.player_name})>"
        )
