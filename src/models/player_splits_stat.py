"""ORM model for player situational/split statistics."""

from __future__ import annotations

from sqlalchemy import Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class PlayerSplitsStat(Base, TimestampMixin):
    """Player Situational & Split Statistics (득점권, 좌/우투수, 월별 등)."""

    __tablename__ = "player_splits_stats"

    __table_args__ = (
        UniqueConstraint("season", "player_id", "split_type", "split_key", name="uq_player_splits_stat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    player_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    player_name: Mapped[str] = mapped_column(String(50), nullable=False)
    team_code: Mapped[str | None] = mapped_column(String(20), nullable=True)

    split_type: Mapped[str] = mapped_column(
        String(30), nullable=False, comment="scoring_position, vs_pitcher_type, venue, monthly"
    )
    split_key: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="득점권, 좌투수, 잠실, 4월 등"
    )

    ab: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="타수")
    hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="안타")
    hr: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="홈런")
    rbi: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="타점")
    bb: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="볼넷")
    so: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="삼진")

    avg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, comment="타율")
    obp: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, comment="출루율")
    slg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, comment="장타율")
    ops: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, comment="OPS")

    def __repr__(self) -> str:
        """Return representation string."""
        return (
            f"<PlayerSplitsStat(season={self.season}, player={self.player_name}, "
            f"type={self.split_type}:{self.split_key}, AVG={self.avg:.3f})>"
        )
