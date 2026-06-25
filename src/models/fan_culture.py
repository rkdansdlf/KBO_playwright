"""데이터 모델: fan culture."""

from __future__ import annotations

from sqlalchemy import ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TeamRivalry(Base, TimestampMixin):
    """TeamRivalry class."""

    __tablename__ = "team_rivalries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id_a: Mapped[str] = mapped_column(String(10), nullable=False, comment="First team code")
    team_id_b: Mapped[str] = mapped_column(String(10), nullable=False, comment="Second team code")
    rivalry_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Rivalry name (e.g. Korean Series, Cannons Derby)",
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Rivalry description and history")
    intensity: Mapped[str] = mapped_column(String(10), nullable=False, default="MEDIUM", comment="HIGH / MEDIUM / LOW")

    __table_args__ = (UniqueConstraint("team_id_a", "team_id_b", name="uq_team_rivalry"),)

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<TeamRivalry(a='{self.team_id_a}', b='{self.team_id_b}', name='{self.rivalry_name}')>"


class CheerSong(Base, TimestampMixin):
    """CheerSong class."""

    __tablename__ = "cheer_songs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    player_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    song_name: Mapped[str] = mapped_column(String(200), nullable=False, comment="Song title")
    song_type: Mapped[str] = mapped_column(String(20), nullable=False, comment="TEAM / PERSONAL / FIGHT_SONG")
    lyrics: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Lyrics text")
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment="When/how this song is used")
    video_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Video URL")
    introduction_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Year introduced")

    __table_args__ = (
        UniqueConstraint("team_id", "song_name", "song_type", name="uq_cheer_song"),
        Index("idx_cheer_song_team", "team_id"),
    )

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<CheerSong(team='{self.team_id}', name='{self.song_name}', type='{self.song_type}')>"


class CheerChant(Base, TimestampMixin):
    """CheerChant class."""

    __tablename__ = "cheer_chants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    chant_text: Mapped[str] = mapped_column(String(500), nullable=False, comment="Cheer chant text")
    situation: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="When used (e.g. TOP_1ST, AFTER_HR, DEFENSE)",
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Description of the chant")

    __table_args__ = (
        UniqueConstraint("team_id", "chant_text", name="uq_cheer_chant"),
        Index("idx_cheer_chant_team", "team_id"),
    )

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<CheerChant(team='{self.team_id}', chant='{self.chant_text[:30]}...')>"
