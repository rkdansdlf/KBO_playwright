"""
Team-related models (franchises, team identities, ballparks)
Based on Docs/schema/KBO_teams_schema.md
"""
from sqlalchemy import (
    Column, Integer, String, Text, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship, mapped_column, Mapped
from typing import Optional

from .base import Base, TimestampMixin


class Franchise(Base, TimestampMixin):
    """
    Franchise table - represents historical continuity of a team
    (e.g., Samsung Lions, LG Twins including MBC Cheongryong history)
    """
    __tablename__ = "franchises"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    canonical_name: Mapped[str] = mapped_column(String(64), nullable=False)
    first_season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default='ACTIVE')
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self):
        return f"<Franchise(key='{self.key}', name='{self.canonical_name}', status='{self.status}')>"


class TeamIdentity(Base, TimestampMixin):
    """
    Team identity - represents branding/naming changes over time
    (e.g., MBC Cheongryong → LG Twins, Haitai Tigers → KIA Tigers)
    """
    __tablename__ = "team_identities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    franchise_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("franchises.id", ondelete="CASCADE"),
        nullable=False
    )
    name_kor: Mapped[str] = mapped_column(String(64), nullable=False)
    name_eng: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    short_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    city_kor: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    city_eng: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # Added for Supabase compatibility
    start_season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    end_season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_current: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index('idx_team_identities_franchise', 'franchise_id'),
        Index('idx_team_identities_period', 'franchise_id', 'start_season', 'end_season'),
    )

    def __repr__(self):
        return f"<TeamIdentity(name='{self.name_kor}', period={self.start_season}-{self.end_season})>"


class FranchiseEvent(Base):
    """
    Franchise events - tracks major changes (acquisition, rename, fold, etc.)
    """
    __tablename__ = "franchise_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    franchise_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("franchises.id", ondelete="CASCADE"),
        nullable=False
    )
    event_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    from_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    to_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index('idx_franchise_events', 'franchise_id', 'event_year'),
    )

    def __repr__(self):
        return f"<FranchiseEvent(year={self.event_year}, type='{self.event_type}')>"


class Ballpark(Base, TimestampMixin):
    """
    Ballpark (stadium) information
    """
    __tablename__ = "ballparks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name_kor: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name_eng: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)  # Added for Supabase compatibility
    city_kor: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    city_eng: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # Added for Supabase compatibility
    opened_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    closed_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_dome: Mapped[Optional[bool]] = mapped_column(Integer, nullable=True)
    capacity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self):
        return f"<Ballpark(name='{self.name_kor}', city='{self.city_kor}')>"


class HomeBallparkAssignment(Base, TimestampMixin):
    """
    Home ballpark assignment - maps franchises to ballparks with time periods
    """
    __tablename__ = "home_ballpark_assignments"

    franchise_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("franchises.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    )
    ballpark_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ballparks.id", ondelete="RESTRICT"),
        primary_key=True,
        nullable=False
    )
    start_season: Mapped[Optional[int]] = mapped_column(
        Integer,
        primary_key=True,
        nullable=True,
        default=-1
    )
    end_season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Integer, nullable=False, default=1)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index('idx_home_ballpark_period', 'franchise_id', 'start_season', 'end_season'),
    )

    def __repr__(self):
        return f"<HomeBallparkAssignment(franchise_id={self.franchise_id}, ballpark_id={self.ballpark_id})>"
