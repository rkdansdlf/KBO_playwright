"""The quality gate's recorded damage, as tables rather than a file on one machine.

A count answers "is anything wrong", and a backlog nobody can repair answers yes
forever. These two tables hold the parts of the answer that change: which season
PA rows were already failing when the record was taken, and which rows lost a
count that no lawful source can supply.

They live in the database because the scheduler runs on a different host from the
one holding the repository. A repository-relative file would exist on the machine
that recorded it and nowhere else, so the daily gate would read "not compared"
forever without anything failing to explain why.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

#: Season scope for a row whose own season is not known.
#:
#: Zero rather than NULL: the season is part of the primary key, and a NULL
#: cannot appear in one. Every KBO season is four digits, so nothing collides.
UNKNOWN_SEASON_SCOPE = 0


class PaViolationBaseline(Base):
    """A season PA row that was already failing when the record was taken.

    The key includes the season the row was found in rather than the scope of the
    run that recorded it. 452 of the recorded players fail in more than one
    season, so a record holding bare player ids cannot tell a ``--year`` run which
    rows are its own: every row it does not name would read as repaired, 714 of
    them, on every run.
    """

    __tablename__ = "pa_violation_baseline"

    check_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope_season: Mapped[int] = mapped_column(Integer, primary_key=True, default=UNKNOWN_SEASON_SCOPE)
    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        Index("idx_pa_violation_baseline_recorded", "recorded_at"),
        Index("idx_pa_violation_baseline_season", "scope_season"),
    )


class PaUnrecoverable(Base):
    """A season row missing a count that no available source can supply.

    Kept apart from :class:`PaViolationBaseline` because the question asked of
    them differs: not "did this get worse" but "is this still the same set of
    rows nobody can fix". Recomputing a lost count from the PA identity would
    fabricate baseball statistics, so the row is recorded instead.
    """

    __tablename__ = "pa_unrecoverable"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    league: Mapped[str | None] = mapped_column(String(16), nullable=True)
    source: Mapped[str | None] = mapped_column(String(20), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (Index("idx_pa_unrecoverable_recorded", "recorded_at"),)
