"""crawl run repository 리포지토리."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.models.crawl import CrawlRun

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session


@dataclass
class RunStats:
    """RunStats class."""

    label: str | None
    started_at: datetime
    finished_at: datetime
    active_count: int
    retired_count: int
    staff_count: int
    confirmed_profiles: int
    heuristic_only: int


class CrawlRunRepository:
    """CrawlRunRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.

        """
        self.session = session

    def create_run(self, stats: RunStats) -> CrawlRun:
        """Create create run.

        Args:
            stats: Stats.
            stats: Stats.
            stats: Stats.

        Returns:
            CrawlRun instance.

        """
        run = CrawlRun(
            label=stats.label,
            started_at=stats.started_at,
            finished_at=stats.finished_at,
            active_count=stats.active_count,
            retired_count=stats.retired_count,
            staff_count=stats.staff_count,
            confirmed_profiles=stats.confirmed_profiles,
            heuristic_only=stats.heuristic_only,
        )
        self.session.add(run)
        self.session.flush()
        return run
