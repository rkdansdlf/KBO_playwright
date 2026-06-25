"""crawl run repository 리포지토리."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.models.crawl import CrawlRun

if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class RunStats:
    label: str | None
    started_at: datetime
    finished_at: datetime
    active_count: int
    retired_count: int
    staff_count: int
    confirmed_profiles: int
    heuristic_only: int


class CrawlRunRepository:
    def create_run(self, stats: RunStats) -> CrawlRun:
        with SessionLocal() as session:
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
            session.add(run)
            session.commit()
            session.refresh(run)
            return run
