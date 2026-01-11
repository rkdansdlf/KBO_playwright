from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.crawl import CrawlRun


class CrawlRunRepository:
    def create_run(
        self,
        *,
        label: Optional[str],
        started_at: datetime,
        finished_at: datetime,
        active_count: int,
        retired_count: int,
        staff_count: int,
        confirmed_profiles: int,
        heuristic_only: int,
    ) -> CrawlRun:
        with SessionLocal() as session:
            run = CrawlRun(
                label=label,
                started_at=started_at,
                finished_at=finished_at,
                active_count=active_count,
                retired_count=retired_count,
                staff_count=staff_count,
                confirmed_profiles=confirmed_profiles,
                heuristic_only=heuristic_only,
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            return run
