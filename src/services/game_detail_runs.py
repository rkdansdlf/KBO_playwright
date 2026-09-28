"""One crawl run per game, opened before the fetch and closed after the save.

Game detail is a batch: one crawl covers many games, and each game can end up
complete, degraded, or failed. Wrapping the whole batch in a single run would say
nothing useful, and wrapping the save in a run of its own would leave the fetch
time out of the duration. So the run is opened before the crawl, kept open
across it, and closed only after the write -- one row per game.

Every transition commits on its own short session. A game write that rolls back
must not take the run record with it, and vice versa: the ledger is the only
durable trace of what was attempted, so it has to survive a failed write.

No dead letter is created here. That belongs with the replay path, which has to
decide what a partial run means for a re-fetch; writing one from inside the save
path would double-enqueue.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.crawlers.failure_taxonomy import FailureCode
from src.db.engine import SessionLocal
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_run_service import CrawlRunService

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

GAME_DETAIL_CRAWLER_NAME = "game_detail"
GAME_DETAIL_TARGET_TYPE = "game"
_GAME_ID_YEAR_LEN = 4


@dataclass(frozen=True)
class RunCounts:
    """Rows moved by one game, as the run ledger records them.

    `written` counts rows actually stored, which includes a degraded partial:
    the payload was persisted. A "full detail" flag must not be used here,
    because it is false for exactly the partial case that did write.
    """

    read: int = 0
    written: int = 0
    failed: int = 0


#: Shared empty counts, so a default is not a fresh object per call.
_NO_ROWS = RunCounts()


def _season_of(game_id: str) -> int | None:
    """Return the season encoded in a game ID, when it looks like a KBO one."""
    prefix = game_id[:_GAME_ID_YEAR_LEN]
    return int(prefix) if len(prefix) == _GAME_ID_YEAR_LEN and prefix.isdigit() else None


def _spec_for(game_id: str) -> CrawlRunSpec:
    """Describe one game as a unit of crawl work."""
    return CrawlRunSpec(
        crawler=GAME_DETAIL_CRAWLER_NAME,
        target_type=GAME_DETAIL_TARGET_TYPE,
        target_id=game_id,
        game_id=game_id,
        season=_season_of(game_id),
    )


class GameDetailRunLedger:
    """Record the outcome of each game in a detail batch.

    Each method owns its own transaction, so a failure in one game never rolls
    back the records of the others.
    """

    def open_runs(self, game_ids: Sequence[str]) -> dict[str, str]:
        """Start a run for every game, before any of them is fetched.

        Args:
            game_ids: The games about to be crawled.

        Returns:
            A mapping of game ID to run ID. A game whose run could not be started
            is simply absent, and its outcome is then recorded nowhere rather than
            half-recorded.

        """
        started: dict[str, str] = {}
        for game_id in game_ids:
            try:
                with SessionLocal() as session:
                    run = CrawlRunService(session).start(_spec_for(game_id))
                    session.commit()
                    started[game_id] = run.run_id
            except Exception:
                logger.exception("Failed to open crawl run for %s", game_id)
        return started

    def record_success(self, run_id: str, *, counts: RunCounts) -> None:
        """Close a run whose game was fetched completely and stored.

        Args:
            run_id: The run to close.
            counts: Rows moved.

        """
        self._finalize(run_id, "success", counts=counts)

    def record_partial(
        self,
        run_id: str,
        *,
        error_code: str,
        error_message: str,
        counts: RunCounts,
    ) -> None:
        """Close a run whose game was stored but is not a complete box score.

        Args:
            run_id: The run to close.
            error_code: Failure taxonomy code describing the shortfall.
            error_message: Human-readable explanation.
            counts: Rows moved, with `written` counting the stored payload.

        """
        self._finalize(
            run_id,
            "partial",
            counts=counts,
            error_code=error_code,
            error_message=error_message,
        )

    def record_failed(
        self,
        run_id: str,
        *,
        error_code: str,
        error_message: str,
        counts: RunCounts = _NO_ROWS,
    ) -> None:
        """Close a run that produced nothing storable.

        Args:
            run_id: The run to close.
            error_code: Failure taxonomy code.
            error_message: Human-readable explanation.
            counts: Rows moved, normally all zero when the fetch itself failed.

        """
        self._finalize(
            run_id,
            "failed",
            counts=counts,
            error_code=error_code,
            error_message=error_message,
        )

    def _finalize(
        self,
        run_id: str,
        status: str,
        *,
        counts: RunCounts,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Apply a terminal transition on its own session.

        A failure to record is logged and swallowed: the game's data is already
        written or already lost, and raising here would turn a recoverable
        bookkeeping problem into a lost crawl.
        """
        try:
            with SessionLocal() as session:
                service = CrawlRunService(session)
                run = CrawlExecutionRepository(session).get_by_run_id(run_id)
                if run is None:
                    logger.warning("Crawl run %s vanished before finalize", run_id)
                    return
                if status == "success":
                    service.success(run, records_read=counts.read, records_written=counts.written)
                elif status == "partial":
                    service.partial(
                        run,
                        records_read=counts.read,
                        records_written=counts.written,
                        error_code=error_code,
                        error_message=error_message,
                    )
                else:
                    service.failed(
                        run,
                        error_code=error_code or FailureCode.UNKNOWN.value,
                        error_message=error_message or status,
                        records_read=counts.read,
                        records_written=counts.written,
                        records_failed=counts.failed,
                    )
                session.commit()
        except Exception:
            logger.exception("Failed to finalize crawl run %s", run_id)


__all__ = [
    "GAME_DETAIL_CRAWLER_NAME",
    "GAME_DETAIL_TARGET_TYPE",
    "GameDetailRunLedger",
    "RunCounts",
]
