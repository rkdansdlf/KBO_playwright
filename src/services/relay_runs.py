"""One crawl run per game, opened before the fetch and closed after the save.

Relay is the second unit of work to be recorded this way, and it borrows only the
value types from game detail. The transitions are separate because the questions
differ: game detail asks whether a box score is complete, relay asks whether the
innings arrived, and a game can be complete on one axis and missing on the other
without either of them being wrong.

What makes relay's terminal different is that a fetch can stop *after* some
innings have already arrived. Those rows are real and get stored, so the run
records `written > 0` and the status a caller must not read as success. Recording
that as a failure with nothing written would be equally wrong: the data exists,
and throwing away the fact that it exists loses the reason the game is short.

Every transition commits on its own short session, so a relay write that rolls
back cannot take the record of the attempt with it.

No dead letter is created here. That belongs to the DLQ boundary, which has to
decide what a partial relay means for a retry, and enqueueing from inside the save
path would double-enqueue.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.crawlers.failure_taxonomy import FailureCode, classify_persist_failure
from src.db.engine import SessionLocal
from src.monitoring.crawler_metrics import (
    LEDGER_OPERATION_FINALIZE,
    LEDGER_OPERATION_OPEN,
    record_ledger_failure,
)
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_run_ledger import RunCounts, RunOpenResult
from src.services.crawl_run_service import CrawlRunService

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

RELAY_CRAWLER_NAME = "relay"
RELAY_TARGET_TYPE = "game"

#: Length of the ``YYYY`` season prefix of a KBO game ID.
_GAME_ID_YEAR_LEN = 4

#: Shared empty counts, so a default is not a fresh object per call.
_NO_ROWS = RunCounts()


@dataclass(frozen=True)
class RelayOutcome:
    """What became of one game after its relay fetch and its relay write.

    Returned only when the ledger transition committed. A caller must not report
    an outcome the ledger never accepted, and for a replay the persisted run row
    remains the authority over this value.
    """

    status: str
    error_code: str | None = None
    error_message: str | None = None
    counts: RunCounts = _NO_ROWS
    run_id: str | None = None


def _spec_for(game_id: str) -> CrawlRunSpec:
    """Describe one game as a unit of relay crawl work."""
    return CrawlRunSpec(
        crawler=RELAY_CRAWLER_NAME,
        target_type=RELAY_TARGET_TYPE,
        target_id=game_id,
        game_id=game_id,
        season=season_of(game_id),
    )


def season_of(game_id: str) -> int | None:
    """Return the season encoded in a game ID, when it looks like a KBO one."""
    prefix = game_id[:_GAME_ID_YEAR_LEN]
    return int(prefix) if len(prefix) == _GAME_ID_YEAR_LEN and prefix.isdigit() else None


class RelayRunLedger:
    """Record the outcome of each game's relay collection.

    `session_factory` builds the session each transition is recorded on. A
    caller on a database other than the default has to pass its own, or its
    ledger rows land somewhere its data does not.
    """

    def __init__(self, session_factory: Callable[[], Session] | None = None) -> None:
        """Keep the session factory every transition is recorded through."""
        # Resolved at construction, not bound as a default argument: a default is
        # evaluated when the `def` runs, so rebinding `SessionLocal` later would
        # be ignored and the ledger would keep using the original session.
        self._session_factory = session_factory or SessionLocal

    def open_runs(self, game_ids: Sequence[str]) -> RunOpenResult:
        """Start a run for every game, before any of them is fetched.

        Args:
            game_ids: The games about to be crawled.

        Returns:
            Which games got a run, and the classified cause for each that did
            not. A game with no run is fetched and written by nobody, so the
            caller has to be able to tell that it was skipped.

        """
        started: dict[str, str] = {}
        failures: dict[str, tuple[str, str]] = {}
        for game_id in game_ids:
            try:
                with self._session_factory() as session:
                    run = CrawlRunService(session).start(_spec_for(game_id))
                    session.commit()
                    started[game_id] = run.run_id
            except Exception as exc:
                _stage, code = classify_persist_failure(exc)
                failures[game_id] = (code.value, f"{type(exc).__name__}: {exc}")
                record_ledger_failure(RELAY_CRAWLER_NAME, LEDGER_OPERATION_OPEN, code.value)
                logger.exception("Failed to open relay run for %s", game_id)
        return RunOpenResult(started=started, failures=failures)

    def open_run(self, spec: CrawlRunSpec) -> RunOpenResult:
        """Start a run from a spec the caller already built.

        A replay runs under the identity the dead letter's dispatcher allocated.
        Minting a fresh id here would record the retry as an unrelated crawl and
        break the lineage from incident to recovery.

        Args:
            spec: The run identity, including the replay link.

        Returns:
            The run that was started, or the classified reason none was.

        """
        game_id = str(spec.target_id)
        try:
            with self._session_factory() as session:
                run = CrawlRunService(session).start(spec)
                session.commit()
        except Exception as exc:
            _stage, code = classify_persist_failure(exc)
            record_ledger_failure(RELAY_CRAWLER_NAME, LEDGER_OPERATION_OPEN, code.value)
            logger.exception("Failed to open relay replay run %s", spec.run_id)
            return RunOpenResult(failures={game_id: (code.value, f"{type(exc).__name__}: {exc}")})
        return RunOpenResult(started={game_id: run.run_id})

    def record_success(self, run_id: str, *, counts: RunCounts) -> bool:
        """Close a run whose relay was obtained and stored.

        An unchanged payload and a game the source does not carry both land here.
        Neither wrote a row, and both are finished work rather than unfinished
        work, so `counts.written` carries the distinction and the status does not.

        Returns:
            True when the transition was committed.

        """
        return self._finalize(run_id, "success", counts=counts)

    def record_partial(
        self,
        run_id: str,
        *,
        error_code: str,
        error_message: str,
        counts: RunCounts,
    ) -> bool:
        """Close a run that stored some innings and then stopped.

        Args:
            run_id: The run to close.
            error_code: The taxonomy code that stopped the fetch.
            error_message: Human-readable detail.
            counts: Rows moved.

        Returns:
            True when the transition was committed.

        """
        return self._finalize(
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
        counts: RunCounts,
    ) -> bool:
        """Close a run whose relay could not be obtained or stored.

        Returns:
            True when the transition was committed.

        """
        return self._finalize(
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
    ) -> bool:
        """Apply a terminal transition on its own session.

        A failure to record is logged and swallowed: the relay rows are already
        written or already lost, and raising here would turn a recoverable
        bookkeeping problem into a lost crawl. The return value says whether the
        transition landed, so a caller never treats an unrecorded run as final.

        Returns:
            True when the transition was committed.

        """
        try:
            with self._session_factory() as session:
                service = CrawlRunService(session)
                run = CrawlExecutionRepository(session).get_by_run_id(run_id)
                if run is None:
                    record_ledger_failure(
                        RELAY_CRAWLER_NAME,
                        LEDGER_OPERATION_FINALIZE,
                        FailureCode.REPLAY_RUN_MISSING.value,
                    )
                    logger.warning("Relay run %s vanished before finalize", run_id)
                    return False
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
                return True
        except Exception as exc:
            _stage, code = classify_persist_failure(exc)
            record_ledger_failure(RELAY_CRAWLER_NAME, LEDGER_OPERATION_FINALIZE, code.value)
            logger.exception("Failed to finalize relay run %s", run_id)
            return False


__all__ = [
    "RELAY_CRAWLER_NAME",
    "RELAY_TARGET_TYPE",
    "RelayOutcome",
    "RelayRunLedger",
    "season_of",
]
