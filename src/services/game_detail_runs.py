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
from dataclasses import field as dataclasses_field
from typing import TYPE_CHECKING

from src.crawlers.failure_taxonomy import FailureCode, classify_persist_failure
from src.db.engine import SessionLocal
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_run_service import CrawlRunService

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

GAME_DETAIL_CRAWLER_NAME = "game_detail"
GAME_DETAIL_TARGET_TYPE = "game"
_GAME_ID_YEAR_LEN = 4
#: ``YYYYMMDD`` at the head of a KBO game ID.
_GAME_ID_DATE_LEN = 8


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


@dataclass(frozen=True)
class TerminalOutcome:
    """What became of one game after its fetch and its write.

    Returned only when the ledger transition actually committed. A caller must
    not treat a run it could not record as finished, and must not report a
    success the ledger never accepted.

    For a replay, the persisted run row is still the authority: this is how the
    collection service reports back to its own caller, not a substitute for
    reading the run back.
    """

    status: str
    error_code: str | None = None
    error_message: str | None = None
    counts: RunCounts = _NO_ROWS
    run_id: str | None = None


@dataclass(frozen=True)
class RunOpenResult:
    """Which runs could be started, and why the rest could not.

    Failures are reported rather than absorbed because an unopened run is not a
    bookkeeping detail: it is the one case where a game's data would otherwise be
    written with nothing to attribute it to. The caller decides what to do with a
    game that got no run, and it cannot do that if the answer was discarded here.
    """

    started: dict[str, str] = dataclasses_field(default_factory=dict)
    failures: dict[str, tuple[str, str]] = dataclasses_field(default_factory=dict)

    def run_id_for(self, game_id: str) -> str | None:
        """Return the run started for a game, or None when it got none."""
        return self.started.get(game_id)

    def failure_for(self, game_id: str) -> tuple[str, str] | None:
        """Return the classified ``(code, message)`` for a game that got no run."""
        return self.failures.get(game_id)


def game_date_of(game_id: str) -> str:
    """Return the ``YYYYMMDD`` date a KBO game ID carries.

    A replay cannot read this from the database: the row may be exactly what is
    broken or missing, and the letter would then be unreplayable. The ID is
    self-describing, so the date is taken from it instead.
    """
    prefix = game_id[:_GAME_ID_DATE_LEN]
    if len(prefix) == _GAME_ID_DATE_LEN and prefix.isdigit():
        return prefix
    return ""


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

    def open_runs(self, game_ids: Sequence[str]) -> RunOpenResult:
        """Start a run for every game, before any of them is fetched.

        Args:
            game_ids: The games about to be crawled.

        Returns:
            Which games got a run, and the classified cause for each that did
            not. A run is opened before the fetch so the crawl time is part of
            the duration, which means a game can reach this method's caller with
            no run at all; the caller needs the reason to do anything about it.

        """
        started: dict[str, str] = {}
        failures: dict[str, tuple[str, str]] = {}
        for game_id in game_ids:
            try:
                with SessionLocal() as session:
                    run = CrawlRunService(session).start(_spec_for(game_id))
                    session.commit()
                    started[game_id] = run.run_id
            except Exception as exc:
                _stage, code = classify_persist_failure(exc)
                failures[game_id] = (code.value, f"{type(exc).__name__}: {exc}")
                logger.exception("Failed to open crawl run for %s", game_id)
        return RunOpenResult(started=started, failures=failures)

    def open_run(self, spec: CrawlRunSpec) -> RunOpenResult:
        """Start a run from a spec the caller already built.

        A replay has to run under the identity the dead letter's dispatcher
        allocated -- its `run_id`, and the link back to the run that failed.
        Minting a fresh id here would record the retry as an unrelated crawl and
        break the lineage from incident to recovery.

        Args:
            spec: The run identity, including the replay link.

        Returns:
            The run that was started, or the classified reason none was. A
            replay that cannot open its run must not write anything: it would
            store data with no record of having stored it, and the letter would
            have nothing to resolve against.

        """
        game_id = str(spec.target_id)
        try:
            with SessionLocal() as session:
                run = CrawlRunService(session).start(spec)
                session.commit()
        except Exception as exc:
            _stage, code = classify_persist_failure(exc)
            logger.exception("Failed to open replay run %s", spec.run_id)
            return RunOpenResult(failures={game_id: (code.value, f"{type(exc).__name__}: {exc}")})
        return RunOpenResult(started={game_id: run.run_id})

    def record_success(self, run_id: str, *, counts: RunCounts) -> bool:
        """Close a run whose game was fetched completely and stored.

        Args:
            run_id: The run to close.
            counts: Rows moved.

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
        """Close a run whose game was stored but is not a complete box score.

        Args:
            run_id: The run to close.
            error_code: Failure taxonomy code describing the shortfall.
            error_message: Human-readable explanation.
            counts: Rows moved, with `written` counting the stored payload.

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
        counts: RunCounts = _NO_ROWS,
    ) -> bool:
        """Close a run that produced nothing storable.

        Args:
            run_id: The run to close.
            error_code: Failure taxonomy code.
            error_message: Human-readable explanation.
            counts: Rows moved, normally all zero when the fetch itself failed.

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

        A failure to record is logged and swallowed: the game's data is already
        written or already lost, and raising here would turn a recoverable
        bookkeeping problem into a lost crawl. The return value says whether the
        transition landed, so a caller never treats an unrecorded run as final.

        Returns:
            True when the transition was committed.

        """
        try:
            with SessionLocal() as session:
                service = CrawlRunService(session)
                run = CrawlExecutionRepository(session).get_by_run_id(run_id)
                if run is None:
                    logger.warning("Crawl run %s vanished before finalize", run_id)
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
        except Exception:
            logger.exception("Failed to finalize crawl run %s", run_id)
            return False


__all__ = [
    "GAME_DETAIL_CRAWLER_NAME",
    "GAME_DETAIL_TARGET_TYPE",
    "GameDetailRunLedger",
    "RunCounts",
    "RunOpenResult",
    "TerminalOutcome",
    "game_date_of",
]
