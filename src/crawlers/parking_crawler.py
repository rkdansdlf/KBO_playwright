"""Crawler for stadium parking lot and fee information from team websites."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING, Any

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.base import BaseHttpCrawler
from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    FailureCode,
    classify_failure,
    classify_persist_failure,
    stage_for_code,
)
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.parking_lot_repository import ParkingLotRepository
from src.repositories.source_registry_repository import save_raw_snapshots
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.http_client import DEFAULT_HEADERS as HEADERS

if TYPE_CHECKING:
    from src.models.crawl_execution import CrawlExecutionRun
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

PARKING_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, OSError)
PARKING_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)

PARKING_CRAWLER_NAME = "parking"
PARKING_TARGET_TYPE = "parking"

TEAM_PARKING_SOURCES: dict[str, dict[str, Any]] = {
    "LG": {
        "source_key": "jamsil_parking_official",
        "stadium_id": "JAMSIL",
        "url": "https://stadium.seoul.go.kr/about/park-info",
    },
    "SK": {
        "source_key": "ssg_landers_parking",
        "stadium_id": "MUNHAK",
        "url": "https://www.ssglanders.com/stadium/parking",
    },
    "SS": {
        "source_key": "daegu_parking",
        "stadium_id": "DAEGU",
        "url": "https://www.samsunglions.com/stadium/waytocome",
    },
}

PARKING_FEE_PATTERN = re.compile(
    r"(기본|추가|일일|행사|경기|무료)\s*(?:요금|시간|금액)?\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)",
)


class ParkingCrawler(BaseHttpCrawler):
    """ParkingCrawler class."""

    def __init__(
        self,
        request_delay: float = 0.5,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize ParkingCrawler.

        Args:
            request_delay: Request delay in seconds.
            policy: Optional request policy.

        """
        super().__init__(request_delay=request_delay, policy=policy, default_headers=HEADERS)
        self._raw_pages: list[dict] = []
        #: Teams whose page could not be read, kept so the ledger and DLQ can see
        #: them instead of a silent empty list.
        self._team_failures: list[tuple[str, str, str]] = []
        #: Teams whose rows could not be written, kept apart from
        #: ``_team_failures``. Both end with the same table contents, so the run
        #: status cannot distinguish "the source said nothing" from "the source
        #: answered and the write died" unless the two are counted separately.
        self._persist_failures: list[tuple[str, str, str]] = []
        self._http = CrawlerHttpClient(
            name=type(self).__name__,
            policy=HttpPolicy(base_delay_seconds=request_delay, timeout_seconds=15.0),
            headers=HEADERS,
        )

    async def run(
        self,
        *,
        save: bool = False,
        team_filter: str | None = None,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
        raise_on_persist_error: bool = False,
    ) -> list[dict[str, Any]]:
        """Crawl stadium parking pages under one tracked run.

        A team is the replay unit. A page that cannot be read used to be an empty
        list with a log line and nothing else; it is now recorded in the run
        ledger, reflected as a ``partial`` run, and enqueued as its own dead
        letter so the team can be replayed without re-crawling the rest.

        A team whose rows cannot be written is the same unit of work and is
        recorded the same way. That failure used to be logged and then absorbed:
        a single flush error poisoned the session, the commit rolled the whole
        sweep back, and the run reported ``success`` with zero rows written.

        Args:
            save: Whether to persist the results.
            team_filter: Restrict the sweep to one team code.
            run_spec: Optional pre-built ledger spec (replay supplies one).
            record_dead_letters: Whether failures enqueue DLQ entries.
            raise_on_persist_error: Re-raise a write failure as
                :class:`CrawlPersistError` so the caller learns the replay stored
                nothing. The ledger and the dead letter queue already recorded it
                either way.

        Returns:
            The parsed parking lots.

        """
        selected = {code: info for code, info in TEAM_PARKING_SOURCES.items() if not team_filter or code == team_filter}
        spec = run_spec or CrawlRunSpec(
            crawler=PARKING_CRAWLER_NAME,
            target_type=PARKING_TARGET_TYPE,
            target_id=team_filter or "all",
            source_url=next(iter(selected.values()))["url"] if len(selected) == 1 else None,
        )

        with track_crawl_run(spec) as run:
            self._team_failures = []
            self._persist_failures = []
            all_lots: list[dict[str, Any]] = []
            for team_code, info in selected.items():
                try:
                    lots = await self._crawl_team_parking(team_code, info)
                except PARKING_CRAWL_EXCEPTIONS as exc:
                    logger.exception("Failed to crawl parking for %s", team_code)
                    _, code = classify_failure(exc)
                    self._team_failures.append((team_code, code.value, str(exc)))
                    continue
                for entry in lots:
                    # The team is the transaction unit, the dead letter unit and
                    # therefore the only thing that can attribute a write
                    # failure, so it travels with the row rather than being
                    # recovered from a stadium id later.
                    entry["team_code"] = team_code
                all_lots.extend(lots)
                logger.info("[PARKING] %s: %s lots found", team_code, len(lots))

            logger.info("[PARKING] Total: %s lots", len(all_lots))
            run.records_read = len(all_lots)

            if save:
                written, failed = await asyncio.to_thread(self._save_to_db, all_lots)
                run.records_written = written
                run.records_failed = failed
            else:
                for lot in all_lots[:5]:
                    logger.info(lot)

            self._record_team_failures(run, selected, record_dead_letters=record_dead_letters)
            self._raise_persist_error(raise_on_persist_error=raise_on_persist_error)
            return all_lots

    def _record_team_failures(
        self,
        run: CrawlExecutionRun,
        selected: dict[str, dict[str, Any]],
        *,
        record_dead_letters: bool,
    ) -> None:
        """Reflect unreadable and unwritten teams in the run and the dead letter queue.

        A run that read rows but wrote none is ``failed``, not ``partial``. Both
        leave the table in the same state, so calling that a partial success
        would report a total write loss as a short sweep -- and the dead letter
        would then be the only trace that anything went wrong.
        """
        failures = [*self._team_failures, *self._persist_failures]
        if not failures:
            return

        failed_teams = [team_code for team_code, _, _ in failures]
        wrote_nothing = bool(self._persist_failures) and not run.records_written
        run.error_message = f"teams failed: {sorted(set(failed_teams))}" + (
            " (nothing was written)" if wrote_nothing else ""
        )
        if run.records_read and not wrote_nothing:
            run.status = RUN_STATUS_PARTIAL
        else:
            run.status = RUN_STATUS_FAILED
            run.error_code = failures[0][1]
        logger.warning("[PARKING] teams failed: %s", failed_teams)

        if not record_dead_letters:
            return
        for team_code, error_code, message in failures:
            info = selected.get(team_code, {})
            self._enqueue_dead_letter(run.run_id, team_code, error_code, message, source_url=info.get("url"))

    def _raise_persist_error(self, *, raise_on_persist_error: bool) -> None:
        """Re-raise the first write failure when the caller asked to hear about it.

        A replay is judged on the stored run, so the ledger is enough on its own;
        this exists so a caller that wants the exception -- rather than a status
        it has to remember to check -- cannot miss a write that stored nothing.
        """
        if not raise_on_persist_error or not self._persist_failures:
            return
        team_code, error_code, message = self._persist_failures[0]
        detail = f"parking save failed for {team_code}: {message}"
        raise CrawlPersistError(detail, error_code=FailureCode(error_code))

    def _enqueue_dead_letter(
        self,
        original_run_id: str,
        target_id: str,
        error_code: str,
        error_message: str | None,
        *,
        source_url: str | None = None,
    ) -> None:
        """Enqueue one dead letter for a team whose page could not be read."""
        try:
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=original_run_id,
                    crawler=PARKING_CRAWLER_NAME,
                    target_type=PARKING_TARGET_TYPE,
                    target_id=target_id,
                    source_url=source_url,
                    # Derived from the code, never supplied beside it.
                    failure_stage=stage_for_code(error_code).value,
                    error_code=error_code,
                    error_message=error_message,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue dead letter for parking %s", target_id)

    async def _crawl_team_parking(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        result = await self._http.fetch_text(info["url"])
        if not result.ok:
            code = result.error_code or FailureCode.UNKNOWN.value
            message = result.error or str(result.outcome)
            if result.outcome is CrawlOutcome.SCHEMA_CHANGED:
                logger.error(
                    "[PARKING] %s page structure changed at %s: %s",
                    team_code,
                    info["url"],
                    result.error,
                )
            else:
                logger.warning(
                    "[PARKING] %s fetch failed (%s, status=%s): %s",
                    team_code,
                    result.outcome,
                    result.http_status,
                    result.error,
                )
            self._team_failures.append((team_code, code, message))
            return []

        html = result.data
        self._raw_pages.append(
            {
                "source_key": info["source_key"],
                "url": info["url"],
                "html": html,
                "status_code": result.http_status,
            },
        )
        return self._parse_parking_page(html, info)

    def _parse_parking_page(self, html: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        lots = []

        fees = []
        for match in PARKING_FEE_PATTERN.finditer(text):
            label, amount = match.group(1), match.group(2).replace(",", "")
            fees.append({"label": label, "amount": int(amount)})

        lot_name = f"{info['stadium_id']} 주차장"
        lot_data = {
            "stadium_id": info["stadium_id"],
            "name": lot_name,
            "lot_type": "official",
            "is_event_day_available": True,
            "reservation_required": False,
        }
        lots.append({"lot": lot_data, "fee_rules": fees})

        return lots

    def _save_to_db(self, data: list[dict]) -> tuple[int, int]:
        """Persist the sweep, one transaction per team.

        The lot repository flushes on insert, so a constraint violation used to
        leave the session needing a rollback: every later save in the same sweep
        then failed with ``PendingRollbackError``, the batch commit rolled the
        whole sweep back, and the caller logged ``0`` and reported success. One
        team cannot undo another team's rows, and the team that failed can be
        replayed on its own.

        Args:
            data: Parsed lot entries, each carrying the ``team_code`` it came from.

        Returns:
            ``(saved, failed)`` lot counts. A team either wrote all of its lots or
            wrote none, so a failed team contributes every entry it contributed.

        """
        saved_snaps = self._save_snapshots()
        saved = failed = 0
        try:
            for team_code, entries in self._group_by_team(data).items():
                if self._save_team(team_code, entries):
                    saved += len(entries)
                else:
                    failed += len(entries)
        finally:
            self._raw_pages.clear()
        logger.info("[PARKING] Saved %s of %s lots, %s snapshots.", saved, saved + failed, saved_snaps)
        return saved, failed

    def _save_snapshots(self) -> int:
        """Commit the raw pages on their own, before the domain rows.

        The snapshots are the evidence a replay re-parses, so a domain write that
        fails must not discard them: a page stored without its rows is something
        ``kbo snapshot replay`` can finish later, while a page lost with a failed
        transaction is gone until the next sweep happens to re-fetch it.
        """
        try:
            with SessionLocal() as session:
                count = save_raw_snapshots(session, self._raw_pages)
                session.commit()
        except PARKING_SAVE_EXCEPTIONS:
            logger.exception("[PARKING] Snapshot save failed; continuing with the domain rows")
            return 0
        return count

    def _group_by_team(self, data: list[dict]) -> dict[str, list[dict]]:
        """Group parsed entries by the team that produced them.

        A missing ``team_code`` raises out of here rather than being bucketed:
        the caller attaches it, so its absence is a bug in the parse path, and
        hiding it under a sentinel would enqueue a dead letter nobody could
        replay.
        """
        grouped: dict[str, list[dict]] = {}
        for entry in data:
            grouped.setdefault(entry["team_code"], []).append(entry)
        return grouped

    def _save_team(self, team_code: str, entries: list[dict]) -> bool:
        """Write one team's lots, returning whether they committed.

        The parsed fee rules are deliberately not written. They are keyed by fee
        kind -- 기본/추가/일일/행사 -- while ``parking_fee_rules`` is keyed by
        vehicle class (``compact``/``sedan``/``van``/``bus``) and requires a
        non-null base duration. The stadium pages state neither, so filling those
        columns would mean inventing them, and writing the kinds into
        ``vehicle_type`` would put a fabricated vehicle class in a column a real
        one later keys on. The fee text stays in the raw snapshot, which is
        committed ahead of these rows and can be re-parsed by
        ``kbo snapshot replay`` whenever the schema grows a kind that fits.
        """
        try:
            with SessionLocal() as session:
                lot_repo = ParkingLotRepository(session)
                for entry in entries:
                    lot_repo.save(entry["lot"])
                session.commit()
        except PARKING_SAVE_EXCEPTIONS as exc:
            self._record_persist_failure(team_code, exc)
            return False
        return True

    def _record_persist_failure(self, team_code: str, exc: BaseException) -> None:
        """Record a write failure against the team whose transaction died."""
        stage, code = classify_persist_failure(exc)
        logger.error(
            "[PARKING] save failed for %s (%s/%s)",
            team_code,
            stage.value,
            code.value,
            exc_info=exc,
        )
        self._persist_failures.append((team_code, code.value, str(exc)))
