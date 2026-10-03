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
from src.crawlers.failure_taxonomy import FailureCode, classify_failure, stage_for_code
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository
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
    ) -> list[dict[str, Any]]:
        """Crawl stadium parking pages under one tracked run.

        A team is the replay unit. A page that cannot be read used to be an empty
        list with a log line and nothing else; it is now recorded in the run
        ledger, reflected as a ``partial`` run, and enqueued as its own dead
        letter so the team can be replayed without re-crawling the rest.

        Args:
            save: Whether to persist the results.
            team_filter: Restrict the sweep to one team code.
            run_spec: Optional pre-built ledger spec (replay supplies one).
            record_dead_letters: Whether failures enqueue DLQ entries.

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
            all_lots: list[dict[str, Any]] = []
            for team_code, info in selected.items():
                try:
                    lots = await self._crawl_team_parking(team_code, info)
                except PARKING_CRAWL_EXCEPTIONS as exc:
                    logger.exception("Failed to crawl parking for %s", team_code)
                    _, code = classify_failure(exc)
                    self._team_failures.append((team_code, code.value, str(exc)))
                    continue
                all_lots.extend(lots)
                logger.info("[PARKING] %s: %s lots found", team_code, len(lots))

            logger.info("[PARKING] Total: %s lots", len(all_lots))
            run.records_read = len(all_lots)

            if save:
                run.records_written = await asyncio.to_thread(self._save_to_db, all_lots)
            else:
                for lot in all_lots[:5]:
                    logger.info(lot)

            self._record_team_failures(run, selected, record_dead_letters=record_dead_letters)
            return all_lots

    def _record_team_failures(
        self,
        run: CrawlExecutionRun,
        selected: dict[str, dict[str, Any]],
        *,
        record_dead_letters: bool,
    ) -> None:
        """Reflect unreadable teams in the run status and the dead letter queue."""
        if not self._team_failures:
            return

        failed_teams = [team_code for team_code, _, _ in self._team_failures]
        run.error_message = f"teams failed: {failed_teams}"
        if run.records_read:
            run.status = RUN_STATUS_PARTIAL
        else:
            run.status = RUN_STATUS_FAILED
            run.error_code = self._team_failures[0][1]
        logger.warning("[PARKING] teams failed: %s", failed_teams)

        if not record_dead_letters:
            return
        for team_code, error_code, message in self._team_failures:
            info = selected.get(team_code, {})
            self._enqueue_dead_letter(run.run_id, team_code, error_code, message, source_url=info.get("url"))

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

    def _save_to_db(self, data: list[dict]) -> int:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                lot_repo = ParkingLotRepository(session)
                fee_repo = ParkingFeeRuleRepository(session)
                lot_count = 0
                fee_count = 0
                for entry in data:
                    try:
                        lot = lot_repo.save(entry["lot"])
                        lot_count += 1
                        for fee in entry.get("fee_rules", []):
                            fee_repo.save({"parking_lot_id": lot.id, **fee})
                            fee_count += 1
                    except PARKING_SAVE_EXCEPTIONS:
                        logger.exception("Parking save failed: %s", entry.get("lot", {}).get("name", ""))
                session.commit()
                logger.info("[PARKING] Saved %s lots, %s fee rules, %s snapshots.", lot_count, fee_count, saved_snaps)
            except PARKING_SAVE_EXCEPTIONS:
                session.rollback()
                logger.exception("Parking batch save error")
                return 0
            else:
                return lot_count
            finally:
                self._raw_pages.clear()
