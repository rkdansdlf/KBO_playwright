"""Crawler for stadium food vendor and menu information from team websites."""

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
from src.repositories.source_registry_repository import save_raw_snapshots
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.http_client import DEFAULT_HEADERS as HEADERS

if TYPE_CHECKING:
    from src.models.crawl_execution import CrawlExecutionRun
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

FOOD_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, KeyError, OSError)
FOOD_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)

FOOD_CRAWLER_NAME = "food"
FOOD_TARGET_TYPE = "food"

TEAM_FOOD_SOURCES: dict[str, dict[str, Any]] = {
    "ALL": {
        "source_key": "gujangfood_com",
        "stadium_id": "UNKNOWN",
        "url": "https://www.gujangfood.com",
    },
    "LT": {
        "source_key": "lotte_giants_fnb",
        "stadium_id": "SAJIK",
        "url": "https://www.giantsclub.com/food",
    },
    "NC": {
        "source_key": "nc_dinos_food_seat",
        "stadium_id": "CHANGWON",
        "url": "https://www.ncdinos.com/dinos/stadium.do",
    },
}

MENU_PATTERN = re.compile(r"([가-힣a-zA-Z0-9\s]{2,30})\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)")


class FoodCrawler(BaseHttpCrawler):
    """FoodCrawler class."""

    def __init__(
        self,
        request_delay: float = 0.5,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize FoodCrawler.

        Args:
            request_delay: Request delay in seconds.
            policy: Optional request policy.

        """
        super().__init__(request_delay=request_delay, policy=policy, default_headers=HEADERS)
        self._raw_pages: list[dict] = []
        #: 읽지 못한 팀의 페이지. 원장과 DLQ가 볼 수 있도록 보관한다.
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
        """Crawl stadium food pages under one tracked run.

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
            The parsed food vendors.

        """
        selected = {code: info for code, info in TEAM_FOOD_SOURCES.items() if not team_filter or code == team_filter}
        spec = run_spec or CrawlRunSpec(
            crawler=FOOD_CRAWLER_NAME,
            target_type=FOOD_TARGET_TYPE,
            target_id=team_filter or "all",
            source_url=next(iter(selected.values()))["url"] if len(selected) == 1 else None,
        )

        with track_crawl_run(spec) as run:
            self._team_failures = []
            all_vendors: list[dict[str, Any]] = []
            for team_code, info in selected.items():
                try:
                    vendors = await self._crawl_team_food(team_code, info)
                except FOOD_CRAWL_EXCEPTIONS as exc:
                    logger.exception("Failed to crawl food for %s", team_code)
                    _, code = classify_failure(exc)
                    self._team_failures.append((team_code, code.value, str(exc)))
                    continue
                all_vendors.extend(vendors)
                logger.info("[FOOD] %s: %s vendors found", team_code, len(vendors))

            logger.info("[FOOD] Total: %s vendors", len(all_vendors))
            run.records_read = len(all_vendors)

            if save:
                run.records_written = await asyncio.to_thread(self._save_to_db, all_vendors)
            else:
                for vendor in all_vendors[:5]:
                    logger.info(vendor)

            self._record_team_failures(run, selected, record_dead_letters=record_dead_letters)
            return all_vendors

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
        logger.warning("[FOOD] teams failed: %s", failed_teams)

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
                    crawler=FOOD_CRAWLER_NAME,
                    target_type=FOOD_TARGET_TYPE,
                    target_id=target_id,
                    source_url=source_url,
                    # Derived from the code, never supplied beside it.
                    failure_stage=stage_for_code(error_code).value,
                    error_code=error_code,
                    error_message=error_message,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue dead letter for food %s", target_id)

    async def _crawl_team_food(self, team_code: str, info: dict) -> list[dict[str, Any]]:
        result = await self._http.fetch_text(info["url"])
        if not result.ok:
            code = result.error_code or FailureCode.UNKNOWN.value
            message = result.error or str(result.outcome)
            if result.outcome is CrawlOutcome.SCHEMA_CHANGED:
                logger.error(
                    "[FOOD] %s page structure changed at %s: %s",
                    team_code,
                    info["url"],
                    result.error,
                )
            else:
                logger.warning(
                    "[FOOD] %s fetch failed (%s, status=%s): %s",
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
        return self._parse_food_page(html, info)

    def _parse_food_page(self, html: str, info: dict) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        vendors = []

        menu_matches = MENU_PATTERN.findall(text)
        menus = []
        for name, price_str in menu_matches:
            menus.append(
                {
                    "menu_name": name.strip(),
                    "price": int(price_str.replace(",", "")),
                    "category": "etc",
                },
            )

        if menus:
            vendors.append(
                {
                    "vendor": {
                        "stadium_id": info["stadium_id"],
                        "vendor_name": f"{info['stadium_id']} 구장 매점",
                        "order_method": "onsite",
                        "confidence": "low",
                    },
                    "menus": menus,
                },
            )

        return vendors

    def _save_to_db(self, data: list[dict]) -> int:
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)
                vendor_repo = StadiumFoodVendorRepository(session)
                menu_repo = StadiumFoodMenuItemRepository(session)
                vendor_count = 0
                menu_count = 0
                for entry in data:
                    try:
                        vendor = vendor_repo.save(entry["vendor"])
                        vendor_count += 1
                        for menu in entry.get("menus", []):
                            menu_repo.save({"vendor_id": vendor.id, **menu})
                            menu_count += 1
                    except FOOD_DB_EXCEPTIONS:
                        logger.exception("Food save failed: %s", entry.get("vendor", {}).get("vendor_name", ""))
                session.commit()
                logger.info("[FOOD] Saved %s vendors, %s menus, %s snapshots.", vendor_count, menu_count, saved_snaps)
            except SQLAlchemyError:
                session.rollback()
                logger.exception("[FOOD] Database error")
                return 0
            else:
                return vendor_count
            finally:
                self._raw_pages.clear()
