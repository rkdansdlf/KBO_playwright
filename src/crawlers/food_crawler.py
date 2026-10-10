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
    # The `ALL` source (gujangfood.com) was removed on 2026-10-10. The domain
    # expired on 2026-08-08 and is in the registry's redemption period with no
    # NS records, so it cannot resolve and never will again without the former
    # owner acting. Keeping it produced one DNS failure per run and nothing else.
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
        #: 쓰지 못한 팀. 읽기 실패와 분리해 세어야 "소스가 말을 안 한 것"과
        #: "소스는 답했는데 쓰기가 죽은 것"을 구분할 수 있다. 둘 다 결과적으로
        #: 빈 테이블을 남기므로, 이 구분이 없으면 동일하게 보인다.
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
        """Crawl stadium food pages under one tracked run.

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
            self._persist_failures = []
            all_vendors: list[dict[str, Any]] = []
            for team_code, info in selected.items():
                try:
                    vendors = await self._crawl_team_food(team_code, info)
                except FOOD_CRAWL_EXCEPTIONS as exc:
                    logger.exception("Failed to crawl food for %s", team_code)
                    _, code = classify_failure(exc)
                    self._team_failures.append((team_code, code.value, str(exc)))
                    continue
                for entry in vendors:
                    # 팀이 트랜잭션 단위이자 DLQ 단위이자 쓰기 실패를 귀속시킬
                    # 유일한 기준이므로, 행과 함께 이동시키고 나중에 구장에서
                    # 역산하지 않는다.
                    entry["team_code"] = team_code
                all_vendors.extend(vendors)
                logger.info("[FOOD] %s: %s vendors found", team_code, len(vendors))

            logger.info("[FOOD] Total: %s vendors", len(all_vendors))
            run.records_read = len(all_vendors)

            if save:
                written, failed = await asyncio.to_thread(self._save_to_db, all_vendors)
                run.records_written = written
                run.records_failed = failed
            else:
                for vendor in all_vendors[:5]:
                    logger.info(vendor)

            self._record_team_failures(run, selected, record_dead_letters=record_dead_letters)
            self._raise_persist_error(raise_on_persist_error=raise_on_persist_error)
            return all_vendors

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
        logger.warning("[FOOD] teams failed: %s", failed_teams)

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
        detail = f"food save failed for {team_code}: {message}"
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
        vendors = self._parse_food_page(html, info)
        if not vendors:
            # A page that answered 200 but carries no menu is not a success.
            # The source is plain HTTP, so nothing here renders late: a page
            # whose prices cannot be found will not have them on the next
            # attempt either. Without this the empty parse passed silently and
            # the run closed as `success` with `records_written=0`, which is
            # how three unusable sources stayed invisible for a month.
            self._team_failures.append(
                (team_code, FailureCode.PARSE_EMPTY.value, f"no menu items found at {info['url']}"),
            )
            logger.warning("[FOOD] %s returned no menu items at %s", team_code, info["url"])
        return vendors

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

    def _save_to_db(self, data: list[dict]) -> tuple[int, int]:
        """Persist the sweep, one transaction per team.

        The vendor repository flushes on insert, so a constraint violation used
        to leave the session needing a rollback: every later save in the same
        sweep then failed with ``PendingRollbackError``, the batch commit rolled
        the whole sweep back, and the caller logged ``0`` and reported success.
        One team cannot undo another team's rows, and the team that failed can be
        replayed on its own.

        Args:
            data: Parsed vendor entries, each carrying the ``team_code`` it came from.

        Returns:
            ``(saved, failed)`` vendor counts. A team either wrote all of its
            vendors or wrote none, so a failed team contributes every entry it
            contributed.

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
        logger.info("[FOOD] Saved %s of %s vendors, %s snapshots.", saved, saved + failed, saved_snaps)
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
        except FOOD_DB_EXCEPTIONS:
            logger.exception("[FOOD] Snapshot save failed; continuing with the domain rows")
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
        """Write one team's vendors and menus, returning whether they committed."""
        try:
            with SessionLocal() as session:
                vendor_repo = StadiumFoodVendorRepository(session)
                menu_repo = StadiumFoodMenuItemRepository(session)
                for entry in entries:
                    vendor = vendor_repo.save(entry["vendor"])
                    for menu in entry.get("menus", []):
                        menu_repo.save({"vendor_id": vendor.id, **menu})
                session.commit()
        except FOOD_DB_EXCEPTIONS as exc:
            self._record_persist_failure(team_code, exc)
            return False
        return True

    def _record_persist_failure(self, team_code: str, exc: BaseException) -> None:
        """Record a write failure against the team whose transaction died."""
        stage, code = classify_persist_failure(exc)
        logger.error(
            "[FOOD] save failed for %s (%s/%s)",
            team_code,
            stage.value,
            code.value,
            exc_info=exc,
        )
        self._persist_failures.append((team_code, code.value, str(exc)))
