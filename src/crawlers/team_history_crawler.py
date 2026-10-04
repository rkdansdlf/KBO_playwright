"""KBO team history crawler 크롤러."""

from __future__ import annotations

import asyncio
import contextlib
import logging

from playwright.async_api import Browser, BrowserContext, Locator, Page, Playwright, async_playwright
from playwright.async_api import Error as PlaywrightError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    classify_failure,
    classify_persist_failure,
    stage_for_code,
)
from src.crawlers.team_history_outcome import (
    TeamHistoryRead,
    TeamHistoryStatus,
    classify_history_failure,
    read_team_history,
)
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_FAILED
from src.models.team import Team
from src.models.team_history import TeamHistory
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.repositories.source_registry_repository import save_raw_snapshots
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run
from src.utils.compliance import compliance, log_source_limited
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.team_codes import resolve_team_code

logger = logging.getLogger(__name__)

TEAM_HISTORY_PARSE_EXCEPTIONS = (PlaywrightError, ValueError, TypeError)
TEAM_HISTORY_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)
#: Failures recorded in the ledger and DLQ instead of aborting the caller.
TEAM_HISTORY_FAILURE_EXCEPTIONS = (*TEAM_HISTORY_PARSE_EXCEPTIONS, TimeoutError, OSError, RuntimeError)
TEAM_HISTORY_SLOT_COUNT = 12

TEAM_HISTORY_CRAWLER_NAME = "team_history"
TEAM_HISTORY_TARGET_TYPE = "team_history"
#: One page covers every season, so the page itself is the replay unit.
TEAM_HISTORY_TARGET_ID = "kbo_team_history"


class TeamHistoryCrawler:
    """crawl KBO Team History page (https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx).

    Collects: Annual Team Names, Logos, Rankings, Season Info.

    """

    BASE_URL = "https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx"

    def __init__(self) -> None:
        """Initialize a new instance."""
        self.browser: Browser | None = None
        self.page: Page | None = None
        self.playwright: Playwright | None = None
        self.context: BrowserContext | None = None
        self._raw_pages: list[dict] = []
        self._last_failure_reason: str | None = None
        self._last_read: TeamHistoryRead | None = None

    def get_last_failure_reason(self) -> str | None:
        """Return the latest crawl failure reason, if any."""
        return self._last_failure_reason

    def get_last_read(self) -> TeamHistoryRead | None:
        """Return what the last page read meant, before any ledger ran.

        ``crawl`` returns a list, and a list cannot say whether an empty result
        was a blank page or an unreadable one. This is where that answer lives.
        """
        return self._last_read

    async def start(self) -> None:
        """Handle the start operation."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        await install_async_resource_blocking(self.context)
        self.page = await self.context.new_page()

    async def close(self) -> None:
        """Handle the close operation."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl(self) -> list[dict]:
        """Crawl crawl.

        Returns:
            List of results.

        """
        logger.info("📜 Crawling Team History from %s", self.BASE_URL)

        if not await compliance.is_allowed(self.BASE_URL):
            self._last_failure_reason = log_source_limited("team_history", self.BASE_URL)
            return []
        self._last_failure_reason = None

        if not self.page:
            await self.start()
        if self.page is None:
            msg = "Page not initialized"
            raise RuntimeError(msg)

        await self.page.goto(self.BASE_URL, wait_until="networkidle")

        raw_html = await self.page.content()
        self._raw_pages.append({"url": self.BASE_URL, "html": raw_html, "source_key": "kbo_team_history"})

        rows = await self.page.locator("table.tData.tbd02 tbody tr").all()
        logger.info("Found %s year entries.", len(rows))

        history_data = []
        years_parsed = 0

        # State tracking: 12 slots for teams (KBO has max 10 active + history slots?)
        # Subagent said 12 columns.
        # We store {name: str, logo: str} for each column index.
        team_slots: list[dict[str, str | None]] = [{"name": None, "logo": None} for _ in range(TEAM_HISTORY_SLOT_COUNT)]

        for row in rows:
            year = await self._parse_history_year(row)
            if year is None:
                # A row the parser could not use. Counted rather than logged so
                # that a page whose every row fails can be told apart from one
                # that is genuinely blank -- both used to return [].
                continue
            years_parsed += 1
            cells = await row.locator("td").all()
            for i, cell in enumerate(cells):
                if i >= TEAM_HISTORY_SLOT_COUNT:
                    break  # Safety
                entry = await self._parse_history_cell(cell, i, year, team_slots)
                if entry is not None:
                    history_data.append(entry)

            logger.info("Processed %s: %s teams.", year, len([h for h in history_data if h["season"] == year]))

        self._last_read = read_team_history(
            rows_found=len(rows),
            years_parsed=years_parsed,
            entries=history_data,
        )
        return history_data

    async def run(
        self,
        *,
        save: bool = True,
        run_spec: CrawlRunSpec | None = None,
        record_dead_letters: bool = True,
        raise_on_persist_error: bool = False,
    ) -> list[dict]:
        """Crawl (and optionally persist) team history under one tracked run.

        The unit of work is the history page, because a single page carries every
        season. Failures are recorded in the run ledger and the dead letter queue
        instead of being raised: the ledger is the failure record, so the caller
        keeps running and the DLQ drives reprocessing. A compliance skip is not a
        failure at all -- the source was deliberately not consulted -- so it is
        recorded as ``source_limited``.

        Args:
            save: Persist the parsed rows and their raw snapshots.
            run_spec: Optional pre-built ledger spec (replay supplies one).
            record_dead_letters: Whether a failure enqueues a DLQ entry.
            raise_on_persist_error: Re-raise persistence failures instead of
                reporting them through the ledger only.

        Returns:
            The parsed history entries, or an empty list when the page failed.

        """
        spec = run_spec or CrawlRunSpec(
            crawler=TEAM_HISTORY_CRAWLER_NAME,
            target_type=TEAM_HISTORY_TARGET_TYPE,
            target_id=TEAM_HISTORY_TARGET_ID,
            source_url=self.BASE_URL,
        )

        with track_crawl_run(spec) as run:
            try:
                data = await self.crawl()
            except TEAM_HISTORY_FAILURE_EXCEPTIONS as exc:
                stage, code = classify_failure(exc)
                logger.exception("[TEAM_HISTORY] crawl failed (%s/%s)", stage.value, code.value)
                run.status = RUN_STATUS_FAILED
                run.error_code = code.value
                run.error_message = str(exc)
                if record_dead_letters:
                    self._enqueue_dead_letter(run.run_id, code.value, str(exc))
                return []

            run.records_read = len(data)
            if self._last_failure_reason:
                run.checkpoint = {
                    "outcome": "source_limited",
                    "reason": self._last_failure_reason,
                    "source_url": self.BASE_URL,
                }
                logger.info("[TEAM_HISTORY] skipped: %s", self._last_failure_reason)
                return data

            read = self._last_read
            if read is not None and read.status is not TeamHistoryStatus.SUCCESS:
                # An unreadable page returned [] and the run used to record that
                # as a successful read of a page that carried nothing. The rows
                # found and parsed are carried in the message because "no rows"
                # and "rows but none readable" call for different responses.
                code, _terminal = classify_history_failure(read.reason or "")
                run.status = RUN_STATUS_FAILED
                run.error_code = code
                run.error_message = f"{read.reason}: rows_found={read.rows_found} years_parsed={read.years_parsed}"
                logger.warning("[TEAM_HISTORY] %s", run.error_message)
                if record_dead_letters:
                    self._enqueue_dead_letter(run.run_id, code, run.error_message)
                return []

            if save:
                try:
                    written, failed = await self.save(data, raise_on_error=True)
                except CrawlPersistError as exc:
                    logger.exception("[TEAM_HISTORY] persist failed (%s)", exc.error_code)
                    run.status = RUN_STATUS_FAILED
                    run.error_code = exc.error_code
                    run.error_message = str(exc)
                    if record_dead_letters:
                        self._enqueue_dead_letter(run.run_id, exc.error_code, str(exc))
                    if raise_on_persist_error:
                        raise
                    return data
                run.records_written = written
                run.records_failed = failed

            return data

    def _enqueue_dead_letter(
        self,
        original_run_id: str,
        error_code: str,
        error_message: str | None,
    ) -> None:
        """Enqueue one dead letter for the history page that could not be obtained."""
        try:
            enqueue_failure(
                DeadLetterSpec(
                    original_run_id=original_run_id,
                    crawler=TEAM_HISTORY_CRAWLER_NAME,
                    target_type=TEAM_HISTORY_TARGET_TYPE,
                    # One page covers every season, so the page is the replay unit.
                    target_id=TEAM_HISTORY_TARGET_ID,
                    source_url=self.BASE_URL,
                    # Derived from the code, never supplied beside it.
                    failure_stage=stage_for_code(error_code).value,
                    error_code=error_code,
                    error_message=error_message,
                ),
            )
        except Exception:
            logger.exception("Failed to enqueue dead letter for team history")

    async def _parse_history_year(self, row: Locator) -> int | None:
        year_th = row.locator("th")
        if await year_th.count() == 0:
            return None
        year_text = await year_th.inner_text()
        try:
            return int(year_text.strip())
        except ValueError:
            logger.warning("Skipping invalid year: %s", year_text)
            return None

    async def _parse_history_cell(
        self,
        cell: Locator,
        slot_index: int,
        year: int,
        team_slots: list[dict[str, str | None]],
    ) -> dict | None:
        rank = await self._parse_rank(cell)
        new_name, new_logo = await self._parse_team_identity(cell)
        if new_name:
            team_slots[slot_index]["name"] = new_name
        if new_logo:
            team_slots[slot_index]["logo"] = new_logo
        current_name = team_slots[slot_index]["name"]
        if rank is None or not current_name:
            return None
        return {
            "season": year,
            "team_name": current_name,
            "logo_url": team_slots[slot_index]["logo"],
            "ranking": rank,
            "slot_index": slot_index,
        }

    async def _parse_rank(self, cell: Locator) -> int | None:
        rank_el = cell.locator("span.nums")
        if await rank_el.count() == 0:
            return None
        with contextlib.suppress(*TEAM_HISTORY_PARSE_EXCEPTIONS):
            return int((await rank_el.inner_text()).strip())
        return None

    async def _parse_team_identity(self, cell: Locator) -> tuple[str | None, str | None]:
        img = cell.locator("img")
        name_span = cell.locator("span:not(.nums)")
        if await img.count() > 0:
            return await img.get_attribute("alt"), await img.get_attribute("src")
        if await name_span.count() > 0:
            return (await name_span.inner_text()).strip(), None
        return None, None

    async def save(self, data: list[dict], *, raise_on_error: bool = False) -> tuple[int, int]:
        """Persist history rows and their raw snapshots.

        Args:
            data: Parsed history entries.
            raise_on_error: Re-raise persistence failures as
                :class:`CrawlPersistError` so the run ledger and dead letter queue
                can classify them. Default keeps the historical best-effort
                behavior of logging and returning zero.

        Returns:
            ``(saved, failed)``. A team whose code or franchise cannot be resolved
            is counted as failed rather than dropped silently.

        """
        logger.info("💾 Saving %s history entries...", len(data))

        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)

                teams = session.execute(select(Team)).scalars().all()
                team_map = {t.team_id: t.franchise_id for t in teams}

                saved_count = 0
                failed_count = 0
                for entry in data:
                    team_name = entry["team_name"]
                    season = entry["season"]

                    code = resolve_team_code(team_name)
                    if not code:
                        logger.warning("   ⚠️ Could not resolve code for '%s' (%s)", team_name, season)
                        failed_count += 1
                        continue

                    franchise_id = team_map.get(code)
                    if not franchise_id:
                        logger.warning("   ⚠️ No franchise_id for code '%s'", code)
                        failed_count += 1
                        continue

                    stmt = select(TeamHistory).where(TeamHistory.season == season, TeamHistory.team_code == code)
                    existing = session.execute(stmt).scalars().first()

                    if existing:
                        existing.team_name = team_name
                        existing.logo_url = entry["logo_url"]
                        existing.ranking = entry["ranking"]
                        existing.franchise_id = franchise_id
                    else:
                        session.add(
                            TeamHistory(
                                season=season,
                                team_code=code,
                                team_name=team_name,
                                logo_url=entry["logo_url"],
                                ranking=entry["ranking"],
                                franchise_id=franchise_id,
                            ),
                        )
                    saved_count += 1

                session.commit()
                logger.info(
                    "✅ Saved/Updated %s records, %s failed (%s snapshots).",
                    saved_count,
                    failed_count,
                    saved_snaps,
                )
            except TEAM_HISTORY_DB_EXCEPTIONS as exc:
                session.rollback()
                logger.exception("Error saving team history")
                if raise_on_error:
                    _, code = classify_persist_failure(exc)
                    raise CrawlPersistError(str(exc), error_code=code) from exc
                return 0, len(data)
            else:
                return saved_count, failed_count
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    crawler = TeamHistoryCrawler()
    asyncio.run(crawler.crawl())
