"""KBO team history crawler 크롤러."""

from __future__ import annotations

import asyncio
import contextlib
import logging

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Locator, async_playwright
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.models.team import Team
from src.models.team_history import TeamHistory
from src.repositories.source_registry_repository import save_raw_snapshots
from src.utils.playwright_blocking import install_async_resource_blocking
from src.utils.team_codes import resolve_team_code

logger = logging.getLogger(__name__)

TEAM_HISTORY_PARSE_EXCEPTIONS = (PlaywrightError, ValueError, TypeError)
TEAM_HISTORY_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


class TeamHistoryCrawler:
    """Crawls KBO Team History page (https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx)
    Collects: Annual Team Names, Logos, Rankings, Season Info.
    """

    BASE_URL = "https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx"

    def __init__(self) -> None:
        """Initializes a new instance."""
        self.browser = None
        self.page = None
        self.playwright = None
        self.context = None
        self._raw_pages: list[dict] = []

    async def start(self) -> None:
        """Handles the start operation."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        await install_async_resource_blocking(self.context)
        self.page = await self.context.new_page()

    async def close(self) -> None:
        """Handles the close operation."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl(self) -> list[dict]:
        """Crawls crawl.

        Returns:
            List of results.

        """
        logger.info("📜 Crawling Team History from %s", self.BASE_URL)
        if not self.page:
            await self.start()

        await self.page.goto(self.BASE_URL, wait_until="networkidle")

        raw_html = await self.page.content()
        self._raw_pages.append({"url": self.BASE_URL, "html": raw_html, "source_key": "kbo_team_history"})

        rows = await self.page.locator("table.tData.tbd02 tbody tr").all()
        logger.info("Found %s year entries.", len(rows))

        history_data = []

        # State tracking: 12 slots for teams (KBO has max 10 active + history slots?)
        # Subagent said 12 columns.
        # We store {name: str, logo: str} for each column index.
        team_slots = [{"name": None, "logo": None} for _ in range(12)]

        for row in rows:
            year = await self._parse_history_year(row)
            if year is None:
                continue
            cells = await row.locator("td").all()
            for i, cell in enumerate(cells):
                if i >= 12:
                    break  # Safety
                entry = await self._parse_history_cell(cell, i, year, team_slots)
                if entry is not None:
                    history_data.append(entry)

            logger.info("Processed %s: %s teams.", year, len([h for h in history_data if h["season"] == year]))

        return history_data

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

    async def save(self, data: list[dict]) -> None:
        """Saves save.

        Args:
            data: Data.

        """
        logger.info("💾 Saving %s history entries...", len(data))
        with SessionLocal() as session:
            try:
                saved_snaps = save_raw_snapshots(session, self._raw_pages)

                teams = session.execute(select(Team)).scalars().all()
                team_map = {t.team_id: t.franchise_id for t in teams}

                saved_count = 0
                for entry in data:
                    team_name = entry["team_name"]
                    season = entry["season"]

                    code = resolve_team_code(team_name)
                    if not code:
                        logger.warning("   ⚠️ Could not resolve code for '%s' (%s)", team_name, season)
                        continue

                    franchise_id = team_map.get(code)
                    if not franchise_id:
                        logger.warning("   ⚠️ No franchise_id for code '%s'", code)
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
                logger.info("✅ Saved/Updated %s records (%s snapshots).", saved_count, saved_snaps)
            except TEAM_HISTORY_DB_EXCEPTIONS:
                session.rollback()
                logger.exception("Error saving team history")
            finally:
                self._raw_pages.clear()


if __name__ == "__main__":
    crawler = TeamHistoryCrawler()
    asyncio.run(crawler.crawl())
