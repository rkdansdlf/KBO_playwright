"""Futures League Schedule & Standings Crawler."""

from __future__ import annotations

import logging
from typing import Any

from playwright.async_api import Error as PlaywrightError

from src.utils.compliance import compliance, log_source_limited
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)


class FuturesScheduleCrawler:
    """Crawler for Futures League schedules and standings."""

    SCHEDULE_URL = "https://www.koreabaseball.com/Futures/Schedule/GameList.aspx"

    def __init__(
        self,
        request_delay: float = 1.5,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize futures schedule crawler."""
        self.request_delay = request_delay
        self.pool = pool
        self.policy = policy or RequestPolicy.with_delay(request_delay)

    async def crawl_futures_schedule(self, year: int, month: int) -> list[dict[str, Any]]:
        """Crawl Futures League schedule for a given year and month.

        Args:
            year: Year.
            month: Month.

        Returns:
            List of game schedule dicts.

        """
        results: list[dict[str, Any]] = []

        if not await compliance.is_allowed(self.SCHEDULE_URL):
            log_source_limited("futures_schedule", self.SCHEDULE_URL)
            return []

        try:
            async with AsyncPlaywrightPool() as pool, pool.page() as page:
                url = f"{self.SCHEDULE_URL}?year={year}&month={month:02d}"
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_selector("table", timeout=10000)

                rows = await page.query_selector_all("table tbody tr")
                current_date_str = ""

                for row in rows:
                    date_td = await row.query_selector("td.day")
                    if date_td:
                        dt_text = (await date_td.inner_text()).strip()
                        if dt_text:
                            # format e.g. 03.24(화)
                            day_num = dt_text.split("(")[0].split(".")[-1]
                            if day_num.isdigit():
                                current_date_str = f"{year}-{month:02d}-{int(day_num):02d}"

                    game_td = await row.query_selector("td.play")
                    if not game_td or not current_date_str:
                        continue

                    game_text = (await game_td.inner_text()).strip()
                    stadium_td = await row.query_selector("td.stadium")
                    stadium = (await stadium_td.inner_text()).strip() if stadium_td else ""

                    # parsing away vs home
                    parts = game_text.split("vs")
                    if len(parts) == 2:  # noqa: PLR2004
                        away_team = parts[0].strip()
                        home_team = parts[1].strip()
                        game_id = f"FUT_{current_date_str.replace('-', '')}_{away_team}_{home_team}"

                        results.append(
                            {
                                "season": year,
                                "game_date": current_date_str,
                                "game_id": game_id,
                                "away_team": away_team,
                                "home_team": home_team,
                                "stadium": stadium,
                                "away_score": None,
                                "home_score": None,
                                "game_status": "SCHEDULED",
                                "cancel_reason": None,
                            }
                        )
        except PlaywrightError as exc:
            logger.warning("Playwright error in FuturesScheduleCrawler: %s", exc)
        except Exception:
            logger.exception("Unexpected error in FuturesScheduleCrawler")

        return results
