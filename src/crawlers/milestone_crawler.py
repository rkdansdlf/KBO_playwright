"""KBO Player Milestone Crawler."""

from __future__ import annotations

import logging
from typing import Any

from playwright.async_api import Error as PlaywrightError

from src.utils.compliance import compliance, log_source_limited
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)


async def _milestone_sources_allowed(urls: list[tuple[str, str]]) -> bool:
    """Return whether every milestone source is allowed by robots policy."""
    for url, _ in urls:
        if not await compliance.is_allowed(url):
            return False
    return True


class MilestoneCrawler:
    """Crawler for KBO upcoming milestones (달성 임박 기록)."""

    HIT_MILESTONE_URL = "https://www.koreabaseball.com/Record/History/Top/Hitter.aspx"
    PIT_MILESTONE_URL = "https://www.koreabaseball.com/Record/History/Top/Pitcher.aspx"

    def __init__(
        self,
        request_delay: float = 1.5,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize milestone crawler."""
        self.request_delay = request_delay
        self.pool = pool
        self.policy = policy or RequestPolicy.with_delay(request_delay)

    async def crawl_upcoming_milestones(self, season: int = 2026) -> list[dict[str, Any]]:
        """Crawl upcoming milestones for hitters and pitchers.

        Args:
            season: Target season year.

        Returns:
            List of milestone items.

        """
        urls = [
            (self.HIT_MILESTONE_URL, "통산 안타/홈런"),
            (self.PIT_MILESTONE_URL, "통산 다승/탈삼진"),
        ]
        return await self._crawl_if_allowed(season, urls)

    async def _crawl_if_allowed(self, season: int, urls: list[tuple[str, str]]) -> list[dict[str, Any]]:
        if not await _milestone_sources_allowed(urls):
            log_source_limited("milestone", self.HIT_MILESTONE_URL)
            return []
        return await self._crawl_sources(season, urls)

    async def _crawl_sources(self, season: int, urls: list[tuple[str, str]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        try:
            async with AsyncPlaywrightPool() as pool, pool.page() as page:
                for url, category_label in urls:
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                        await page.wait_for_selector("table", timeout=10000)

                        rows = await page.query_selector_all("table tbody tr")
                        for row in rows:
                            cols = await row.query_selector_all("td")
                            if len(cols) < 4:  # noqa: PLR2004
                                continue

                            name = (await cols[1].inner_text()).strip() if len(cols) > 1 else ""
                            if not name:
                                continue
                            team = (await cols[2].inner_text()).strip() if len(cols) > 2 else ""  # noqa: PLR2004
                            current_str = (
                                (await cols[3].inner_text()).strip().replace(",", "") if len(cols) > 3 else "0"  # noqa: PLR2004
                            )

                            player_link = await cols[1].query_selector("a")
                            player_id = ""
                            if player_link:
                                href = await player_link.get_attribute("href") or ""
                                if "playerId=" in href:
                                    player_id = href.split("playerId=")[-1].split("&")[0]

                            current_val = int(current_str) if current_str.isdigit() else 0
                            target_val = current_val + 10
                            remaining_val = 10

                            results.append(
                                {
                                    "season": season,
                                    "player_id": player_id or f"name_{name}",
                                    "player_name": name,
                                    "team_code": team,
                                    "milestone_category": category_label,
                                    "current_val": current_val,
                                    "target_val": target_val,
                                    "remaining_val": remaining_val,
                                    "is_achieved": False,
                                    "achieved_date": None,
                                }
                            )
                    except PlaywrightError as exc:
                        logger.warning("Playwright error crawling milestone URL %s: %s", url, exc)
                    except Exception:
                        logger.exception("Error crawling milestone URL %s", url)
        except Exception:
            logger.exception("Pool error in MilestoneCrawler")

        return results
