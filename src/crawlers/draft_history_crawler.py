"""KBO Rookie Draft History Crawler."""

from __future__ import annotations

import logging
from typing import Any

from playwright.async_api import Error as PlaywrightError

from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)


class DraftHistoryCrawler:
    """Crawler for KBO Rookie Draft history."""

    DRAFT_URL = "https://www.koreabaseball.com/Kbo/BusinessAndEvent/Draft.aspx"

    def __init__(
        self,
        request_delay: float = 1.5,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize draft history crawler."""
        self.request_delay = request_delay
        self.pool = pool
        self.policy = policy or RequestPolicy.with_delay(request_delay)

    async def crawl_draft_history(self, season: int = 2026) -> list[dict[str, Any]]:
        """Crawl KBO rookie draft history for a given year.

        Args:
            season: Target draft season year.

        Returns:
            List of draft entry dictionaries.

        """
        results: list[dict[str, Any]] = []

        try:
            async with AsyncPlaywrightPool() as pool, pool.page() as page:
                    url = f"{self.DRAFT_URL}?year={season}"
                    await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    await page.wait_for_selector("table", timeout=10000)

                    rows = await page.query_selector_all("table tbody tr")
                    pick_seq = 1

                    for row in rows:
                        cols = await row.query_selector_all("td")
                        if len(cols) < 5:  # noqa: PLR2004
                            continue

                        round_str = (await cols[0].inner_text()).strip()
                        team_name = (await cols[1].inner_text()).strip()
                        player_name = (await cols[2].inner_text()).strip()
                        position = (await cols[3].inner_text()).strip()
                        school = (await cols[4].inner_text()).strip()
                        sign_fee = (await cols[5].inner_text()).strip() if len(cols) > 5 else None  # noqa: PLR2004

                        round_num = (
                            int(round_str.replace("라운드", "").strip())
                            if round_str.isdigit() or "라운드" in round_str
                            else 1
                        )

                        results.append(
                            {
                                "season": season,
                                "draft_type": " 신인드래프트",
                                "round_num": round_num,
                                "pick_seq": pick_seq,
                                "team_code": team_name,
                                "player_name": player_name,
                                "player_id": f"draft_{season}_{pick_seq}",
                                "position": position,
                                "school": school,
                                "sign_fee": sign_fee,
                            }
                        )
                        pick_seq += 1
        except PlaywrightError as exc:
            logger.warning("Playwright error in DraftHistoryCrawler: %s", exc)
        except Exception:
            logger.exception("Unexpected error in DraftHistoryCrawler")

        return results
