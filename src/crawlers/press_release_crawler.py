"""KBO Official Press Release & Notice Crawler."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from playwright.async_api import Error as PlaywrightError

from src.constants import KST
from src.crawlers.base import BasePlaywrightCrawler
from src.utils.compliance import compliance, log_source_limited

if TYPE_CHECKING:
    from src.utils.playwright_pool import AsyncPlaywrightPool
    from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)


class PressReleaseCrawler(BasePlaywrightCrawler):
    """Crawler for KBO official press releases and administrative notices."""

    PRESS_URL = "https://www.koreabaseball.com/MediaNews/Notice/List.aspx"

    def __init__(
        self,
        request_delay: float = 1.5,
        pool: AsyncPlaywrightPool | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize crawler.

        Args:
            request_delay: Request delay in seconds.
            pool: Optional playwright pool.
            policy: Optional request policy.

        """
        super().__init__(request_delay=request_delay, pool=pool, policy=policy)

    async def crawl_press_releases(self, max_pages: int = 1) -> list[dict[str, Any]]:
        """Crawl press releases from KBO notice board.

        Args:
            max_pages: Maximum number of pages to crawl.

        Returns:
            List of parsed press release items.

        """
        results: list[dict[str, Any]] = []
        if not await compliance.is_allowed(self.PRESS_URL):
            log_source_limited("press_release", self.PRESS_URL)
            return []

        try:
            async with self.page_context() as page:
                for page_num in range(1, max_pages + 1):
                    url = f"{self.PRESS_URL}?page={page_num}" if page_num > 1 else self.PRESS_URL
                    await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    await page.wait_for_selector("table", timeout=10000)

                    rows = await page.query_selector_all("table tbody tr")
                    for row in rows:
                        cols = await row.query_selector_all("td")
                        if len(cols) < 3:  # noqa: PLR2004
                            continue

                        num_text = (await cols[0].inner_text()).strip()
                        title_elem = await cols[1].query_selector("a")
                        title_text = (
                            (await cols[1].inner_text()).strip()
                            if not title_elem
                            else (await title_elem.inner_text()).strip()
                        )
                        href = await title_elem.get_attribute("href") if title_elem else ""

                        date_text = (await cols[2].inner_text()).strip() if len(cols) > 2 else ""  # noqa: PLR2004

                        notice_id = num_text if num_text.isdigit() else f"notice_{len(results) + 1}"
                        if href:
                            if href.startswith("http"):
                                source_url = href
                            elif href.startswith("/"):
                                source_url = f"https://www.koreabaseball.com{href}"
                            else:
                                source_url = f"https://www.koreabaseball.com/MediaNews/Notice/{href}"
                        else:
                            source_url = self.PRESS_URL

                        published_date = (
                            date_text.replace(".", "-") if date_text else datetime.now(tz=KST).strftime("%Y-%m-%d")
                        )

                        results.append(
                            {
                                "notice_id": notice_id,
                                "title": title_text,
                                "published_date": published_date,
                                "category": "공시/공지",
                                "source_url": source_url,
                                "content_summary": title_text,
                            }
                        )
        except PlaywrightError as exc:
            logger.warning("Playwright error while crawling KBO press releases: %s", exc)
        except Exception:
            logger.exception("Unexpected error in PressReleaseCrawler")

        return results
