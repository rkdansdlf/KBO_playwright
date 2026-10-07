"""KBO base naver crawler 크롤러."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

import httpx

from src.constants import KST
from src.crawlers.base import BaseHttpCrawler
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy

if TYPE_CHECKING:
    from src.crawlers.result import CrawlResult

logger = logging.getLogger(__name__)

NAVER_NEWS_CRAWLER_NAME = "naver_news_crawler"
NAVER_CRAWL_EXCEPTIONS = (httpx.HTTPError, RuntimeError, ValueError, TypeError, KeyError, OSError)

NAVER_API_URL = (
    "https://api-gw.sports.naver.com/news/articles/kbaseball?sort=latest&date={date}&page=1&pageSize=30&isPhoto=N"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://sports.news.naver.com/kbaseball/news/index",
    "Origin": "https://sports.news.naver.com",
}


class NaverNewsCrawlerBase(BaseHttpCrawler, ABC):
    """NaverNewsCrawlerBase class."""

    KEYWORDS: ClassVar[list[str]] = []
    LABEL: str = "news"

    def __init__(self, request_delay: float = 0.5, http_client: CrawlerHttpClient | None = None) -> None:
        """Initialize NaverNewsCrawlerBase.

        Args:
            request_delay: Request delay in seconds.
            http_client: Transport to use. Defaults to a client named for this
                base crawler. Tests inject a client backed by a mock transport.

        """
        super().__init__(request_delay=request_delay, default_headers=HEADERS, timeout=15.0)
        # The shared client rather than `BaseHttpCrawler.http_client()`. That
        # method hands out a bare `httpx.AsyncClient`: it merges headers and
        # validates the URL, but it does not throttle, retry, or classify the
        # outcome -- so a Naver news day that failed once simply contributed
        # nothing. Three crawlers inherit this, so the change lands on all of
        # them at once.
        self._http = http_client or CrawlerHttpClient(
            name=NAVER_NEWS_CRAWLER_NAME,
            policy=HttpPolicy(base_delay_seconds=request_delay, timeout_seconds=15.0),
            headers=HEADERS,
        )

    async def run(self, *, save: bool = False) -> None:
        """Run run.

        Args:
            save: Whether to persist the results.

        """
        data = await self._fetch_news()

        logger.info("Found %d %s entries.", len(data), self.LABEL)
        if save:
            self._save_to_db(data)
        else:
            for d in data[:10]:
                logger.info(d)

    async def _fetch_day(self, date_str: str) -> CrawlResult[Any]:
        """Fetch one day's Naver news feed, classifying the outcome.

        A classified result rather than an exception, because the sweep reads
        seven days and a failure on one of them should cost that day only. It
        used to cost whatever `resp.json()` happened to raise on.
        """
        return await self._http.fetch_json(NAVER_API_URL.format(date=date_str))

    async def _fetch_news(self) -> list[dict]:
        results: list[dict] = []
        today = datetime.now(KST)
        for days_ago in range(7):
            date_str = (today - timedelta(days=days_ago)).strftime("%Y%m%d")
            try:
                result = await self._fetch_day(date_str)
                if not result.ok:
                    logger.warning(
                        "%s news fetch failed for date %s: %s",
                        self.LABEL,
                        date_str,
                        result.error_code or result.outcome.value,
                    )
                    continue
                news_list = result.data.get("result", {}).get("newsList", [])
                for article in news_list:
                    title = article.get("title", "")
                    if not any(kw in title for kw in self.KEYWORDS):
                        continue
                    parsed = self._parse_article(article)
                    if parsed:
                        results.append(parsed)
            except NAVER_CRAWL_EXCEPTIONS:
                logger.exception("%s news parse failed for date %s", self.LABEL, date_str)
        return results

    @abstractmethod
    def _parse_article(self, article: dict) -> dict[str, Any] | None: ...

    @abstractmethod
    def _save_to_db(self, data: list[dict]) -> None: ...
