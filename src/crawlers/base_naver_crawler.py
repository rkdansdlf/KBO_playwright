from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

NAVER_API_URL = (
    "https://api-gw.sports.naver.com/news/articles/kbaseball?sort=latest&date={date}&page=1&pageSize=30&isPhoto=N"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://sports.news.naver.com/kbaseball/news/index",
    "Origin": "https://sports.news.naver.com",
}


class NaverNewsCrawlerBase(ABC):
    KEYWORDS: list[str] = []
    LABEL: str = "news"

    async def run(self, save: bool = False) -> None:
        data = await self._fetch_news()
        logger.info("Found %d %s entries.", len(data), self.LABEL)
        if save:
            self._save_to_db(data)
        else:
            for d in data[:10]:
                logger.info(d)

    async def _fetch_news(self) -> list[dict]:
        results: list[dict] = []
        today = datetime.now()
        client = httpx.Client(headers=HEADERS, timeout=15)
        try:
            for days_ago in range(7):
                date_str = (today - timedelta(days=days_ago)).strftime("%Y%m%d")
                url = NAVER_API_URL.format(date=date_str)
                try:
                    resp = client.get(url)
                    if resp.status_code != 200:
                        continue
                    news_list = resp.json().get("result", {}).get("newsList", [])
                    for article in news_list:
                        title = article.get("title", "")
                        if not any(kw in title for kw in self.KEYWORDS):
                            continue
                        parsed = self._parse_article(article)
                        if parsed:
                            results.append(parsed)
                except Exception:
                    logger.exception("%s news fetch failed for date %s", self.LABEL, date_str)
        finally:
            client.close()
        return results

    @abstractmethod
    def _parse_article(self, article: dict) -> dict[str, Any] | None:
        ...

    @abstractmethod
    def _save_to_db(self, data: list[dict]) -> None:
        ...
