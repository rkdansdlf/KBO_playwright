"""
Crawler for real-time issue text: Naver Sports baseball news headlines and MLBPark popular threads.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from http import HTTPStatus
from typing import Any

import httpx
from bs4 import BeautifulSoup

from src.constants import KST
from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import save_raw_snapshots
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)


class RealtimeIssueCrawler:
    """
    Scrapes real-time baseball topics, headlines, and forum discussions.
    """

    def __init__(self, timeout: int = 15) -> None:
        self.timeout = timeout
        self._raw_pages: list[dict] = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def fetch_naver_news_headlines(self, *, save: bool = False) -> list[dict[str, Any]]:
        """
        Fetches latest baseball news headlines from Naver Sports GW API (JSON)
        with fallback to web scraping if API is down.
        """
        articles = self._fetch_naver_news_from_api()
        if articles is None:
            articles = self._fetch_naver_news_from_html()
        if save and self._raw_pages:
            with SessionLocal() as session:
                save_raw_snapshots(session, self._raw_pages)
        return articles

    def _fetch_naver_news_from_api(self) -> list[dict[str, Any]] | None:
        api_url = self._naver_news_api_url()
        logger.info("Fetching Naver news from API: %s", api_url)
        try:
            custom_headers = self.headers.copy()
            custom_headers["Referer"] = "https://sports.news.naver.com/kbaseball/news/index"
            custom_headers["Origin"] = "https://sports.news.naver.com"
            with httpx.Client(headers=custom_headers, timeout=self.timeout) as client:
                throttle.wait_sync("api-gw.sports.naver.com")
                res = client.get(api_url)
                self._raw_pages.append(
                    {
                        "source_key": "naver_sports_news",
                        "url": api_url,
                        "html": res.text,
                        "status_code": res.status_code,
                    },
                )
                if res.status_code == HTTPStatus.OK:
                    articles = self._parse_naver_news_api_response(res.json())
                    logger.info("   Fetched %d headlines from JSON API.", len(articles))
                    return articles
                logger.info("Naver news API returned status code %d", res.status_code)
        except httpx.HTTPError:
            logger.exception("Naver news API failed. Falling back to HTML scraping...")
        return None

    @staticmethod
    def _naver_news_api_url() -> str:
        date_str = datetime.now(KST).strftime("%Y%m%d")
        return f"https://api-gw.sports.naver.com/news/articles/kbaseball?sort=latest&date={date_str}&page=1&pageSize=20&isPhoto=N"

    @staticmethod
    def _parse_naver_news_api_response(data: dict[str, Any]) -> list[dict[str, Any]]:
        result_data = data.get("result", {})
        news_list = result_data.get("newsList", [])
        return [RealtimeIssueCrawler._build_naver_api_article(item) for item in news_list]

    @staticmethod
    def _build_naver_api_article(item: dict[str, Any]) -> dict[str, Any]:
        title = item.get("title", "")
        sub_content = item.get("subContent", "")
        oid = item.get("oid", "")
        offset_id = item.get("aid", "")
        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={offset_id}"
        return {
            "title": title,
            "content": sub_content if sub_content else title,
            "meta": {
                "source": url,
                "office_name": item.get("officeName", ""),
                "published_at": item.get("datetime", ""),
                "crawled_at": datetime.now(KST).isoformat(),
                "category": "naver_news",
            },
        }

    def _fetch_naver_news_from_html(self) -> list[dict[str, Any]]:
        # HTML Scraping Fallback
        fallback_url = "https://sports.news.naver.com/kbaseball/news/index"
        articles = []
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                throttle.wait_sync("sports.news.naver.com")
                res = client.get(fallback_url)
                self._raw_pages.append(
                    {
                        "source_key": "naver_sports_news",
                        "url": fallback_url,
                        "html": res.text,
                        "status_code": res.status_code,
                    },
                )
                if res.status_code == HTTPStatus.OK:
                    soup = BeautifulSoup(res.text, "html.parser")
                    articles = self._parse_naver_news_html(soup)
            logger.info("   Fetched %s headlines from HTML fallback.", len(articles))
        except httpx.HTTPError:
            logger.exception("Naver News HTML fallback also failed")
        return articles

    @staticmethod
    def _parse_naver_news_html(soup: BeautifulSoup) -> list[dict[str, Any]]:
        links = []
        for a in soup.find_all("a"):
            href = a.get("href", "")
            title = a.get("title") or a.text.strip()
            if href and ("read" in href or "read.nhn" in href) and title:
                if href.startswith("/"):
                    href = "https://sports.news.naver.com" + href
                links.append((title, href))

        articles = []
        seen = set()
        for title, href in links:
            if href in seen:
                continue
            seen.add(href)
            articles.append(
                {
                    "title": title,
                    "content": title,
                    "meta": {
                        "source": href,
                        "crawled_at": datetime.now(KST).isoformat(),
                        "category": "naver_news",
                    },
                },
            )
        return articles

    def fetch_mlbpark_bullpen_posts(self, *, save: bool = False) -> list[dict[str, Any]]:
        """
        Crawls popular titles and post details from MLBPark Bullpen forum.
        """
        url = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
        logger.info("Fetching posts from MLBPark Bullpen: %s", url)

        posts = []
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                throttle.wait_sync("mlbpark.donga.com")
                res = client.get(url)
                self._raw_pages.append(
                    {
                        "source_key": "mlbpark_bullpen",
                        "url": url,
                        "html": res.text,
                        "status_code": res.status_code,
                    },
                )
                if res.status_code == HTTPStatus.OK:
                    soup = BeautifulSoup(res.text, "html.parser")

                    seen_urls = set()
                    for a in soup.find_all("a"):
                        href = a.get("href", "")
                        title = a.text.strip()

                        if "id=" in href and "b=bullpen" in href and "m=view" in href:
                            if "pos=reply" in href or not title or (title.startswith("[") and title.endswith("]")):
                                continue

                            # Clean up comment/reply counts (e.g. Title [15] -> Title)
                            title = re.sub(r"\s*\[\d+\]$", "", title)

                            if href.startswith("/"):
                                href = "https://mlbpark.donga.com" + href

                            if href in seen_urls:
                                continue
                            seen_urls.add(href)

                            posts.append(
                                {
                                    "title": title,
                                    "content": f"MLBPark Bullpen popular discussion thread: {title}",
                                    "meta": {
                                        "source": href,
                                        "crawled_at": datetime.now(KST).isoformat(),
                                        "category": "mlbpark",
                                    },
                                },
                            )
            logger.info("   Fetched %d posts from MLBPark.", len(posts))
        except httpx.HTTPError:
            logger.exception("Error fetching MLBPark bullpen posts")

        if save and self._raw_pages:
            with SessionLocal() as session:
                save_raw_snapshots(session, self._raw_pages)
        return posts
