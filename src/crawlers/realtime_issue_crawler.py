"""
Crawler for real-time issue text: Naver Sports baseball news headlines and MLBPark popular threads.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import save_raw_snapshots
from src.utils.safe_print import safe_print as print
from src.utils.throttle import throttle

logger = logging.getLogger(__name__)


class RealtimeIssueCrawler:
    """
    Scrapes real-time baseball topics, headlines, and forum discussions.
    """

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self._raw_pages: list[dict] = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def fetch_naver_news_headlines(self, save: bool = False) -> list[dict[str, Any]]:
        """
        Fetches latest baseball news headlines from Naver Sports GW API (JSON)
        with fallback to web scraping if API is down.
        """
        date_str = datetime.now().strftime("%Y%m%d")
        api_url = f"https://api-gw.sports.naver.com/news/articles/kbaseball?sort=latest&date={date_str}&page=1&pageSize=20&isPhoto=N"
        print("Fetching Naver news from API: %s", api_url)

        articles = []
        try:
            custom_headers = self.headers.copy()
            custom_headers["Referer"] = "https://sports.news.naver.com/kbaseball/news/index"
            custom_headers["Origin"] = "https://sports.news.naver.com"
            with httpx.Client(headers=custom_headers, timeout=self.timeout) as client:
                throttle.wait_sync("api-gw.sports.naver.com")
                res = client.get(api_url)
                self._raw_pages.append({
                    "source_key": "naver_sports_news",
                    "url": api_url,
                    "html": res.text,
                    "status_code": res.status_code,
                })
                if res.status_code == 200:
                    data = res.json()
                    result_data = data.get("result", {})
                    news_list = result_data.get("newsList", [])

                    for item in news_list:
                        title = item.get("title", "")
                        sub_content = item.get("subContent", "")
                        oid = item.get("oid", "")
                        offset_id = item.get("aid", "")
                        office_name = item.get("officeName", "")
                        dt_str = item.get("datetime", "")

                        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={offset_id}"

                        articles.append(
                            {
                                "title": title,
                                "content": sub_content if sub_content else title,
                                "meta": {
                                    "source": url,
                                    "office_name": office_name,
                                    "published_at": dt_str,
                                    "crawled_at": datetime.now().isoformat(),
                                    "category": "naver_news",
                                },
                            }
                        )
                    print("   Fetched %d headlines from JSON API.", len(articles))
                    return articles
                else:
                    print("Naver news API returned status code %d", res.status_code)
        except httpx.HTTPError:
            logger.exception("Naver news API failed. Falling back to HTML scraping...")

        # HTML Scraping Fallback
        fallback_url = "https://sports.news.naver.com/kbaseball/news/index"
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                throttle.wait_sync("sports.news.naver.com")
                res = client.get(fallback_url)
                self._raw_pages.append({
                    "source_key": "naver_sports_news",
                    "url": fallback_url,
                    "html": res.text,
                    "status_code": res.status_code,
                })
                if res.status_code == 200:
                    soup = BeautifulSoup(res.text, "html.parser")
                    links = []
                    for a in soup.find_all("a"):
                        href = a.get("href", "")
                        title = a.get("title") or a.text.strip()
                        if href and ("read" in href or "read.nhn" in href) and title:
                            if href.startswith("/"):
                                href = "https://sports.news.naver.com" + href
                            links.append((title, href))

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
                                    "crawled_at": datetime.now().isoformat(),
                                    "category": "naver_news",
                                },
                            }
                        )
            print(f"   Fetched {len(articles)} headlines from HTML fallback.")
        except httpx.HTTPError:
            logger.exception("Naver News HTML fallback also failed")

        if save and self._raw_pages:
            with SessionLocal() as session:
                save_raw_snapshots(session, self._raw_pages)
        return articles

    def fetch_mlbpark_bullpen_posts(self, save: bool = False) -> list[dict[str, Any]]:
        """
        Crawls popular titles and post details from MLBPark Bullpen forum.
        """
        url = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
        print("Fetching posts from MLBPark Bullpen: %s", url)

        posts = []
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                throttle.wait_sync("mlbpark.donga.com")
                res = client.get(url)
                self._raw_pages.append({
                    "source_key": "mlbpark_bullpen",
                    "url": url,
                    "html": res.text,
                    "status_code": res.status_code,
                })
                if res.status_code == 200:
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
                                        "crawled_at": datetime.now().isoformat(),
                                        "category": "mlbpark",
                                    },
                                }
                            )
            print("   Fetched %d posts from MLBPark.", len(posts))
        except httpx.HTTPError:
            logger.exception("Error fetching MLBPark bullpen posts")

        if save and self._raw_pages:
            with SessionLocal() as session:
                save_raw_snapshots(session, self._raw_pages)
        return posts
