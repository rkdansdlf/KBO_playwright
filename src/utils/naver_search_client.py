"""Naver Search API client for KBO operation notice crawling.

Uses the Naver Open API (검색 API) to query news and blog posts
related to stadium operations. This is a free alternative to Twitter/X API.

Naver Open API limits:
  - 하루 25,000 요청 (기본 쿼터)
  - 뉴스: 최대 100건/요청, 블로그: 최대 100건/요청

Endpoint:
  https://openapi.naver.com/v1/search/news.json
  https://openapi.naver.com/v1/search/cafearticle.json

Environment variables:
  NAVER_CLIENT_ID       — Naver Open API Client ID
  NAVER_CLIENT_SECRET   — Naver Open API Client Secret

Reference:
  https://developers.naver.com/docs/serviceapi/search/news/news.md
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

import httpx

from src.constants import KST

logger = logging.getLogger(__name__)

SearchType = Literal["news", "cafearticle", "blog"]

NAVER_SEARCH_BASE = "https://openapi.naver.com/v1/search"

# 잠실구장 관련 공지 탐지 쿼리
# 팀별 + 공지 키워드 조합
NOTICE_QUERIES: list[dict] = [
    {
        "query": "LG트윈스 (입장 OR 취소 OR 게이트 OR 공지 OR 우천 OR 주차)",
        "team": "LG",
        "notice_types": ["CANCEL", "GATE_CHANGE", "ENTRY_RULE", "PARKING"],
    },
    {
        "query": "두산베어스 (취소 OR 공지 OR 우천 OR 게이트 OR 입장)",
        "team": "OB",
        "notice_types": ["CANCEL", "GATE_CHANGE", "ENTRY_RULE"],
    },
    {
        "query": "NC다이노스 (취소 OR 공지 OR 우천 OR 게이트 OR 입장 OR 이벤트)",
        "team": "NC",
        "notice_types": ["CANCEL", "GATE_CHANGE", "ENTRY_RULE"],
    },
    {
        "query": "잠실야구장 (교통 OR 혼잡 OR 셔틀 OR 주차 OR 입장)",
        "team": None,  # 구장 전반
        "notice_types": ["ENTRY_RULE", "PARKING"],
    },
]


@dataclass
class NaverSearchResult:
    title: str
    description: str
    link: str
    pub_date: datetime | None
    source_type: SearchType
    team_hint: str | None
    raw: dict


def _parse_naver_date(pub_date_str: str) -> datetime | None:
    """Parse Naver API date string (RFC 2822 format)."""
    if not pub_date_str:
        return None
    try:
        # Naver returns: "Tue, 03 Jun 2026 14:32:00 +0900"
        from email.utils import parsedate_to_datetime

        return parsedate_to_datetime(pub_date_str).replace(tzinfo=None)
    except (ValueError, TypeError):
        logger.debug("Failed to parse Naver date: %s", pub_date_str)
    # Fallback: simple date only
    for fmt in ("%Y.%m.%d.", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(pub_date_str.strip().rstrip("."), fmt).replace(tzinfo=KST)
        except ValueError:
            continue
    return None


def _clean_html(text: str) -> str:
    """Remove Naver search result HTML tags (e.g. <b>, </b>)."""
    return re.sub(r"<[^>]+>", "", text).strip()


class NaverSearchClient:
    """Async client for Naver Open Search API.

    Usage:
        client = NaverSearchClient()
        results = await client.search_news("LG트윈스 입장")
    """

    def __init__(self) -> None:
        """Initializes a new instance."""
        self.client_id = os.getenv("NAVER_CLIENT_ID", "")
        self.client_secret = os.getenv("NAVER_CLIENT_SECRET", "")

    def _is_configured(self) -> bool:
        """Handles the is configured operation.

        Returns:
            True if the condition is met, False otherwise.

        """
        return bool(self.client_id and self.client_secret)

    def _headers(self) -> dict[str, Any]:
        """Handles the headers operation.

        Returns:
            Dictionary mapping.

        """
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }

    async def search(
        self,
        query: str,
        search_type: SearchType = "news",
        display: int = 20,
        sort: str = "date",
    ) -> list[NaverSearchResult]:
        """Search Naver for news/blog/cafe articles.

        Args:
            query: Search query string.
            search_type: 'news', 'blog', or 'cafearticle'.
            display: Number of results (max 100).
            sort: 'date' (newest) or 'sim' (relevance).

        """
        if not self._is_configured():
            logger.warning("[NaverSearch] API keys not configured. Skipping.")
            return []

        url = f"{NAVER_SEARCH_BASE}/{search_type}.json"
        params = {
            "query": query,
            "display": min(display, 100),
            "start": 1,
            "sort": sort,
        }

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params, headers=self._headers())
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPError as e:
            logger.warning("[NaverSearch] HTTP error for query=%r: %s", query, e)
            return []

        items = data.get("items", [])
        results = []
        for item in items:
            pub_date = _parse_naver_date(item.get("pubDate", ""))
            results.append(
                NaverSearchResult(
                    title=_clean_html(item.get("title", "")),
                    description=_clean_html(item.get("description", "")),
                    link=item.get("link") or item.get("originallink", ""),
                    pub_date=pub_date,
                    source_type=search_type,
                    team_hint=None,
                    raw=item,
                ),
            )
        return results

    async def search_kbo_notices(
        self,
        days_back: int = 3,
    ) -> list[NaverSearchResult]:
        """Run all KBO notice queries and return deduplicated results.

        Args:
            days_back: Only return results from the last N days.

        """
        import asyncio

        cutoff = datetime.now(KST) - timedelta(days=days_back)
        all_results: list[NaverSearchResult] = []
        seen_links: set[str] = set()

        tasks = [(q_config, stype) for q_config in NOTICE_QUERIES for stype in ("news",)]

        for q_config, stype in tasks:
            results = await self.search(
                q_config["query"],
                search_type=stype,
                display=30,
                sort="date",
            )
            for r in results:
                if r.link in seen_links:
                    continue
                if r.pub_date and r.pub_date < cutoff:
                    continue
                seen_links.add(r.link)
                r.team_hint = q_config.get("team")
                all_results.append(r)
            # Brief delay between queries
            await asyncio.sleep(0.3)

        logger.info("[NaverSearch] Total deduplicated results: %d", len(all_results))
        return all_results
