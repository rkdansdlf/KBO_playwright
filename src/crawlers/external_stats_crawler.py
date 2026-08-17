"""Collect season statistics from explicitly selected third-party providers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from src.sources.stats.base import (
    ExternalStatRecord,
    ExternalStatsAccessError,
    ExternalStatsAdapter,
    ExternalStatsError,
    source_content_hash,
)
from src.sources.stats.fangraphs import FanGraphsKboAdapter
from src.sources.stats.statiz import StatizKboAdapter
from src.utils.request_policy import RequestPolicy, RequestPolicyConfig

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

EXTERNAL_PROVIDER_ADAPTERS: dict[str, type[ExternalStatsAdapter]] = {
    "fangraphs": FanGraphsKboAdapter,
    "statiz": StatizKboAdapter,
}
EXTERNAL_STAT_TYPES = ("batting", "pitching")
EXTERNAL_PARSER_VERSION = "external-stats-v1"


@dataclass(frozen=True)
class FetchedExternalPage:
    """Capture one successful provider response for lineage and optional archival."""

    source_key: str
    url: str
    body: str
    status_code: int
    content_hash: str
    content_type: str | None = None


@dataclass(frozen=True)
class ExternalCrawlResult:
    """Return normalized records, fetched pages, and endpoint failures."""

    records: list[ExternalStatRecord]
    pages: list[FetchedExternalPage]
    failures: list[str]


class ExternalStatsCrawler:
    """Fetch public provider pages without browser or anti-bot bypasses."""

    def __init__(
        self,
        *,
        adapters: dict[str, ExternalStatsAdapter] | None = None,
        client: httpx.AsyncClient | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize the crawler with optional test doubles and request policy."""
        self.adapters = adapters or {name: adapter() for name, adapter in _adapter_names().items()}
        self._client = client
        self._owns_client = client is None
        self.policy = policy or _build_policy()

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the shared HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                follow_redirects=True,
                timeout=float(os.getenv("EXTERNAL_STATS_HTTP_TIMEOUT", "30")),
                headers={
                    "User-Agent": os.getenv(
                        "EXTERNAL_STATS_USER_AGENT",
                        "KBOPlaywrightExternalStats/1.0 (research; contact: kbo@example.com)",
                    ),
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
        return self._client

    async def close(self) -> None:
        """Close an internally owned HTTP client."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def crawl(
        self,
        season: int,
        *,
        providers: Iterable[str] = ("fangraphs", "statiz"),
        stat_types: Iterable[str] = EXTERNAL_STAT_TYPES,
    ) -> ExternalCrawlResult:
        """Fetch and parse selected provider/season/stat-type endpoints."""
        records: list[ExternalStatRecord] = []
        pages: list[FetchedExternalPage] = []
        failures: list[str] = []
        selected_types = tuple(stat_types)
        for provider in providers:
            adapter = self.adapters.get(provider)
            if adapter is None:
                failures.append(f"{provider}: unsupported provider")
                continue
            for stat_type in selected_types:
                try:
                    fetched = await self._fetch_page(adapter, season, stat_type)
                    parsed = adapter.parse_html(fetched.body, season, stat_type, fetched.url)
                    if not parsed:
                        failures.append(f"{provider}/{stat_type}: provider returned zero rows")
                        continue
                except (ExternalStatsError, httpx.HTTPError, OSError, ValueError) as exc:
                    message = f"{provider}/{stat_type}: {exc}"
                    failures.append(message)
                    logger.warning("External stats endpoint skipped: %s", message)
                    continue
                records.extend(parsed)
                pages.append(fetched)
                logger.info("External stats %s/%s -> %s rows", provider, stat_type, len(parsed))
        return ExternalCrawlResult(records=records, pages=pages, failures=failures)

    async def _fetch_page(self, adapter: ExternalStatsAdapter, season: int, stat_type: str) -> FetchedExternalPage:
        """Fetch one endpoint and stop on provider access controls."""
        url = adapter.build_url(season, stat_type)
        await self.policy.delay_async(host=adapter.host)
        client = await self._get_client()

        async def request() -> httpx.Response:
            statiz_cookie = os.getenv("STATIZ_COOKIE") if adapter.provider == "statiz" else None
            if statiz_cookie:
                response = await client.get(url, headers={"Cookie": statiz_cookie})
            else:
                response = await client.get(url)
            if response.status_code in {403, 429}:
                msg = f"HTTP {response.status_code}; no browser fallback is attempted"
                raise ExternalStatsAccessError(msg)
            response.raise_for_status()
            return response

        response = await self.policy.run_with_retry_async(request)
        body = response.text
        return FetchedExternalPage(
            source_key=adapter.source_keys[stat_type],
            url=str(response.url),
            body=body,
            status_code=response.status_code,
            content_hash=source_content_hash(body),
            content_type=response.headers.get("content-type"),
        )


def _adapter_names() -> dict[str, type[ExternalStatsAdapter]]:
    """Return the default provider adapter classes."""
    return EXTERNAL_PROVIDER_ADAPTERS


def _build_policy() -> RequestPolicy:
    """Build a slow, single-attempt policy for third-party hosts."""
    minimum = float(os.getenv("EXTERNAL_STATS_REQUEST_DELAY_MIN", "3"))
    maximum = float(os.getenv("EXTERNAL_STATS_REQUEST_DELAY_MAX", "6"))
    return RequestPolicy(
        RequestPolicyConfig(
            min_delay=minimum,
            max_delay=maximum,
            max_retries=1,
            retry_exceptions=(httpx.TimeoutException, httpx.NetworkError),
        ),
    )
