"""Collect season statistics from explicitly selected third-party providers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome
from src.sources.stats.base import (
    ExternalStatRecord,
    ExternalStatsAccessError,
    ExternalStatsAdapter,
    ExternalStatsError,
    ExternalStatsParseError,
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
        client: CrawlerHttpClient | None = None,
        policy: RequestPolicy | None = None,
    ) -> None:
        """Initialize the crawler with optional transport and request policy."""
        self.adapters = adapters or {name: adapter() for name, adapter in _adapter_names().items()}
        self._injected_client = client
        self._clients: dict[str, CrawlerHttpClient] = {}
        self.policy = policy or _build_policy()

    def _client_for_host(self, host: str) -> CrawlerHttpClient:
        """Return a governed client whose breaker is isolated to one provider host."""
        if self._injected_client is not None:
            return self._injected_client
        if host not in self._clients:
            self._clients[host] = CrawlerHttpClient(
                name=f"external_stats:{host}",
                policy=HttpPolicy(
                    base_delay_seconds=max(0.0, self.policy.min_delay),
                    timeout_seconds=float(os.getenv("EXTERNAL_STATS_HTTP_TIMEOUT", "30")),
                    max_attempts=1,
                ),
                headers={
                    "User-Agent": os.getenv(
                        "EXTERNAL_STATS_USER_AGENT",
                        "KBOPlaywrightExternalStats/1.0 (research; contact: kbo@example.com)",
                    ),
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
        return self._clients[host]

    async def close(self) -> None:
        """Retain the old lifecycle API; the shared client closes per request."""
        return

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
        statiz_cookie = os.getenv("STATIZ_COOKIE") if adapter.provider == "statiz" else None
        request_headers = {"Cookie": statiz_cookie} if statiz_cookie else None
        response = await self._client_for_host(adapter.host).fetch_text(url, headers=request_headers)
        if not response.ok:
            if response.http_status in {403, 429}:
                message = f"HTTP {response.http_status}; no browser fallback is attempted"
                raise ExternalStatsAccessError(message)
            if response.outcome is CrawlOutcome.EMPTY:
                message = "provider returned an empty response body"
                raise ExternalStatsParseError(message)
            message = response.error or f"HTTP {response.http_status or 'request failed'}"
            raise ExternalStatsError(message)
        if not isinstance(response.data, str):
            message = "provider returned a non-text response"
            raise ExternalStatsParseError(message)
        body = response.data
        return FetchedExternalPage(
            source_key=adapter.source_keys[stat_type],
            url=response.url or url,
            body=body,
            status_code=response.http_status or 200,
            content_hash=source_content_hash(body),
            content_type=response.content_type,
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
