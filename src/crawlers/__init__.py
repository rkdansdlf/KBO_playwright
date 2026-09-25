"""KBO Crawlers and Scraping Engine Package."""

from __future__ import annotations

from src.crawlers.base import (
    BaseCrawler,
    BaseHttpCrawler,
    BasePlaywrightCrawler,
)
from src.crawlers.dto import (
    CrawlExecutionStats,
    CrawlRequest,
    CrawlResponse,
    ExtractorResult,
)
from src.crawlers.http_client import (
    CircuitPolicy,
    CrawlerHttpClient,
    HttpPolicy,
)
from src.crawlers.registry import (
    CrawlerCategory,
    CrawlerMetadata,
    CrawlerRegistry,
)
from src.crawlers.resilience import (
    AdaptiveRateLimiter,
    CircuitBreaker,
    CircuitBreakerState,
)
from src.crawlers.result import (
    CrawlOutcome,
    CrawlResult,
)
from src.crawlers.retry_after import parse_retry_after

__all__ = [
    "AdaptiveRateLimiter",
    "BaseCrawler",
    "BaseHttpCrawler",
    "BasePlaywrightCrawler",
    "CircuitBreaker",
    "CircuitBreakerState",
    "CircuitPolicy",
    "CrawlExecutionStats",
    "CrawlOutcome",
    "CrawlRequest",
    "CrawlResponse",
    "CrawlResult",
    "CrawlerCategory",
    "CrawlerHttpClient",
    "CrawlerMetadata",
    "CrawlerRegistry",
    "ExtractorResult",
    "HttpPolicy",
    "parse_retry_after",
]
