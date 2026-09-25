"""KBO Crawlers and Scraping Engine Package."""

from __future__ import annotations

from src.crawlers.base import (
    BaseCrawler,
    BaseHttpCrawler,
    BasePlaywrightCrawler,
)
from src.crawlers.circuit_breaker import (
    CircuitBreaker,
    circuit_breaker,
    circuit_registry,
)
from src.crawlers.circuit_breaker_dto import (
    CircuitOpenError,
    CircuitState,
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
    "CircuitOpenError",
    "CircuitPolicy",
    "CircuitState",
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
    "circuit_breaker",
    "circuit_registry",
    "parse_retry_after",
]
