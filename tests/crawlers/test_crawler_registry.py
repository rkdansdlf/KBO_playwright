"""Unit tests for src.crawlers.registry."""

from __future__ import annotations

from src.crawlers.base import BaseCrawler
from src.crawlers.registry import (
    CrawlerCategory,
    CrawlerMetadata,
    CrawlerRegistry,
)


class DummyCrawler(BaseCrawler):
    pass


def test_crawler_registry_lookup_and_filter() -> None:
    meta = CrawlerMetadata(
        crawler_name="dummy_crawler",
        category=CrawlerCategory.GENERAL,
        crawler_cls=DummyCrawler,
        description="Dummy test crawler",
    )
    CrawlerRegistry.register(meta)

    found = CrawlerRegistry.get("dummy_crawler")
    assert found is not None
    assert found.crawler_name == "dummy_crawler"
    assert found.category == CrawlerCategory.GENERAL

    general_crawlers = CrawlerRegistry.list_by_category(CrawlerCategory.GENERAL)
    assert any(m.crawler_name == "dummy_crawler" for m in general_crawlers)

    schedule_crawlers = CrawlerRegistry.list_by_category(CrawlerCategory.SCHEDULE)
    assert any(m.crawler_name == "schedule" for m in schedule_crawlers)


def test_crawler_registry_instantiate() -> None:
    instance = CrawlerRegistry.instantiate("dummy_crawler", request_delay=0.5)
    assert isinstance(instance, DummyCrawler)
    assert instance.request_delay == 0.5
