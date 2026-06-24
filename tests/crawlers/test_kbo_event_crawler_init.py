from __future__ import annotations

from src.crawlers.kbo_event_crawler import KboEventCrawler


class TestKboEventCrawler:
    def test_init_defaults(self):
        crawler = KboEventCrawler()
        assert crawler is not None

    def test_init_custom_url(self):
        crawler = KboEventCrawler(base_url="https://example.com")
        assert crawler.urls == ("https://example.com",)
