# This script runs async Playwright; just verify imports and function signatures.
from scripts.verification.debug_crawl import main


class TestMain:
    def test_is_coroutine(self):
        import asyncio
        assert asyncio.iscoroutinefunction(main)
