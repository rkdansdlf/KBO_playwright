import asyncio
import os
import pytest
from src.crawlers.player_profile_crawler import PlayerProfileCrawler

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_DEBUG_TESTS") != "1",
    reason="Live debug crawler test is disabled unless RUN_LIVE_DEBUG_TESTS=1",
)

async def test():
    crawler = PlayerProfileCrawler()
    result = await crawler.crawl_player_profile("52605", position="내야수")
    print(f"Result for 김도영 (52605):")
    for k, v in result.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    asyncio.run(test())
