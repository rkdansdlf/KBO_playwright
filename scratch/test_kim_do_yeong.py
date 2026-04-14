import asyncio
from src.crawlers.player_profile_crawler import PlayerProfileCrawler

async def test():
    crawler = PlayerProfileCrawler()
    result = await crawler.crawl_player_profile("52605", position="내야수")
    print(f"Result for 김도영 (52605):")
    for k, v in result.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    asyncio.run(test())
