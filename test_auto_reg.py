import asyncio
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal

async def test():
    session = SessionLocal()
    resolver = PlayerIdResolver(session)
    crawler = GameDetailCrawler(resolver=resolver)
    print("Testing crawl on 20260322WOSK0 to see if missing players auto-register:")
    detail = await crawler.crawl_game('20260322WOSK0', '20260322')
    if detail:
        print("Success!")
    else:
        print("Failed.")

if __name__ == "__main__":
    asyncio.run(test())
