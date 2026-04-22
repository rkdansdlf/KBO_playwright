import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler

async def main():
    crawler = GameDetailCrawler(request_delay=0.5)
    game_id = '20260314SKHH0'
    game_date = '2026-03-14'
    res = await crawler.crawl_game(game_id, game_date, lightweight=False)
    print("Found away pitchers:", len(res['pitchers']['away']))
    print("Found home pitchers:", len(res['pitchers']['home']))

if __name__ == '__main__':
    asyncio.run(main())
