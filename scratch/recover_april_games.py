import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail

async def main():
    crawler = GameDetailCrawler(request_delay=0.5)
    
    missing_games = [
        "20260419HHLT0", "20260419HTOB0", 
        "20260419LGSS0", "20260419SKNC0", 
        "20260419WOKT0"
    ]
    
    for game_id in missing_games:
        game_date = game_id[:8]
        game_date_formatted = f"{game_date[:4]}-{game_date[4:6]}-{game_date[6:]}"
        print(f"Crawling {game_id} ({game_date_formatted})...")
        detail = await crawler.crawl_game(game_id, game_date_formatted, lightweight=False)
        if detail:
            if save_game_detail(detail):
                print(f"✅ Saved Game Detail for {game_id}")
            else:
                print(f"❌ Failed to save Game Detail for {game_id}")
        else:
            print(f"⚠️ Could not fetch detail for {game_id}")

if __name__ == '__main__':
    asyncio.run(main())
