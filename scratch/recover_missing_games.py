import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail

async def main():
    crawler = GameDetailCrawler(request_delay=0.5)
    
    missing_games = [
        "20260314KTHT0", "20260314SKHH0", "20260314WONC0",
        "20260321NCKT0", "20260321WOSK0", "20260322NCKT0",
        "20260322WOSK0", "20260323LTSK0", "20260323OBKT0",
        "20260323WOLG0", "20260324LTSK0", "20260324OBKT0",
        "20260324WOLG0", "20260328HTSK0", "20260328WOHH0",
        "20260329HTSK0", "20260329KTLG0", "20260329WOHH0",
        "20260331KTHH0", "20260331WOSK0"
    ]
    
    concurrency = 5
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_game(game_id):
        async with semaphore:
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

    tasks = [process_game(game_id) for game_id in missing_games]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
