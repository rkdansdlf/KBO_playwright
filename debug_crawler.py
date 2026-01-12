
import asyncio
import json
from src.crawlers.game_detail_crawler import GameDetailCrawler

async def main():
    crawler = GameDetailCrawler()
    # Game ID: 20250322LTLG0
    # Date: 20250322
    print("Running crawler for 20250322LTLG0...")
    data = await crawler.crawl_game("20250322LTLG0", "20250322")
    
    if data:
        teams = data.get("teams", {})
        print("--- Home Team Info ---")
        print(json.dumps(teams.get("home", {}), indent=2, ensure_ascii=False))
        print("--- Away Team Info ---")
        print(json.dumps(teams.get("away", {}), indent=2, ensure_ascii=False))
    else:
        print("❌ No data returned.")

if __name__ == "__main__":
    asyncio.run(main())
