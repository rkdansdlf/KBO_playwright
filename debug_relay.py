
import asyncio
from src.crawlers.relay_crawler import RelayCrawler
from src.utils.safe_print import safe_print as print

async def main():
    crawler = RelayCrawler(request_delay=2.0)
    game_id = "20241001LTNC0"
    date = "20241001"
    
    print(f"🕵️ Debugging Relay for {game_id}...")
    data = await crawler.crawl_game_relay(game_id, date)
    
    if data and data.get('innings'):
        print(f"✅ Success! Found {len(data['innings'])} innings.")
        for inn in data['innings']:
            print(f"   Inning {inn['inning']} ({inn['half']}): {len(inn['plays'])} plays")
            if inn['plays']:
                print(f"   Sample: {inn['plays'][0]}")
    else:
        print("❌ Failed: No innings extracted.")

if __name__ == "__main__":
    asyncio.run(main())
