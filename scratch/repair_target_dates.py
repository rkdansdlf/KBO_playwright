import asyncio
from datetime import date
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.db.engine import SessionLocal
from src.models.game import Game
from src.services.player_id_resolver import PlayerIdResolver

async def repair_games(dates):
    session = SessionLocal()
    try:
        resolver = PlayerIdResolver(session)
        resolver.preload_season_index(2026)
        
        detail_crawler = GameDetailCrawler(request_delay=0.5, resolver=resolver)
        relay_crawler = NaverRelayCrawler()
        
        for game_date in dates:
            print(f"Repairing games for {game_date}...")
            games = session.query(Game).filter(Game.game_date == game_date).all()
            if not games:
                print(f"No games found in DB for {game_date}")
                continue
            
            inputs = [{"game_id": g.game_id, "game_date": game_date.strftime("%Y%m%d")} for g in games]
            detail_payloads = await detail_crawler.crawl_games(inputs, concurrency=3)
            
            for detail_data in detail_payloads:
                game_id = detail_data.get("game_id")
                print(f"Saving {game_id}...")
                save_game_detail(detail_data)
                
                # Verify PBP as well
                relay_data = await relay_crawler.crawl_game_events(game_id)
                if relay_data and relay_data.get("events"):
                    save_relay_data(game_id, relay_data["events"])
                    print(f"Saved PBP for {game_id}")
    finally:
        session.close()

if __name__ == "__main__":
    target_dates = [
        date(2026, 4, 14),
        date(2026, 4, 15),
        date(2026, 4, 16),
        date(2026, 4, 17)
    ]
    asyncio.run(repair_games(target_dates))
