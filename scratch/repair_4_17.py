
import asyncio
from datetime import date
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail
from src.db.engine import SessionLocal
from src.models.game import Game
from src.services.player_id_resolver import PlayerIdResolver

async def repair():
    session = SessionLocal()
    try:
        # Target only 2026-04-17
        game_date = "20260417"
        query = session.query(Game).filter(Game.game_id.like(f"{game_date}%"))
        games = query.all()
        
        if not games:
            print("No games found for 2026-04-17")
            return

        print(f"Repairing {len(games)} games for {game_date}...")
        
        resolver = PlayerIdResolver(session)
        resolver.preload_season_index(2026)
        
        crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        
        inputs = [
            {"game_id": g.game_id, "game_date": game_date}
            for g in games
        ]
        
        payloads = await crawler.crawl_games(inputs, concurrency=2)
        
        for payload in payloads:
            game_id = payload.get("game_id")
            print(f"Updating {game_id}...")
            # save_game_detail uses _replace_records which handles the update
            success = save_game_detail(payload)
            if success:
                print(f" ✅ {game_id} updated successfully")
            else:
                print(f" ❌ {game_id} update failed")
                
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(repair())
