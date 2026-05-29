import asyncio
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import save_game_detail
from src.services.player_id_resolver import PlayerIdResolver


async def recrawl_bad_games(year=2026, limit=10):
    with SessionLocal() as session:
        # Find games with PA=0 violations
        query = """
            SELECT DISTINCT g.game_id, g.game_date
            FROM game g
            JOIN game_batting_stats b ON g.game_id = b.game_id
            WHERE b.plate_appearances = 0
              AND b.at_bats > 0
              AND g.game_status IN ('COMPLETED', 'DRAW')
              AND g.game_date LIKE :year_pattern
        """
        rows = session.execute(text(query), {"year_pattern": f"{year}%"}).all()
        print(f"🛠️ Found {len(rows)} games to re-crawl.")

        if not rows:
            return

        resolver = PlayerIdResolver(session)
        crawler = GameDetailCrawler(resolver=resolver)

        # Limit for safety in this task, user can run more later
        targets = rows[:limit]

        for gid, gdate in targets:
            print(f"🚀 Re-crawling {gid} ({gdate})...")
            # Format date as YYYYMMDD
            date_str = gdate.replace("-", "")
            try:
                # Standard pattern for our crawlers is to use asyncio.run(crawler.main())
                # but here we call internal method.
                data = await crawler.crawl_game(gid, date_str)
                if data:
                    saved = save_game_detail(data)
                    if saved:
                        print(f"   ✅ Successfully re-crawled and saved {gid}")
                    else:
                        print(f"   ❌ Failed to save {gid}")
                else:
                    print(f"   ❌ Crawler returned no data for {gid}")
            except Exception as e:
                print(f"   🔥 Error re-crawling {gid}: {e}")


if __name__ == "__main__":
    limit = 10
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
    asyncio.run(recrawl_bad_games(limit=limit))
