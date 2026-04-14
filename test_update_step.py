
import asyncio
from datetime import datetime
from src.crawlers.player_movement_crawler import PlayerMovementCrawler
from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.repositories.player_repository import PlayerRepository
from src.repositories.team_repository import TeamRepository
from src.db.engine import SessionLocal

async def test_step_4_2(target_date: str):
    year = int(target_date[:4])
    print(f"\n🔄 Testing Player Movements & Roster Snapshots for {target_date}...")
    try:
        # 1. Player Movements
        m_crawler = PlayerMovementCrawler()
        # We can limit to one year to speed up
        movements = await m_crawler.crawl_years(year, year)
        if movements:
            m_repo = PlayerRepository()
            m_count = m_repo.save_player_movements(movements)
            print(f"   ✅ Saved {m_count} player movements for {year}")

        # 2. Daily Roster Snapshot
        r_target_date = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
        r_crawler = DailyRosterCrawler()
        rosters = await r_crawler.crawl_date_range(r_target_date, r_target_date)
        if rosters:
            with SessionLocal() as session:
                r_repo = TeamRepository(session)
                r_count = r_repo.save_daily_rosters(rosters)
                print(f"   ✅ Saved {r_count} daily roster records for {r_target_date}")
    except Exception as exc:
        print(f"   ❌ Error: {exc}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_step_4_2("20240521"))
