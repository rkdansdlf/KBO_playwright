"""
End-to-end test: Fetch Futures stats and save to database.
"""
import asyncio
from src.crawlers.futures.futures_batting import fetch_and_parse_futures_batting
from src.repositories.save_futures_batting import save_futures_batting
from src.repositories.player_repository import PlayerRepository
from src.utils.safe_print import safe_print as print

PLAYER_ID = "51868"  # KBO player ID (string)
PLAYER_URL = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={PLAYER_ID}"


async def main():
    print(f"=== Futures Batting E2E Test ===\n")

    # Step 1: Crawl and parse
    print(f"Step 1: Crawling Futures stats for player {PLAYER_ID}...")
    rows = await fetch_and_parse_futures_batting(PLAYER_ID, PLAYER_URL)
    print(f"✓ Parsed {len(rows)} season records\n")

    if not rows:
        print("No data to save. Exiting.")
        return

    # Show sample
    for row in rows[:3]:
        print(f"  {row.get('season')}: AVG={row.get('AVG')}, G={row.get('G')}, H={row.get('H')}, HR={row.get('HR')}")
    print()

    # Step 2: Get or create player in database
    print(f"Step 2: Ensuring player {PLAYER_ID} exists in database...")
    repo = PlayerRepository()

    # Try to get existing player
    from src.parsers.player_profile_parser import PlayerProfileParsed
    player = repo.upsert_player_profile(
        PLAYER_ID,
        PlayerProfileParsed(is_active=True, player_name="고명준")
    )

    if not player:
        print("Failed to create player record")
        return

    print(f"✓ Player DB ID: {player.id}\n")

    # Step 3: Save Futures stats
    print(f"Step 3: Saving {len(rows)} Futures records to database...")
    saved = save_futures_batting(player_id_db=player.id, rows=rows)
    print(f"✓ Saved {saved} records\n")

    # Step 4: Verify
    print("Step 4: Verifying records in database...")
    from sqlalchemy import select
    from src.db.engine import SessionLocal
    from src.models.player import PlayerSeasonBatting

    with SessionLocal() as session:
        stmt = select(PlayerSeasonBatting).where(
            PlayerSeasonBatting.player_id == player.id,
            PlayerSeasonBatting.league == "FUTURES"
        ).order_by(PlayerSeasonBatting.season)

        results = session.execute(stmt).scalars().all()

        print(f"✓ Found {len(results)} Futures records in database:")
        for record in results:
            print(f"  {record.season}: AVG={record.avg}, G={record.games}, H={record.hits}, HR={record.home_runs}")

    print("\n=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
