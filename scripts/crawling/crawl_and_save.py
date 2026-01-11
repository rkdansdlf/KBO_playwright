"""
Integrated KBO Data Collection Pipeline
Collects player IDs and game IDs, then stores them in database

Usage:
    python crawl_and_save.py              # Default: Collect current season data
    python crawl_and_save.py --season 2024 --year 2025 --months 3,4,5
"""
import asyncio
import argparse
from datetime import datetime

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.player_list_crawler import PlayerListCrawler
from src.repositories.game_repository import GameRepository
from src.db.engine import SessionLocal
from src.models.player import Player, PlayerIdentity, PlayerCode
from src.utils.safe_print import safe_print as print


async def crawl_and_save_schedule(year: int, month: int):
    """Crawl schedule and save to database"""
    print("\n" + "=" * 60)
    print("STEP 1: Crawl and Save Schedule")
    print("=" * 60)

    crawler = ScheduleCrawler()
    repo = GameRepository()

    # Crawl schedule
    games = await crawler.crawl_schedule(year, month)

    if games:
        # Save to database
        saved_count = repo.save_schedules(games)
        print(f"✅ Saved {saved_count} games to database")
        return games
    else:
        print("❌ No games found")
        return []


async def crawl_and_save_game_detail(game_id: str, game_date: str):
    """Crawl game detail and save to database"""
    print("\n" + "=" * 60)
    print(f"STEP 2: Crawl and Save Game Detail ({game_id})")
    print("=" * 60)

    crawler = GameDetailCrawler()
    repo = GameRepository()

    # Update status to 'ready'
    repo.update_crawl_status(game_id, 'ready')

    # Crawl game detail
    game_data = await crawler.crawl_game(game_id, game_date)

    if game_data:
        # Save to database
        success = repo.save_game_detail(game_data)

        if success:
            repo.update_crawl_status(game_id, 'crawled')
            print(f"✅ Successfully crawled and saved game: {game_id}")
            return game_data
        else:
            repo.update_crawl_status(game_id, 'failed', 'Error saving to database')
            print(f"❌ Failed to save game: {game_id}")
            return None
    else:
        repo.update_crawl_status(game_id, 'failed', 'Error crawling data')
        print(f"❌ Failed to crawl game: {game_id}")
        return None


async def crawl_and_save_player_ids(season_year: int = 2024):
    """
    Crawl player IDs from KBO ranking pages and save to database

    Returns:
        Dict with hitters and pitchers lists
    """
    print("\n" + "=" * 60)
    print(f"STEP 1: Collect Player IDs for {season_year} Season")
    print("=" * 60)

    crawler = PlayerListCrawler(request_delay=1.5)
    result = await crawler.crawl_all_players(season_year=season_year)

    print(f"\n✅ Player ID Collection Complete:")
    print(f"   - Hitters: {len(result['hitters'])}")
    print(f"   - Pitchers: {len(result['pitchers'])}")

    # Save to database
    with SessionLocal() as session:
        saved_count = 0
        skipped_count = 0

        all_players = result['hitters'] + result['pitchers']

        for player_data in all_players:
            player_id_code = player_data.get('player_id')
            player_name = player_data.get('player_name')

            if not player_id_code or not player_name:
                skipped_count += 1
                continue

            # Check if player code already exists
            existing_code = session.query(PlayerCode).filter_by(
                source='KBO',
                code=player_id_code
            ).first()

            if existing_code:
                skipped_count += 1
                continue

            # Create new player
            player = Player(
                kbo_person_id=player_id_code,
                status='ACTIVE',
                debut_year=season_year
            )
            session.add(player)
            session.flush()

            # Create player identity
            identity = PlayerIdentity(
                player_id=player.id,
                name_kor=player_name,
                is_primary=True
            )
            session.add(identity)

            # Create player code mapping
            code = PlayerCode(
                player_id=player.id,
                source='KBO',
                code=player_id_code
            )
            session.add(code)

            saved_count += 1

        session.commit()

        print(f"\n💾 Saved Player IDs to Database:")
        print(f"   - New players: {saved_count}")
        print(f"   - Already exists: {skipped_count}")

    return result


async def main():
    """Main integrated workflow"""
    parser = argparse.ArgumentParser(description='KBO Data Collection Pipeline')
    parser.add_argument('--season', type=int, default=2024, help='Season year for players')
    parser.add_argument('--year', type=int, default=2025, help='Year for game schedule')
    parser.add_argument('--months', type=str, default='3', help='Months for games (comma-separated)')
    parser.add_argument('--players-only', action='store_true', help='Only collect player IDs')
    parser.add_argument('--games-only', action='store_true', help='Only collect game schedule')

    args = parser.parse_args()

    print("\n" + "🚀" * 30)
    print("KBO Data Collection Pipeline")
    print("🚀" * 30)

    # Collect players (unless --games-only)
    if not args.games_only:
        await crawl_and_save_player_ids(season_year=args.season)

    # Collect game schedule (unless --players-only)
    if not args.players_only:
        months = [int(m.strip()) for m in args.months.split(',')]

        for month in months:
            await crawl_and_save_schedule(args.year, month)

    # Show database summary
    print("\n" + "=" * 60)
    print("Database Summary")
    print("=" * 60)

    with SessionLocal() as session:
        player_count = session.query(Player).count()

        repo = GameRepository()
        pending_games = repo.get_pending_games(limit=5)

        print(f"\n📊 Current Database Status:")
        print(f"   - Players: {player_count}")
        print(f"   - Pending games: {len(pending_games)}")

    print("\n" + "✅" * 30)
    print("Pipeline Complete!")
    print("✅" * 30)


if __name__ == "__main__":
    asyncio.run(main())
