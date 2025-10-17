"""
Collect detailed player and game data using collected IDs

This script:
1. Reads player_ids from database (player_basic table)
2. Reads game_ids from database (game_schedules table)
3. Crawls detailed data for each ID
4. Saves to database
5. Syncs to Supabase

Usage:
    python collect_detailed_data.py --players --limit 10
    python collect_detailed_data.py --games --limit 5
    python collect_detailed_data.py --all --limit 10
"""
import asyncio
import argparse
from typing import List, Dict
from datetime import datetime

from src.db.engine import SessionLocal
from src.models.player import Player, PlayerBasic
from src.models.game import GameSchedule
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import GameRepository
from src.utils.safe_print import safe_print as print


async def collect_player_profiles(limit: int = 10):
    """
    Collect detailed player profiles using stored player IDs

    Args:
        limit: Maximum number of players to process
    """
    print("\n" + "=" * 60)
    print(f"👤 Collecting Player Profiles (limit: {limit})")
    print("=" * 60)

    with SessionLocal() as session:
        # Get player IDs from database
        players = session.query(PlayerBasic).limit(limit).all()

        if not players:
            print("⚠️  No players found in database")
            return

        print(f"📊 Found {len(players)} players to crawl")

    # Crawl profiles
    crawler = PlayerProfileCrawler(request_delay=2.0)
    success_count = 0
    error_count = 0

    for i, player in enumerate(players, 1):
        print(f"\n[{i}/{len(players)}] Processing player: {player.player_id}")

        try:
            profile = await crawler.crawl_player_profile(player.player_id)

            if profile:
                # TODO: Save profile data to database
                # For now, just log success
                print(f"✅ Successfully crawled profile for {player.player_id}")
                success_count += 1
            else:
                print(f"⚠️  No profile data for {player.player_id}")
                error_count += 1

            # Rate limiting
            await asyncio.sleep(1.5)

        except Exception as e:
            print(f"❌ Error processing {player.player_id}: {e}")
            error_count += 1

    print(f"\n📈 Player Profile Collection Summary:")
    print(f"   - Success: {success_count}")
    print(f"   - Errors: {error_count}")
    print(f"   - Total: {len(players)}")


async def collect_game_details(limit: int = 5):
    """
    Collect detailed game data using stored game IDs

    Args:
        limit: Maximum number of games to process
    """
    print("\n" + "=" * 60)
    print(f"🎮 Collecting Game Details (limit: {limit})")
    print("=" * 60)

    repo = GameRepository()

    # Get pending games from database
    pending_games = repo.get_pending_games(limit=limit)

    if not pending_games:
        print("⚠️  No pending games found in database")
        return

    print(f"📊 Found {len(pending_games)} games to crawl")

    # Crawl game details
    crawler = GameDetailCrawler(request_delay=2.0)
    success_count = 0
    error_count = 0

    for i, game in enumerate(pending_games, 1):
        game_id = game['game_id']
        game_date = game['game_date']

        print(f"\n[{i}/{len(pending_games)}] Processing game: {game_id}")

        try:
            # Update status
            repo.update_crawl_status(game_id, 'crawling')

            # Crawl game detail
            game_data = await crawler.crawl_game(game_id, game_date)

            if game_data:
                # Save to database
                success = repo.save_game_detail(game_data)

                if success:
                    repo.update_crawl_status(game_id, 'completed')
                    print(f"✅ Successfully saved game {game_id}")
                    success_count += 1
                else:
                    repo.update_crawl_status(game_id, 'failed', 'Error saving to database')
                    print(f"❌ Failed to save game {game_id}")
                    error_count += 1
            else:
                repo.update_crawl_status(game_id, 'failed', 'No data returned from crawler')
                print(f"⚠️  No data for game {game_id}")
                error_count += 1

            # Rate limiting
            await asyncio.sleep(2.0)

        except Exception as e:
            repo.update_crawl_status(game_id, 'failed', str(e))
            print(f"❌ Error processing game {game_id}: {e}")
            error_count += 1

    print(f"\n📈 Game Detail Collection Summary:")
    print(f"   - Success: {success_count}")
    print(f"   - Errors: {error_count}")
    print(f"   - Total: {len(pending_games)}")


async def main():
    parser = argparse.ArgumentParser(description='Collect detailed KBO data using stored IDs')
    parser.add_argument('--players', action='store_true', help='Collect player profiles')
    parser.add_argument('--games', action='store_true', help='Collect game details')
    parser.add_argument('--all', action='store_true', help='Collect both players and games')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of records to process')
    parser.add_argument('--sync', action='store_true', help='Sync to Supabase after collection')

    args = parser.parse_args()

    print("\n" + "-" * 30)
    print("KBO Detailed Data Collection")
    print("-" * 30)

    # Show current database state
    with SessionLocal() as session:
        player_count = session.query(Player).count()
        game_count = session.query(GameSchedule).count()
        pending_count = session.query(GameSchedule).filter_by(crawl_status='pending').count()

        print(f"\n📊 Current Database State:")
        print(f"   - Players: {player_count}")
        print(f"   - Game Schedules: {game_count}")
        print(f"   - Pending Games: {pending_count}")

    # Collect player profiles
    if args.players or args.all:
        await collect_player_profiles(limit=args.limit)

    # Collect game details
    if args.games or args.all:
        await collect_game_details(limit=args.limit)

    # Sync to Supabase
    if args.sync:
        print("\n" + "=" * 60)
        print("🔄 Syncing to Supabase")
        print("=" * 60)

        from src.sync.supabase_sync import SupabaseSync
        import os

        supabase_url = os.getenv('SUPABASE_DB_URL')
        if supabase_url:
            with SessionLocal() as session:
                sync = SupabaseSync(supabase_url, session)
                if sync.test_connection():
                    # Sync data
                    team_results = sync.sync_all_team_data()
                    player_results = sync.sync_all_player_data()

                    print(f"\n✅ Sync Complete!")
                    print(f"   Team records: {sum(team_results.values())}")
                    print(f"   Player records: {sum(player_results.values())}")
                sync.close()
        else:
            print("⚠️  SUPABASE_DB_URL not set, skipping sync")

    print("\n" + "√" * 30)
    print("Detailed Data Collection Complete!")
    print("√" * 30 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
