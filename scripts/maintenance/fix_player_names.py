"""
Fix Player Names - Re-crawl all players from KBO website

This script:
1. Clears bad data from player_basic table
2. Re-crawls all players from KBO website
3. Saves correct player names to database
4. Optionally syncs to Supabase

Usage:
    python fix_player_names.py --crawl --save
    python fix_player_names.py --crawl --save --sync-supabase
"""
import asyncio
import argparse
import os
from src.crawlers.player_search_crawler import crawl_all_players, player_row_to_dict
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.db.engine import SessionLocal, init_db

async def main():
    parser = argparse.ArgumentParser(description='Fix player names by re-crawling')
    parser.add_argument('--crawl', action='store_true', help='Crawl players from website')
    parser.add_argument('--save', action='store_true', help='Save to SQLite database')
    parser.add_argument('--sync-supabase', action='store_true', help='Sync to Supabase after crawl')
    parser.add_argument('--max-pages', type=int, help='Limit number of pages (for testing)')
    args = parser.parse_args()

    print("=" * 70)
    print("🔧 FIX PLAYER NAMES - Re-crawl from KBO Website")
    print("=" * 70)

    if not args.crawl:
        print("\n⚠️  Use --crawl flag to start crawling")
        print("   Example: python fix_player_names.py --crawl --save")
        return

    # Initialize database
    print("\n📦 Initializing database...")
    init_db()

    # Crawl players
    print(f"\n🕷️  Crawling players from KBO website...")
    if args.max_pages:
        print(f"   (Limited to {args.max_pages} pages for testing)")

    players = await crawl_all_players(
        max_pages=args.max_pages,
        headless=True,
        request_delay=1.5
    )

    print(f"\n✅ Crawled {len(players)} players")

    if not players:
        print("❌ No players collected!")
        return

    # Validate names
    print("\n🔍 Validating player names...")
    valid_players = []
    invalid_players = []

    for p in players:
        if p.name and p.name.strip() and p.name not in ['Unknown Player', 'Unknown', '-', 'N/A']:
            valid_players.append(p)
        else:
            invalid_players.append(p)

    print(f"   ✅ Valid: {len(valid_players)}")
    print(f"   ❌ Invalid: {len(invalid_players)}")

    if invalid_players:
        print("\n⚠️  Invalid player names found:")
        for p in invalid_players[:10]:
            print(f"   - player_id={p.player_id}, name='{p.name}'")

    # Show sample
    print("\n📋 Sample (first 10 valid players):")
    for p in valid_players[:10]:
        print(f"   - {p.name} (ID: {p.player_id}, #{p.uniform_no}, {p.team}/{p.position})")

    if args.save:
        # Convert to dicts
        print("\n🔄 Converting to database format...")
        player_dicts = [player_row_to_dict(p) for p in valid_players]

        # Save to database
        print(f"\n💾 Saving {len(player_dicts)} players to SQLite...")
        repo = PlayerBasicRepository()

        try:
            saved_count = repo.upsert_players(player_dicts)
            print(f"✅ Saved {saved_count} players to SQLite")

            # Verify
            print("\n🔍 Verifying database...")
            total = repo.count()
            print(f"   Total players in database: {total}")

            # Check for any remaining "Unknown Player" entries
            with SessionLocal() as session:
                unknown_count = session.execute(
                    "SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'"
                ).scalar()
                if unknown_count > 0:
                    print(f"   ⚠️  Still have {unknown_count} 'Unknown Player' entries!")
                else:
                    print(f"   ✅ No 'Unknown Player' entries found")

                # Show sample from database
                sample = session.execute(
                    "SELECT player_id, name, team, position FROM player_basic ORDER BY player_id LIMIT 10"
                ).fetchall()

                print("\n📋 Sample from database:")
                for row in sample:
                    print(f"   - {row[1]} (ID: {row[0]}, {row[2]}/{row[3]})")

        except Exception as e:
            print(f"❌ Error saving to database: {e}")
            import traceback
            traceback.print_exc()
            return
    else:
        print("\n⚠️  Skipping save (use --save flag to save to database)")

    if args.sync_supabase:
        supabase_url = os.getenv('SUPABASE_DB_URL')

        if not supabase_url:
            print("\n❌ SUPABASE_DB_URL not set; cannot sync to Supabase")
            return

        print("\n🔄 Syncing to Supabase...")
        from src.sync.supabase_sync import SupabaseSync

        with SessionLocal() as sqlite_session:
            sync = SupabaseSync(supabase_url, sqlite_session)
            try:
                if not sync.test_connection():
                    print("❌ Supabase connection failed")
                    return

                synced = sync.sync_player_basic()
                print(f"✅ Synced {synced} players to Supabase")

                # Verify Supabase
                print("\n🔍 Verifying Supabase...")
                with sync.supabase_session as sb_session:
                    unknown_count = sb_session.execute(
                        "SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'"
                    ).scalar()

                    if unknown_count > 0:
                        print(f"   ⚠️  Supabase still has {unknown_count} 'Unknown Player' entries!")
                    else:
                        print(f"   ✅ No 'Unknown Player' entries in Supabase")

            finally:
                sync.close()
    else:
        print("\nℹ️  Skipping Supabase sync (use --sync-supabase flag to sync)")

    print("\n" + "=" * 70)
    print("✅ Complete!")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(main())
