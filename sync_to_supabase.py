#!/usr/bin/env python3
"""
Sync player_basic table from SQLite to Supabase
"""
import os
from sqlalchemy import text
from src.db.engine import SessionLocal
from src.sync.supabase_sync import SupabaseSync

def main():
    supabase_url = os.getenv('SUPABASE_DB_URL')

    if not supabase_url:
        print("❌ SUPABASE_DB_URL environment variable not set")
        print("   Please set it in .env file or export it:")
        print("   export SUPABASE_DB_URL='postgresql://...'")
        return

    print("=" * 70)
    print("🔄 Supabase Sync - player_basic Table")
    print("=" * 70)
    print()

    print("📡 Connecting to Supabase...")

    with SessionLocal() as session:
        sync = SupabaseSync(supabase_url, session)
        try:
            if not sync.test_connection():
                print("❌ Supabase connection failed")
                return

            print("✅ Connection successful")
            print()

            # Get local count
            local_count = session.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()
            print(f"📊 Local SQLite: {local_count} players")

            # Check for Unknown Player entries
            unknown_count = session.execute(
                text("SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'")
            ).scalar()

            if unknown_count > 0:
                print(f"⚠️  Warning: {unknown_count} 'Unknown Player' entries in local DB")
                response = input("Continue with sync? (y/n): ")
                if response.lower() != 'y':
                    print("❌ Sync cancelled")
                    return
            else:
                print("✅ No 'Unknown Player' entries in local DB")

            print()
            print("📤 Syncing to Supabase...")
            synced = sync.sync_player_basic()
            print(f"✅ Synced {synced} players to Supabase")

            print()
            print("🔍 Verifying Supabase data...")

            # Check Supabase count
            with sync.supabase_session as sb_session:
                sb_count = sb_session.execute(
                    text("SELECT COUNT(*) FROM player_basic")
                ).scalar()
                print(f"📊 Supabase: {sb_count} players")

                # Check for Unknown Player in Supabase
                sb_unknown = sb_session.execute(
                    text("SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'")
                ).scalar()

                if sb_unknown > 0:
                    print(f"⚠️  Warning: {sb_unknown} 'Unknown Player' entries in Supabase")
                else:
                    print("✅ No 'Unknown Player' entries in Supabase")

                # Show sample
                print()
                print("📋 Sample from Supabase (5 random players):")
                sample = sb_session.execute(
                    text("SELECT player_id, name, team, position FROM player_basic ORDER BY RANDOM() LIMIT 5")
                ).fetchall()

                for row in sample:
                    print(f"   - {row[1]} (ID: {row[0]}, {row[2]}/{row[3]})")

            print()
            print("=" * 70)
            print("✅ Sync Complete!")
            print("=" * 70)

        finally:
            sync.close()

if __name__ == "__main__":
    main()
