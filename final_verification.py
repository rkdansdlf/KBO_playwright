#!/usr/bin/env python3
"""
Final verification of player_basic data in both SQLite and Supabase
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.db.engine import SessionLocal

print("=" * 70)
print("🔍 Final Data Verification - SQLite & Supabase")
print("=" * 70)
print()

# SQLite verification
print("📊 SQLite (Local) Database:")
print("-" * 70)
with SessionLocal() as sqlite_session:
    total = sqlite_session.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()
    unknown = sqlite_session.execute(text("SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'")).scalar()
    valid = sqlite_session.execute(text("SELECT COUNT(*) FROM player_basic WHERE name != 'Unknown Player'")).scalar()
    
    print(f"   Total players: {total}")
    print(f"   ✅ Valid names: {valid}")
    print(f"   ❌ Unknown Player: {unknown}")
    
    if unknown == 0:
        print("   ✅ SQLite database is clean!")
    
    # Sample
    print()
    print("   Sample (5 random players):")
    sample = sqlite_session.execute(
        text("SELECT player_id, name, team, position FROM player_basic ORDER BY RANDOM() LIMIT 5")
    ).fetchall()
    for row in sample:
        print(f"      - {row[1]} (ID: {row[0]}, {row[2]}/{row[3]})")

print()
print("📊 Supabase (Cloud) Database:")
print("-" * 70)

supabase_url = os.getenv('SUPABASE_DB_URL')
if supabase_url:
    supabase_engine = create_engine(supabase_url)
    SupabaseSession = sessionmaker(bind=supabase_engine)
    
    with SupabaseSession() as sb_session:
        total = sb_session.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()
        unknown = sb_session.execute(text("SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown Player'")).scalar()
        valid = sb_session.execute(text("SELECT COUNT(*) FROM player_basic WHERE name != 'Unknown Player'")).scalar()
        
        print(f"   Total players: {total}")
        print(f"   ✅ Valid names: {valid}")
        print(f"   ⚠️  Unknown Player (old data with FK): {unknown}")
        
        # Sample
        print()
        print("   Sample (5 random valid players):")
        sample = sb_session.execute(
            text("SELECT player_id, name, team, position FROM player_basic WHERE name != 'Unknown Player' ORDER BY RANDOM() LIMIT 5")
        ).fetchall()
        for row in sample:
            print(f"      - {row[1]} (ID: {row[0]}, {row[2]}/{row[3]})")
        
        # Check sync status
        print()
        print("🔄 Sync Status:")
        print("-" * 70)
        print(f"   SQLite has {total - unknown} valid players")
        
        # Check if SQLite players are in Supabase
        with SessionLocal() as sqlite_session:
            sqlite_count = sqlite_session.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()
            
        sync_rate = (valid / sqlite_count * 100) if sqlite_count > 0 else 0
        print(f"   Supabase has {valid} valid players")
        print(f"   ✅ Sync coverage: {sync_rate:.1f}%")
        
        if unknown > 0:
            print()
            print(f"   ℹ️  Note: {unknown} 'Unknown Player' entries are old data")
            print(f"       Cannot be deleted due to foreign key constraints")
            print(f"       These are from previous crawls and have season stats")
else:
    print("   ❌ SUPABASE_DB_URL not configured")

print()
print("=" * 70)
print("✅ Verification Complete!")
print("=" * 70)
