#!/usr/bin/env python3
"""
Robust migration script to normalize team codes in both SQLite and Postgres.
Handles unique constraint conflicts by deleting legacy duplicates before updating.
"""
import os
import sys
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

# Legacy to Canonical Mapping (Modern default)
CANONICAL_TARGETS = {
    'HT': 'KIA',
    'SK': 'SSG',
    'OB': 'DB',
    'WO': 'KH',
    'KI': 'KH', # Sometimes appears in old game IDs
    'NX': 'KH', # Nexen -> Kiwoom Franchise
    'HU': 'KH', # Hyundai -> Kiwoom Franchise (approx)
    'BE': 'HH', # Binggre -> Hanwha
    'MBC': 'LG', # MBC -> LG
    'PC': 'TP', # Pacific -> Taepyeongyang (Normalized to TP)
    'SW': 'SL', # Ssangbangwool -> Ssangbangwool (Normalized to SL)
}

# Tables and their unique columns (excluding team_code) for conflict check
TABLE_CONSTRAINTS = {
    "team_daily_roster": ["roster_date", "player_id"],
    "player_season_batting": ["player_id", "season", "league", "level"],
    "player_season_pitching": ["player_id", "season", "league", "level"],
    "game_lineups": ["game_id", "team_side", "appearance_seq"],
    "game_batting_stats": ["game_id", "player_id", "appearance_seq"],
    "game_pitching_stats": ["game_id", "player_id", "appearance_seq"],
    "game_inning_scores": ["game_id", "team_side", "inning"],
    "team_history": ["season", "team_code"],
}

def resolve_conflicts(conn, table_name, team_col, legacy, canonical, unique_cols):
    """Delete legacy rows if a canonical row already exists to avoid UniqueConstraint errors."""
    if not unique_cols:
        return 0
    
    where_clauses = [f"t1.{col} = t2.{col}" for col in unique_cols]
    where_str = " AND ".join(where_clauses)
    
    # Generic SQL: Use table alias and EXISTS
    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE {team_col} = :legacy
    AND EXISTS (
        SELECT 1 FROM {table_name} t2
        WHERE t2.{team_col} = :canonical
        AND {where_str.replace('t1.', table_name + '.')}
    )
    """
    result = conn.execute(text(delete_sql), {"legacy": legacy, "canonical": canonical})
    return result.rowcount

def migrate_table(conn, table_name, team_col, dry_run=False):
    print(f"  Processing table '{table_name}' on column '{team_col}'...")
    
    total_updated = 0
    total_deleted = 0
    
    unique_cols = TABLE_CONSTRAINTS.get(table_name, [])
    
    for legacy, canonical in CANONICAL_TARGETS.items():
        if legacy == canonical: continue
        
        # 1. Check for rows to update
        count_query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {team_col} = :legacy")
        count = conn.execute(count_query, {"legacy": legacy}).scalar()
        
        if count > 0:
            if not dry_run:
                # 2. Resolve conflicts if any
                deleted = resolve_conflicts(conn, table_name, team_col, legacy, canonical, unique_cols)
                if deleted > 0:
                    print(f"    - Cleaned up {deleted} duplicates for '{legacy}' -> '{canonical}'")
                    total_deleted += deleted
                
                # 3. Update
                update_query = text(f"UPDATE {table_name} SET {team_col} = :canonical WHERE {team_col} = :legacy")
                result = conn.execute(update_query, {"canonical": canonical, "legacy": legacy})
                total_updated += result.rowcount
                print(f"    - Updated {result.rowcount} rows ('{legacy}' -> '{canonical}')")
            else:
                print(f"    - Potential: {count} rows with '{legacy}' (To be '{canonical}')")
                total_updated += count
                
    return total_updated, total_deleted

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Migrate team codes to canonical aggressively")
    parser.add_argument("--oci", action="store_true", help="Target OCI database")
    parser.add_argument("--sqlite", action="store_true", help="Target local SQLite database")
    parser.add_argument("--db-url", help="Override database URL")
    parser.add_argument("--dry-run", action="store_true", help="Don't perform updates")
    args = parser.parse_args()

    urls = []
    if args.db_url:
        urls.append(args.db_url)
    else:
        if args.sqlite:
            urls.append(os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db"))
        if args.oci:
            oci_url = os.getenv("OCI_DB_URL") or os.getenv("SUPABASE_DB_URL")
            if oci_url:
                urls.append(oci_url)
    
    if not urls:
        print("❌ Error: No target database specified. Use --oci or --sqlite.")
        sys.exit(1)

    tables_to_migrate = [
        ("game", "home_team"),
        ("game", "away_team"),
        ("game", "winning_team"),
        ("player_season_batting", "team_code"),
        ("player_season_pitching", "team_code"),
        ("game_lineups", "team_code"),
        ("game_batting_stats", "team_code"),
        ("game_pitching_stats", "team_code"),
        ("game_inning_scores", "team_code"),
        ("team_daily_roster", "team_code"),
        ("team_history", "team_code"),
        ("player_basic", "team"),
    ]

    for url in urls:
        print(f"\n🚀 Processing Database: {url}")
        is_sqlite = url.startswith("sqlite:")
        engine = create_engine(url)
        
        try:
            with engine.begin() as conn:
                if args.dry_run:
                    print("⚠️  DRY RUN MODE - No changes will be saved.")
                
                total_rows_updated = 0
                total_rows_deleted = 0
                
                for table, col in tables_to_migrate:
                    # Generic table existence check
                    if is_sqlite:
                        check_table = text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table")
                    else:
                        check_table = text("SELECT table_name FROM information_schema.tables WHERE table_name = :table")
                    
                    exists = conn.execute(check_table, {"table": table}).scalar()
                    if not exists:
                        # print(f"  ⚠️  Table '{table}' does not exist, skipping.")
                        continue
                    
                    updated, deleted = migrate_table(conn, table, col, args.dry_run)
                    total_rows_updated += updated
                    total_rows_deleted += deleted

                print("-" * 30)
                if args.dry_run:
                    print(f"✅ Dry run complete. {total_rows_updated} rows would be updated.")
                else:
                    print(f"✅ Migration complete for {url}.")
                    print(f"   Updated: {total_rows_updated} rows")
                    print(f"   Deleted (duplicates): {total_rows_deleted} rows")

        except Exception as e:
            print(f"❌ Error during migration of {url}: {e}")
            continue

if __name__ == "__main__":
    main()
