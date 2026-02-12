#!/usr/bin/env python3
"""
Robust season-aware team code repair script.
Handles duplicate records (merged or deleted) and transaction failures.
"""
import os
import sys
import argparse
from sqlalchemy import create_engine, text, exc
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

from src.utils.team_history import resolve_team_code_for_season

# Tables where season/year is directly available
SEASON_DIRECT_TABLES = [
    ("player_season_batting", "season", "team_code"),
    ("player_season_pitching", "season", "team_code"),
    ("team_history", "season", "team_code"),
]

# Tables where season must be extracted from a date column
DATE_TABLES = [
    ("team_daily_roster", "roster_date", "team_code"),
]

# Tables where season must be extracted from game_id (first 4 chars)
GAME_ID_TABLES = [
    ("game", "game_id", "home_team"),
    ("game", "game_id", "away_team"),
    ("game", "game_id", "winning_team"),
    ("game_lineups", "game_id", "team_code"),
    ("game_batting_stats", "game_id", "team_code"),
    ("game_pitching_stats", "game_id", "team_code"),
    ("game_inning_scores", "game_id", "team_code"),
]

def repair_direct(conn, table, season_col, team_col, dry_run=True):
    print(f"  Repairing {table}.{team_col} (Direct season via {season_col})...")
    
    pairs = conn.execute(text(f"SELECT DISTINCT {season_col}, {team_col} FROM {table}")).fetchall()
    
    total_repaired = 0
    for season, current_code in pairs:
        if not current_code or season is None: continue
        
        correct_code = resolve_team_code_for_season(current_code, int(season))
        if correct_code and correct_code != current_code:
            print(f"    - Season {season}: '{current_code}' -> '{correct_code}'")
            if not dry_run:
                try:
                    # Try to update. Wrapping in SAVEPOINT to handle constraint violations.
                    conn.execute(text("SAVEPOINT repair_row"))
                    update_query = text(f"UPDATE {table} SET {team_col} = :correct WHERE {season_col} = :season AND {team_col} = :current")
                    res = conn.execute(update_query, {"correct": correct_code, "season": season, "current": current_code})
                    total_repaired += res.rowcount
                    conn.execute(text("RELEASE SAVEPOINT repair_row"))
                except exc.IntegrityError:
                    conn.execute(text("ROLLBACK TO SAVEPOINT repair_row"))
                    print(f"      ⚠️ Conflict detected for {season} {correct_code}. Handing duplicates...")
                    # For player_season tables, if we have a conflict, it means a record for correct_code ALREADY exists.
                    # We should probably delete the incorrect one (current_code).
                    delete_query = text(f"DELETE FROM {table} WHERE {season_col} = :season AND {team_col} = :current")
                    res = conn.execute(delete_query, {"season": season, "current": current_code})
                    print(f"      🗑️ Deleted {res.rowcount} duplicate rows.")
                    total_repaired += res.rowcount
                except Exception as e:
                    conn.execute(text("ROLLBACK TO SAVEPOINT repair_row"))
                    print(f"      ❌ Failed update {season} {current_code}->{correct_code}: {e}")
            else:
                count_query = text(f"SELECT COUNT(*) FROM {table} WHERE {season_col} = :season AND {team_col} = :current")
                total_repaired += conn.execute(count_query, {"season": season, "current": current_code}).scalar()
                
    return total_repaired

def repair_by_date(conn, table, date_col, team_col, dry_run=True, is_sqlite=False):
    print(f"  Repairing {table}.{team_col} (Season via {date_col})...")
    
    year_func = f"strftime('%Y', {date_col})" if is_sqlite else f"EXTRACT(YEAR FROM {date_col})"
    pairs = conn.execute(text(f"SELECT DISTINCT CAST({year_func} AS INTEGER) as year, {team_col} FROM {table}")).fetchall()
    
    total_repaired = 0
    for year, current_code in pairs:
        if not current_code or not year: continue
        
        correct_code = resolve_team_code_for_season(current_code, int(year))
        if correct_code and correct_code != current_code:
            print(f"    - Year {year}: '{current_code}' -> '{correct_code}'")
            if not dry_run:
                try:
                    conn.execute(text("SAVEPOINT repair_row"))
                    update_query = text(f"UPDATE {table} SET {team_col} = :correct WHERE CAST({year_func} AS INTEGER) = :year AND {team_col} = :current")
                    res = conn.execute(update_query, {"correct": correct_code, "year": year, "current": current_code})
                    total_repaired += res.rowcount
                    conn.execute(text("RELEASE SAVEPOINT repair_row"))
                except exc.IntegrityError:
                    conn.execute(text("ROLLBACK TO SAVEPOINT repair_row"))
                    print(f"      ⚠️ Conflict detected for {year} {correct_code}. Deleting duplicates...")
                    delete_query = text(f"DELETE FROM {table} WHERE CAST({year_func} AS INTEGER) = :year AND {team_col} = :current")
                    res = conn.execute(delete_query, {"year": year, "current": current_code})
                    print(f"      🗑️ Deleted {res.rowcount} duplicate rows.")
                    total_repaired += res.rowcount
                except Exception as e:
                    conn.execute(text("ROLLBACK TO SAVEPOINT repair_row"))
                    print(f"      ❌ Failed update {year} {current_code}->{correct_code}: {e}")
            else:
                count_query = text(f"SELECT COUNT(*) FROM {table} WHERE CAST({year_func} AS INTEGER) = :year AND {team_col} = :current")
                total_repaired += conn.execute(count_query, {"year": year, "current": current_code}).scalar()
                
    return total_repaired

def repair_by_game_id(conn, table, id_col, team_col, dry_run=True, is_sqlite=False):
    print(f"  Repairing {table}.{team_col} (Season via {id_col})...")
    
    year_extract = f"CAST(SUBSTR({id_col}, 1, 4) AS INTEGER)"
    pairs = conn.execute(text(f"SELECT DISTINCT {year_extract} as year, {team_col} FROM {table}")).fetchall()
    
    total_repaired = 0
    for year, current_code in pairs:
        if not current_code or not year: continue
        
        correct_code = resolve_team_code_for_season(current_code, int(year))
        if correct_code and correct_code != current_code:
            print(f"    - Year {year}: '{current_code}' -> '{correct_code}'")
            if not dry_run:
                try:
                    conn.execute(text("SAVEPOINT repair_row"))
                    update_query = text(f"UPDATE {table} SET {team_col} = :correct WHERE {year_extract} = :year AND {team_col} = :current")
                    res = conn.execute(update_query, {"correct": correct_code, "year": year, "current": current_code})
                    total_repaired += res.rowcount
                    conn.execute(text("RELEASE SAVEPOINT repair_row"))
                except exc.IntegrityError:
                    conn.execute(text("ROLLBACK TO SAVEPOINT repair_row"))
                    print(f"      ⚠️ Conflict in game table for {year} {correct_code}. This is rare.")
                    # In game tables, a conflict is weird but we delete the legacy one if it exists.
                    delete_query = text(f"DELETE FROM {table} WHERE {year_extract} = :year AND {team_col} = :current")
                    res = conn.execute(delete_query, {"year": year, "current": current_code})
                    print(f"      🗑️ Deleted {res.rowcount} problematic rows.")
                except Exception as e:
                    conn.execute(text("ROLLBACK TO SAVEPOINT repair_row"))
                    print(f"      ❌ Failed update {year} {current_code}->{correct_code}: {e}")
            else:
                count_query = text(f"SELECT COUNT(*) FROM {table} WHERE {year_extract} = :year AND {team_col} = :current")
                total_repaired += conn.execute(count_query, {"year": year, "current": current_code}).scalar()
                
    return total_repaired

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Robust repair of team codes")
    parser.add_argument("--oci", action="store_true", help="Target OCI postgres database")
    parser.add_argument("--bega", action="store_true", help="Target OCI bega_backend database")
    parser.add_argument("--sqlite", action="store_true", help="Target local SQLite database")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Dry run mode")
    args = parser.parse_args()

    urls = []
    if args.sqlite:
        urls.append(os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db"))
    
    oci_url = os.getenv("OCI_DB_URL")
    if args.oci and oci_url:
        urls.append(oci_url)
    if args.bega and oci_url:
        if oci_url.endswith("/postgres"):
            urls.append(oci_url[:-9] + "/bega_backend")
        else:
            urls.append(oci_url)
    
    if not urls:
        print("❌ Error: Specify --oci, --bega, or --sqlite")
        sys.exit(1)

    for url in urls:
        if not url: continue
        print(f"\n🚀 Processing {url}...")
        is_sqlite = url.startswith("sqlite")
        engine = create_engine(url)
        
        with engine.connect() as conn:
            # For Postgres, we need a transaction to use SAVEPOINTs effectively if we are in one.
            # But here we'll just handle each separately.
            trans = conn.begin()
            
            repaired_count = 0
            try:
                for table, scol, tcol in SEASON_DIRECT_TABLES:
                    exists = conn.execute(text(f"SELECT 1 FROM {'sqlite_master' if is_sqlite else 'information_schema.tables'} WHERE {'name' if is_sqlite else 'table_name'} = :table"), {"table": table}).scalar()
                    if exists:
                        repaired_count += repair_direct(conn, table, scol, tcol, args.dry_run)
                    
                for table, dcol, tcol in DATE_TABLES:
                    exists = conn.execute(text(f"SELECT 1 FROM {'sqlite_master' if is_sqlite else 'information_schema.tables'} WHERE {'name' if is_sqlite else 'table_name'} = :table"), {"table": table}).scalar()
                    if exists:
                        repaired_count += repair_by_date(conn, table, dcol, tcol, args.dry_run, is_sqlite)
                        
                for table, icol, tcol in GAME_ID_TABLES:
                    exists = conn.execute(text(f"SELECT 1 FROM {'sqlite_master' if is_sqlite else 'information_schema.tables'} WHERE {'name' if is_sqlite else 'table_name'} = :table"), {"table": table}).scalar()
                    if exists:
                        repaired_count += repair_by_game_id(conn, table, icol, tcol, args.dry_run, is_sqlite)
                
                trans.commit()
            except Exception as e:
                trans.rollback()
                print(f"💥 Failed to process {url}: {e}")

            print(f"\n✅ Total rows repaired (or potential): {repaired_count}")

if __name__ == "__main__":
    main()
