#!/usr/bin/env python3
"""
Backfill script to populate franchise_id and canonical_team_code in fact tables.
Also populates the team_code_map table.
"""
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.getcwd())

from src.utils.team_history import _TEAM_HISTORY, FRANCHISE_CANONICAL_CODE
from src.models.team import TeamCodeMap

def populate_team_code_map(engine):
    print("  🗺️  Populating TeamCodeMap...")
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 1. Clear existing
    if "sqlite" in engine.url.drivername:
        session.execute(text("DELETE FROM team_code_map"))
    else:
        session.execute(text("TRUNCATE TABLE team_code_map RESTART IDENTITY"))
    
    # 2. Build mapping
    # Group history by franchise
    franchise_history = {}
    for entry in _TEAM_HISTORY:
        if entry.franchise_id not in franchise_history:
            franchise_history[entry.franchise_id] = []
        franchise_history[entry.franchise_id].append(entry)
        
    entries_to_add = []
    
    for fid, history_entries in franchise_history.items():
        canonical = FRANCHISE_CANONICAL_CODE.get(fid)
        if not canonical:
            continue
            
        # Determine range for this franchise
        # Start from min start_season, go up to 2026
        min_year = min(e.start_season for e in history_entries)
        max_year = 2026
        
        for year in range(min_year, max_year + 1):
            # Find active code for this year
            active_code = None
            for entry in history_entries:
                end = entry.end_season or 9999
                if entry.start_season <= year <= end:
                    active_code = entry.team_code
                    break
            
            if active_code:
                # Add to DB
                # Note: This doesn't handle overlapping ranges well but for KBO history it's mostly distinct
                entries_to_add.append(TeamCodeMap(
                    franchise_id=fid,
                    season=year,
                    curr_code=active_code,
                    canonical_code=canonical,
                    is_canonical=(active_code == canonical)
                ))
            
    # Session add all
    # Need to be careful about session handling
    try:
        session.bulk_save_objects(entries_to_add) # bulk_save is faster
        session.commit()
    except Exception as e:
        session.rollback()
        # Fallback to individual add
        for e_obj in entries_to_add:
            session.add(e_obj)
        session.commit()
        
    print(f"  ✅ Added {len(entries_to_add)} rows to TeamCodeMap.")
    session.close()

def backfill_fact_tables(conn):
    print("  🏗️  Backfilling Fact Tables...")
    
    # Map code -> franchise_id (For all codes in history)
    # Note: _TEAM_HISTORY has multiple entries for same code (e.g. OB 1982, OB 1983...)
    # But code -> franchise_id is stable (except maybe weird edge cases not in KBO)
    code_to_fid = {}
    for entry in _TEAM_HISTORY:
        code_to_fid[entry.team_code] = entry.franchise_id
        
    with conn.begin():
        # 1. Update Game Franchise IDs
        print("    - Updating Game table...")
        game_updates = []
        for code, fid in code_to_fid.items():
            # Using param binding would be safer but constructing raw sql for speed
            # These are internal codes, safe from injection
            conn.execute(text(f"UPDATE game SET home_franchise_id = {fid} WHERE home_team = '{code}' AND home_franchise_id IS NULL"))
            conn.execute(text(f"UPDATE game SET away_franchise_id = {fid} WHERE away_team = '{code}' AND away_franchise_id IS NULL"))
            conn.execute(text(f"UPDATE game SET winning_franchise_id = {fid} WHERE winning_team = '{code}' AND winning_franchise_id IS NULL"))
        
        # 2. Update Other Fact Tables
        fact_tables = [
            "player_season_batting", "player_season_pitching",
            "game_lineups", "game_batting_stats", "game_pitching_stats", "game_inning_scores"
        ]
        
        print("    - Updating Fact tables (franchise_id)...")
        for table in fact_tables:
            for code, fid in code_to_fid.items():
                 conn.execute(text(f"UPDATE {table} SET franchise_id = {fid} WHERE team_code = '{code}' AND franchise_id IS NULL"))
            
        # 3. Update Canonical Codes based on Franchise ID
        print("    - Updating Canonical Codes...")
        for fid, canonical in FRANCHISE_CANONICAL_CODE.items():
            for table in fact_tables:
                 conn.execute(text(f"UPDATE {table} SET canonical_team_code = '{canonical}' WHERE franchise_id = {fid} AND canonical_team_code IS NULL"))

    print("  ✅ Fact tables backfilled.")

def run_backfill(url):
    print(f"🚀 Processing {url}...")
    engine = create_engine(url)
    populate_team_code_map(engine)
    
    with engine.connect() as conn:
        backfill_fact_tables(conn)

if __name__ == "__main__":
    load_dotenv()
    
    # Local
    run_backfill("sqlite:///./data/kbo_dev.db")
    
    # OCI
    oci_url = os.getenv("OCI_DB_URL")
    if oci_url:
        run_backfill(oci_url)
        
        if oci_url.endswith("/postgres"):
            bega_url = oci_url[:-9] + "/bega_backend"
            run_backfill(bega_url)
