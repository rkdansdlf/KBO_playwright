#!/usr/bin/env python3
"""
Migration script to add franchise_id and canonical_team_code to fact tables.
Also creates the team_code_map table.
"""
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.append(os.getcwd())

# Import Models to register with Base
from src.models.base import Base
from src.models.team import Team, TeamCodeMap, TeamDailyRoster 
from src.models.game import Game, GameLineup, GameBattingStat, GamePitchingStat, GameInningScore
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

def add_column_if_not_exists(engine, table, column, type_def):
    conn = engine.connect()
    try:
        if "sqlite" in engine.url.drivername:
            # SQLite specific PRAGMA
            cols = [col[1] for col in conn.execute(text(f"PRAGMA table_info({table})")).fetchall()]
            if column not in cols:
                print(f"    + Adding {column} to {table} (SQLite)...")
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}"))
        else:
            # Postgres: Check if column exists via information_schema
            query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND column_name='{column}'")
            res = conn.execute(query).fetchone()
            if not res:
                print(f"    + Adding {column} to {table} (Postgres)...")
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}"))
            
        conn.commit()
    except Exception as e:
        print(f"    ⚠️ Error adding {column} to {table}: {e}")
    finally:
        conn.close()

def migrate_database(url):
    print(f"🚀 Migrating {url}...")
    engine = create_engine(url)
    
    # 1. Add columns to PlayerSeason tables
    print("  Updating PlayerSeason tables...")
    for table in ["player_season_batting", "player_season_pitching"]:
        add_column_if_not_exists(engine, table, "franchise_id", "INTEGER")
        add_column_if_not_exists(engine, table, "canonical_team_code", "VARCHAR(10)")

    # 2. Add columns to Game Fact tables
    print("  Updating Game Fact tables...")
    fact_tables = ["game_lineups", "game_batting_stats", "game_pitching_stats", "game_inning_scores"]
    for table in fact_tables:
        add_column_if_not_exists(engine, table, "franchise_id", "INTEGER")
        add_column_if_not_exists(engine, table, "canonical_team_code", "VARCHAR(10)")
        
    # 3. Add columns to Game table
    print("  Updating Game table...")
    # These columns are specific to Game
    add_column_if_not_exists(engine, "game", "home_franchise_id", "INTEGER")
    add_column_if_not_exists(engine, "game", "away_franchise_id", "INTEGER")
    add_column_if_not_exists(engine, "game", "winning_franchise_id", "INTEGER")
    
    # 4. Create TeamCodeMap table
    print("  Creating TeamCodeMap table...")
    try:
        # Create all tables that don't exist, specifically TeamCodeMap
        Base.metadata.create_all(bind=engine, tables=[TeamCodeMap.__table__])
        print("    ✅ TeamCodeMap table ensured.")
    except Exception as e:
        print(f"    ⚠️ Error creating TeamCodeMap: {e}")

    print("✅ Migration complete.")

if __name__ == "__main__":
    load_dotenv()
    
    # Local SQLite
    migrate_database("sqlite:///./data/kbo_dev.db")
    
    # OCI Postgres
    oci_url = os.getenv("OCI_DB_URL")
    if oci_url:
        migrate_database(oci_url)
        
        # Bega Backend
        if oci_url.endswith("/postgres"):
            bega_url = oci_url[:-9] + "/bega_backend"
            migrate_database(bega_url)
