
import os
from sqlalchemy import text
from src.db.engine import Engine

def standardize():
    with Engine.connect() as conn:
        # 1. Get official mapping (ID per year/type)
        print("🔍 Loading official season mapping from local kbo_seasons...")
        # We prefer the sequential/small IDs (official metadata)
        # Avoid the custom-looking IDs if they leaked into kbo_seasons
        result = conn.execute(text("""
            SELECT season_year, league_type_code, MIN(season_id) as official_id
            FROM kbo_seasons
            WHERE season_id < 10000
            GROUP BY season_year, league_type_code
        """))
        mapping = {}
        for r in result:
            mapping[(r.season_year, r.league_type_code)] = r.official_id
        
        print(f"✅ Loaded {len(mapping)} official mappings.")
        
        # 2. Update Games
        total_updated = 0
        
        # We'll use a single query to update each year/type combo
        # internal_type_map: custom_suffix -> official_league_type_code
        internal_type_map = {
            0: 0, # Regular
            1: 1, # Exhibition
            2: 2, # Wildcard
            3: 3, # Semi-playoff
            4: 4, # Playoff
            5: 5, # Korean Series
            # Some scripts might use different conventions, but these are the main ones.
        }
        
        # Heuristic for unmapped games (e.g. 1982-2000 track B which might have NULL season_id)
        # Default them to Regular (0) for that year.
        
        for year in range(1982, 2026):
            # A. Standardize existing custom IDs
            for suffix, type_code in internal_type_map.items():
                custom_id = int(f"{year}{suffix}")
                official_id = mapping.get((year, type_code))
                
                if official_id:
                    res = conn.execute(text("""
                        UPDATE game 
                        SET season_id = :official_id 
                        WHERE season_id = :custom_id
                    """), {"official_id": official_id, "custom_id": custom_id})
                    
                    if res.rowcount > 0:
                        print(f"   [Update] {year} Type {type_code}: {res.rowcount} games ({custom_id} -> {official_id})")
                        total_updated += res.rowcount
            
            # B. Backfill NULL season_ids for that year (track B/historical)
            # Default to Regular (0) - season_id suffix 0
            official_reg_id = mapping.get((year, 0))
            if official_reg_id:
                res = conn.execute(text("""
                    UPDATE game 
                    SET season_id = :official_id 
                    WHERE season_id IS NULL AND game_id LIKE :year_prefix
                """), {"official_id": official_reg_id, "year_prefix": f"{year}%"})
                
                if res.rowcount > 0:
                    print(f"   [Backfill] {year} NULL -> {official_reg_id}: {res.rowcount} games")
                    total_updated += res.rowcount

        conn.commit()
        print(f"\n🏁 Finished. Standardized {total_updated} total game records.")

if __name__ == "__main__":
    standardize()
