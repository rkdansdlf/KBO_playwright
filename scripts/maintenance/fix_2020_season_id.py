
import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.append(os.getcwd())

from src.db.engine import Engine

def fix_2020_season_ids():
    print("🛠️ Fixing 2020 Season IDs in DB...")
    
    with Engine.connect() as conn:
        # 1. Standardize 2020 Regular Season (2020-05-05 to 2020-10-31)
        # Official ID for 2020 Regular is 229
        res = conn.execute(text("""
            UPDATE game 
            SET season_id = 229 
            WHERE game_id LIKE '2020%' 
            AND game_date BETWEEN '2020-05-05' AND '2020-10-31'
        """))
        print(f"✅ Updated {res.rowcount} games to Regular Season (229)")
        
        # 2. Standardize 2020 Exhibition
        # Official ID for 2020 Exhibition is 230
        res = conn.execute(text("""
            UPDATE game 
            SET season_id = 230
            WHERE game_id LIKE '2020%' 
            AND game_date < '2020-05-05'
        """))
        print(f"✅ Updated {res.rowcount} games to Exhibition (230)")
        
        # 3. Standardize 2020 Postseason (November)
        # This is more complex, but we can do a rough assignment or check specific dates
        # For now, let's just label them as 234 (Korean Series) or generic postseason
        res = conn.execute(text("""
            UPDATE game 
            SET season_id = 234
            WHERE game_id LIKE '2020%' 
            AND game_date > '2020-10-31'
        """))
        print(f"✅ Updated {res.rowcount} games to Postseason (234)")
        
        conn.commit()
    print("🏁 2020 Season ID fix complete.")

if __name__ == "__main__":
    fix_2020_season_ids()
