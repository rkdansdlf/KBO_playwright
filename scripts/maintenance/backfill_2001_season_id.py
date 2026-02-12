
from sqlalchemy import text
from src.db.engine import Engine

def backfill_2001_season_id():
    # Mapping based on KBO 2001 season ranges (approximate for verification)
    # Regular season: April to Early Oct
    # Post season: Oct
    
    # Official OCI IDs:
    # 115: 정규시즌 (0)
    # 116: 시범경기 (1)
    # 118: 준플레이오프 (3)
    # 119: 플레이오프 (4)
    # 120: 한국시리즈 (5)
    
    with Engine.connect() as conn:
        print("🔍 Backfilling 2001 season_id locally...")
        
        # Default all 2001 games to 115 (Regular) first
        res = conn.execute(text("UPDATE game SET season_id = 115 WHERE game_id LIKE '2001%'"))
        print(f"✅ Updated {res.rowcount} games to season_id=115 (Regular)")
        
        # TODO: Refine for Postseason if we have those games.
        # 2001 Postseason started around 2001-10-07 (Semi-playoff)
        # We can check game dates.
        
        # Semi-playoff (3): 2001-10-07 ~ 2001-10-09
        res = conn.execute(text("UPDATE game SET season_id = 118 WHERE game_id LIKE '2001%' AND game_date BETWEEN '2001-10-07' AND '2001-10-09'"))
        print(f"✅ Updated {res.rowcount} games to season_id=118 (Semi-playoff)")
        
        # Playoff (4): 2001-10-12 ~ 2001-10-18
        res = conn.execute(text("UPDATE game SET season_id = 119 WHERE game_id LIKE '2001%' AND game_date BETWEEN '2001-10-12' AND '2001-10-18'"))
        print(f"✅ Updated {res.rowcount} games to season_id=119 (Playoff)")
        
        # Korean Series (5): 2001-10-20 ~ 2001-10-28
        res = conn.execute(text("UPDATE game SET season_id = 120 WHERE game_id LIKE '2001%' AND game_date BETWEEN '2001-10-20' AND '2001-10-28'"))
        print(f"✅ Updated {res.rowcount} games to season_id=120 (Korean Series)")
        
        conn.commit()

if __name__ == "__main__":
    backfill_2001_season_id()
