import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.models.game import Game, GameSummary, GameBattingStat, GamePitchingStat
from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal
from tqdm import tqdm

# Enhanced Name Change and Special Player Map
FINAL_MAP = {
    '한동민': 62895,  # 한유섬
    '신용수': 69508,  # 신윤후
    '유장혁': 69706,  # 유로결 (Check ID: 69706)
    '유로결': 69706,
    '노성호': 64917,  # 노유상? Check.
    '채지선': 65103,
    '박병호': 75125,  # Usually refers to the star Park Byung-ho
    '이재원': 68106,  # Usually the catcher (SK/SSG)
    '김현수': 76290,  # Usually LG star
    '김상수': 79402,  # Usually Samsung SS (now KT)
}

# Team code map fixes for All-Star
SPECIAL_TEAMS = {'EA': 'East', 'WE': 'West', 'DRE': 'Dream', 'NAN': 'Nanum'}

def final_repair():
    session = SessionLocal()
    resolver = PlayerIdResolver(session)
    
    # Get the remaining NULL player_id rows (excluding umpires)
    summaries = session.query(GameSummary).filter(
        GameSummary.player_id == None,
        GameSummary.player_name != None,
        GameSummary.summary_type != '심판'
    ).all()
    
    print(f"🛠 Running final repair for {len(summaries)} records...")
    
    updated_count = 0
    for s in summaries:
        name = s.player_name
        
        # 1. Check manual map
        if name in FINAL_MAP:
            s.player_id = FINAL_MAP[name]
            updated_count += 1
            continue
            
        # 2. Try to resolve by name alone if it's potentially unique but resolver missed it 
        # (e.g. if resolver had a bug or strict team check)
        stmt = text("SELECT player_id FROM player_basic WHERE name = :name")
        res = session.execute(stmt, {"name": name}).fetchall()
        if len(res) == 1:
            s.player_id = res[0][0]
            updated_count += 1
            continue
            
    session.commit()
    print(f"✅ Final repair complete. Updated {updated_count} rows.")
    session.close()

if __name__ == "__main__":
    final_repair()
