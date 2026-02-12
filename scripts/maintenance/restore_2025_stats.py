#!/usr/bin/env python3
"""
Surgical restoration of missing 2025 pitching stats for DB and KH.
Checks natural keys (game_id, player_id, appearance_seq) before inserting.
"""
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.game import GamePitchingStat, GameBattingStat
from src.models.team import Team

def restore_missing_2025_pitching(source_url, target_url):
    src_engine = create_engine(source_url)
    dst_engine = create_engine(target_url)
    
    SrcSession = sessionmaker(bind=src_engine)
    DstSession = sessionmaker(bind=dst_engine)
    
    with SrcSession() as src, DstSession() as dst:
        # 1. PlayerSeason tables (already mostly done, but re-run safely)
        print("🚚 Checking 2025 PlayerSeason stats...")
        for model in [PlayerSeasonBatting, PlayerSeasonPitching]:
            rows = src.query(model).filter_by(season=2025).all()
            for row in rows:
                data = {col.key: getattr(row, col.key) for col in row.__table__.columns if col.key != 'id'}
                # Check existance by domain keys (player_id, season, league, level)
                exists = dst.query(model).filter_by(
                    player_id=row.player_id, 
                    season=row.season, 
                    league=row.league, 
                    level=row.level
                ).first()
                if not exists:
                    dst.add(model(**data))
        
        # 2. GamePitchingStat (The main missing piece)
        print("🚚 Backfilling missing 2025 GamePitchingStat (DB/KH)...")
        # Target only DB and KH as reported missing
        missing_pitching = src.query(GamePitchingStat).filter(
            GamePitchingStat.game_id.like("2025%"),
            GamePitchingStat.team_code.in_(["DB", "KH"])
        ).all()
        
        added_count = 0
        for row in missing_pitching:
            data = {col.key: getattr(row, col.key) for col in row.__table__.columns if col.key != 'id'}
            # Check natural key (game_id, player_id, appearance_seq)
            exists = dst.query(GamePitchingStat).filter_by(
                game_id=row.game_id,
                player_id=row.player_id,
                appearance_seq=row.appearance_seq
            ).first()
            if not exists:
                dst.add(GamePitchingStat(**data))
                added_count += 1
                
        dst.commit()
        print(f"✅ Successfully backfilled {added_count} missing game-pitching rows.")

if __name__ == "__main__":
    load_dotenv()
    source = os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db")
    oci_url = os.getenv("OCI_DB_URL")
    
    bega_url = None
    if oci_url and oci_url.endswith("/postgres"):
        bega_url = oci_url[:-9] + "/bega_backend"
    else:
        bega_url = oci_url
        
    if bega_url:
        restore_missing_2025_pitching(source, bega_url)
