import os
import sys
import asyncio
import argparse
import json
from datetime import datetime
from sqlalchemy import create_engine, text

# Project imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.db.engine import SessionLocal
from src.models.game import Game, GameSummary, GameEvent
from src.services.context_aggregator import ContextAggregator
from src.sync.oci_sync import OCISync
from src.utils.team_codes import resolve_team_code
from src.utils.safe_print import safe_print as print

async def backfill_2025():
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not set")
        return

    print("🚀 Starting 2025 Context Backfill...")
    
    # 1. Get all 2025 games from OCI (to ensure we cover what's in prod)
    engine = create_engine(oci_url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT game_id, game_date, home_team, away_team, game_status FROM game WHERE game_id LIKE '2025%' ORDER BY game_date"))
        games = result.fetchall()
    
    print(f"📊 Found {len(games)} games for 2025 in OCI.")

    # 2. Process each game locally
    with SessionLocal() as session:
        agg = ContextAggregator(session)
        sync = OCISync(oci_url, session)
        
        count = 0
        for g_id, g_date, home_team, away_team, status in games:
            # We need the game date object
            if isinstance(g_date, str):
                dt_obj = datetime.strptime(g_date, "%Y-%m-%d").date()
            else:
                dt_obj = g_date
            
            season_year = dt_obj.year
            
            # 2.1. Generate Preview Context
            print(f"[{count+1}/{len(games)}] Processing {g_id} ({away_team} @ {home_team})...")
            
            # Preview Data Structure (simulating what PreviewCrawler would provide)
            preview_data = {
                "game_id": g_id,
                "game_date": dt_obj.strftime("%Y%m%d"),
                "home_team_name": home_team,
                "away_team_name": away_team,
            }
            
            # Add context metrics
            preview_data['matchup_h2h'] = agg.get_head_to_head_summary(away_team, home_team, season_year, dt_obj)
            preview_data['away_recent_l10'] = agg.get_team_l10_summary(away_team, dt_obj)
            preview_data['home_recent_l10'] = agg.get_team_l10_summary(home_team, dt_obj)
            preview_data['away_metrics'] = agg.get_team_recent_metrics(away_team, dt_obj)
            preview_data['home_metrics'] = agg.get_team_recent_metrics(home_team, dt_obj)
            
            # Save Preview to local DB
            preview_json = json.dumps(preview_data, ensure_ascii=False)
            session.merge(GameSummary(game_id=g_id, summary_type="프리뷰", detail_text=preview_json))
            
            # 2.2. Generate Review Context (if COMPLETED)
            if status == 'COMPLETED':
                review_data = {
                    "game_id": g_id,
                    "game_date": dt_obj.strftime("%Y-%m-%d"),
                    "crucial_moments": agg.get_crucial_moments(g_id, limit=5)
                }
                review_json = json.dumps(review_data, ensure_ascii=False)
                session.merge(GameSummary(game_id=g_id, summary_type="리뷰_WPA", detail_text=review_json))
            
            count += 1
            if count % 20 == 0:
                session.commit()
                print(f"💾 Committed {count} games to local SQLite.")

        session.commit()
        print("✅ Local generation complete. Starting OCI Sync...")
        
        # 3. Sync to OCI
        # We only sync game_summary for 2025
        sync.sync_game_details(year=2025)
        print("🎉 OCI Sync complete!")

if __name__ == "__main__":
    asyncio.run(backfill_2025())
