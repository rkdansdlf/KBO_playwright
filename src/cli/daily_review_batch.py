"""
Daily Review Batch Script
Generates Post-game context (WPA Crucial Moments, Final Summary) 
and saves it to GameSummary.
"""
import asyncio
import argparse
import json
from datetime import datetime

from src.db.engine import SessionLocal
from src.models.game import Game, GameSummary
from src.models.player import PlayerBasic
from src.services.context_aggregator import ContextAggregator
from src.utils.safe_print import safe_print as print

async def run_review_batch(target_date: str):
    print(f"🚀 Starting Post-game Review Data Batch for {target_date}...")
    
    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        
        # Find completed games for the date
        games = session.query(Game).filter(
            Game.game_date == target_dt_obj,
            Game.game_status == 'COMPLETED'
        ).all()
        
        if not games:
            print(f"ℹ️ No completed games found for {target_date}.")
            return

        saved_count = 0
        for game in games:
            game_id = game.game_id
            
            print(f"📊 Generating review context for {game_id}...")
            
            review_data = {
                "game_id": game_id,
                "game_date": target_date,
                "final_score": f"{game.away_team} {game.away_score} : {game.home_score} {game.home_team}",
                "crucial_moments": agg.get_crucial_moments(game_id, limit=5)
            }
            
            if not review_data["crucial_moments"]:
                print(f"  ⚠️ No WPA events found for {game_id}. (PBP might not be crawled yet)")
            
            # Convert dict to JSON string
            review_json = json.dumps(review_data, ensure_ascii=False)
            
            # Upsert into GameSummary
            existing = session.query(GameSummary).filter(
                GameSummary.game_id == game_id,
                GameSummary.summary_type == "리뷰_WPA"
            ).first()
            
            if existing:
                existing.detail_text = review_json
            else:
                new_summary = GameSummary(
                    game_id=game_id,
                    summary_type="리뷰_WPA",
                    detail_text=review_json
                )
                session.add(new_summary)
            
            saved_count += 1
            
        try:
            session.commit()
            print(f"✅ Successfully saved {saved_count} game review contexts to DB.")
        except Exception as e:
            session.rollback()
            print(f"❌ Failed to save reviews to DB: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KBO Daily Review Context Generator")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    args = parser.parse_args()
    
    target = args.date if args.date else datetime.now().strftime("%Y%m%d")
    asyncio.run(run_review_batch(target))
