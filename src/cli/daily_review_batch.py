"""
Daily Review Batch Script
Generates post-game review context from game_events/WPA
and saves it to GameSummary.
"""
import asyncio
import argparse
import json
import os
from datetime import datetime
from contextlib import contextmanager

from src.db.engine import SessionLocal
from src.models.game import Game, GameSummary
from src.models.player import PlayerBasic
from src.services.context_aggregator import ContextAggregator
from src.utils.safe_print import safe_print as print

async def run_review_batch(target_date: str, custom_session=None):
    print(f"🚀 Starting Post-game Review Data Batch for {target_date}...")
    
    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()

    # Use custom session factory if provided, else use default SessionLocal
    if custom_session:
        # custom_session is a sessionmaker — call it to get a session, then wrap in contextmanager
        @contextmanager
        def session_ctx():
            session = custom_session()
            try:
                yield session
            finally:
                session.close()
    else:
        session_ctx = SessionLocal

    with session_ctx() as session:
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
                print(
                    f"  ⚠️ No WPA-backed game_events found for {game_id}. "
                    "Raw event crawl may be missing or incomplete."
                )
            
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

    oci_url = os.getenv("OCI_DB_URL")
    custom_session = None
    if oci_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(oci_url)
        custom_session = sessionmaker(bind=engine)
        print(f"🔗 Using OCI Database for review generation.")

    asyncio.run(run_review_batch(target, custom_session=custom_session))
