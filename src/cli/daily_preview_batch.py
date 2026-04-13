"""
Daily Preview Batch Script
Fetches Pre-game context (Starting Pitchers, Lineups) and saves it to GameSummary.
Designed to be run 1~2 hours before the games start via GitHub Actions.
"""
import asyncio
import argparse
import json
import os
from contextlib import contextmanager
from datetime import datetime

from src.crawlers.preview_crawler import PreviewCrawler
from src.db.engine import SessionLocal
from src.models.game import GameSummary
from src.models.player import PlayerBasic
from src.services.context_aggregator import ContextAggregator
from src.utils.team_codes import resolve_team_code
from src.utils.safe_print import safe_print as print

async def run_preview_batch(target_date: str, custom_session=None):
    print(f"🚀 Starting Preview Data Batch for {target_date}...")
    
    crawler = PreviewCrawler(request_delay=1.0)
    previews = await crawler.crawl_preview_for_date(target_date)
    
    if not previews:
        print("ℹ️ No preview data found. Games might be cancelled or not scheduled today.")
        return

    saved_count = 0
    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()
    season_year = target_dt_obj.year

    # Use custom session factory if provided, else use default SessionLocal
    if custom_session:
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
        for preview in previews:
            game_id = preview.get("game_id")
            if not game_id:
                continue
            
            # Resolve canonical codes for aggregation
            away_code = resolve_team_code(preview['away_team_name'], season_year)
            home_code = resolve_team_code(preview['home_team_name'], season_year)
            
            if away_code and home_code:
                print(f"📊 Aggregating context for {away_code} vs {home_code}...")
                preview['matchup_h2h'] = agg.get_head_to_head_summary(away_code, home_code, season_year, target_dt_obj)
                preview['away_recent_l10'] = agg.get_team_l10_summary(away_code, target_dt_obj)
                preview['home_recent_l10'] = agg.get_team_l10_summary(home_code, target_dt_obj)
                preview['away_metrics'] = agg.get_team_recent_metrics(away_code, target_dt_obj)
                preview['home_metrics'] = agg.get_team_recent_metrics(home_code, target_dt_obj)
                
                # Postseason Context (Optional)
                series_context = agg.get_postseason_series_summary(away_code, home_code, season_year, target_dt_obj)
                if series_context:
                    preview['series_context'] = series_context

            # Fetch Starting Pitcher Season Stats
            away_starter_id = preview.get('away_starter_id')
            home_starter_id = preview.get('home_starter_id')
            if away_starter_id:
                preview['away_starter_stats'] = agg.get_pitcher_season_stats(away_starter_id, season_year)
            if home_starter_id:
                preview['home_starter_stats'] = agg.get_pitcher_season_stats(home_starter_id, season_year)

            # Convert dict to JSON string for storage
            preview_json = json.dumps(preview, ensure_ascii=False)
            
            # Upsert into GameSummary table
            existing = session.query(GameSummary).filter(
                GameSummary.game_id == game_id,
                GameSummary.summary_type == "프리뷰"
            ).first()
            
            if existing:
                existing.detail_text = preview_json
            else:
                new_summary = GameSummary(
                    game_id=game_id,
                    summary_type="프리뷰",
                    detail_text=preview_json
                )
                session.add(new_summary)
            
            saved_count += 1
            
        try:
            session.commit()
            print(f"✅ Successfully saved {saved_count} game previews to DB.")
        except Exception as e:
            session.rollback()
            print(f"❌ Failed to save previews to DB: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KBO Daily Preview Crawler")
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
        print(f"🔗 Using OCI Database for preview generation.")

    asyncio.run(run_preview_batch(target, custom_session=custom_session))