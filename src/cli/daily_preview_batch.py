"""
Daily Preview Batch Script
Fetches pre-game context and persists both preview JSON and core pregame tables.
"""
from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime
from typing import List, Sequence

from src.crawlers.preview_crawler import PreviewCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import save_pregame_lineups
from src.services.context_aggregator import ContextAggregator
from src.sync.oci_sync import OCISync
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print
from src.utils.team_codes import resolve_team_code


async def run_preview_batch(target_date: str, *, sync_to_oci: bool | None = None) -> List[str]:
    print(f"🚀 Starting Preview Data Batch for {target_date}...")

    crawler = PreviewCrawler(request_delay=1.0)
    previews = await crawler.crawl_preview_for_date(target_date)
    if not previews:
        manifest_path = write_refresh_manifest(
            phase="pregame",
            target_date=target_date,
            game_ids=[],
            datasets=["game", "game_metadata", "game_lineups", "game_summary"],
        )
        print(f"ℹ️ No preview data found. manifest={manifest_path}")
        return []

    saved_ids: List[str] = []
    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()
    season_year = target_dt_obj.year

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        for preview in previews:
            game_id = preview.get("game_id")
            if not game_id:
                continue

            away_code = resolve_team_code(preview.get("away_team_name"), season_year)
            home_code = resolve_team_code(preview.get("home_team_name"), season_year)

            if away_code and home_code:
                print(f"📊 Aggregating pregame context for {game_id}...")
                preview["matchup_h2h"] = agg.get_head_to_head_summary(away_code, home_code, season_year, target_dt_obj)
                preview["away_recent_l10"] = agg.get_team_l10_summary(away_code, target_dt_obj)
                preview["home_recent_l10"] = agg.get_team_l10_summary(home_code, target_dt_obj)
                preview["away_metrics"] = agg.get_team_recent_metrics(away_code, target_dt_obj)
                preview["home_metrics"] = agg.get_team_recent_metrics(home_code, target_dt_obj)
                preview["away_movements"] = agg.get_recent_player_movements(away_code, target_dt_obj)
                preview["home_movements"] = agg.get_recent_player_movements(home_code, target_dt_obj)
                preview["away_roster_changes"] = agg.get_daily_roster_changes(away_code, target_dt_obj)
                preview["home_roster_changes"] = agg.get_daily_roster_changes(home_code, target_dt_obj)

                series_context = agg.get_postseason_series_summary(away_code, home_code, season_year, target_dt_obj)
                if series_context:
                    preview["series_context"] = series_context

            away_starter_id = preview.get("away_starter_id")
            home_starter_id = preview.get("home_starter_id")
            if away_starter_id:
                preview["away_starter_stats"] = agg.get_pitcher_season_stats(away_starter_id, season_year)
            if home_starter_id:
                preview["home_starter_stats"] = agg.get_pitcher_season_stats(home_starter_id, season_year)

            if save_pregame_lineups(preview):
                saved_ids.append(game_id)

    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    if should_sync and saved_ids:
        oci_url = os.getenv("OCI_DB_URL")
        if oci_url:
            with SessionLocal() as sync_session:
                syncer = OCISync(oci_url, sync_session)
                try:
                    for game_id in sorted(set(saved_ids)):
                        syncer.sync_specific_game(game_id)
                finally:
                    syncer.close()

    manifest_path = write_refresh_manifest(
        phase="pregame",
        target_date=target_date,
        game_ids=saved_ids,
        datasets=["game", "game_metadata", "game_lineups", "game_summary"],
    )
    print(f"✅ Pregame batch finished. saved={len(saved_ids)} manifest={manifest_path}")
    return saved_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Daily Preview Crawler")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    target = args.date if args.date else datetime.now().strftime("%Y%m%d")
    asyncio.run(run_preview_batch(target, sync_to_oci=not args.no_sync))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
