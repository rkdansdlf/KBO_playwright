"""Daily Preview Batch Script
Fetches pre-game context and persists both preview JSON and core pregame tables.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.preview_crawler import PreviewCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import save_pregame_lineups
from src.services.context_aggregator import ContextAggregator
from src.sync.oci_sync import OCISync
from src.utils.date_helpers import parse_date_str
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.team_codes import resolve_team_code

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

PREVIEW_CONTEXT_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError)


def _write_pregame_manifest(target_date: str, game_ids: list[str]) -> str:
    return write_refresh_manifest(
        phase="pregame",
        target_date=target_date,
        game_ids=game_ids,
        datasets=["game", "game_metadata", "game_lineups", "game_summary"],
    )


def _add_team_context(
    preview: dict[str, object],
    agg: ContextAggregator,
    season_year: int,
    target_dt_obj: datetime.date,
) -> None:
    game_id = preview.get("game_id")
    away_code = resolve_team_code(preview.get("away_team_name"), season_year)
    home_code = resolve_team_code(preview.get("home_team_name"), season_year)
    if not away_code or not home_code:
        return

    try:
        logger.info("📊 Aggregating pregame context for %s...", game_id)
        preview["matchup_h2h"] = agg.get_head_to_head_summary(
            away_code,
            home_code,
            season_year,
            target_dt_obj,
        )
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
    except PREVIEW_CONTEXT_EXCEPTIONS:
        logger.exception("⚠️ Context aggregation failed for %s", game_id)


def _add_pitcher_context(preview: dict[str, object], agg: ContextAggregator, season_year: int) -> None:
    game_id = preview.get("game_id")
    try:
        away_starter_id = preview.get("away_starter_id")
        home_starter_id = preview.get("home_starter_id")
        if away_starter_id:
            preview["away_starter_stats"] = agg.get_pitcher_season_stats(away_starter_id, season_year)
        if home_starter_id:
            preview["home_starter_stats"] = agg.get_pitcher_season_stats(home_starter_id, season_year)
    except PREVIEW_CONTEXT_EXCEPTIONS:
        logger.exception("⚠️ Pitcher stats aggregation failed for %s", game_id)


def _save_preview_contexts(previews: list[dict[str, object]], target_date: str) -> list[str]:
    saved_ids: list[str] = []
    target_dt_obj = parse_date_str(target_date)
    season_year = target_dt_obj.year

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        for preview in previews:
            game_id = preview.get("game_id")
            if not game_id:
                continue
            _add_team_context(preview, agg, season_year, target_dt_obj)
            _add_pitcher_context(preview, agg, season_year)
            if save_pregame_lineups(preview):
                saved_ids.append(str(game_id))
    return saved_ids


def _sync_saved_pregame_games(saved_ids: list[str]) -> None:
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        return
    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            logger.info("🛡️ Syncing pregame games and referenced players...")
            for game_id in sorted(set(saved_ids)):
                syncer.sync_pregame_game(game_id)
        finally:
            syncer.close()


async def run_preview_batch(target_date: str, *, sync_to_oci: bool | None = None) -> list[str]:
    logger.info("🚀 Starting Preview Data Batch for %s...", target_date)

    crawler = PreviewCrawler(request_delay=1.0)
    previews = await crawler.crawl_preview_for_date(target_date)
    if not previews:
        manifest_path = _write_pregame_manifest(target_date, [])
        logger.info("ℹ️ No preview data found. manifest=%s", manifest_path)
        return []

    saved_ids = _save_preview_contexts(previews, target_date)
    should_sync = sync_to_oci if sync_to_oci is not None else bool(os.getenv("OCI_DB_URL"))
    if should_sync and saved_ids:
        _sync_saved_pregame_games(saved_ids)

    manifest_path = _write_pregame_manifest(target_date, saved_ids)
    logger.info("✅ Pregame batch finished. saved=%s manifest=%s", len(saved_ids), manifest_path)
    return saved_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Daily Preview Crawler")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    target = args.date or datetime.now(KST).strftime("%Y%m%d")
    asyncio.run(run_preview_batch(target, sync_to_oci=not args.no_sync))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
