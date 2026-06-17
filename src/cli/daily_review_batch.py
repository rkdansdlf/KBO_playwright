"""
Daily Review Batch Script
Generates post-game review context from game_events/WPA and persists it locally.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GameSummary, GameValidationMetrics
from src.repositories.game_repository import refresh_game_status_for_date
from src.services.context_aggregator import ContextAggregator
from src.sync.oci_sync import OCISync
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.team_codes import team_code_from_game_id_segment

logger = logging.getLogger(__name__)

REVIEW_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


REVIEW_SUMMARY_TYPE = "리뷰_WPA"
TRUSTED_RELAY_STATUSES = {"verified", "recovered"}


def _upsert_review_summary(session, game_id: str, review_json: str) -> None:
    existing_summaries = (
        session.query(GameSummary)
        .filter(
            GameSummary.game_id == game_id,
            GameSummary.summary_type == REVIEW_SUMMARY_TYPE,
        )
        .all()
    )
    if existing_summaries:
        for summary in existing_summaries:
            summary.detail_text = review_json
        return

    session.add(
        GameSummary(
            game_id=game_id,
            summary_type=REVIEW_SUMMARY_TYPE,
            detail_text=review_json,
        ),
    )


def _build_review_data(agg: ContextAggregator, game: Game) -> dict[str, Any]:
    target_date = game.game_date.strftime("%Y%m%d")
    season_year = game.game_date.year
    away_code = team_code_from_game_id_segment(game.away_team, season_year)
    home_code = team_code_from_game_id_segment(game.home_team, season_year)

    review_data = {
        "game_id": game.game_id,
        "game_date": target_date,
        "final_score": f"{game.away_team} {game.away_score} : {game.home_score} {game.home_team}",
        "crucial_moments": agg.get_crucial_moments(game.game_id, limit=5),
        "pitching_breakdown": agg.get_completed_game_pitching_breakdown(
            game.game_id,
            season_year=season_year,
        ),
    }
    if away_code and home_code:
        review_data["away_movements"] = agg.get_recent_player_movements(away_code, game.game_date)
        review_data["home_movements"] = agg.get_recent_player_movements(home_code, game.game_date)
        review_data["away_roster_changes"] = agg.get_daily_roster_changes(away_code, game.game_date)
        review_data["home_roster_changes"] = agg.get_daily_roster_changes(home_code, game.game_date)

    return review_data


def _trusted_relay_game_ids(session, game_ids: Sequence[str]) -> set[str]:
    if not game_ids:
        return set()
    target_ids = {str(game_id) for game_id in game_ids}
    metric_rows = (
        session.query(GameValidationMetrics.game_id, GameValidationMetrics.validation_status)
        .filter(GameValidationMetrics.game_id.in_(list(target_ids)))
        .all()
    )
    statuses = {str(row.game_id): row.validation_status for row in metric_rows}
    trusted_ids = {game_id for game_id, status in statuses.items() if status in TRUSTED_RELAY_STATUSES}

    missing_metric_ids = target_ids - set(statuses)
    if missing_metric_ids:
        wpa_rows = (
            session.query(GameEvent.game_id)
            .filter(GameEvent.game_id.in_(list(missing_metric_ids)), GameEvent.wpa.isnot(None))
            .distinct()
            .all()
        )
        trusted_ids.update(str(row[0]) for row in wpa_rows)

    return trusted_ids


async def run_review_batch(target_date: str, *, sync_to_oci: bool | None = None) -> list[str]:
    logger.info("🚀 Starting Post-game Review Data Batch for %s...", target_date)

    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()
    status_result = refresh_game_status_for_date(target_date)
    if status_result.get("updated", 0):
        logger.info(
            "🔄 Refreshed game statuses before review: updated=%s counts=%s",
            status_result.get("updated", 0),
            status_result.get("status_counts", {}),
        )
    saved_ids: list[str] = []

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        games = (
            session.query(Game)
            .filter(
                Game.game_date == target_dt_obj,
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            )
            .all()
        )

        if not games:
            manifest_path = write_refresh_manifest(
                phase="postgame_review",
                target_date=target_date,
                game_ids=[],
                datasets=[
                    "game",
                    "game_events",
                    "game_pitching_stats",
                    "player_season_pitching",
                    "game_summary",
                ],
            )
            logger.info("ℹ️ No completed games found for %s. manifest=%s", target_date, manifest_path)
            return []

        trusted_game_ids = _trusted_relay_game_ids(session, [game.game_id for game in games])

        for game in games:
            game_id = game.game_id
            if game_id not in trusted_game_ids:
                logger.warning("  ⚠️ Skipping review for %s: relay validation is not trusted", game_id)
                continue

            logger.info("📊 Generating review context for %s...", game_id)
            review_data = _build_review_data(agg, game)

            if not review_data["crucial_moments"]:
                logger.info(
                    "  ⚠️ No WPA-backed game_events found for %s. Raw event crawl may be missing or incomplete.",
                    game_id,
                )

            review_json = json.dumps(review_data, ensure_ascii=False)
            _upsert_review_summary(session, game_id, review_json)
            saved_ids.append(game_id)

        try:
            session.commit()
        except REVIEW_DB_EXCEPTIONS:
            session.rollback()
            logger.exception("❌ Failed to save reviews to DB")
            raise

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
        phase="postgame_review",
        target_date=target_date,
        game_ids=saved_ids,
        datasets=[
            "game",
            "game_events",
            "game_pitching_stats",
            "player_season_pitching",
            "game_summary",
        ],
    )
    logger.info("✅ Review batch finished. saved=%s manifest=%s", len(saved_ids), manifest_path)
    return saved_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Daily Review Context Generator")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD). Defaults to today.", default=None)
    parser.add_argument("--no-sync", action="store_true", help="Skip explicit OCI sync after local writes")
    args = parser.parse_args(argv)

    target = args.date if args.date else datetime.now().strftime("%Y%m%d")
    asyncio.run(run_review_batch(target, sync_to_oci=not args.no_sync))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
