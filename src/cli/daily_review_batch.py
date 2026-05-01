"""
Daily Review Batch Script
Generates post-game review context from game_events/WPA and persists it locally.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from typing import List, Sequence

from src.db.engine import SessionLocal
from src.models.game import Game, GameSummary
from src.services.context_aggregator import ContextAggregator
from src.sync.oci_sync import OCISync
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.refresh_manifest import write_refresh_manifest
from src.utils.safe_print import safe_print as print
from src.utils.team_codes import team_code_from_game_id_segment


REVIEW_SUMMARY_TYPE = "리뷰_WPA"


def _upsert_review_summary(session, game_id: str, review_json: str) -> None:
    existing_summaries = session.query(GameSummary).filter(
        GameSummary.game_id == game_id,
        GameSummary.summary_type == REVIEW_SUMMARY_TYPE,
    ).all()
    if existing_summaries:
        for summary in existing_summaries:
            summary.detail_text = review_json
        return

    session.add(
        GameSummary(
            game_id=game_id,
            summary_type=REVIEW_SUMMARY_TYPE,
            detail_text=review_json,
        )
    )


def _build_review_data(agg: ContextAggregator, game: Game) -> dict:
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


async def run_review_batch(target_date: str, *, sync_to_oci: bool | None = None) -> List[str]:
    print(f"🚀 Starting Post-game Review Data Batch for {target_date}...")

    target_dt_obj = datetime.strptime(target_date, "%Y%m%d").date()
    saved_ids: List[str] = []

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        games = session.query(Game).filter(
            Game.game_date == target_dt_obj,
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
        ).all()

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
            print(f"ℹ️ No completed games found for {target_date}. manifest={manifest_path}")
            return []

        for game in games:
            game_id = game.game_id

            print(f"📊 Generating review context for {game_id}...")
            review_data = _build_review_data(agg, game)

            if not review_data["crucial_moments"]:
                print(
                    f"  ⚠️ No WPA-backed game_events found for {game_id}. "
                    "Raw event crawl may be missing or incomplete."
                )

            review_json = json.dumps(review_data, ensure_ascii=False)
            _upsert_review_summary(session, game_id, review_json)
            saved_ids.append(game_id)

        try:
            session.commit()
        except Exception as exc:
            session.rollback()
            print(f"❌ Failed to save reviews to DB: {exc}")
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
    print(f"✅ Review batch finished. saved={len(saved_ids)} manifest={manifest_path}")
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
