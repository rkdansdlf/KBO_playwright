"""
KBO Pipeline Auto-Healer

Detects past games stuck in SCHEDULED state (no scores) and attempts
to self-correct by re-crawling from the KBO GameCenter.

Resolution logic per game:
  - crawl_result=data   → save_game_detail() → COMPLETED
  - failure_reason=cancelled → update status → CANCELLED
  - failure_reason=missing   → update status → UNRESOLVED_MISSING
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Sequence

from sqlalchemy import select

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.models.game import Game
from src.services.player_id_resolver import PlayerIdResolver
from src.repositories.game_repository import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    save_game_detail,
    update_game_status,
)
from src.utils.alerting import SlackWebhookClient
from src.utils.safe_print import safe_print as print


def _find_stuck_games() -> List[Game]:
    """Return all past games whose status is still SCHEDULED (no scores yet)."""
    yesterday = (datetime.now().date() - timedelta(days=1))
    with SessionLocal() as session:
        stmt = select(Game).where(
            Game.game_status == GAME_STATUS_SCHEDULED,
            Game.game_date <= yesterday,
        )
        return session.execute(stmt).scalars().all()


async def _heal_game(
    crawler: GameDetailCrawler,
    game_id: str,
    game_date: str,
    dry_run: bool,
) -> str:
    """
    Attempt to heal a single stuck game.

    Returns one of: 'completed', 'cancelled', 'unresolved', 'dry_run'
    """
    if dry_run:
        print(f"  [DRY-RUN] Would re-crawl {game_id}")
        return "dry_run"

    detail = await crawler.crawl_game(game_id, game_date)
    if detail is not None:
        # Box score found → persist and set COMPLETED
        if save_game_detail(detail):
            print(f"  ✅ {game_id} → COMPLETED (score saved)")
            return "completed"
        else:
            print(f"  ⚠️  {game_id} → detail found but DB save failed")
            return "unresolved"

    failure_reason = crawler.get_last_failure_reason(game_id)
    if failure_reason == "cancelled":
        update_game_status(game_id, GAME_STATUS_CANCELLED)
        print(f"  🚫 {game_id} → CANCELLED")
        return "cancelled"

    # Timeout / missing / unknown failure
    update_game_status(game_id, GAME_STATUS_UNRESOLVED)
    print(f"  ❓ {game_id} → UNRESOLVED_MISSING (reason={failure_reason})")
    return "unresolved"


async def run_healer_async(dry_run: bool = False) -> int:
    print("\n🩺 Running KBO Pipeline Auto-Healer...")

    stuck_games = _find_stuck_games()

    if not stuck_games:
        print("✅ No anomalies detected. Pipeline is healthy.")
        return 0

    total = len(stuck_games)
    anomaly_dates = sorted({g.game_date for g in stuck_games})
    print(f"⚠️  Anomaly Detected: {total} past game(s) stuck in SCHEDULED state!")
    for d in anomaly_dates:
        print(f"  - {d}")

    # Slack alert
    if not dry_run:
        date_range = (
            f"`{anomaly_dates[0]}`"
            if len(anomaly_dates) == 1
            else f"`{anomaly_dates[0]}` ~ `{anomaly_dates[-1]}`"
        )
        SlackWebhookClient.send_alert(
            f"Pipeline Anomaly: {total} stuck game(s) detected. Auto-healing started.",
            blocks=[
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "⚠️ KBO Pipeline Anomaly"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"Found *{total}* past game(s) stuck in `SCHEDULED` status.\n"
                            f"Affected dates: {date_range}\n\n"
                            "*Auto-Healing initiated.*"
                        ),
                    },
                },
            ],
        )

    print(f"\n🚀 Initiating self-recovery for {total} game(s)...")
    
    with SessionLocal() as db_session:
        resolver = PlayerIdResolver(db_session)
        # Preload for potentially multiple years found in anomalies
        years = {d.year for d in anomaly_dates}
        for y in years:
            resolver.preload_season_index(y)
            
        crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)

        results = {"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 0}
        for game in stuck_games:
            game_date_str = game.game_date.strftime("%Y%m%d")
            outcome = await _heal_game(crawler, game.game_id, game_date_str, dry_run)
            results[outcome] = results.get(outcome, 0) + 1

    # Summary
    print("\n📊 Auto-Healer Summary:")
    for outcome, count in results.items():
        if count:
            print(f"  {outcome}: {count}")

    unresolved_count = results.get("unresolved", 0)
    if not dry_run:
        if unresolved_count == 0:
            SlackWebhookClient.send_alert(
                f"✅ Auto-healing complete. "
                f"completed={results['completed']}, cancelled={results['cancelled']}."
            )
        else:
            SlackWebhookClient.send_alert(
                f"⚠️ Auto-healing finished with {unresolved_count} unresolved game(s). "
                "Manual intervention may be needed."
            )

    return 0 if unresolved_count == 0 else 1


def run_healer(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="KBO Data Auto-Healer daemon")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report anomalies without fixing",
    )
    args = parser.parse_args(argv)
    return asyncio.run(run_healer_async(dry_run=args.dry_run))


if __name__ == "__main__":
    import sys
    sys.exit(run_healer())
