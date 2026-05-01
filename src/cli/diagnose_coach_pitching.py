"""Diagnose completed-game Coach pitcher data flow.

Shows, per game, where starter and bullpen data is present or missing across:
1. raw crawl tables (game_pitching_stats),
2. repository payload construction,
3. final postgame Coach review JSON (game_summary / 리뷰_WPA).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Sequence

from src.db.engine import SessionLocal
from src.models.game import Game
from src.services.context_aggregator import ContextAggregator
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


def _game_ids_for_date(session, target_date: str) -> list[str]:
    target = datetime.strptime(target_date, "%Y%m%d").date()
    return [
        row[0]
        for row in session.query(Game.game_id)
        .filter(
            Game.game_date == target,
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
        )
        .order_by(Game.game_id.asc())
        .all()
    ]


def _print_text_report(rows: list[dict]) -> None:
    if not rows:
        print("No completed games matched.")
        return

    for row in rows:
        raw = row["raw_tables"]
        repo = row["repository"]
        final = row["final_payload"]
        print(f"{row['game_id']}: {row['drop_stage']}")
        print(
            "  raw: "
            f"pitching={raw['game_pitching_rows']} "
            f"starters={raw['starter_rows']} "
            f"bullpen={raw['bullpen_rows']} "
            f"missing_player_ids={raw['player_id_missing_rows']}"
        )
        print(
            "  repository: "
            f"starters={repo['starter_rows']} "
            f"bullpen={repo['bullpen_rows']} "
            f"season_matches={repo['season_pitching_matches']} "
            f"unmatched={len(repo['unmatched_season_stats'])}"
        )
        print(
            "  final_payload: "
            f"review={final['review_summary_found']} "
            f"summary_rows={final['review_summary_rows']} "
            f"pitching_breakdown={final['pitching_breakdown_found']} "
            f"starters={final['starter_rows']} "
            f"bullpen={final['bullpen_rows']}"
        )
        if row.get("warnings"):
            print(f"  warnings: {', '.join(row['warnings'])}")
        if repo["unmatched_season_stats"]:
            examples = ", ".join(
                f"{item.get('player_name')}({item.get('player_id')})"
                for item in repo["unmatched_season_stats"][:5]
            )
            print(f"  unmatched_examples: {examples}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Trace completed-game Coach starter/bullpen data by game."
    )
    parser.add_argument("--date", help="Completed game date to inspect (YYYYMMDD)")
    parser.add_argument(
        "--game-id",
        action="append",
        dest="game_ids",
        help="Specific game ID to inspect. Repeatable.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    args = parser.parse_args(argv)

    if not args.date and not args.game_ids:
        parser.error("provide --date or at least one --game-id")

    with SessionLocal() as session:
        game_ids = list(args.game_ids or [])
        if args.date:
            game_ids.extend(_game_ids_for_date(session, args.date))
        game_ids = list(dict.fromkeys(game_ids))

        agg = ContextAggregator(session)
        rows = [agg.diagnose_completed_game_coach_pitching(game_id) for game_id in game_ids]

    if args.json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        _print_text_report(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
