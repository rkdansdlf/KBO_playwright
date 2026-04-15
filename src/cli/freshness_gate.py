"""Freshness gate for operational KBO game data."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Sequence

from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GameInningScore, GameLineup, GameMetadata
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


def collect_freshness_issues(
    session: Session,
    *,
    target_date: str | None = None,
    days: int | None = None,
) -> Dict[str, List[str]]:
    query = session.query(Game).filter(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))

    if target_date:
        dt = datetime.strptime(target_date, "%Y%m%d").date()
        query = query.filter(Game.game_date == dt)
    elif days:
        since_date = datetime.now().date() - timedelta(days=days)
        query = query.filter(Game.game_date >= since_date)

    games = query.order_by(Game.game_date, Game.game_id).all()
    issues: Dict[str, List[str]] = {
        "missing_start_time": [],
        "missing_lineups": [],
        "missing_inning_scores": [],
        "missing_events": [],
        "missing_wpa": [],
        "inning_score_mismatch": [],
    }

    for game in games:
        metadata = session.query(GameMetadata).filter(GameMetadata.game_id == game.game_id).one_or_none()
        if metadata is None or metadata.start_time is None:
            issues["missing_start_time"].append(game.game_id)

        lineup_count = session.query(GameLineup).filter(GameLineup.game_id == game.game_id).count()
        if lineup_count == 0:
            issues["missing_lineups"].append(game.game_id)

        inning_rows = session.query(GameInningScore).filter(GameInningScore.game_id == game.game_id).all()
        if not inning_rows:
            issues["missing_inning_scores"].append(game.game_id)
        else:
            away_sum = sum((row.runs or 0) for row in inning_rows if row.team_side == "away")
            home_sum = sum((row.runs or 0) for row in inning_rows if row.team_side == "home")
            if game.away_score is not None and away_sum != game.away_score:
                issues["inning_score_mismatch"].append(game.game_id)
            elif game.home_score is not None and home_sum != game.home_score:
                issues["inning_score_mismatch"].append(game.game_id)

        events = session.query(GameEvent).filter(GameEvent.game_id == game.game_id).all()
        if not events:
            issues["missing_events"].append(game.game_id)
        elif not any(event.wpa is not None for event in events):
            issues["missing_wpa"].append(game.game_id)

    return issues


def evaluate_freshness_gate(
    session: Session,
    *,
    target_date: str | None = None,
    days: int | None = None,
) -> List[str]:
    issues = collect_freshness_issues(session, target_date=target_date, days=days)
    failures: List[str] = []
    for key, game_ids in issues.items():
        if game_ids:
            failures.append(f"{key}: {len(game_ids)} game(s) -> {', '.join(sorted(game_ids))}")
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate operational freshness requirements for completed games")
    parser.add_argument("--date", type=str, help="Target date in YYYYMMDD format")
    parser.add_argument("--days", type=int, help="Validate completed games from the last N days")
    parser.add_argument("--json", action="store_true", help="Print issues as JSON")
    args = parser.parse_args(argv)

    with SessionLocal() as session:
        issues = collect_freshness_issues(session, target_date=args.date, days=args.days)
        failures = evaluate_freshness_gate(session, target_date=args.date, days=args.days)

    if args.json:
        print(json.dumps({"ok": not failures, "issues": issues}, ensure_ascii=False, indent=2))
    else:
        if failures:
            print("❌ Freshness gate failed")
            for failure in failures:
                print(f"  - {failure}")
        else:
            print("✅ Freshness gate passed")

    return 1 if failures else 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
