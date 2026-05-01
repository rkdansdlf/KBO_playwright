"""Freshness gate for operational KBO game data."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Sequence

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from src.db.engine import SessionLocal
from src.models.game import (
    Game,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.relay_text import is_relay_noise_text


KBO_FRESHNESS_TEAM_CODES = {
    "DB",
    "HH",
    "HT",
    "KIA",
    "KH",
    "KT",
    "LG",
    "LT",
    "NC",
    "OB",
    "SK",
    "SS",
    "SSG",
    "WO",
    "EA",
    "WE",
}


def collect_freshness_issues(
    session: Session,
    *,
    target_date: str | None = None,
    days: int | None = None,
) -> Dict[str, List[str]]:
    query = session.query(Game).filter(
        Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
        Game.away_team.in_(KBO_FRESHNESS_TEAM_CODES),
        Game.home_team.in_(KBO_FRESHNESS_TEAM_CODES),
        ~session.query(GameIdAlias.alias_game_id)
        .filter(GameIdAlias.alias_game_id == Game.game_id)
        .exists(),
    )

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
        "missing_starting_pitchers": [],
        "missing_pitching_stats": [],
        "missing_pitching_starters": [],
        "missing_review_wpa": [],
        "missing_review_moments": [],
        "review_moment_noise": [],
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

        if not (game.away_pitcher and str(game.away_pitcher).strip()) or not (
            game.home_pitcher and str(game.home_pitcher).strip()
        ):
            issues["missing_starting_pitchers"].append(game.game_id)

        pitching_rows = session.query(GamePitchingStat).filter(GamePitchingStat.game_id == game.game_id).all()
        if not pitching_rows:
            issues["missing_pitching_stats"].append(game.game_id)
        else:
            starter_sides = {
                row.team_side
                for row in pitching_rows
                if row.is_starting and row.team_side in {"away", "home"}
            }
            if starter_sides != {"away", "home"}:
                issues["missing_pitching_starters"].append(game.game_id)

        review = (
            session.query(GameSummary)
            .filter(
                GameSummary.game_id == game.game_id,
                GameSummary.summary_type == "리뷰_WPA",
            )
            .first()
        )
        if review is None:
            issues["missing_review_wpa"].append(game.game_id)
        elif not _has_review_moments(review.detail_text):
            issues["missing_review_moments"].append(game.game_id)
        elif _review_moments_have_noise(review.detail_text):
            issues["review_moment_noise"].append(game.game_id)

    return issues


def _has_review_moments(detail_text: str | None) -> bool:
    if not detail_text:
        return False
    try:
        payload = json.loads(detail_text)
    except (TypeError, json.JSONDecodeError):
        return False
    moments = payload.get("crucial_moments")
    return isinstance(moments, list) and len(moments) > 0


def _review_moments_have_noise(detail_text: str | None) -> bool:
    if not detail_text:
        return False
    try:
        payload = json.loads(detail_text)
    except (TypeError, json.JSONDecodeError):
        return False
    moments = payload.get("crucial_moments")
    if not isinstance(moments, list):
        return False
    for moment in moments:
        if isinstance(moment, dict) and is_relay_noise_text(moment.get("description")):
            return True
    return False


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
    parser.add_argument("--source-url", type=str, help="Optional database URL to validate instead of local DATABASE_URL")
    parser.add_argument(
        "--source-url-env",
        type=str,
        help="Environment variable containing the database URL to validate instead of local DATABASE_URL",
    )
    parser.add_argument("--json", action="store_true", help="Print issues as JSON")
    args = parser.parse_args(argv)

    engine = None
    session_factory = SessionLocal
    source_url = args.source_url
    if args.source_url_env:
        source_url = os.getenv(args.source_url_env)
        if not source_url:
            raise SystemExit(f"{args.source_url_env} is not set")
    if source_url:
        engine = create_engine(source_url, pool_pre_ping=True)
        session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    try:
        with session_factory() as session:
            issues = collect_freshness_issues(session, target_date=args.date, days=args.days)
            failures = evaluate_freshness_gate(session, target_date=args.date, days=args.days)
    finally:
        if engine is not None:
            engine.dispose()

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
