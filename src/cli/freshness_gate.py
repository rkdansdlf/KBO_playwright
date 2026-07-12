"""Freshness gate for operational KBO game data."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.orm import Query, Session, sessionmaker

from src.constants import KST
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
from src.utils.alerting import SlackWebhookClient
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES, GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED
from src.utils.relay_text import is_relay_noise_text

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
FRESHNESS_ALERT_FAILURE_LIMIT = 20

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


def _freshness_base_query(session: Session) -> Query:
    return session.query(
        Game.game_id,
        Game.game_date,
        Game.away_score,
        Game.home_score,
        Game.away_pitcher,
        Game.home_pitcher,
    ).filter(
        Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
        Game.away_team.in_(KBO_FRESHNESS_TEAM_CODES),
        Game.home_team.in_(KBO_FRESHNESS_TEAM_CODES),
        ~session.query(GameIdAlias.alias_game_id).filter(GameIdAlias.alias_game_id == Game.game_id).exists(),
    )


def _apply_freshness_date_filter(
    query: Query,
    target_date: str | None,
    days: int | None,
    max_hours: int | None = None,
) -> Query:
    if target_date:
        return query.filter(Game.game_date == parse_date_str(target_date))
    if max_hours:
        return query.filter(Game.game_date >= (datetime.now(KST) - timedelta(hours=max_hours)).date())
    if days:
        return query.filter(Game.game_date >= datetime.now(KST).date() - timedelta(days=days))
    return query


def _empty_issue_map() -> dict[str, list[str]]:
    return {
        "missing_start_time": [],
        "missing_scores": [],
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
        "past_scheduled_games": [],
    }


def _scheduled_base_query(session: Session) -> Query:
    return session.query(Game.game_id, Game.game_date).filter(
        Game.game_status.in_((GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED)),
        Game.away_team.in_(KBO_FRESHNESS_TEAM_CODES),
        Game.home_team.in_(KBO_FRESHNESS_TEAM_CODES),
        ~session.query(GameIdAlias.alias_game_id).filter(GameIdAlias.alias_game_id == Game.game_id).exists(),
    )


def _check_past_scheduled_games(
    session: Session,
    *,
    target_date: str | None,
    days: int | None,
    max_hours: int | None,
    issues: dict[str, list[str]],
) -> None:
    query = _apply_freshness_date_filter(_scheduled_base_query(session), target_date, days, max_hours)
    yesterday = datetime.now(KST).date() - timedelta(days=1)
    rows = query.filter(Game.game_date <= yesterday).order_by(Game.game_date, Game.game_id).all()
    issues["past_scheduled_games"].extend(row.game_id for row in rows)


def _check_metadata_start_time(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    metadata = session.query(GameMetadata.start_time).filter(GameMetadata.game_id == game.game_id).one_or_none()  # type: ignore[attr-defined]
    if metadata is None or metadata.start_time is None:
        issues["missing_start_time"].append(game.game_id)  # type: ignore[attr-defined]


def _check_scores(game: object, issues: dict[str, list[str]]) -> None:
    if game.away_score is None or game.home_score is None:  # type: ignore[attr-defined]
        issues["missing_scores"].append(game.game_id)  # type: ignore[attr-defined]


def _check_lineups(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    if session.query(GameLineup).filter(GameLineup.game_id == game.game_id).count() == 0:  # type: ignore[attr-defined]
        issues["missing_lineups"].append(game.game_id)  # type: ignore[attr-defined]


def _check_inning_scores(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    inning_rows = (
        session.query(GameInningScore.team_side, GameInningScore.runs)
        .filter(GameInningScore.game_id == game.game_id)  # type: ignore[attr-defined]
        .all()
    )
    if not inning_rows:
        issues["missing_inning_scores"].append(game.game_id)  # type: ignore[attr-defined]
        return
    away_sum = sum((row.runs or 0) for row in inning_rows if row.team_side == "away")
    home_sum = sum((row.runs or 0) for row in inning_rows if row.team_side == "home")
    if (game.away_score is not None and away_sum != game.away_score) or (  # type: ignore[attr-defined]
        game.home_score is not None and home_sum != game.home_score  # type: ignore[attr-defined]
    ):
        issues["inning_score_mismatch"].append(game.game_id)  # type: ignore[attr-defined]


def _check_events_wpa(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    event_count = session.query(GameEvent.id).filter(GameEvent.game_id == game.game_id).count()  # type: ignore[attr-defined]
    if not event_count:
        issues["missing_events"].append(game.game_id)  # type: ignore[attr-defined]
    elif (
        session.query(GameEvent.id).filter(GameEvent.game_id == game.game_id, GameEvent.wpa.isnot(None)).first() is None  # type: ignore[attr-defined]
    ):
        issues["missing_wpa"].append(game.game_id)  # type: ignore[attr-defined]


def _check_starting_pitchers(game: object, issues: dict[str, list[str]]) -> None:
    if not (game.away_pitcher and str(game.away_pitcher).strip()) or not (  # type: ignore[attr-defined]
        game.home_pitcher and str(game.home_pitcher).strip()  # type: ignore[attr-defined]
    ):
        issues["missing_starting_pitchers"].append(game.game_id)  # type: ignore[attr-defined]


def _check_pitching_stats(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    pitching_rows = (
        session.query(GamePitchingStat.team_side, GamePitchingStat.is_starting)
        .filter(GamePitchingStat.game_id == game.game_id)  # type: ignore[attr-defined]
        .all()
    )
    if not pitching_rows:
        issues["missing_pitching_stats"].append(game.game_id)  # type: ignore[attr-defined]
        return
    starter_sides = {row.team_side for row in pitching_rows if row.is_starting and row.team_side in {"away", "home"}}
    if starter_sides != {"away", "home"}:
        issues["missing_pitching_starters"].append(game.game_id)  # type: ignore[attr-defined]


def _check_review_summary(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    review = (
        session.query(GameSummary.detail_text)
        .filter(GameSummary.game_id == game.game_id, GameSummary.summary_type == "리뷰_WPA")  # type: ignore[attr-defined]
        .first()
    )
    if review is None:
        issues["missing_review_wpa"].append(game.game_id)  # type: ignore[attr-defined]
    elif not _has_review_moments(review.detail_text):
        issues["missing_review_moments"].append(game.game_id)  # type: ignore[attr-defined]
    elif _review_moments_have_noise(review.detail_text):
        issues["review_moment_noise"].append(game.game_id)  # type: ignore[attr-defined]


def _check_freshness_game(session: Session, game: object, issues: dict[str, list[str]]) -> None:
    _check_metadata_start_time(session, game, issues)
    _check_scores(game, issues)
    _check_lineups(session, game, issues)
    _check_inning_scores(session, game, issues)
    _check_events_wpa(session, game, issues)
    _check_starting_pitchers(game, issues)
    _check_pitching_stats(session, game, issues)
    _check_review_summary(session, game, issues)


def collect_freshness_issues(
    session: Session,
    *,
    target_date: str | None = None,
    days: int | None = None,
    max_hours: int | None = None,
) -> dict[str, list[str]]:
    """Handle the collect freshness issues operation.

    Args:
        session: Session.
        target_date: Target date for the operation.
        days: Days.
        max_hours: Max Hours.
        session: Session.

    Returns:
        Dictionary result.

    """
    query = _apply_freshness_date_filter(_freshness_base_query(session), target_date, days, max_hours)

    games = query.order_by(Game.game_date, Game.game_id).all()
    issues = _empty_issue_map()

    for game in games:
        _check_freshness_game(session, game, issues)

    _check_past_scheduled_games(
        session,
        target_date=target_date,
        days=days,
        max_hours=max_hours,
        issues=issues,
    )

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
    return any(isinstance(moment, dict) and is_relay_noise_text(moment.get("description")) for moment in moments)


def evaluate_freshness_gate(
    session: Session,
    *,
    target_date: str | None = None,
    days: int | None = None,
    max_hours: int | None = None,
) -> list[str]:
    """Handle the evaluate freshness gate operation.

    Args:
        session: Session.
        target_date: Target date for the operation.
        days: Days.
        max_hours: Max Hours.
        session: Session.

    Returns:
        List of results.

    """
    issues = collect_freshness_issues(session, target_date=target_date, days=days, max_hours=max_hours)

    failures: list[str] = []
    for key, game_ids in issues.items():
        if game_ids:
            failures.append(f"{key}: {len(game_ids)} game(s) -> {', '.join(sorted(game_ids))}")
    return failures


def _send_freshness_alert(failures: list[str]) -> None:
    header = "<b>\u2757 KBO Freshness Gate Failed</b>"
    body = "\n".join(f"\u2022 {f}" for f in failures[:20])
    if len(failures) > FRESHNESS_ALERT_FAILURE_LIMIT:
        body += f"\n... and {len(failures) - FRESHNESS_ALERT_FAILURE_LIMIT} more failures"
    message = f"{header}\n\n{body}"
    SlackWebhookClient.send_alert(message)


def _log_sla_metrics(session: Session, issues: dict[str, list[str]]) -> None:
    import zoneinfo
    from datetime import date, time
    from typing import Any, cast

    from sqlalchemy.exc import SQLAlchemyError

    from src.models.game import GameMetadata
    from src.models.sla_metrics import SlaMetrics

    kst_tz = zoneinfo.ZoneInfo("Asia/Seoul")
    now = datetime.now(kst_tz)

    categories: dict[str, dict[str, Any]] = {
        "game": {
            "issues": [
                "missing_scores",
                "missing_starting_pitchers",
                "missing_pitching_stats",
                "missing_pitching_starters",
            ],
            "threshold": 3,
        },
        "relay": {
            "issues": ["missing_events", "missing_lineups", "missing_inning_scores"],
            "threshold": 1,
        },
        "analysis": {
            "issues": ["missing_review_wpa", "missing_review_moments", "inning_score_mismatch"],
            "threshold": 12,
        },
    }

    for cat_name, cfg in categories.items():
        threshold = cast("int", cfg["threshold"])
        cfg_issues = cast("list[str]", cfg["issues"])
        cat_game_ids = set()
        for issue_key in cfg_issues:
            cat_game_ids.update(issues.get(issue_key, []))

        for game_id in cat_game_ids:
            try:
                game_date_str = game_id.split("_")[0]
                game_year = int(game_date_str[:4])
                game_month = int(game_date_str[4:6])
                game_day = int(game_date_str[6:8])
                end_time_val: Any = time(22, 0)

                meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).first()
                if meta and meta.end_time:
                    end_time_val = meta.end_time

                end_time_kst = datetime.combine(
                    date(game_year, game_month, game_day),
                    end_time_val,
                    tzinfo=kst_tz,
                )
                delay_hours = max(0.0, (now - end_time_kst).total_seconds() / 3600.0)
            except (ValueError, TypeError, KeyError, AttributeError, SQLAlchemyError):
                delay_hours = 0.0

            is_violation = delay_hours > threshold

            metric = SlaMetrics(
                check_time=now,
                category=cat_name,
                sla_threshold_hours=threshold,
                actual_delay_hours=round(delay_hours, 2),
                is_violation=is_violation,
                notes=f"Game {game_id} has issues: "
                + ", ".join([k for k in cfg_issues if game_id in issues.get(k, [])]),
            )
            session.add(metric)

    try:
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.warning("Failed to commit SLA metrics: %s", e)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Validate operational freshness requirements for completed games")
    parser.add_argument("--date", type=str, help="Target date in YYYYMMDD format")
    parser.add_argument("--days", type=int, help="Validate completed games from the last N days")
    parser.add_argument("--max-hours", type=int, help="Validate completed games from the last N hours")
    parser.add_argument(
        "--source-url",
        type=str,
        help="Optional database URL to validate instead of local DATABASE_URL",
    )
    parser.add_argument(
        "--source-url-env",
        type=str,
        help="Environment variable containing the database URL to validate instead of local DATABASE_URL",
    )
    parser.add_argument("--json", action="store_true", help="Print issues as JSON")
    parser.add_argument("--alert", action="store_true", help="Send alert (Telegram/Slack) on failures")
    args = parser.parse_args(argv)

    engine = None
    session_factory = SessionLocal
    source_url = args.source_url
    if args.source_url_env:
        source_url = os.getenv(args.source_url_env)
        if not source_url:
            msg = f"{args.source_url_env} is not set"
            raise SystemExit(msg)
    if source_url:
        engine = create_engine(source_url, pool_pre_ping=True)
        session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    try:
        with session_factory() as session:
            issues = collect_freshness_issues(session, target_date=args.date, days=args.days, max_hours=args.max_hours)
            failures = evaluate_freshness_gate(session, target_date=args.date, days=args.days, max_hours=args.max_hours)
            _log_sla_metrics(session, issues)
    finally:
        if engine is not None:
            engine.dispose()

    if args.json:
        logger.info(json.dumps({"ok": not failures, "issues": issues}, ensure_ascii=False, indent=2))
    elif failures:
        logger.error("❌ Freshness gate failed")
        for failure in failures:
            logger.info("  - %s", failure)
        if args.alert:
            _send_freshness_alert(failures)
    else:
        logger.info("✅ Freshness gate passed")

    return 1 if failures else 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
