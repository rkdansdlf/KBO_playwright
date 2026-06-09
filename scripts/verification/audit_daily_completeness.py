#!/usr/bin/env python3
"""
Audit script to verify that games have essential detail data.

By default, this keeps existing behavior:
  - checks only completed-like games in a rolling lookback window
  - verifies batting_stats / pitching_stats / play_by_play

For date-specific recovery validation, use:
  --date YYYYMMDD --include-incomplete --strict
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Sequence
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text

from src.db.engine import DATABASE_URL as _ENGINE_DB_URL
from src.utils.game_status import (
    COMPLETED_LIKE_GAME_STATUSES,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)

logger = logging.getLogger(__name__)
_REQUIRED_INNINGS = 9


def _parse_statuses(raw: str | None, include_incomplete: bool) -> list[str]:
    statuses: list[str] = []
    for token in (raw or "").split(","):
        token = token.strip().upper()
        if token:
            statuses.append(token)

    if not statuses:
        statuses = sorted(COMPLETED_LIKE_GAME_STATUSES)

    if include_incomplete:
        statuses.extend([GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED])

    return list(dict.fromkeys(statuses))


def _format_scope(
    target_date: date | None,
    lookback_days: int,
    statuses: Sequence[str],
    strict: bool,
) -> str:
    modes = "strict" if strict else "basic"
    status_text = ", ".join(statuses)
    if target_date:
        return f"date={target_date.isoformat()}, statuses=[{status_text}], mode={modes}"
    return f"last {lookback_days} days, statuses=[{status_text}], mode={modes}"


def _coerce_date(raw: str | None) -> date | None:
    if not raw:
        return None

    text = raw.strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()

    if len(text) == 10 and text.count("-") == 2:
        return datetime.strptime(text, "%Y-%m-%d").date()

    raise ValueError(f"Unsupported date format: {raw}. Use YYYYMMDD or YYYY-MM-DD.")


def _resolve_db_url(raw: str | None) -> str:
    if raw and raw.startswith("env:"):
        env_name = raw[4:]
        db_url = os.getenv(env_name)
        if not db_url:
            raise ValueError(f"Environment variable {env_name} is not set for --db-url.")
        return db_url

    if raw:
        return raw

    return os.getenv("DATABASE_URL") or _ENGINE_DB_URL


def audit_completeness(
    db_url: str,
    lookback_days: int,
    *,
    target_date: date | None = None,
    statuses: Sequence[str],
    strict: bool = False,
) -> int:
    engine = create_engine(db_url)

    today = date.today()
    if target_date:
        params = {"target_date": target_date.isoformat()}
        where_clause = "g.game_date = :target_date"
        logger.info("🔍 Auditing games for %s", _format_scope(target_date, lookback_days, statuses, strict))
    else:
        start_date = (today - timedelta(days=lookback_days)).isoformat()
        end_date = today.isoformat()
        params = {"start_date": start_date, "end_date": end_date}
        where_clause = "g.game_date >= :start_date AND g.game_date < :end_date"
        logger.info("🔍 Auditing games for %s", _format_scope(None, lookback_days, statuses, strict))

    if strict:
        template_query = """
            SELECT
                g.game_id,
                CAST(g.game_date AS TEXT) AS game_date,
                g.home_score,
                g.away_score,
                (SELECT COUNT(*) FROM game_metadata m WHERE m.game_id = g.game_id) AS metadata_cnt,
                (SELECT COUNT(*) FROM game_inning_scores i WHERE i.game_id = g.game_id AND i.team_side = 'away') AS inning_away_cnt,
                (SELECT COUNT(*) FROM game_inning_scores i WHERE i.game_id = g.game_id AND i.team_side = 'home') AS inning_home_cnt,
                (SELECT COUNT(*) FROM game_lineups l WHERE l.game_id = g.game_id AND l.team_side = 'away') AS lineup_away_cnt,
                (SELECT COUNT(*) FROM game_lineups l WHERE l.game_id = g.game_id AND l.team_side = 'home') AS lineup_home_cnt,
                (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id AND b.team_side = 'away') AS batting_away_cnt,
                (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id AND b.team_side = 'home') AS batting_home_cnt,
                (SELECT COUNT(*) FROM game_pitching_stats p WHERE p.game_id = g.game_id AND p.team_side = 'away') AS pitching_away_cnt,
                (SELECT COUNT(*) FROM game_pitching_stats p WHERE p.game_id = g.game_id AND p.team_side = 'home') AS pitching_home_cnt,
                (SELECT COUNT(*) FROM game_events e WHERE e.game_id = g.game_id) AS event_cnt,
                (SELECT COUNT(*) FROM game_play_by_play p WHERE p.game_id = g.game_id) AS pbp_cnt
            FROM game g
            WHERE {where_clause}
              AND g.game_status IN :status_list
            ORDER BY g.game_date DESC, g.game_id;
        """
    else:
        template_query = """
            SELECT
                CAST(g.game_date AS TEXT) AS game_date,
                g.game_id,
                (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id) AS hitter_cnt,
                (SELECT COUNT(*) FROM game_pitching_stats p WHERE p.game_id = g.game_id) AS pitcher_cnt,
                (SELECT COUNT(*) FROM game_play_by_play p WHERE p.game_id = g.game_id) AS relay_cnt
            FROM game g
            WHERE {where_clause}
              AND g.game_status IN :status_list
            ORDER BY g.game_date DESC, g.game_id;
        """

    query = text(template_query.format(where_clause=where_clause)).bindparams(
        bindparam("status_list", expanding=True),
    )

    failures: list[str] = []
    game_count = 0

    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"status_list": list(statuses), **params}).fetchall()
            game_count = len(rows)

            for row in rows:
                missing = []
                if strict:
                    (
                        g_id,
                        g_date,
                        home_score,
                        away_score,
                        metadata_cnt,
                        inning_away_cnt,
                        inning_home_cnt,
                        lineup_away_cnt,
                        lineup_home_cnt,
                        batting_away_cnt,
                        batting_home_cnt,
                        pitching_away_cnt,
                        pitching_home_cnt,
                        event_cnt,
                        pbp_cnt,
                    ) = row

                    if home_score is None or away_score is None:
                        missing.append("scores")
                    if metadata_cnt == 0:
                        missing.append("metadata")
                    if inning_away_cnt < _REQUIRED_INNINGS:
                        missing.append("away_inning_scores")
                    if inning_home_cnt < _REQUIRED_INNINGS:
                        missing.append("home_inning_scores")
                    if lineup_away_cnt == 0:
                        missing.append("away_lineups")
                    if lineup_home_cnt == 0:
                        missing.append("home_lineups")
                    if batting_away_cnt == 0:
                        missing.append("away_batting_stats")
                    if batting_home_cnt == 0:
                        missing.append("home_batting_stats")
                    if pitching_away_cnt == 0:
                        missing.append("away_pitching_stats")
                    if pitching_home_cnt == 0:
                        missing.append("home_pitching_stats")
                    if event_cnt == 0 and pbp_cnt == 0:
                        missing.append("relay_or_event")
                else:
                    g_date, g_id, h_cnt, p_cnt, r_cnt = row
                    if h_cnt == 0:
                        missing.append("batting_stats")
                    if p_cnt == 0:
                        missing.append("pitching_stats")
                    if r_cnt == 0:
                        missing.append("play_by_play")

                if missing:
                    failures.append(f"  - [{g_date}] {g_id}: missing {', '.join(missing)}")

    except Exception as e:
        logger.info("❌ Database error during audit: %s", e)
        return 2

    if failures:
        logger.info("❌ Found %s incomplete games out of %s checked:", len(failures), game_count)
        for f in failures:
            logger.info("%s", f)
        logger.info("\nPossible causes: crawler timeout, site structure change, or database connection issues.")
        logger.info("Action: run backfill for the missing game IDs.")
        return 1

    if game_count == 0:
        if target_date and target_date.weekday() == 0:
            logger.info(
                "ℹ️  No matching games found for target Monday %s; treating as KBO rest day.",
                target_date.isoformat(),
            )
            return 0
        if target_date:
            logger.info(
                "❌ No matching games found for target date %s with scope: %s",
                target_date.isoformat(),
                _format_scope(target_date, lookback_days, statuses, strict),
            )
            return 1
        logger.info(
            "ℹ️  No matching games found for scope: %s",
            _format_scope(target_date, lookback_days, statuses, strict),
        )
    else:
        logger.info("✅ All %s matching games have sufficient detail data.", game_count)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit daily game data completeness")
    parser.add_argument("--date", type=str, help="Target date in YYYYMMDD or YYYY-MM-DD format")
    parser.add_argument("--db-url", help="Database URL (can be env:VAR_NAME)")
    parser.add_argument("--days", type=int, default=14, help="Lookback days (default: 14)")
    parser.add_argument(
        "--statuses",
        default="",
        help="Comma-separated game_status values (default: COMPLETED,DRAW)",
    )
    parser.add_argument(
        "--include-incomplete",
        action="store_true",
        help="Include SCHEDULED and UNRESOLVED_MISSING as audit targets",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Require metadata/inning/lineup and relay-or-event completeness in addition to batting/pitching",
    )
    args = parser.parse_args()
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    load_dotenv()
    try:
        db_url = _resolve_db_url(args.db_url)
    except ValueError as e:
        logger.error(str(e))
        return 2

    try:
        target_date = _coerce_date(args.date)
    except ValueError as e:
        logger.error(str(e))
        return 2

    statuses = _parse_statuses(args.statuses, include_incomplete=args.include_incomplete)
    return audit_completeness(
        db_url,
        args.days,
        target_date=target_date,
        statuses=statuses,
        strict=args.strict,
    )


if __name__ == "__main__":
    sys.exit(main())
