"""PlayerGame data quality verification.

Verifies integrity of player_game_batting and player_game_pitching tables:
  - No duplicate (game_id, player_id) pairs
  - No NULL player_id or player_name
  - Rate stats within valid ranges (avg, obp, era, whip)
  - Coverage by game status and season year

Usage:
  python -m scripts.verification.verify_player_game_stats
  python -m scripts.verification.verify_player_game_stats --exit-code
  python -m scripts.verification.verify_player_game_stats --verbose
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import text

from src.db.engine import SessionLocal

logger = logging.getLogger(__name__)


def _scope_clause(*, year: int | None = None, date: str | None = None, column: str = "game_id") -> tuple[str, dict]:
    conditions = []
    params = {}
    if year is not None:
        conditions.append(f"SUBSTR({column}, 1, 4) = :year_text")
        params["year_text"] = str(year)
    if date is not None:
        conditions.append(f"SUBSTR({column}, 1, 8) = :date_text")
        params["date_text"] = date
    return (" WHERE " + " AND ".join(conditions), params) if conditions else ("", params)


def _and_scope_clause(*, year: int | None = None, date: str | None = None, column: str = "game_id") -> tuple[str, dict]:
    clause, params = _scope_clause(year=year, date=date, column=column)
    if clause:
        clause = " AND " + clause.removeprefix(" WHERE ")
    return clause, params


def _normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.replace("-", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError("--date must be YYYYMMDD or YYYY-MM-DD")
    return text


def check_duplicates(session, *, year: int | None = None, date: str | None = None) -> list[str]:
    issues = []
    for tbl in ("player_game_batting", "player_game_pitching"):
        scope_sql, params = _scope_clause(year=year, date=date)
        count = session.execute(
            text(
                f"SELECT COALESCE(COUNT(*), 0) FROM (SELECT game_id, player_id, COUNT(*) FROM {tbl}{scope_sql} GROUP BY game_id, player_id HAVING COUNT(*) > 1)"
            ),
            params,
        ).scalar()
        if count:
            issues.append(f"{tbl}: {count} duplicate (game_id, player_id) pair(s)")
    return issues


def check_nulls(session, *, year: int | None = None, date: str | None = None) -> list[str]:
    issues = []
    for tbl in ("player_game_batting", "player_game_pitching"):
        scope_sql, params = _and_scope_clause(year=year, date=date)
        null_player_id = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE player_id IS NULL{scope_sql}"),
            params,
        ).scalar()
        if null_player_id:
            issues.append(f"{tbl}: {null_player_id} row(s) with NULL player_id")
        null_name = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE player_name IS NULL{scope_sql}"),
            params,
        ).scalar()
        if null_name:
            issues.append(f"{tbl}: {null_name} row(s) with NULL player_name")
        null_side = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE team_side IS NULL{scope_sql}"),
            params,
        ).scalar()
        if null_side:
            issues.append(f"{tbl}: {null_side} row(s) with NULL team_side")
    return issues


def check_rate_stats(session, *, year: int | None = None, date: str | None = None) -> list[str]:
    issues = []
    # Batting rate stat ranges
    checks = [
        ("player_game_batting", "avg", 0, 1),
        ("player_game_batting", "obp", 0, 1),
        ("player_game_batting", "slg", 0, 5),
    ]
    for tbl, col, lo, hi in checks:
        scope_sql, params = _and_scope_clause(year=year, date=date)
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi}){scope_sql}"),
            params,
        ).scalar()
        if count:
            issues.append(f"{tbl}.{col}: {count} row(s) outside [{lo}, {hi}]")

    # avg > obp is possible with sacrifice flies — log as info, not error
    scope_sql, params = _and_scope_clause(year=year, date=date)
    avg_gt_obp = session.execute(
        text(
            f"SELECT COUNT(*) FROM player_game_batting WHERE avg IS NOT NULL AND obp IS NOT NULL AND avg > obp{scope_sql}"
        ),
        params,
    ).scalar()
    if avg_gt_obp:
        issues.append(f"INFO: player_game_batting avg > obp: {avg_gt_obp} row(s) (expected with sacrifice flies)")

    # Pitching rate stat ranges
    # ERA can exceed 100 with very short outings (1 out, 4+ ER → ERA=108+)
    checks = [
        ("player_game_pitching", "era", 0, 200),
        ("player_game_pitching", "whip", 0, 30),
    ]
    for tbl, col, lo, hi in checks:
        scope_sql, params = _and_scope_clause(year=year, date=date)
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi}){scope_sql}"),
            params,
        ).scalar()
        if count:
            issues.append(f"{tbl}.{col}: {count} row(s) outside [{lo}, {hi}]")

    return issues


def check_coverage(session, verbose: bool = False, *, year: int | None = None, date: str | None = None) -> list[str]:
    issues = []
    scope_sql, params = _and_scope_clause(year=year, date=date, column="g.game_id")
    rows = session.execute(
        text(f"""
        SELECT CAST(SUBSTR(g.game_id, 1, 4) AS INTEGER) as yr,
               g.game_status,
               COUNT(DISTINCT g.game_id) as total_games,
               COUNT(DISTINCT pgb.game_id) as covered_games
        FROM game g
        LEFT JOIN player_game_batting pgb ON pgb.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          {scope_sql}
        GROUP BY yr, g.game_status
        ORDER BY yr, g.game_status
    """),
        params,
    ).fetchall()

    for yr, status, total, covered in rows:
        pct = 100.0 * covered / total if total else 0
        if verbose:
            issues.append(f"{yr} {status:<12} games={total:>5} covered={covered:>5} {pct:5.1f}%")
        if yr >= 2018 and pct < 90:
            issues.append(f"WARN: {yr} {status} coverage {pct:.1f}%")

    return issues


def check_draw_missing_sources(session, *, year: int | None = None, date: str | None = None) -> list[str]:
    """Diagnose DRAW games that lack source-level stats."""
    issues = []
    for src in ("game_batting_stats", "game_pitching_stats", "game_play_by_play"):
        scope_sql, params = _and_scope_clause(year=year, date=date, column="g.game_id")
        count = session.execute(
            text(f"""
                SELECT COUNT(*)
                FROM game g
                LEFT JOIN {src} s ON s.game_id = g.game_id
                WHERE g.game_status = 'DRAW'
                  AND s.game_id IS NULL
                  {scope_sql}
            """),
            params,
        ).scalar()
        if count:
            issues.append(f"INFO: DRAW games missing {src}: {count}")
    return issues


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify PlayerGame data quality")
    parser.add_argument("--exit-code", action="store_true", help="Return non-zero exit code on issues")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed coverage breakdown")
    parser.add_argument("--year", type=int, help="Scope checks to a season year")
    parser.add_argument("--date", help="Scope checks to a game date (YYYYMMDD or YYYY-MM-DD)")
    args = parser.parse_args(argv)
    target_date = _normalize_date(args.date)

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    logger.info("PlayerGame Data Quality Verification — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if args.year or target_date:
        logger.info("Scope: year=%s date=%s", args.year or "all", target_date or "all")
    logger.info("=" * 60)

    with SessionLocal() as session:
        all_bad = []
        for label, fn in [
            ("Duplicates", lambda s: check_duplicates(s, year=args.year, date=target_date)),
            ("NULL fields", lambda s: check_nulls(s, year=args.year, date=target_date)),
            ("Rate stats", lambda s: check_rate_stats(s, year=args.year, date=target_date)),
            ("DRAW source gaps", lambda s: check_draw_missing_sources(s, year=args.year, date=target_date)),
            ("Coverage", lambda s: check_coverage(s, verbose=args.verbose, year=args.year, date=target_date)),
        ]:
            logger.info("\n--- %s ---", label)
            issues = fn(session)
            if issues:
                for i in issues:
                    logger.info("  %s", i)
                all_bad.extend(issues)
            else:
                logger.info("  OK")

        total_batting = session.execute(text("SELECT COUNT(*) FROM player_game_batting")).scalar()
        total_pitching = session.execute(text("SELECT COUNT(*) FROM player_game_pitching")).scalar()
        total_games = session.execute(text("SELECT COUNT(DISTINCT game_id) FROM player_game_batting")).scalar()

    logger.info("\n%s", "=" * 60)
    logger.info(
        "Summary: batting=%s pitching=%s games=%s", f"{total_batting:,}", f"{total_pitching:,}", f"{total_games:,}"
    )
    if all_bad:
        info_count = sum(1 for i in all_bad if i.startswith("INFO:"))
        warn_count = sum(1 for i in all_bad if i.startswith("WARN"))
        error_count = len(all_bad) - info_count - warn_count
        logger.info("Issues: %s (%s errors, %s warnings, %s info)", len(all_bad), error_count, warn_count, info_count)
        if args.exit_code:
            if error_count:
                return 1
            if warn_count:
                return 2
    else:
        logger.info("All checks passed")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
