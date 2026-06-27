"""
Live boxscore CLI — query inning-by-inning scores for today's in-progress KBO games.

Usage:
    python3 -m src.cli.live_boxscore
    python3 -m src.cli.live_boxscore --date 20260627
    python3 -m src.cli.live_boxscore --json
    python3 -m src.cli.live_boxscore --game-id 20260627HTOB0
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameInningScore

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _configure_cli_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live boxscore for KBO games")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in YYYYMMDD format (default: today KST)",
    )
    parser.add_argument(
        "--game-id",
        type=str,
        default=None,
        help="Query a specific game_id instead of all games for the date",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output in JSON format",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of games to display (default: 20)",
    )
    return parser.parse_args(argv)


def _resolve_target_date(date_str: str | None) -> str:
    if date_str:
        if len(date_str) != 8 or not date_str.isdigit():
            msg = f"Invalid date format: {date_str!r}, expected YYYYMMDD"
            raise ValueError(msg)
        return date_str
    return datetime.now(KST).strftime("%Y%m%d")


def _fetch_live_games(
    session: Session,
    target_date: str,
    game_id: str | None,
    limit: int,
) -> list[Game]:
    query = select(Game).where(
        Game.game_date == target_date,
        Game.game_status.in_(("live", "in_progress", "delayed", "suspended")),
    )
    if game_id:
        query = query.where(Game.game_id == game_id)
    query = query.order_by(Game.game_id).limit(limit)
    return list(session.execute(query).scalars().all())


def _fetch_inning_scores(
    session: Session,
    game_ids: list[str],
) -> dict[str, list[GameInningScore]]:
    if not game_ids:
        return {}
    rows = (
        session.execute(
            select(GameInningScore)
            .where(GameInningScore.game_id.in_(game_ids))
            .order_by(GameInningScore.team_side, GameInningScore.inning)
        )
        .scalars()
        .all()
    )
    result: dict[str, list[GameInningScore]] = {}
    for row in rows:
        result.setdefault(row.game_id, []).append(row)
    return result


def _build_game_payload(
    game: Game,
    innings: list[GameInningScore],
) -> dict[str, Any]:
    away_innings = [i for i in innings if i.team_side == "away"]
    home_innings = [i for i in innings if i.team_side == "home"]

    def _line_score(items: list[GameInningScore]) -> list[int | None]:
        return [i.runs for i in sorted(items, key=lambda x: x.inning)]

    def _total_runs(items: list[GameInningScore]) -> int:
        return sum(i.runs or 0 for i in items)

    away_code = None
    home_code = None
    if innings:
        away_code = next((i.team_code for i in away_innings), None)
        home_code = next((i.team_code for i in home_innings), None)

    return {
        "game_id": game.game_id,
        "game_date": game.game_date,
        "game_status": game.game_status,
        "away": {
            "code": away_code or game.away_team_code,
            "score": game.away_score,
            "hits": game.away_hits,
            "errors": game.away_errors,
            "line_score": _line_score(away_innings),
            "runs": _total_runs(away_innings),
        },
        "home": {
            "code": home_code or game.home_team_code,
            "score": game.home_score,
            "hits": game.home_hits,
            "errors": game.home_errors,
            "line_score": _line_score(home_innings),
            "runs": _total_runs(home_innings),
        },
        "stadium": game.stadium,
        "attendance": game.attendance,
    }


def _format_text(payload: dict[str, Any]) -> str:
    lines = []
    away = payload["away"]
    home = payload["home"]
    lines.append(
        f"{payload['game_id']} [{payload['game_status']}] "
        f"{away['code']} {away['runs']} vs {home['code']} {home['runs']}"
    )
    away_ls = away["line_score"]
    home_ls = home["line_score"]
    max_innings = max(len(away_ls), len(home_ls), 9)
    if max_innings > 0:
        header = "      " + " ".join(f"{i + 1:>3}" for i in range(max_innings)) + "  R   H   E"
        lines.append(header)
        for side, ls, score, hits, errs in (
            ("AWAY", away_ls, away["runs"], away["hits"], away["errors"]),
            ("HOME", home_ls, home["runs"], home["hits"], home["errors"]),
        ):
            cells = " ".join(f"{(v if v is not None else '-'):>3}" for v in ls[:max_innings])
            lines.append(f"  {side} {cells}  {score or 0:>2} {hits or 0:>2} {errs or 0:>2}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run live boxscore CLI."""
    _configure_cli_logging()
    args = _parse_args(argv)

    try:
        target_date = _resolve_target_date(args.date)
    except ValueError:
        logger.exception("Invalid date format")
        return 1

    with SessionLocal() as session:
        games = _fetch_live_games(session, target_date, args.game_id, args.limit)

        if not games:
            date_label = target_date if not args.date else args.date
            if args.json:
                sys.stdout.write(json.dumps({"date": date_label, "games": []}, ensure_ascii=False) + "\n")
            else:
                logger.info("No live games found for %s", date_label)
            return 0

        game_ids = [g.game_id for g in games]
        inning_map = _fetch_inning_scores(session, game_ids)

        payloads = [_build_game_payload(g, inning_map.get(g.game_id, [])) for g in games]

    if args.json:
        sys.stdout.write(
            json.dumps(
                {"date": target_date, "game_count": len(payloads), "games": payloads},
                ensure_ascii=False,
                indent=2 if sys.stdout.isatty() else None,
            )
            + "\n"
        )
    else:
        for payload in payloads:
            logger.info("\n%s", _format_text(payload))

    return 0


if __name__ == "__main__":
    sys.exit(main())
