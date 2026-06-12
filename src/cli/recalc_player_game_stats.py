"""
CLI tool to recalculate player-game-level stats from game-level (transactional) data.
Aggregates GameBattingStat -> PlayerGameBatting
Aggregates GamePitchingStat -> PlayerGamePitching

Usage:
  python -m src.cli.recalc_player_game_stats --game-id 20250401LGSS0 --save
  python -m src.cli.recalc_player_game_stats --date 20250401 --save
  python -m src.cli.recalc_player_game_stats --season 2025 --save
  python -m src.cli.recalc_player_game_stats --season 2025 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import text

from src.db.engine import SessionLocal
from src.models.game import Game
from src.repositories.player_game_stats import (
    aggregate_game_batting,
    aggregate_game_batting_batch,
    aggregate_game_pitching,
    aggregate_game_pitching_batch,
    bulk_upsert_player_game_batting,
    bulk_upsert_player_game_pitching,
    upsert_player_game_batting,
    upsert_player_game_pitching,
)
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

logger = logging.getLogger(__name__)


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


def _print_batting_records(records) -> None:
    for r in sorted(records, key=lambda x: x.get("plate_appearances", 0), reverse=True)[:20]:
        logger.info(
            "  PID=%-6s %-8s PA=%-3s H=%-2s AVG=%-5s OPS=%-5s",
            r.get("player_id", ""),
            r.get("player_name", ""),
            r.get("plate_appearances", 0),
            r.get("hits", 0),
            r.get("avg", 0),
            r.get("ops", 0),
        )


def _print_pitching_records(records) -> None:
    for r in sorted(records, key=lambda x: x.get("innings_outs", 0), reverse=True)[:20]:
        ip = r.get("innings_outs", 0) / 3.0
        logger.info(
            "  PID=%-6s %-8s IP=%-5.1f ERA=%-5s WHIP=%-5s",
            r.get("player_id", ""),
            r.get("player_name", ""),
            ip,
            r.get("era", 0),
            r.get("whip", 0),
        )


def recalc_for_game(session, game_id: str, dry_run: bool = False) -> dict[str, int]:
    batting = aggregate_game_batting(session, game_id)
    pitching = aggregate_game_pitching(session, game_id)

    if dry_run:
        if batting:
            logger.info("[DRY-RUN] %s batting (%s players):", game_id, len(batting))
            _print_batting_records(batting)
        if pitching:
            logger.info("[DRY-RUN] %s pitching (%s players):", game_id, len(pitching))
            _print_pitching_records(pitching)
        return {"batting": len(batting), "pitching": len(pitching)}

    b_saved = upsert_player_game_batting(session, batting)
    p_saved = upsert_player_game_pitching(session, pitching)
    return {"batting": b_saved, "pitching": p_saved}


def recalc_for_games_batch(session, game_ids: list[str], dry_run: bool = False) -> dict[str, int]:
    """Batch recalc: single query per side, single commit for all games."""
    batting = aggregate_game_batting_batch(session, game_ids)
    pitching = aggregate_game_pitching_batch(session, game_ids)

    if dry_run:
        logger.info("[DRY-RUN] %s games, batting=%s, pitching=%s", len(game_ids), len(batting), len(pitching))
        return {"batting": len(batting), "pitching": len(pitching)}

    b_saved = bulk_upsert_player_game_batting(session, batting)
    p_saved = bulk_upsert_player_game_pitching(session, pitching)
    session.commit()
    logger.info("Batch done: %s games, upserted batting=%s, pitching=%s", len(game_ids), b_saved, p_saved)
    return {"batting": b_saved, "pitching": p_saved}


def run_recalc(
    game_id: str | None = None,
    date: str | None = None,
    season: int | None = None,
    dry_run: bool = False,
    include_futures: bool = False,
) -> int:
    with SessionLocal() as session:
        game_ids: list[str] = []

        if game_id:
            game_ids.append(game_id)

        if date:
            game_ids.extend(_game_ids_for_date(session, date))

        if season:
            league_codes = [0]
            if include_futures:
                league_codes.append(5)
            from sqlalchemy import bindparam

            stmt = text(
                "SELECT season_id FROM kbo_seasons WHERE season_year = :year AND league_type_code IN :codes",
            ).bindparams(bindparam("codes", expanding=True))
            season_ids = (
                session.execute(
                    stmt,
                    {"year": season, "codes": league_codes},
                )
                .scalars()
                .all()
            )
            rows = (
                session.query(Game.game_id)
                .filter(
                    Game.season_id.in_(season_ids),
                    Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                )
                .order_by(Game.game_id.asc())
                .all()
            )
            game_ids.extend(row[0] for row in rows)

        game_ids = list(dict.fromkeys(game_ids))

        if not game_ids:
            logger.warning("No completed games matched.")
            return 0

        if len(game_ids) == 1:
            totals = recalc_for_game(session, game_ids[0], dry_run=dry_run)
        else:
            totals = recalc_for_games_batch(session, game_ids, dry_run=dry_run)

        if dry_run:
            logger.info(
                f"[DRY-RUN] Total: {len(game_ids)} games, batting={totals['batting']}, pitching={totals['pitching']}",
            )
        else:
            logger.info(
                f"Done: {len(game_ids)} games, upserted batting={totals['batting']}, pitching={totals['pitching']}",
            )

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recalculate player-game-level stats from game-level data.")
    parser.add_argument("--game-id", help="Single game ID to recalc")
    parser.add_argument("--date", help="Game date (YYYYMMDD) to recalc all completed games")
    parser.add_argument("--season", type=int, help="Season year to recalc all completed games")
    parser.add_argument(
        "--include-futures",
        action="store_true",
        help="Include Futures (2nd league) games (default: KBO only)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print results without saving")
    parser.add_argument("--save", action="store_true", help="Persist results (default if not --dry-run)")
    args = parser.parse_args(argv)

    if not args.game_id and not args.date and not args.season:
        parser.error("provide --game-id, --date, or --season")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    dry_run = args.dry_run or not args.save
    return run_recalc(
        game_id=args.game_id,
        date=args.date,
        season=args.season,
        dry_run=dry_run,
        include_futures=args.include_futures,
    )


if __name__ == "__main__":
    sys.exit(main())
