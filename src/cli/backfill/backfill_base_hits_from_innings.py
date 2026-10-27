"""Backfill per-batter doubles and triples from the stored inning breakdown.

Both the Naver record API and the current KBO hitter table report a batter's
hits only as a total, so a Naver-sourced game records zero doubles and zero
triples for every batter even though the inning breakdown carries the split:
the cell for a double reads ``좌2`` (position, then the number of bases), a
triple reads ``중3``, and a home run is spelled out ``좌홈``.

This command re-derives those two counts from the inning breakdown already
stored in ``game_batting_stats.extra_stats.inning_results`` and writes them
back. It changes no other column and only fills counts that are currently
zero, so a row that already carries a value is never overwritten.

The inning breakdown for innings 10 and beyond is not stored, so doubles and
triples hit in an extra inning stay uncounted. Re-crawl those games to close
the gap.
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from src.db.engine import SessionLocal
from src.utils.inning_cells import derive_hitter_stats_from_inning_cells

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Environment guard required alongside ``--apply``.
APPLY_GUARD_ENV = "KBO_ALLOW_BASE_HIT_BACKFILL"

#: How many candidate rows to print before summarising the rest.
PREVIEW_ROWS = 10


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse args.

    Args:
        argv: Command line arguments.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--season", help="Season year to repair, e.g. 2026.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help=f"Write the derived counts to the database (default: dry run). Requires {APPLY_GUARD_ENV}=1.",
    )
    parser.add_argument("--limit", type=int, help="Maximum number of rows to repair.")
    return parser.parse_args(argv)


def _load_candidates(session: Session, args: argparse.Namespace) -> list[dict[str, Any]]:
    """Load the rows whose inning breakdown can supply the missing counts.

    Args:
        session: Session.
        args: Parsed arguments.

    Returns:
        Candidate rows with their derived counts.

    """
    query = text(
        """
        SELECT
            game_id,
            player_id,
            player_name,
            doubles,
            triples,
            extra_stats::jsonb -> 'inning_results' AS cells
        FROM game_batting_stats
        WHERE extra_stats::jsonb ? 'inning_results'
          AND (:season IS NULL OR SUBSTRING(game_id, 1, 4) = :season)
          AND (coalesce(doubles, 0) = 0 OR coalesce(triples, 0) = 0)
        ORDER BY game_id, player_id
        LIMIT :limit
        """,
    )
    params: dict[str, Any] = {
        "season": args.season,
        "limit": args.limit or 1_000_000,
    }
    rows = session.execute(query, params).mappings().all()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        cells = {key: value for key, value in (row["cells"] or {}).items() if value}
        if not cells:
            continue
        derived = derive_hitter_stats_from_inning_cells(cells)
        doubles = derived["doubles"]
        triples = derived["triples"]
        if doubles == (row["doubles"] or 0) and triples == (row["triples"] or 0):
            continue
        candidates.append(
            {
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "current_doubles": row["doubles"],
                "current_triples": row["triples"],
                "doubles": doubles,
                "triples": triples,
            },
        )
    return candidates


def main(argv: list[str] | None = None) -> int:
    """Run the backfill.

    Args:
        argv: Command line arguments.

    Returns:
        Process exit code: 0 on success, 3 when the apply guard is missing.

    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)

    if args.apply and os.environ.get(APPLY_GUARD_ENV) != "1":
        logger.error("refusing to write: set %s=1 to allow --apply", APPLY_GUARD_ENV)
        return 3

    with SessionLocal() as session:
        candidates = _load_candidates(session, args)
        if not candidates:
            logger.info("no rows need a doubles/triples backfill")
            return 0

        logger.info(
            "%d rows would change (doubles +%d, triples +%d)",
            len(candidates),
            sum(c["doubles"] for c in candidates),
            sum(c["triples"] for c in candidates),
        )
        for candidate in candidates[:PREVIEW_ROWS]:
            logger.info(
                "  %s %s: doubles %s -> %d, triples %s -> %d",
                candidate["game_id"],
                candidate["player_name"],
                candidate["current_doubles"],
                candidate["doubles"],
                candidate["current_triples"],
                candidate["triples"],
            )
        if len(candidates) > PREVIEW_ROWS:
            logger.info("  ... and %d more", len(candidates) - PREVIEW_ROWS)

        if not args.apply:
            logger.info("dry run: no rows updated (pass --apply with %s=1 to write)", APPLY_GUARD_ENV)
            return 0

        update = text(
            """
            UPDATE game_batting_stats
            SET doubles = :doubles,
                triples = :triples
            WHERE game_id = :game_id
              AND player_id = :player_id
            """,
        )
        changed = 0
        for candidate in candidates:
            result = session.execute(update, candidate)
            changed += result.rowcount or 0
        session.commit()
        logger.info("updated %d rows", changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
