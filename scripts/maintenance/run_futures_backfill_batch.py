"""Script to batch backfill Futures team codes in chunks to avoid command line limits."""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Callable

from sqlalchemy import text

from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
FUTURES_MISSING_PLAYER_QUERY = (
    "SELECT DISTINCT player_id FROM player_season_batting "
    "WHERE team_code IS NULL AND league = 'FUTURES' AND level = 'KBO2'"
)
FUTURES_BACKFILL_CHUNK_SIZE = 60


def _load_futures_player_ids() -> list[str]:
    with SessionLocal() as session:
        rows = session.execute(text(FUTURES_MISSING_PLAYER_QUERY)).fetchall()
    return [str(row[0]) for row in rows]


def _futures_backfill_command(player_ids: list[str]) -> list[str]:
    return [
        "venv/bin/python",
        "-m",
        "src.cli.crawl_futures",
        "--player-ids",
        ",".join(player_ids),
        "--concurrency",
        "8",
    ]


def _run_futures_backfill_command(command: list[str]) -> None:
    subprocess.run(command, check=True)


def run_batch_backfill(*, runner: Callable[[list[str]], None] = _run_futures_backfill_command) -> None:
    """Recrawl only Futures players whose scoped season rows lack a team code."""
    player_ids = _load_futures_player_ids()
    logger.info("Found %d players with missing team_codes to process.", len(player_ids))

    if not player_ids:
        logger.info("No players to process. Exiting.")
        return

    chunks = [
        player_ids[index : index + FUTURES_BACKFILL_CHUNK_SIZE]
        for index in range(0, len(player_ids), FUTURES_BACKFILL_CHUNK_SIZE)
    ]

    for idx, chunk in enumerate(chunks):
        logger.info("Processing chunk %d/%d (Size: %d)...", idx + 1, len(chunks), len(chunk))
        try:
            runner(_futures_backfill_command(chunk))
        except subprocess.CalledProcessError:
            logger.exception("Error executing chunk %d", idx + 1)


if __name__ == "__main__":
    run_batch_backfill()
