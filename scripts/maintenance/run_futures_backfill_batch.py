"""Script to batch backfill Futures team codes in chunks to avoid command line limits."""

from __future__ import annotations

import logging
import subprocess
from sqlalchemy import text
from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_batch_backfill() -> None:
    session = SessionLocal()
    try:
        # 2010~2025 범위 전체에서 여전히 team_code가 NULL인 고유 player_id 수집
        rows = session.execute(
            text("SELECT DISTINCT player_id FROM player_season_batting WHERE team_code IS NULL;")
        ).fetchall()
    finally:
        session.close()

    player_ids = [str(r[0]) for r in rows]
    logger.info("Found %d players with missing team_codes to process.", len(player_ids))

    if not player_ids:
        logger.info("No players to process. Exiting.")
        return

    # 60명씩 청크로 쪼개기
    chunk_size = 60
    chunks = [player_ids[i : i + chunk_size] for i in range(0, len(player_ids), chunk_size)]

    for idx, chunk in enumerate(chunks):
        ids_str = ",".join(chunk)
        logger.info("Processing chunk %d/%d (Size: %d)...", idx + 1, len(chunks), len(chunk))

        # Subprocess로 crawl_futures CLI 기동 (동시성 8)
        cmd = [
            "venv/bin/python",
            "-m",
            "src.cli.crawl_futures",
            "--player-ids",
            ids_str,
            "--concurrency",
            "8",
        ]
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            logger.exception("Error executing chunk %d", idx + 1)


if __name__ == "__main__":
    run_batch_backfill()
