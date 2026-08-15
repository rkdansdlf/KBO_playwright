"""CLI command for ingesting and validating historical KBO seasons (1982-2000).

Supports schedule, boxscore, and detailed stats ingestion with automated integrity auditing.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.services.historical_1982_pilot_service import Historical1982PilotService
from src.services.historical_boxscore_ingestor import HistoricalBoxscoreIngestor

if TYPE_CHECKING:
    from collections.abc import Sequence

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HISTORICAL_1982_YEAR = 1982


def ingest_season(
    season_year: int,
    *,
    with_boxscores: bool = True,
    dry_run: bool = False,
) -> dict[str, object]:
    """Ingest schedule and boxscores for a historical season."""
    with SessionLocal() as session:
        if season_year == HISTORICAL_1982_YEAR:
            pilot_svc = Historical1982PilotService(session)
            boxscore_svc = HistoricalBoxscoreIngestor(session)

            if dry_run:
                fixtures = pilot_svc.generate_1982_schedule_fixtures()
                return {
                    "season": season_year,
                    "status": "DRY_RUN",
                    "fixtures_generated": len(fixtures),
                    "with_boxscores": with_boxscores,
                }

            # 1. Seed Games/Schedule
            saved_games = pilot_svc.seed_1982_fixtures()
            session.commit()

            # 2. Seed Boxscores and Player Stats
            inns, bats, pits = (0, 0, 0)
            if with_boxscores:
                inns, bats, pits = boxscore_svc.seed_1982_season_boxscores()
                session.commit()

            # 3. Verify Integrity
            integrity_report = boxscore_svc.audit_1982_boxscore_integrity()

            return {
                "season": season_year,
                "status": "APPLIED",
                "saved_games": saved_games,
                "inning_score_rows": inns,
                "batting_rows": bats,
                "pitching_rows": pits,
                "audit": asdict(integrity_report),
            }

        msg = f"Season {season_year} is not yet supported in historical ingestor"
        raise ValueError(msg)


def main(argv: Sequence[str] | None = None) -> int:
    """Run CLI main entrypoint."""
    parser = argparse.ArgumentParser(description="Ingest historical KBO season data")
    parser.add_argument("--season", type=int, default=1982, help="Season year to ingest (1982-2000)")
    parser.add_argument("--with-boxscores", action="store_true", default=True, help="Include detailed boxscore lines")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Simulate without saving to DB")
    parser.add_argument("--json", action="store_true", default=False, help="Output JSON result")

    args = parser.parse_args(argv)
    try:
        result = ingest_season(
            args.season,
            with_boxscores=args.with_boxscores,
            dry_run=args.dry_run,
        )
    except (ValueError, RuntimeError):
        logger.exception("[Historical Ingestor] Error during historical season ingestion")
        return 1
    else:
        if args.json:
            sys.stdout.write(json.dumps(result, indent=2, default=str, ensure_ascii=False) + "\n")
        else:
            logger.info("Historical Season Ingestion Result: %s", result)
        return 0


if __name__ == "__main__":
    sys.exit(main())
