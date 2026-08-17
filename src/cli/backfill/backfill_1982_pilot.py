"""CLI for seeding and auditing 1982 inaugural KBO historical pilot data."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict

from src.db.engine import SessionLocal
from src.services.historical_1982_pilot_service import Historical1982PilotService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    """Run 1982 pilot historical backfill and verification."""
    parser = argparse.ArgumentParser(description="Seed and audit 1982 inaugural KBO pilot season data.")
    parser.add_argument("--verify", action="store_true", help="Run integrity audit on 1982 season data.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Simulate 1982 fixture generation without writing.",
    )
    parser.add_argument("--apply", action="store_true", help="Write 1982 fixtures into database.")
    parser.add_argument("--json", action="store_true", dest="json_output", help="Output result as JSON.")

    args = parser.parse_args()

    with SessionLocal() as session:
        service = Historical1982PilotService(session)

        if args.dry_run:
            fixtures = service.generate_1982_schedule_fixtures()
            payload = {
                "status": "DRY_RUN",
                "total_fixtures_generated": len(fixtures),
                "sample_fixture": fixtures[0] if fixtures else None,
            }
            if args.json_output:
                sys.stdout.write(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
            else:
                logger.info("[1982 Pilot] Dry-run completed: %d fixtures generated successfully.", len(fixtures))
            return 0

        if args.apply:
            saved_count = service.seed_1982_fixtures()
            session.commit()
            report = service.verify_1982_season_integrity()
            payload = {
                "status": "APPLIED",
                "saved_count": saved_count,
                "integrity_report": asdict(report),
            }
            if args.json_output:
                sys.stdout.write(json.dumps(payload, indent=2, default=str, ensure_ascii=False) + "\n")
            else:
                logger.info("[1982 Pilot] Applied %d fixtures. Total in DB: %d", saved_count, report.total_games)
            return 0 if report.is_count_valid else 1

        if args.verify:
            report = service.verify_1982_season_integrity()
            if args.json_output:
                sys.stdout.write(json.dumps(asdict(report), indent=2, default=str, ensure_ascii=False) + "\n")
            else:
                logger.info("[1982 Pilot] Total Games: %d / 240, Valid: %s", report.total_games, report.is_count_valid)
            return 0 if report.is_count_valid else 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
