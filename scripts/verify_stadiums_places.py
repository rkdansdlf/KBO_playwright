"""Read-only drift check for stadiums/places master data.

Checks (hard fail, exit 1):
- every places.stadium_id exists in stadiums (no orphans).
Reports (warnings, exit 0):
- food vendors with no matching 음식점 place (import gap).
- stadiums missing park factor / phone (coverage gaps).
- stadium_info codes with no stadiums row (scope gap).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select

from src.db.engine import SessionLocal
from src.models.place import Place
from src.models.stadium import Stadium
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.models.stadium_info import StadiumInfo

logger = logging.getLogger(__name__)


def run() -> int:
    """Run the drift check.

    Returns:
        Process exit code (0 ok, 1 orphan rows found).

    """
    failures = 0
    with SessionLocal() as session:
        stadium_ids = {r[0] for r in session.execute(select(Stadium.stadium_id)).all()}
        place_rows = list(session.execute(select(Place.stadium_id, Place.name)).all())
        orphans = [(s, n) for s, n in place_rows if s not in stadium_ids]
        if orphans:
            logger.error("[VERIFY] Orphan places (no stadiums row): %s", orphans[:10])
            failures += 1
        else:
            logger.info("[VERIFY] No orphan places (%d rows checked)", len(place_rows))

        vendors = list(session.execute(select(StadiumFoodVendor.stadium_id, StadiumFoodVendor.vendor_name)).all())
        food_names = {
            (s, n)
            for s, c, n in session.execute(select(Place.stadium_id, Place.category, Place.name)).all()
            if c == "음식점"
        }
        missing = [(s, n) for s, n in vendors if (s, n) not in food_names]
        if missing:
            logger.warning("[VERIFY] Vendors without 음식점 place: %d (e.g. %s)", len(missing), missing[:5])

        null_pf = session.query(Stadium).filter(Stadium.homerun_park_factor.is_(None)).count()
        null_phone = session.query(Stadium).filter(Stadium.phone.is_(None)).count()
        total = session.query(Stadium).count()
        logger.info("[VERIFY] stadiums=%d null_park_factor=%d null_phone=%d", total, null_pf, null_phone)

        info_codes = {r[0] for r in session.execute(select(StadiumInfo.stadium_code)).all()}
        uncovered = sorted(info_codes - stadium_ids)
        if uncovered:
            logger.warning("[VERIFY] stadium_info codes without stadiums row: %s", uncovered[:10])

    if failures:
        logger.error("[VERIFY] FAILED")
        return 1
    logger.info("[VERIFY] OK")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the verify CLI.

    Args:
        argv: Argv.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description="Verify stadiums/places master data (read-only).")
    parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    return run()


if __name__ == "__main__":
    sys.exit(main())
