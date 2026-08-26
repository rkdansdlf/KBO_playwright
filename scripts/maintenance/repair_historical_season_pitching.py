"""Repair column misalignment in player_season_pitching for 2001-2008 and 2010.

Usage:
------
  python3 -m scripts.maintenance.repair_historical_season_pitching --dry-run
  python3 -m scripts.maintenance.repair_historical_season_pitching --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.models.player import PlayerSeasonPitching

logger = logging.getLogger(__name__)
BACKUP_DIR = Path("data/archive")


def _load_profile_lookup(session: Session) -> dict[tuple[int, str], tuple[int, int, int, int]]:
    """Build profile lookup map: (season, player_name) -> (wins, losses, saves, holds)."""
    profile_stmt = text("""
        SELECT p.season, b.name, p.wins, p.losses, p.saves, p.holds
        FROM player_season_pitching p
        JOIN player_basic b ON p.player_id = b.player_id
        WHERE p.source = "PROFILE"
          AND p.season IN (2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2010)
    """)
    profile_lookup: dict[tuple[int, str], tuple[int, int, int, int]] = {}
    for r in session.execute(profile_stmt).fetchall():
        s, name, w_cnt, loss_cnt, sv_cnt, h_cnt = (
            r[0],
            r[1],
            r[2] or 0,
            r[3] or 0,
            r[4] or 0,
            r[5] or 0,
        )
        if name:
            profile_lookup[(s, name)] = (w_cnt, loss_cnt, sv_cnt, h_cnt)
    return profile_lookup


def _apply_pitching_repairs(
    session: Session,
    rows: Sequence[Any],
    profile_lookup: dict[tuple[int, str], tuple[int, int, int, int]],
    *,
    apply_changes: bool,
) -> tuple[int, int, list[dict[str, Any]]]:
    """Apply repaired values to player_season_pitching rows."""
    repaired_with_profile = 0
    repaired_reset = 0
    backup_records: list[dict[str, Any]] = []

    for r in rows:
        rec_id, pid, season, src, team_code, w_cnt, loss_cnt, sv_cnt, h_cnt, name = r
        backup_records.append(
            {
                "id": rec_id,
                "player_id": pid,
                "season": season,
                "source": src,
                "team_code": team_code,
                "wins": w_cnt,
                "losses": loss_cnt,
                "saves": sv_cnt,
                "holds": h_cnt,
                "name": name,
            }
        )

        target_row = session.get(PlayerSeasonPitching, rec_id)
        if not target_row:
            continue

        if name and (season, name) in profile_lookup:
            pw, pl, psv, ph = profile_lookup[(season, name)]
            if apply_changes:
                target_row.wins = pw
                target_row.losses = pl
                target_row.saves = psv
                target_row.holds = ph
            repaired_with_profile += 1
        else:
            if apply_changes:
                target_row.wins = 0
                target_row.losses = 0
                target_row.saves = 0
                target_row.holds = 0
            repaired_reset += 1

    return repaired_with_profile, repaired_reset, backup_records


def repair_pitching_season_records(
    *,
    db_url: str | None = None,
    apply_changes: bool = False,
) -> int:
    target_url = db_url or os.environ.get("DATABASE_URL") or "sqlite:///./data/kbo_dev.db"
    try:
        engine = create_engine(target_url)
        session_factory = sessionmaker(bind=engine)
        session = session_factory()
    except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
        target_url = "sqlite:///./data/kbo_dev.db"
        engine = create_engine(target_url)
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

    try:
        corrupted_stmt = text("""
            SELECT p.id, p.player_id, p.season, p.source, p.team_code,
                   p.wins, p.losses, p.saves, p.holds,
                   b.name
            FROM player_season_pitching p
            LEFT JOIN player_basic b ON p.player_id = b.player_id
            WHERE p.season IN (2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2010)
              AND p.source = "FINAL_VERIFICATION"
        """)
        rows = session.execute(corrupted_stmt).fetchall()
        print(f"Found {len(rows)} FINAL_VERIFICATION rows across 2001-2008 & 2010.")

        profile_lookup = _load_profile_lookup(session)
        print(f"Loaded {len(profile_lookup)} verified PROFILE pitcher records for reference.")

        with_prof, reset_cnt, backups = _apply_pitching_repairs(
            session,
            rows,
            profile_lookup,
            apply_changes=apply_changes,
        )

        if apply_changes:
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_file = BACKUP_DIR / f"pre_pitching_repair_backup_{stamp}.json"
            backup_file.write_text(json.dumps(backups, indent=2, ensure_ascii=False), encoding="utf-8")
            session.commit()
            print(
                f"\nSUCCESS: Repaired {len(rows)} player_season_pitching rows "
                f"({with_prof} mapped to PROFILE, {reset_cnt} reset to 0-0-0). "
                f"Backup saved to {backup_file}"
            )
        else:
            print(
                f"\nDRY-RUN: Would repair {len(rows)} player_season_pitching rows "
                f"({with_prof} mapped to PROFILE, {reset_cnt} reset to 0-0-0). "
                f"No DB changes made."
            )

        return len(rows)
    except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
        session.rollback()
        logger.exception("Error repairing player_season_pitching")
        raise
    finally:
        session.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repair shifted player_season_pitching records.")
    parser.add_argument("--db-url", type=str, default=None, help="Database connection URL")
    parser.add_argument("--apply", action="store_true", default=False, help="Apply changes to database")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Preview changes without writing")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    repair_pitching_season_records(
        db_url=args.db_url,
        apply_changes=args.apply and not args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
