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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.models.player import PlayerSeasonPitching

logger = logging.getLogger(__name__)
BACKUP_DIR = Path("data/archive")
type PitchingCounts = tuple[int, int, int, int]


@dataclass(frozen=True)
class ProfileRecord:
    """Store verified pitching values and their source team."""

    stats: PitchingCounts
    team_code: str | None


@dataclass(frozen=True)
class ProfileLookup:
    """Store exact and conservative fallback matches for profile rows."""

    by_player: dict[tuple[int, int], ProfileRecord]
    by_unique_name: dict[tuple[int, str], ProfileRecord]
    ambiguous_names: set[tuple[int, str]]


def _load_profile_lookup(session: Session) -> ProfileLookup:
    """Build exact and unambiguous profile lookup maps."""
    profile_stmt = text("""
        SELECT p.season, p.player_id, b.name, p.team_code,
               p.wins, p.losses, p.saves, p.holds
        FROM player_season_pitching p
        JOIN player_basic b ON p.player_id = b.player_id
        WHERE p.source = 'PROFILE'
          AND p.league = 'REGULAR'
          AND p.level = 'KBO1'
          AND p.season IN (2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2010)
    """)
    by_player: dict[tuple[int, int], ProfileRecord] = {}
    by_name: dict[tuple[int, str], ProfileRecord] = {}
    name_player_ids: dict[tuple[int, str], set[int]] = {}
    for r in session.execute(profile_stmt).fetchall():
        s, player_id, name, team_code, w_cnt, loss_cnt, sv_cnt, h_cnt = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4] or 0,
            r[5] or 0,
            r[6] or 0,
            r[7] or 0,
        )
        if player_id is None:
            continue
        record = ProfileRecord(
            stats=(w_cnt, loss_cnt, sv_cnt, h_cnt),
            team_code=team_code,
        )
        by_player[(s, player_id)] = record
        if name:
            name_key = (s, name)
            name_player_ids.setdefault(name_key, set()).add(player_id)
            by_name[name_key] = record

    ambiguous_names = {key for key, player_ids in name_player_ids.items() if len(player_ids) > 1}
    return ProfileLookup(
        by_player=by_player,
        by_unique_name={key: stats for key, stats in by_name.items() if key not in ambiguous_names},
        ambiguous_names=ambiguous_names,
    )


def _resolve_profile_record(
    profile_lookup: ProfileLookup,
    season: int,
    player_id: int | None,
    name: str | None,
) -> tuple[str, ProfileRecord | None]:
    """Resolve a row using exact IDs before conservative name fallback."""
    if player_id is not None:
        record = profile_lookup.by_player.get((season, player_id))
        if record is not None:
            return "exact_player_id", record

    name_key = (season, name) if name else None
    if name_key is not None and name_key in profile_lookup.ambiguous_names:
        return "ambiguous_name", None
    if name_key is not None:
        record = profile_lookup.by_unique_name.get(name_key)
        if record is not None:
            return "unique_name", record
    return "no_evidence", None


def _team_codes_match(source_team: str | None, profile_team: str | None) -> bool | None:
    """Compare source and PROFILE team codes when both are available."""
    if not source_team or not profile_team:
        return None
    return source_team == profile_team


def _apply_pitching_repairs(
    session: Session,
    rows: Sequence[Any],
    profile_lookup: ProfileLookup,
    *,
    apply_changes: bool,
) -> tuple[int, int, int, int, list[dict[str, Any]]]:
    """Apply repaired values to player_season_pitching rows."""
    repaired_with_profile = 0
    repaired_reset = 0
    skipped_ambiguous = 0
    skipped_no_evidence = 0
    backup_records: list[dict[str, Any]] = []

    for r in rows:
        rec_id, pid, season, src, team_code, w_cnt, loss_cnt, sv_cnt, h_cnt, name = r
        resolution, profile_record = _resolve_profile_record(profile_lookup, season, pid, name)
        original_values = {
            "wins": w_cnt or 0,
            "losses": loss_cnt or 0,
            "saves": sv_cnt or 0,
            "holds": h_cnt or 0,
        }
        profile_values = (
            {
                "wins": profile_record.stats[0],
                "losses": profile_record.stats[1],
                "saves": profile_record.stats[2],
                "holds": profile_record.stats[3],
            }
            if profile_record
            else None
        )
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
                "resolution": resolution,
                "profile_team_code": profile_record.team_code if profile_record else None,
                "team_code_match": _team_codes_match(
                    team_code,
                    profile_record.team_code if profile_record else None,
                ),
                "original_values": original_values,
                "profile_values": profile_values,
                "would_change": profile_values is not None and original_values != profile_values,
            }
        )

        target_row = session.get(PlayerSeasonPitching, rec_id)
        if not target_row:
            continue

        if resolution == "ambiguous_name":
            skipped_ambiguous += 1
            continue

        if profile_record is not None:
            pw, pl, psv, ph = profile_record.stats
            if apply_changes and original_values != profile_values:
                target_row.wins = pw
                target_row.losses = pl
                target_row.saves = psv
                target_row.holds = ph
            repaired_with_profile += 1
        else:
            skipped_no_evidence += 1

    return repaired_with_profile, repaired_reset, skipped_ambiguous, skipped_no_evidence, backup_records


def repair_pitching_season_records(
    *,
    db_url: str | None = None,
    apply_changes: bool = False,
    report_path: str | Path | None = None,
) -> int:
    target_url = db_url or os.environ.get("DATABASE_URL") or "sqlite:///./data/kbo_dev.db"
    try:
        engine = create_engine(target_url)
        session_factory = sessionmaker(bind=engine)
        session = session_factory()
    except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
        logger.exception("Could not open repair database: %s", target_url)
        raise

    try:
        corrupted_stmt = text("""
            SELECT p.id, p.player_id, p.season, p.source, p.team_code,
                   p.wins, p.losses, p.saves, p.holds,
                   b.name
            FROM player_season_pitching p
            LEFT JOIN player_basic b ON p.player_id = b.player_id
            WHERE p.season IN (2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2010)
              AND p.source = 'FINAL_VERIFICATION'
        """)
        rows = session.execute(corrupted_stmt).fetchall()
        print(f"Found {len(rows)} FINAL_VERIFICATION rows across 2001-2008 & 2010.")

        profile_lookup = _load_profile_lookup(session)
        print(f"Loaded {len(profile_lookup.by_player)} verified PROFILE pitcher records for reference.")

        with_prof, reset_cnt, skipped_ambiguous, skipped_no_evidence, backups = _apply_pitching_repairs(
            session,
            rows,
            profile_lookup,
            apply_changes=apply_changes,
        )
        changed_values = sum(row["would_change"] for row in backups)
        no_op_matches = sum(
            row["resolution"] in {"exact_player_id", "unique_name"} and not row["would_change"] for row in backups
        )

        if report_path is not None:
            report_file = Path(report_path)
            report_file.parent.mkdir(parents=True, exist_ok=True)
            report_file.write_text(
                json.dumps(
                    {
                        "mode": "apply" if apply_changes else "dry-run",
                        "total_rows": len(rows),
                        "profile_records": len(profile_lookup.by_player),
                        "mapped_to_profile": with_prof,
                        "changed_profile_values": changed_values,
                        "no_op_profile_matches": no_op_matches,
                        "reset_to_zero": reset_cnt,
                        "ambiguous_skipped": skipped_ambiguous,
                        "no_evidence_skipped": skipped_no_evidence,
                        "rows": backups,
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            print(f"Candidate report written to {report_file}")

        if apply_changes:
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_file = BACKUP_DIR / f"pre_pitching_repair_backup_{stamp}.json"
            backup_file.write_text(json.dumps(backups, indent=2, ensure_ascii=False), encoding="utf-8")
            session.commit()
            print(
                f"\nSUCCESS: Changed {changed_values + reset_cnt} of {len(rows)} "
                f"player_season_pitching rows ({with_prof} matched to PROFILE, "
                f"{changed_values} value changes, {no_op_matches} no-op matches, "
                f"{reset_cnt} reset to 0-0-0, {skipped_ambiguous} ambiguous skipped, "
                f"{skipped_no_evidence} no-evidence skipped). "
                f"Backup saved to {backup_file}"
            )
        else:
            print(
                f"\nDRY-RUN: Would change {changed_values + reset_cnt} of {len(rows)} "
                f"player_season_pitching rows ({with_prof} matched to PROFILE, "
                f"{changed_values} value changes, {no_op_matches} no-op matches, "
                f"{reset_cnt} reset to 0-0-0, {skipped_ambiguous} ambiguous skipped, "
                f"{skipped_no_evidence} no-evidence skipped). "
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
    parser.add_argument("--report", type=Path, default=None, help="Write an auditable JSON candidate report")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    repair_pitching_season_records(
        db_url=args.db_url,
        apply_changes=args.apply and not args.dry_run,
        report_path=args.report,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
