"""Delete ``awards`` rows that the current crawler would never produce.

The crawl is the source of truth for what an award row should say. A row whose
``(year, award_type, category, player_name)`` the crawler does not emit -- or a
row that duplicates such a key with a differently spelled ``team_name`` -- is a
leftover: either a mislabelled position from the colspan bug (the second
outfielder was stored as ``지명 타자``) or a second copy created because the two
sources spell the club differently (``KIA`` vs ``KIA 타이거즈``) and
``uq_award_record`` includes ``team_name``.

Deciding *which* rows those are is ``plan_awards_cleanup``'s job (it re-crawls
and compares); this script only executes that decision, reading the ids from the
CSV it wrote. Splitting it that way keeps the destructive half dumb: it deletes
what the file lists and nothing else.

Guard: ``--apply`` **and** ``KBO_ALLOW_AWARDS_CLEANUP=1``. Without both it prints
the plan and exits 3 without writing.

Usage:
    python3 -m scripts.maintenance.cleanup_awards_stale_rows \
        --backup-file data/repair_backups/awards_cleanup_20261104.csv --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.audit_trail import CorrectionAuditTrail
from src.models.award import Award

ALLOW_ENV = "KBO_ALLOW_AWARDS_CLEANUP"
REQUIRED_COLUMNS = {"id", "year", "award_type", "category", "player_name", "team_name", "reason"}


@dataclass(frozen=True)
class Target:
    """One row the planner marked for removal."""

    award_id: int
    year: int
    award_type: str
    category: str
    player_name: str
    team_name: str
    reason: str


def load_targets(path: Path) -> list[Target]:
    """Read the planner's CSV into targets, refusing a file that is not one.

    Args:
        path: CSV written by ``plan_awards_cleanup``.

    Returns:
        The rows to delete.

    """
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            message = f"backup file is missing columns: {sorted(missing)}"
            raise ValueError(message)
        return [
            Target(
                award_id=int(row["id"]),
                year=int(row["year"]),
                award_type=row["award_type"],
                category=row["category"],
                player_name=row["player_name"],
                team_name=row["team_name"],
                reason=row["reason"],
            )
            for row in reader
        ]


def _audit(session: Session, target: Target, backup_file: Path) -> None:
    session.add(
        CorrectionAuditTrail(
            entity_type="award",
            entity_id=str(target.award_id),
            field_name="row_deleted",
            raw_value=f"{target.year} {target.award_type} {target.category} {target.player_name} ({target.team_name})",
            raw_source="awards",
            corrected_value=None,
            corrected_source="cleanup_awards_stale_rows",
            correction_reason=target.reason,
            confidence=1.0,
            extra_metadata={
                "backup_file": str(backup_file),
                "year": target.year,
                "award_type": target.award_type,
            },
            created_at=datetime.now(KST),
            updated_at=datetime.now(KST),
        )
    )


def main(argv: list[str] | None = None) -> int:
    """Delete the planned rows, or print the plan.

    Args:
        argv: Command-line arguments.

    Returns:
        Exit code: 0 on success, 3 when the guard denies the write.

    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backup-file", required=True, type=Path, help="CSV of rows to delete")
    parser.add_argument("--apply", action="store_true", help="Actually delete (needs the env guard)")
    args = parser.parse_args(argv)

    targets = load_targets(args.backup_file)
    if not args.apply or os.environ.get(ALLOW_ENV) != "1":
        print(f"plan: {len(targets)} row(s) would be deleted")
        for t in targets[:10]:
            print(f"   id={t.award_id} {t.year} {t.category!r} {t.player_name!r} ({t.reason})")
        if len(targets) > 10:
            print(f"   ... and {len(targets) - 10} more")
        print(f"\nRefusing to write: pass --apply and set {ALLOW_ENV}=1")
        return 3

    with SessionLocal() as session:
        before = session.execute(select(func.count()).select_from(Award)).scalar_one()
        ids = [t.award_id for t in targets]
        found = session.execute(select(Award.id).where(Award.id.in_(ids))).scalars().all()
        if len(found) != len(ids):
            missing = sorted(set(ids) - set(found))
            message = f"refusing: {len(missing)} id(s) no longer exist, e.g. {missing[:5]}"
            raise SystemExit(message)

        for target in targets:
            _audit(session, target, args.backup_file)
        session.execute(delete(Award).where(Award.id.in_(ids)))
        session.commit()
        after = session.execute(select(func.count()).select_from(Award)).scalar_one()

    print(f"deleted {len(ids)} row(s): {before} → {after}")
    print("audit rows written to correction_audit_trail (entity_type='award', field_name='row_deleted')")
    return 0


if __name__ == "__main__":
    sys.exit(main())
