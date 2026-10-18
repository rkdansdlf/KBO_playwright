"""Restore `game` parent rows that were deleted while their metadata survived.

Why this exists
---------------
The 1995 season holds 497 `game` rows. The 10-12 dump holds 504. The seven
missing rows are every game on 1995-06-20 and 1995-06-21 -- whole dates, not a
subset -- and their `game_metadata` rows are all still present.

That shape is the evidence. An `ON DELETE CASCADE` delete of a parent row takes
its children with it, so a surviving `game_metadata` row means the delete
targeted `game` alone. The repository has exactly two places that delete from
`game`: `scripts/historical/apply_season_to_db.py` (whole season, delete then
re-insert) and `src/cli/sync/sync_sqlite_to_postgres.py` (whole table). Neither
deletes seven rows on two dates, and neither ran in that window. The
orphan-cleanup path (`src/maintenance/orchestrator.py`) deletes child rows and
never touches `game`, which rules it out.

So the rows are restored rather than declared intentional cleanup: the answer
set that wrote them is on disk, they were never absent from a dump that predates
the loss, and no procedure explains their removal.

What it does not do
-------------------
It does not re-derive the status. `--status` defaults to ``CANCELLED``, which is
what the 10-12 snapshot recorded for these rows. Restoring them as ``COMPLETED``
would re-run the exact transition that the 2026-09-23 sweep performed on 2,848
rows, so the safer value is the one the last dump observed.

Safety
------
Dry-run by default. Writing needs ``--apply`` and ``KBO_ALLOW_HISTORICAL_RESTORE=1``.
Ids already present are skipped, so re-running writes nothing.

Usage
-----
    python3 -m scripts.maintenance.restore_deleted_game_rows --year 1995
    python3 -m scripts.maintenance.restore_deleted_game_rows --date 1995-06-20 --date 1995-06-21
    python3 -m scripts.maintenance.restore_deleted_game_rows --year 1995 --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import select

from scripts.historical.apply_season_to_db import (
    franchise_map,
    load_answer_set,
    plan_rows,
)
from src.models.game import Game, GameMetadata
from src.utils.game_status import GAME_STATUS_CANCELLED

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Both flags are required. The environment gate keeps a shell history or a
#: scheduled job from reintroducing the rows without a deliberate act.
APPLY_FLAG = "--apply"
ENV_GATE = "KBO_ALLOW_HISTORICAL_RESTORE"

#: Seasons this script may touch. Answer sets exist for exactly these years.
SUPPORTED_YEARS = range(1982, 2001)


@dataclass
class RestoreRow:
    """One game row the answer set knows about."""

    game_id: str
    game_date: date
    away_team: str | None
    home_team: str | None
    away_score: int | None
    home_score: int | None
    stadium: str | None
    game_status: str


@dataclass
class RestoreReport:
    """Summarize what a restore run examined and would write."""

    year: int
    answer_set_games: int = 0
    missing_in_db: int = 0
    restored: int = 0
    already_present: int = 0
    metadata_missing: list[str] = field(default_factory=list)
    rows: list[RestoreRow] = field(default_factory=list)
    applied: bool = False

    def to_dict(self) -> dict[str, object]:
        """Serialize the report."""
        payload = asdict(self)
        payload["rows"] = [asdict(row) | {"game_date": row.game_date.isoformat()} for row in self.rows]
        return payload


def _answer_rows(session: Session, year: int, *, status: str) -> list[RestoreRow]:
    """Build the answer-set rows for a season.

    Args:
        session: DB Session, used only for the franchise map.
        year: Season year.
        status: Status to give every restored row.

    Returns:
        One row per game in the season's answer set.

    """
    games = load_answer_set(year)
    fids = franchise_map(session, year)
    return [
        RestoreRow(
            game_id=str(planned["game_id"]),
            game_date=planned["game_date"],  # type: ignore[arg-type]
            away_team=planned["away_team"],
            home_team=planned["home_team"],
            away_score=planned["away_score"],
            home_score=planned["home_score"],
            stadium=planned["stadium"],
            game_status=status,
        )
        for planned in plan_rows(games, year, fids)
    ]


def _existing_ids(session: Session, game_ids: Sequence[str]) -> set[str]:
    """Return which of the given ids already exist."""
    if not game_ids:
        return set()
    rows = session.execute(select(Game.game_id).where(Game.game_id.in_(list(game_ids)))).scalars().all()
    return {str(value) for value in rows}


def _metadata_ids(session: Session, game_ids: Sequence[str]) -> set[str]:
    """Return which of the given ids already have a metadata row."""
    if not game_ids:
        return set()
    rows = session.execute(select(GameMetadata.game_id).where(GameMetadata.game_id.in_(list(game_ids)))).scalars().all()
    return {str(value) for value in rows}


def _planned_row(row: RestoreRow) -> dict[str, object]:
    """Shape one answer-set row for insertion.

    Franchise ids are left unset on purpose: the answer set carries none, and a
    guessed id is worse than a null one that the dedup pass can still resolve.

    Args:
        row: Answer-set row.

    Returns:
        Column mapping for the insert.

    """
    return {
        "game_id": row.game_id,
        "game_date": row.game_date,
        "away_team": row.away_team,
        "home_team": row.home_team,
        "away_score": row.away_score,
        "home_score": row.home_score,
        "stadium": row.stadium,
        "game_status": row.game_status,
        "season_id": row.game_date.year * 100,
        "is_primary": True,
    }


def restore_year(
    session: Session,
    year: int,
    *,
    only_dates: Sequence[date] | None = None,
    status: str = GAME_STATUS_CANCELLED,
    apply: bool = False,
) -> RestoreReport:
    """Report, and optionally perform, the restore of one season's missing rows.

    Args:
        session: DB Session.
        year: Season year.
        only_dates: Restrict to these game dates when given.
        status: Status for the restored rows.
        apply: Persist the insert instead of previewing it.

    Returns:
        The report, including the rows that would be written.

    """
    report = RestoreReport(year=year, applied=apply)
    candidates = _answer_rows(session, year, status=status)
    if only_dates is not None:
        wanted = set(only_dates)
        candidates = [row for row in candidates if row.game_date in wanted]
    report.answer_set_games = len(candidates)

    present = _existing_ids(session, [row.game_id for row in candidates])
    report.already_present = len(present)
    missing = [row for row in candidates if row.game_id not in present]
    report.missing_in_db = len(missing)
    if not missing:
        return report

    report.metadata_missing = sorted(
        {row.game_id for row in missing} - _metadata_ids(session, [row.game_id for row in missing]),
    )
    report.rows = missing
    if not apply:
        return report

    session.execute(Game.__table__.insert(), [_planned_row(row) for row in missing])
    session.commit()
    report.restored = len(missing)
    return report


def _open_session(db_url: str | None):
    """Return a session on the configured database."""
    if db_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as _Session

        engine = create_engine(db_url)
        return _Session(engine)
    from src.db.engine import SessionLocal

    return SessionLocal()


def _denied_reason(apply: bool) -> str | None:
    """Return why the write is refused, or ``None`` when it is allowed."""
    if not apply:
        return "dry_run"
    if os.getenv(ENV_GATE) != "1":
        return f"{ENV_GATE} is not set to 1"
    return None


def main(argv: Sequence[str] | None = None) -> int:
    """Preview or perform the restore.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, help="Season year to restore")
    parser.add_argument(
        "--date",
        action="append",
        default=[],
        metavar="YYYY-MM-DD",
        help="Restrict to one game date; repeatable",
    )
    parser.add_argument("--status", default=GAME_STATUS_CANCELLED, help="Status for restored rows")
    parser.add_argument("--apply", action="store_true", help="Write; dry-run is the default")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument("--db-url", default=None, help="Target database URL (default: SessionLocal)")
    args = parser.parse_args(argv)

    if args.year is None and not args.date:
        sys.stderr.write("restore_error: pass --year or at least one --date\n")
        return 2
    if args.year is not None and args.year not in SUPPORTED_YEARS:
        sys.stderr.write(
            f"restore_error: year {args.year} has no answer set (supported {SUPPORTED_YEARS[0]}-{SUPPORTED_YEARS[-1]})\n"
        )
        return 2

    only_dates = [date.fromisoformat(value) for value in args.date]
    years = [args.year] if args.year is not None else sorted({value.year for value in only_dates})
    if args.year is None:
        only_dates = [value for value in only_dates if value.year in years]

    denied = _denied_reason(args.apply)
    if denied is not None and args.apply:
        sys.stderr.write(f"restore_denied: {denied}\n")
        return 3

    reports: list[RestoreReport] = []
    session = _open_session(args.db_url)
    try:
        for year in years:
            reports.append(
                restore_year(
                    session,
                    year,
                    only_dates=only_dates or None,
                    status=args.status,
                    apply=args.apply and denied is None,
                ),
            )
    finally:
        session.close()

    payload = {
        "apply": args.apply,
        "status": args.status,
        "reports": [report.to_dict() for report in reports],
    }
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        for report in reports:
            sys.stdout.write(
                f"{report.year}: answer_set={report.answer_set_games} "
                f"present={report.already_present} missing={report.missing_in_db} "
                f"restored={report.restored} metadata_missing={len(report.metadata_missing)}\n",
            )
            for row in report.rows:
                sys.stdout.write(
                    f"    {row.game_id}  {row.game_date}  {row.away_team}@{row.home_team}  {row.away_score}-{row.home_score}\n"
                )
    if denied is not None and args.apply:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
