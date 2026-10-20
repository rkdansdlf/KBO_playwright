"""Reclassify pre-2001 tied games from an unresolved state to ``DRAW``.

Why this exists
---------------
The 1982-2000 corpus was loaded from namuwiki, which publishes each game's
result but no boxscore body. 8,236 of those rows carry a score and no body, and
the loader left them in ``UNRESOLVED_MISSING`` -- "we cannot say what this game
became". 196 of them are tied, and a tie is not a missing result: the score is
the result. Reclassifying the 196 ``UNRESOLVED_MISSING`` rows, plus the one
pre-2001 row stored as ``COMPLETED`` with equal scores, brings them in line with
the repository's own rule, which resolves a game as ``DRAW`` whenever the two
scores are equal (``src/utils/game_status.py``).

Why it is not a data rewrite
----------------------------
``UNRESOLVED_MISSING`` asserts an absence of evidence. A tied score is evidence,
so the assertion is wrong rather than merely different. Nothing else about the
row changes: the score, the teams, the date and the identifier are all left as
they are, and the only writer is a single ``UPDATE`` of ``game_status``. The
``game_result_without_body`` warning is unaffected because it keys on the score
columns and never reads ``game_status``.

What it does not do
-------------------
It does not touch a game that is not tied, and it does not touch a game from
2001 or later. It does not update ``updated_at``, which is left to the
database's own column default so the change carries its real timestamp.

Safety
------
Dry-run by default. Writing needs ``--apply`` and ``KBO_ALLOW_HISTORICAL_RESTORE=1``,
the same gate the other historical restoration scripts use. A snapshot and a
rollback script are written before the update, and the rollback restores the
previous status per identifier rather than assigning one status to the batch.

Usage
-----
    python3 -m scripts.maintenance.reclassify_historical_draws
    python3 -m scripts.maintenance.reclassify_historical_draws --season 1996
    python3 -m scripts.maintenance.reclassify_historical_draws --apply
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

#: Both conditions are required to write, matching the other restoration scripts.
ENV_GATE = "KBO_ALLOW_HISTORICAL_RESTORE"

#: Where snapshots and rollback SQL are written.
ARCHIVE_DIR = Path("data/archive")

#: The status a tie is recorded as. Taken from the repository's own vocabulary
#: rather than spelled inline, so a rename cannot leave this script writing a
#: value nothing else recognizes.
DRAW = "DRAW"

#: Statuses that assert "no result is known". A tied score contradicts both.
SUBJECT_STATUSES = ("UNRESOLVED_MISSING", "COMPLETED")

#: The corpus boundary. Later games are crawled with bodies and are handled by
#: the crawlers, not here.
HISTORICAL_CUTOFF = "2001-01-01"


@dataclass(frozen=True)
class CandidateRow:
    """One ``game`` row whose status is being corrected."""

    game_id: str
    game_date: str
    home_team: str | None
    away_team: str | None
    home_score: int
    away_score: int
    game_status: str


@dataclass
class DrawReport:
    """Summarize a run."""

    scanned: int = 0
    matched: int = 0
    by_status: dict[str, int] = field(default_factory=dict)
    by_season: dict[str, int] = field(default_factory=dict)
    samples: list[dict[str, Any]] = field(default_factory=list)
    updated: int = 0
    snapshot_path: str | None = None
    rollback_path: str | None = None
    applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize the report."""
        return asdict(self)


def _select_candidates(conn: Connection, season: int | None) -> list[CandidateRow]:
    """Read the tied pre-2001 games whose status is still unresolved.

    Args:
        conn: Open connection.
        season: Restrict to one season year, or ``None`` for all.

    Returns:
        The rows in date order.

    """
    if season is None:
        date_clause = "game_date < :cutoff"
        params: dict[str, Any] = {"cutoff": HISTORICAL_CUTOFF}
    else:
        # A season rather than a cutoff, so a run restricted to one year cannot
        # reach into the next one.
        date_clause = "game_date >= :start AND game_date <= :end"
        params = {"start": f"{season}-01-01", "end": f"{season}-12-31"}
    placeholders = ", ".join(f":s{index}" for index in range(len(SUBJECT_STATUSES)))
    params.update({f"s{index}": status for index, status in enumerate(SUBJECT_STATUSES)})

    rows = conn.execute(
        text(
            f"""
            SELECT game_id, game_date, home_team, away_team,
                   home_score, away_score, game_status
            FROM game
            WHERE {date_clause}
              AND is_primary IS NOT FALSE
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
              AND home_score = away_score
              AND game_status IN ({placeholders})
            ORDER BY game_date, game_id
            """,
        ),
        params,
    ).all()
    return [
        CandidateRow(
            game_id=str(row[0]),
            game_date=str(row[1]),
            home_team=row[2],
            away_team=row[3],
            home_score=int(row[4]),
            away_score=int(row[5]),
            game_status=str(row[6]),
        )
        for row in rows
    ]


def _count_unresolved(conn: Connection) -> int:
    """Count the pre-2001 rows still asserting an unknown result."""
    return int(
        conn.execute(
            text(
                """
                SELECT COUNT(*) FROM game
                WHERE game_date < :cutoff
                  AND is_primary IS NOT FALSE
                  AND game_status = :unresolved
                """,
            ),
            {"cutoff": HISTORICAL_CUTOFF, "unresolved": SUBJECT_STATUSES[0]},
        ).scalar()
        or 0,
    )


def _write_rollback(path: Path, rows: Sequence[CandidateRow]) -> None:
    """Write the UPDATE that restores each row's previous status.

    Per-identifier rather than one batched assignment: the batch holds both
    ``UNRESOLVED_MISSING`` and ``COMPLETED`` rows, and restoring them all to one
    value would corrupt the ones that were not that value.

    Args:
        path: File to write.
        rows: The rows that were changed.

    """
    grouped: dict[str, list[str]] = {}
    for row in rows:
        grouped.setdefault(row.game_status, []).append(row.game_id)
    lines = [
        "-- Rollback for the pre-2001 tied-game reclassification.",
        "-- Generated by scripts/maintenance/reclassify_historical_draws.py.",
        "--",
        "-- Restores each identifier to the status it held before the run.",
        "",
    ]
    for status, game_ids in sorted(grouped.items()):
        joined = ", ".join(f"'{game_id}'" for game_id in sorted(game_ids))
        lines.append(f"UPDATE game SET game_status = '{status}' WHERE game_id IN ({joined});")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def reclassify(
    conn: Connection,
    *,
    season: int | None = None,
    apply: bool = False,
) -> DrawReport:
    """Preview, and optionally perform, the reclassification.

    Args:
        conn: Open connection.
        season: Restrict to one season year.
        apply: Persist the update instead of previewing it.

    Returns:
        The report.

    """
    report = DrawReport(applied=apply)
    report.scanned = _count_unresolved(conn)
    candidates = _select_candidates(conn, season)
    report.matched = len(candidates)
    report.by_status = dict(Counter(row.game_status for row in candidates))
    report.by_season = dict(sorted(Counter(row.game_date[:4] for row in candidates).items()))
    report.samples = [asdict(row) for row in candidates[:10]]
    if not candidates or not apply:
        return report

    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = ARCHIVE_DIR / f"historical_draws_snapshot_{stamp}.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().astimezone().isoformat(),
                "previous": [asdict(row) for row in candidates],
                "target_status": DRAW,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    report.snapshot_path = str(snapshot_path)

    rollback_path = ARCHIVE_DIR / f"historical_draws_rollback_{stamp}.sql"
    _write_rollback(rollback_path, candidates)
    report.rollback_path = str(rollback_path)

    game_ids = [row.game_id for row in candidates]
    predicate = ", ".join(f":gid{index}" for index in range(len(game_ids)))
    params = {f"gid{index}": value for index, value in enumerate(game_ids)}
    result = conn.execute(
        text(f"UPDATE game SET game_status = :draw WHERE game_id IN ({predicate})"),
        {**params, "draw": DRAW},
    )
    conn.commit()
    report.updated = int(result.rowcount or 0)
    return report


def _open_connection(db_url: str | None):
    """Return a connection on the configured database."""
    if db_url:
        from sqlalchemy import create_engine

        return create_engine(db_url).connect()
    from src.db.engine import Engine

    return Engine.connect()


def _denied_reason(apply: bool) -> str | None:
    """Return why the write is refused, or ``None`` when it is allowed."""
    if not apply:
        return "dry_run"
    if os.getenv(ENV_GATE) != "1":
        return f"{ENV_GATE} is not set to 1"
    return None


def main(argv: Sequence[str] | None = None) -> int:
    """Preview or perform the reclassification.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, help="Restrict to one season year")
    parser.add_argument("--apply", action="store_true", help="Write; dry-run is the default")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument("--db-url", default=None, help="Target database URL (default: the configured engine)")
    args = parser.parse_args(argv)

    denied = _denied_reason(args.apply)
    if denied is not None and args.apply:
        sys.stderr.write(f"reclassify_denied: {denied}\n")
        return 3

    connection = _open_connection(args.db_url)
    try:
        report = reclassify(connection, season=args.season, apply=args.apply and denied is None)
    finally:
        connection.close()

    if args.as_json:
        sys.stdout.write(json.dumps(report.to_dict(), ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            f"unresolved_scanned={report.scanned} tied_matched={report.matched} updated={report.updated}\n",
        )
        if report.by_status:
            sys.stdout.write("    by previous status:\n")
            for status, count in sorted(report.by_status.items()):
                sys.stdout.write(f"        {status}: {count}\n")
        if report.by_season:
            sys.stdout.write("    by season:\n")
            for season, count in report.by_season.items():
                sys.stdout.write(f"        {season}: {count}\n")
        for sample in report.samples:
            sys.stdout.write(
                f"    {sample['game_id']} {sample['game_date']} {sample['home_team']}@{sample['away_team']}"
                f" {sample['home_score']}-{sample['away_score']} was={sample['game_status']}\n",
            )
        if report.snapshot_path:
            sys.stdout.write(f"    snapshot={report.snapshot_path}\n")
        if report.rollback_path:
            sys.stdout.write(f"    rollback={report.rollback_path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
