"""Report the status distribution of seasons whose boxscore bodies were never published.

Why this report exists
----------------------
1982~2000 hold 8,471 game rows and not one batter, pitcher, inning or lineup row.
That is a source limitation, not a collection gap, and the two look identical to
a status query -- until a pipeline rewrites the status to record the failure.

A date-ordered sweep on 2026-09-23 did exactly that to 2,848 rows, and the
result is now the shape a reader has to interpret:

* dates up to 1995-06-23 read ``CANCELLED``
* dates from 1995-06-24 read ``UNRESOLVED_MISSING``
* 241 duplicated 1982 pairs read ``CANCELLED`` on both sides

The boundary is a calendar edge, not a fact about baseball: no game was
cancelled on 1995-06-23 because a crawl ran out of dates. So the era reads as
six seasons in one status and thirteen in another, which no gate flagged,
because each row was individually well-formed.

This report is read-only and names the rows that need a human decision instead
of an automatic repair. It writes nothing, and it does not propose a status.

Usage
-----
    python3 -m src.cli.reports.report_source_limited_status
    python3 -m src.cli.reports.report_source_limited_status --json --output report.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

#: Oldest season with no published boxscore bodies. Measured 2026-10-12.
SOURCE_LIMITED_START = 1982

#: First season whose bodies were collected.
COLLECTED_START = 2001

#: The label attached to every row this report lists.
MANUAL_REASON = "MANUAL_BASEBALL_DATA_REQUIRED"

#: Why no automatic repair is offered. The 10-12 snapshot and the live rows
#: disagree about what these rows used to say, so there is no agreed "before" to
#: restore to. See WINDOWS_DB_HOST_RECOVERY.md 9.9.
BASELINE_UNCONFIRMED = "baseline_status_unconfirmed_pending_dump_comparison"


def _season_bounds() -> tuple[date, date]:
    """Return the inclusive start and exclusive end of the source-limited era."""
    return date(SOURCE_LIMITED_START, 1, 1), date(COLLECTED_START, 1, 1)


def _status_rows(session: Session, start: date, end: date) -> dict[str, int]:
    """Return status counts for the era."""
    rows = session.execute(
        select(Game.game_status, func.count())
        .where(Game.game_date >= start, Game.game_date < end)
        .group_by(Game.game_status)
        .order_by(func.count().desc()),
    ).all()
    return {str(status or "<NULL>"): int(count) for status, count in rows}


def _season_status_rows(session: Session, start: date, end: date) -> list[dict[str, Any]]:
    """Return per-season status counts for the era."""
    rows = session.execute(
        select(
            func.extract("year", Game.game_date).label("season"),
            Game.game_status,
            func.count().label("rows"),
        )
        .where(Game.game_date >= start, Game.game_date < end)
        .group_by("season", Game.game_status)
        .order_by("season"),
    ).all()
    by_season: dict[int, dict[str, int]] = {}
    for season, status, count in rows:
        by_season.setdefault(int(season), {})[str(status or "<NULL>")] = int(count)
    return [
        {"season": season, "status_counts": counts, "rows": sum(counts.values())}
        for season, counts in sorted(by_season.items())
    ]


def _body_counts(session: Session, start: date, end: date) -> dict[str, int]:
    """Return how many body rows exist in the era, per child table.

    This is the evidence behind the source-limitation claim, so it is reported
    rather than asserted.
    """
    game_ids = select(Game.game_id).where(Game.game_date >= start, Game.game_date < end)
    counts = {"games": int(session.scalar(select(func.count()).select_from(game_ids.subquery())) or 0)}
    for label, model in (
        ("game_metadata", GameMetadata),
        ("game_inning_scores", GameInningScore),
        ("game_lineups", GameLineup),
        ("game_batting_stats", GameBattingStat),
        ("game_pitching_stats", GamePitchingStat),
    ):
        counts[label] = int(
            session.scalar(select(func.count()).select_from(model).where(model.game_id.in_(game_ids))) or 0,
        )
    return counts


def _sweep_boundaries(rows: Sequence[tuple[int, date, str]]) -> list[dict[str, Any]]:
    """Return the date edges where a season's status changes and never changes back.

    This is the sweep signature. Baseball does not run out of cancelled games on
    a particular Tuesday: a status that holds for every earlier date in a season
    and a different one for every later date is a crawl that walked the calendar
    and ran out of road, not a season that changed character mid-year.
    """
    by_season: dict[int, list[tuple[date, str]]] = {}
    for season, game_date, status in rows:
        by_season.setdefault(season, []).append((game_date, status))

    boundaries: list[dict[str, Any]] = []
    for season, entries in sorted(by_season.items()):
        entries.sort()
        segments: list[tuple[date, str, int]] = []
        for game_date, status in entries:
            if segments and segments[-1][1] == status:
                segments[-1] = (segments[-1][0], status, segments[-1][2] + 1)
            else:
                segments.append((game_date, status, 1))
        # One contiguous run per status means the calendar, not the season, is
        # what the status follows.
        statuses = {segment[1] for segment in segments}
        if len(statuses) > 1:
            boundaries.append(
                {
                    "season": season,
                    "segments": [
                        {"status": status, "rows": count, "from": segment[0].isoformat()}
                        for segment, status, count in ((s, s[1], s[2]) for s in segments)
                    ],
                    "statuses_in_date_order": [segment[1] for segment in segments],
                },
            )
    return boundaries


def _asymmetry(seasons: Sequence[dict[str, Any]], boundaries: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Return the signatures that say a sweep, not a season, chose these statuses.

    Two independent shapes are checked because a sweep can leave either:

    - across seasons: neighbouring seasons carrying different statuses
    - within a season: one status for the early dates and another for the late
      ones, which is what the 1995-06-23/24 cut looks like
    """
    if not seasons:
        return {"observed": False, "reason": "no rows in the source-limited era"}
    signatures = {tuple(sorted(season["status_counts"].items())) for season in seasons}
    return {
        "observed": len(signatures) > 1 or bool(boundaries),
        "across_seasons": len(signatures) > 1,
        "within_season_boundaries": list(boundaries),
        "distinct_status_shapes": len(signatures),
        "seasons": [season["season"] for season in seasons],
    }


def _dated_status_rows(session: Session, start: date, end: date) -> list[tuple[int, date, str]]:
    """Return every era row as (season, game_date, status), oldest first."""
    rows = session.execute(
        select(Game.game_date, Game.game_status)
        .where(Game.game_date >= start, Game.game_date < end)
        .order_by(Game.game_date),
    ).all()
    return [(game_date.year, game_date, str(status or "<NULL>")) for game_date, status in rows]


def build_report(session: Session) -> dict[str, Any]:
    """Build the read-only report for the source-limited era.

    Args:
        session: DB Session.

    Returns:
        The report payload.

    """
    start, end = _season_bounds()
    seasons = _season_status_rows(session, start, end)
    statuses = _status_rows(session, start, end)
    boundaries = _sweep_boundaries(_dated_status_rows(session, start, end))
    return {
        "era": f"{start.year}-{end.year - 1}",
        "read_only": True,
        "status_counts": statuses,
        "by_season": seasons,
        "body_rows": _body_counts(session, start, end),
        "asymmetry": _asymmetry(seasons, boundaries),
        "manual_reason": MANUAL_REASON,
        "baseline_status": BASELINE_UNCONFIRMED,
        "automatic_repair": False,
        "note": (
            "Rows here carry a score and no body. That is what the source published, "
            "so the status must not be used to record a crawl failure. Which status "
            "is correct for each row is a source decision, not a derived one."
        ),
    }


def render(payload: dict[str, Any]) -> str:
    """Render the report as text.

    Args:
        payload: Report payload.

    Returns:
        Human-readable lines.

    """
    lines = [
        f"Source-limited era {payload['era']} (read-only)",
        "",
        "Status distribution:",
    ]
    for status, count in payload["status_counts"].items():
        lines.append(f"  {status:22} {count:>7}")
    lines.append("")
    lines.append("Body rows present (the evidence for 'source limited'):")
    for label, count in payload["body_rows"].items():
        lines.append(f"  {label:22} {count:>7}")
    asymmetry = payload["asymmetry"]
    lines.append("")
    if asymmetry.get("observed"):
        lines.append("ASYMMETRY: the era's statuses do not follow the baseball.")
        for boundary in asymmetry.get("within_season_boundaries", []):
            lines.append(
                f"  {boundary['season']}: status changes in date order "
                f"{' -> '.join(boundary['statuses_in_date_order'])}",
            )
            lines.extend(
                f"      {segment['from']} onward: {segment['status']} ({segment['rows']} rows)"
                for segment in boundary["segments"]
            )
        if asymmetry.get("across_seasons"):
            lines.append(
                f"  {asymmetry['distinct_status_shapes']} distinct status shapes across "
                f"{len(asymmetry['seasons'])} seasons.",
            )
        lines.append("  A status boundary at a calendar edge is a sweep signature, not a baseball fact.")
    else:
        lines.append(f"No asymmetry detected: {asymmetry.get('reason', 'uniform')}")
    lines.append("")
    lines.append(f"Manual reason: {payload['manual_reason']}")
    lines.append(f"Baseline status: {payload['baseline_status']}")
    lines.append("Automatic repair: disabled")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Print the report.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description="Report source-limited game statuses")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Print JSON")
    parser.add_argument("--output", type=Path, help="Write the JSON report to this file")
    parser.add_argument("--database-url", default=None, help="Target database URL (default: SessionLocal)")
    args = parser.parse_args(argv)

    if args.database_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as _Session

        session = _Session(create_engine(args.database_url))
    else:
        from src.db.engine import SessionLocal

        session = SessionLocal()
    try:
        payload = build_report(session)
    finally:
        session.close()

    if args.output:
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(render(payload) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
