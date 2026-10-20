"""Merge the 1982 duplicate game pairs, then delete the losing half.

Why a merge and not a delete
----------------------------
1982 stores 482 rows for 241 games. Every game appears twice, under game ids
that put the two teams in opposite order, because the KBO schedule crawl and the
namu.wiki answer-set apply each wrote one row and
``scripts/historical/apply_season_to_db.py`` only removed one of the pair.

The two halves are not redundant. Measured across all 241 pairs on 2026-10-14:

| field | pairs where the two rows differ |
| --- | --- |
| ``winning_score`` | 240 (only the secondary has one) |
| ``away_franchise_id`` | 202 |
| ``home_franchise_id`` | 199 |
| ``winning_franchise_id`` | 194 |
| ``game_metadata`` | 241 rows on the primary, 0 on the secondary |
| ``winning_team`` / ``stadium`` / ``season_id`` / ``game_status`` | 0 -- identical |

No pair has one of the four merge fields set on both sides to different values,
so the merge never has to choose between two answers.

Deleting the ``is_primary = false`` rows without merging would discard 240 win
scores and 595 franchise ids, and nothing can recover them: the answer set that
wrote them carries no franchise column and the boxscore source has no per-player
body.

Order of operations
-------------------
Merge, verify, then delete -- in one transaction. The verification re-reads the
merged row and refuses the delete if any field the secondary uniquely held did
not land, so a partial merge cannot pass as a completed one.

Safety
------
Dry-run by default. Writing needs ``--apply`` and ``KBO_ALLOW_1982_DEDUP=1``. A
snapshot of both halves is written to ``data/archive/`` before the first write.

Usage
-----
    python3 -m scripts.maintenance.merge_1982_duplicate_games
    python3 -m scripts.maintenance.merge_1982_duplicate_games --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from src.constants import KST
from src.models.game import Game

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Both flags are required to write.
APPLY_FLAG = "--apply"
ENV_GATE = "KBO_ALLOW_1982_DEDUP"

#: Season this script may touch. Hard-coded rather than parameterized because the
#: pairing rule below is specific to the 1982 duplication: it relies on exactly
#: two rows per (date, unordered teams, unordered scores), which is what the
#: MBC/MB segment collision produced and what no later season has.
TARGET_SEASON = 1982

#: Fields copied from the secondary row when the primary has none.
#:
#: Every entry was measured across all 241 pairs on 2026-10-14: the value sits on
#: the secondary only, and no pair has the field set on both sides to different
#: values. ``winning_franchise_id`` was added after the first draft of this list
#: omitted it and would have dropped 194 franchise ids -- a field nobody reads
#: until the day somebody does, which is the worst time to find out.
#:
#: ``game_metadata`` is not here on purpose: it is keyed by ``game_id`` and
#: already sits on the primary, so there is nothing to move and a copy attempt
#: would create a duplicate.
MERGED_FIELDS = (
    "winning_score",
    "home_franchise_id",
    "away_franchise_id",
    "winning_franchise_id",
)

SNAPSHOT_DIR = Path("data/archive")


@dataclass
class PairPlan:
    """One duplicate pair and the fields the merge will carry over."""

    primary_game_id: str
    secondary_game_id: str
    game_date: date
    fields_to_move: dict[str, Any] = field(default_factory=dict)
    fields_shared: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Serialize the plan for the report."""
        return asdict(self) | {"game_date": self.game_date.isoformat()}


@dataclass
class MergeReport:
    """Summarize the merge of one season."""

    season: int
    total_rows: int = 0
    pairs: int = 0
    unmatched_dates: list[str] = field(default_factory=list)
    plans: list[PairPlan] = field(default_factory=list)
    moved_fields: int = 0
    merged: bool = False
    deleted: int = 0
    applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize the report."""
        return asdict(self) | {"plans": [plan.to_dict() for plan in self.plans]}


def _pair_key(game: Game) -> tuple[Any, ...]:
    """Return the identity two rows share when they describe the same match.

    The game ids disagree about which team is away, so the key uses the
    unordered team pair and the unordered score pair.
    """
    return (
        game.game_date,
        min(str(game.away_team or ""), str(game.home_team or "")),
        max(str(game.away_team or ""), str(game.home_team or "")),
        min(
            game.away_score if game.away_score is not None else -1,
            game.home_score if game.home_score is not None else -1,
        ),
        max(
            game.away_score if game.away_score is not None else -1,
            game.home_score if game.home_score is not None else -1,
        ),
    )


def _load_season(session: Session, season: int) -> list[Game]:
    """Load every game row of one season."""
    return list(
        session.execute(select(Game).where(Game.season_id == season * 100).order_by(Game.game_id)).scalars().all(),
    )


def _load_by_game_id(session: Session, game_id: str) -> Game:
    """Load one game by its natural key.

    ``Game`` is keyed by a surrogate ``id``; ``game_id`` is a separate column, so
    ``session.get`` would look the row up by a number it does not have.

    Args:
        session: DB Session.
        game_id: Natural key.

    Returns:
        The game row.

    Raises:
        LookupError: When no row carries that ``game_id``.

    """
    row = session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
    if row is None:
        msg = f"no game row for game_id={game_id}"
        raise LookupError(msg)
    return row


def build_pair_plans(games: Sequence[Game], report: MergeReport) -> list[PairPlan]:
    """Group the season's rows into duplicate pairs.

    Args:
        games: Season rows.
        report: Report the unmatched groups are recorded on.

    Returns:
        One plan per group that holds exactly one primary and one secondary.

    """
    groups: dict[tuple[Any, ...], list[Game]] = {}
    for game in games:
        groups.setdefault(_pair_key(game), []).append(game)

    plans: list[PairPlan] = []
    for key, members in sorted(groups.items(), key=lambda item: str(item[0])):
        primaries = [game for game in members if game.is_primary]
        secondaries = [game for game in members if not game.is_primary]
        if len(primaries) == 1 and len(secondaries) == 1 and len(members) == 2:
            primary, secondary = primaries[0], secondaries[0]
            plans.append(
                PairPlan(
                    primary_game_id=primary.game_id,
                    secondary_game_id=secondary.game_id,
                    game_date=key[0],
                    fields_to_move={
                        name: getattr(secondary, name)
                        for name in MERGED_FIELDS
                        if getattr(primary, name, None) is None and getattr(secondary, name, None) is not None
                    },
                    fields_shared=tuple(
                        name
                        for name in ("winning_team", "stadium", "season_id", "game_status")
                        if getattr(primary, name, None) == getattr(secondary, name, None)
                    ),
                ),
            )
        else:
            report.unmatched_dates.append(
                f"{key[0]}:{len(members)}rows:{len(primaries)}primary:{len(secondaries)}secondary"
            )
    return plans


def _write_snapshot(plans: Sequence[PairPlan], games: Sequence[Game], target: Path) -> Path:
    """Write both halves of every pair before anything is changed."""
    target.parent.mkdir(parents=True, exist_ok=True)
    by_id = {game.game_id: game for game in games}
    snapshot_fields = ("game_id", "game_date", "winning_team", *MERGED_FIELDS)
    payload = [
        {
            "primary": {name: getattr(by_id[plan.primary_game_id], name) for name in snapshot_fields},
            "secondary": {name: getattr(by_id[plan.secondary_game_id], name) for name in snapshot_fields},
        }
        for plan in plans
    ]
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    return target


def _verify_merge(session: Session, plans: Sequence[PairPlan]) -> list[str]:
    """Return the fields that did not land, so the delete can be refused.

    The verification columns are derived from ``MERGED_FIELDS`` rather than
    listed again. A second list would drift the moment a field is added, and the
    drift fails as a ``KeyError`` on the column that was just merged -- an
    unrelated-looking traceback pointing at the safety check instead of at the
    edit that caused it.
    """
    rows = (
        session.execute(
            select(Game.game_id, *(getattr(Game, name) for name in MERGED_FIELDS)).where(
                Game.game_id.in_([plan.primary_game_id for plan in plans]),
            ),
        )
        .mappings()
        .all()
    )
    by_id = {row["game_id"]: row for row in rows}
    lost: list[str] = []
    for plan in plans:
        merged = by_id.get(plan.primary_game_id)
        if merged is None:
            lost.append(f"{plan.primary_game_id}:missing_after_merge")
            continue
        for name, expected in plan.fields_to_move.items():
            if merged[name] != expected:
                lost.append(f"{plan.primary_game_id}.{name}:expected={expected!r},got={merged[name]!r}")
    return lost


def merge_season(session: Session, season: int = TARGET_SEASON, *, apply: bool = False) -> MergeReport:
    """Plan, and optionally perform, the merge of one season's duplicate pairs.

    Args:
        session: DB Session.
        season: Season year to merge.
        apply: Persist the merge and the delete.

    Returns:
        The report, including the field moves that were or would be made.

    """
    report = MergeReport(season=season, applied=apply)
    games = _load_season(session, season)
    report.total_rows = len(games)
    report.plans = build_pair_plans(games, report)
    report.pairs = len(report.plans)
    report.moved_fields = sum(len(plan.fields_to_move) for plan in report.plans)
    if report.unmatched_dates:
        # A date whose rows do not pair cleanly is a finding, not a nuisance.
        # Stopping is the only safe response: merging a group of three would
        # pick two of the three by a rule nobody wrote down.
        logger.error(
            "%s date(s) do not pair cleanly; refusing to merge: %s",
            len(report.unmatched_dates),
            report.unmatched_dates[:5],
        )
        return report
    if not apply:
        return report

    for plan in report.plans:
        primary = _load_by_game_id(session, plan.primary_game_id)
        for name, value in plan.fields_to_move.items():
            setattr(primary, name, value)
    session.flush()

    lost = _verify_merge(session, report.plans)
    if lost:
        session.rollback()
        logger.error("Merge verification failed, nothing deleted: %s", lost[:5])
        report.merged = False
        return report

    session.execute(
        Game.__table__.delete().where(Game.game_id.in_([plan.secondary_game_id for plan in report.plans])),
    )
    report.deleted = report.pairs
    session.commit()
    report.merged = True
    return report


def _open_session(db_url: str | None):
    """Return a session on the configured database."""
    if db_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as _Session

        return _Session(create_engine(db_url))
    from src.db.engine import SessionLocal

    return SessionLocal()


def _denied_reason(apply: bool) -> str | None:
    """Return why the write is refused, or ``None`` when it is allowed."""
    if not apply:
        return "dry_run"
    if os.getenv(ENV_GATE) != "1":
        return f"{ENV_GATE} is not set to 1"
    return None


def _season_rows_in_scope(session: Session, season: int) -> int:
    """Return how many rows the season holds, for the pre-run record."""
    return int(
        session.scalar(select(func.count()).select_from(Game).where(Game.season_id == season * 100)) or 0,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Preview or perform the merge.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, default=TARGET_SEASON, help="Season year to merge")
    parser.add_argument("--apply", action="store_true", help="Write; dry-run is the default")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument("--snapshot-dir", default=str(SNAPSHOT_DIR), help="Where the pre-write snapshot goes")
    parser.add_argument("--db-url", default=None, help="Target database URL (default: SessionLocal)")
    args = parser.parse_args(argv)

    if args.season != TARGET_SEASON:
        sys.stderr.write(f"merge_error: this script is specific to {TARGET_SEASON}; --season {args.season} refused\n")
        return 2

    denied = _denied_reason(args.apply)
    if denied is not None and args.apply:
        sys.stderr.write(f"merge_denied: {denied}\n")
        return 3

    session = _open_session(args.db_url)
    try:
        if denied is None and args.apply:
            games = _load_season(session, args.season)
            plans = build_pair_plans(games, MergeReport(season=args.season))
            stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
            snapshot = _write_snapshot(
                plans, games, Path(args.snapshot_dir) / f"{args.season}_merge_snapshot_{stamp}.json"
            )
            logger.info("wrote pre-merge snapshot -> %s (%d pairs)", snapshot, len(plans))
        report = merge_season(session, args.season, apply=args.apply and denied is None)
    finally:
        session.close()

    payload = {"apply": args.apply, "report": report.to_dict()}
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            f"season={report.season} rows={report.total_rows} pairs={report.pairs} "
            f"fields_to_move={report.moved_fields} unmatched={len(report.unmatched_dates)} "
            f"merged={report.merged} deleted={report.deleted}\n",
        )
        for plan in report.plans[:10]:
            sys.stdout.write(f"    {plan.primary_game_id} <- {plan.secondary_game_id}  {plan.fields_to_move}\n")
        if len(report.plans) > 10:
            sys.stdout.write(f"    ... {len(report.plans) - 10} more pairs\n")
    if report.unmatched_dates:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
