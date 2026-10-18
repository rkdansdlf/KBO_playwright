"""Repair the season classification (regular / exhibition / postseason) of stored games.

Issue #40: exhibition games stored as regular season (teams over 144 games),
regular-season games stored as a postseason stage, postseason ``kbo_seasons``
spans that do not match the real calendar, and games with no ``season_id``.

The repair re-derives each game's season type from the season calendar in
:mod:`src.utils.season_classification` and points the game at the matching
``kbo_seasons`` row (creating a missing row), and aligns each ``kbo_seasons``
row's ``start_date``/``end_date`` with the calendar (a postseason stage takes
its actual first game date, and no dates until it has started).

The calendar is not trusted blindly: a date outside the regular season that
falls in no window (a rain-delayed postseason game) keeps its stored postseason
type, a stage move next to a window boundary goes to review, and the All-Star
game is never touched.

Safety:

* **Dry run by default.** Without ``--apply`` it only prints the plan.
* Writing needs ``--apply`` **and** ``KBO_ALLOW_SEASON_REPAIR=1``; with only one
  of them it previews and exits 3 without writing.
* Before committing, every prior value is written to a JSON backup file, and
  ``--rollback <file>`` restores exactly those values (only where the row still
  holds the value the repair wrote, so later corrections are not clobbered).
* Idempotent: a second run plans nothing.
* Cancelled/postponed games dated inside a postseason window are **not**
  changed (they are usually schedule placeholders, e.g. an unneeded wildcard
  game 2); they are listed for review. ``--include-unplayed-postseason``
  reclassifies them too. Nothing is ever deleted except a ``kbo_seasons`` row
  the repair itself created, on rollback, when no game references it.

Exit codes: 0 ok/preview, 1 error, 3 guard denied.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.constants import KST
from src.models.game import Game
from src.models.season import KboSeason
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SUSPENDED,
)
from src.utils.season_classification import (
    ALLSTAR,
    LEAGUE_CODE_TO_SEASON_TYPE,
    LEAGUE_NAME_TO_SEASON_TYPE,
    POSTSEASON_STAGES,
    REGULAR,
    REGULAR_SEASON_SPANS,
    SEASON_DATE_RULES,
    SEASON_TYPE_TO_LEAGUE_CODE,
    SEASON_TYPE_TO_LEAGUE_NAME,
    calendar_season_type,
    in_regular_season_span,
    postseason_window_of,
)
from src.validators.season_team_code import ALL_STAR_TEAM_CODES

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlalchemy.orm import Session

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_GUARD_DENIED = 3

ALLOW_ENV = "KBO_ALLOW_SEASON_REPAIR"
DEFAULT_BACKUP_DIR = Path("data/repair_backups")
BACKUP_KIND = "season_classification_repair"
_UNPLAYED = frozenset({GAME_STATUS_CANCELLED, GAME_STATUS_POSTPONED})
#: Statuses that prove a game was actually played (or is being played).
_STARTED = frozenset({GAME_STATUS_LIVE, GAME_STATUS_SUSPENDED, GAME_STATUS_COMPLETED, GAME_STATUS_DRAW})
#: A postseason stage move this many days from a window edge goes to review.
POSTSEASON_BOUNDARY_DAYS = 1


class RepairError(RuntimeError):
    """The repair cannot be planned or applied safely."""


@dataclass
class SeasonRowChange:
    """One planned ``kbo_seasons`` change."""

    season_id: int
    league_type_code: int
    action: str  # "create" | "update"
    old: dict[str, Any] | None
    new: dict[str, Any]


@dataclass
class GameChange:
    """One planned ``game.season_id`` change."""

    game_id: str
    game_date: str
    game_status: str | None
    old_season_id: int | None
    old_type: str | None
    new_season_id: int
    new_type: str


@dataclass
class RepairPlan:
    """Everything the repair would change for one season."""

    year: int
    season_rows: list[SeasonRowChange] = field(default_factory=list)
    games: list[GameChange] = field(default_factory=list)
    review: list[dict[str, Any]] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        """Whether there is nothing to change."""
        return not self.season_rows and not self.games

    def to_dict(self) -> dict[str, Any]:
        """Serialise the plan (also the backup payload)."""
        return {
            "kind": BACKUP_KIND,
            "year": self.year,
            "season_rows": [asdict(change) for change in self.season_rows],
            "games": [asdict(change) for change in self.games],
            "review": self.review,
        }


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _target_spans(year: int, first_played: dict[str, date]) -> dict[str, tuple[str | None, str | None]]:
    """Return ``season_type -> (start, end)`` for every type of the season.

    Exhibition and regular season take the calendar span. A postseason stage
    takes its *actual* first game date (``first_played``) as ``start_date``;
    a stage that has not started yet gets no dates at all. The backend's
    ``LeagueStageResolver`` promotes games to a higher stage from
    ``start_date``, so a planned date (e.g. PO 10/25) on a stage that has not
    begun would show a delayed 준PO game as PO.
    """
    if year not in SEASON_DATE_RULES or year not in REGULAR_SEASON_SPANS:
        msg = (
            f"No complete season calendar for {year}; add it to SEASON_DATE_RULES and "
            "REGULAR_SEASON_SPANS in src/utils/season_classification.py first"
        )
        raise RepairError(msg)
    spans: dict[str, tuple[str | None, str | None]] = {REGULAR: REGULAR_SEASON_SPANS[year]}
    for start, end, name in SEASON_DATE_RULES[year]:
        season_type = LEAGUE_NAME_TO_SEASON_TYPE.get(name)
        if season_type not in SEASON_TYPE_TO_LEAGUE_CODE:
            continue
        if season_type in POSTSEASON_STAGES:
            first = first_played.get(season_type)
            if first is None:
                spans[season_type] = (None, None)
                continue
            spans[season_type] = (first.isoformat(), max(first, date.fromisoformat(end)).isoformat())
            continue
        spans[season_type] = (start, end)
    return spans


def _rows_by_code(session: Session, year: int) -> dict[int, list[KboSeason]]:
    by_code: dict[int, list[KboSeason]] = {}
    for row in session.query(KboSeason).filter(KboSeason.season_year == year).all():
        by_code.setdefault(int(row.league_type_code), []).append(row)
    for rows in by_code.values():
        rows.sort(key=lambda row: row.season_id)
    return by_code


def _season_ids(session: Session, year: int) -> dict[str, int]:
    """Return ``season_type -> season_id`` (existing row, or the id a create would use)."""
    by_code = _rows_by_code(session, year)
    return {
        season_type: int(by_code[code][0].season_id) if by_code.get(code) else year * 100 + code
        for season_type, code in SEASON_TYPE_TO_LEAGUE_CODE.items()
    }


def _plan_season_rows(session: Session, plan: RepairPlan, first_played: dict[str, date]) -> None:
    """Plan ``kbo_seasons`` creates/updates.

    A missing postseason row is only created once its stage has started or a
    planned game moves into it; an empty not-started row would only be a target
    for a wrong stage move.
    """
    year = plan.year
    spans = _target_spans(year, first_played)
    by_code = _rows_by_code(session, year)
    targeted = {change.new_season_id for change in plan.games}

    for season_type, code in SEASON_TYPE_TO_LEAGUE_CODE.items():
        if season_type not in spans:
            continue
        start, end = spans[season_type]
        existing = by_code.get(code, [])
        if not existing:
            season_id = year * 100 + code
            if season_type in POSTSEASON_STAGES and start is None and season_id not in targeted:
                continue
            clash = session.get(KboSeason, season_id)
            if clash is not None:
                msg = (
                    f"kbo_seasons.season_id={season_id} already exists as "
                    f"year={clash.season_year} code={clash.league_type_code}"
                )
                raise RepairError(msg)
            plan.season_rows.append(
                SeasonRowChange(
                    season_id=season_id,
                    league_type_code=code,
                    action="create",
                    old=None,
                    new={
                        "season_year": year,
                        "league_type_code": code,
                        "league_type_name": SEASON_TYPE_TO_LEAGUE_NAME[season_type],
                        "start_date": start,
                        "end_date": end,
                    },
                ),
            )
            continue
        for row in existing:
            if (_iso(row.start_date), _iso(row.end_date)) != (start, end):
                plan.season_rows.append(
                    SeasonRowChange(
                        season_id=int(row.season_id),
                        league_type_code=code,
                        action="update",
                        old={"start_date": _iso(row.start_date), "end_date": _iso(row.end_date)},
                        new={"start_date": start, "end_date": end},
                    ),
                )


def _row_season_type(row: KboSeason) -> str | None:
    """Season type of a ``kbo_seasons`` row (an all-star name wins over its code)."""
    by_name = LEAGUE_NAME_TO_SEASON_TYPE.get(str(row.league_type_name or "").strip())
    if by_name == ALLSTAR:
        return ALLSTAR
    code_type = None if row.league_type_code is None else LEAGUE_CODE_TO_SEASON_TYPE.get(int(row.league_type_code))
    return code_type or by_name


def _is_allstar_game(game: Game, stored_type: str | None, calendar: str | None) -> bool:
    """Return whether a game is the All-Star game, which the repair never touches.

    The All-Star game has no ``kbo_seasons`` type the repair manages.
    """
    if ALLSTAR in (stored_type, calendar):
        return True
    teams = {str(game.home_team or "").upper(), str(game.away_team or "").upper()}
    return bool(teams & ALL_STAR_TEAM_CODES)


def _near_window_boundary(game_date: date) -> bool:
    window = postseason_window_of(game_date)
    if window is None:
        return False
    _stage, start, end = window
    return (game_date - start).days <= POSTSEASON_BOUNDARY_DAYS or (end - game_date).days <= POSTSEASON_BOUNDARY_DAYS


def _expected_type(game_date: date, stored_type: str | None, calendar: str | None) -> tuple[str | None, str | None]:
    """Return ``(expected_type, review_note)``; a note means "do not change, review".

    The calendar alone is not evidence against a stored postseason type: rain-outs
    push postseason games outside their planned window (10/24 준PO game 5), and
    later into the next stage's window (a 준PO game on 10/27). So a stored stage
    is only ever moved automatically to an *earlier* stage, and not next to a
    window edge; moving it to a later stage always goes to review. Moves between
    the postseason and regular/exhibition are still repaired.
    """
    if calendar is not None:
        if stored_type in POSTSEASON_STAGES and calendar in POSTSEASON_STAGES and stored_type != calendar:
            if POSTSEASON_STAGES.index(stored_type) < POSTSEASON_STAGES.index(calendar):
                return None, "stored postseason stage is earlier than the calendar stage; left unchanged"
            if _near_window_boundary(game_date):
                return None, "postseason stage move next to a window boundary; left unchanged"
        return calendar, None
    if in_regular_season_span(game_date) is not False:
        return REGULAR, None
    if stored_type in POSTSEASON_STAGES:
        return None, "outside the regular season and every postseason window; kept the stored postseason type"
    return None, "outside the regular season and every postseason window; type unknown, left unchanged"


def build_plan(session: Session, year: int, *, include_unplayed_postseason: bool = False) -> RepairPlan:
    """Compute the repair for one season without writing anything.

    Args:
        session: Database session.
        year: Season year.
        include_unplayed_postseason: Also reclassify cancelled/postponed games
            dated inside a postseason window.

    Returns:
        The plan.

    """
    plan = RepairPlan(year=year)
    season_ids = _season_ids(session, year)
    type_by_season_id = {int(row.season_id): _row_season_type(row) for row in session.query(KboSeason).all()}
    first_played: dict[str, date] = {}

    games = (
        session.query(Game)
        .filter(Game.game_date >= date(year, 1, 1), Game.game_date <= date(year, 12, 31))
        .order_by(Game.game_date, Game.game_id)
        .all()
    )
    for game in games:
        old_season_id = None if game.season_id is None else int(game.season_id)
        old_type = type_by_season_id.get(old_season_id) if old_season_id is not None else None
        calendar = calendar_season_type(game.game_date)
        if _is_allstar_game(game, old_type, calendar):
            continue
        status = (game.game_status or "").upper() or None
        confirmed_type = _plan_game(
            plan,
            game,
            season_ids,
            (old_season_id, old_type, calendar, status),
            include_unplayed_postseason=include_unplayed_postseason,
        )
        # Only confirmed types count: a review item keeps its stored type, but
        # that type is exactly what is in doubt.
        if status in _STARTED and confirmed_type in POSTSEASON_STAGES:
            first_played.setdefault(confirmed_type, game.game_date)

    _plan_season_rows(session, plan, first_played)
    return plan


def _plan_game(
    plan: RepairPlan,
    game: Game,
    season_ids: dict[str, int],
    state: tuple[int | None, str | None, str | None, str | None],
    *,
    include_unplayed_postseason: bool,
) -> str | None:
    """Plan one game; return its *confirmed* type after the repair.

    That is the stored type when it is already right, or the new type when the
    game is auto-fixed. A game sent to review (or one whose target type has no
    row) returns None: its stored type is unconfirmed and must not date a
    ``kbo_seasons`` stage (``first_played``), e.g. a 준PO-stored game on the WC
    date must not make 준PO start on 10/14.
    """
    old_season_id, old_type, calendar, status = state

    def review(note: str, calendar_type: str | None = calendar) -> str | None:
        plan.review.append(
            {
                "game_id": game.game_id,
                "game_date": game.game_date.isoformat(),
                "game_status": status,
                "current_type": old_type,
                "calendar_type": calendar_type,
                "note": note,
            },
        )
        return None

    if old_season_id is not None and old_type is None:
        return review("stored season_id is not a known kbo_seasons type; left unchanged")
    expected, note = _expected_type(game.game_date, old_type, calendar)
    if note is not None:
        if old_season_id is None:
            note += "; season_id is NULL and stays NULL: clean it up by hand"
        return review(note)
    if expected == old_type:
        return old_type
    if expected not in season_ids:
        return None
    if (
        old_season_id is not None
        and status in _UNPLAYED
        and expected in POSTSEASON_STAGES
        and not include_unplayed_postseason
    ):
        return review("unplayed game inside a postseason window; left unchanged", expected)
    plan.games.append(
        GameChange(
            game_id=str(game.game_id),
            game_date=game.game_date.isoformat(),
            game_status=status,
            old_season_id=old_season_id,
            old_type=old_type,
            new_season_id=season_ids[expected],
            new_type=expected,
        ),
    )
    return expected


def write_backup(plan: RepairPlan, backup_dir: Path) -> Path:
    """Write the plan with all prior values; return the file path."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    path = backup_dir / f"season_classification_{plan.year}_{stamp}.json"
    payload = plan.to_dict()
    payload["created_at_kst"] = datetime.now(KST).isoformat()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def apply_plan(session: Session, plan: RepairPlan) -> None:
    """Apply a plan inside the caller's transaction (the caller commits)."""
    for change in plan.season_rows:
        values = change.new
        if change.action == "create":
            session.add(
                KboSeason(
                    season_id=change.season_id,
                    season_year=values["season_year"],
                    league_type_code=values["league_type_code"],
                    league_type_name=values["league_type_name"],
                    start_date=date.fromisoformat(values["start_date"]) if values["start_date"] else None,
                    end_date=date.fromisoformat(values["end_date"]) if values["end_date"] else None,
                ),
            )
        else:
            row = session.get(KboSeason, change.season_id)
            if row is None:
                msg = f"kbo_seasons.season_id={change.season_id} disappeared while applying"
                raise RepairError(msg)
            row.start_date = date.fromisoformat(values["start_date"]) if values["start_date"] else None
            row.end_date = date.fromisoformat(values["end_date"]) if values["end_date"] else None
    session.flush()
    for change in plan.games:
        game = session.query(Game).filter(Game.game_id == change.game_id).one()
        current = None if game.season_id is None else int(game.season_id)
        if current != change.old_season_id:
            msg = f"{change.game_id}: season_id changed during the repair ({change.old_season_id} -> {current})"
            raise RepairError(msg)
        game.season_id = change.new_season_id
    session.flush()


def rollback_backup(session: Session, backup: dict[str, Any]) -> dict[str, list[str]]:
    """Restore the values a backup recorded (inside the caller's transaction).

    A value is only restored where the row still holds what the repair wrote;
    anything changed since is reported as a conflict and left alone.

    Returns:
        ``{"restored": [...], "conflicts": [...]}`` descriptions.

    """
    if backup.get("kind") != BACKUP_KIND:
        msg = "not a season classification backup file"
        raise RepairError(msg)
    restored: list[str] = []
    conflicts: list[str] = []
    for change in backup.get("games", []):
        game = session.query(Game).filter(Game.game_id == change["game_id"]).one_or_none()
        if game is None or game.season_id != change["new_season_id"]:
            conflicts.append(f"game {change['game_id']}: now {getattr(game, 'season_id', None)}")
            continue
        game.season_id = change["old_season_id"]
        restored.append(f"game {change['game_id']}: {change['new_season_id']} -> {change['old_season_id']}")
    session.flush()
    for change in reversed(backup.get("season_rows", [])):
        row = session.get(KboSeason, change["season_id"])
        if row is None:
            conflicts.append(f"kbo_seasons {change['season_id']}: missing")
            continue
        if (_iso(row.start_date), _iso(row.end_date)) != (change["new"]["start_date"], change["new"]["end_date"]):
            conflicts.append(f"kbo_seasons {change['season_id']}: dates changed since the repair")
            continue
        if change["action"] == "create":
            in_use = session.query(Game).filter(Game.season_id == change["season_id"]).count()
            if in_use:
                conflicts.append(f"kbo_seasons {change['season_id']}: created row still used by {in_use} games")
                continue
            session.delete(row)
            restored.append(f"kbo_seasons {change['season_id']}: removed (created by the repair)")
        else:
            old = change["old"] or {}
            row.start_date = date.fromisoformat(old["start_date"]) if old.get("start_date") else None
            row.end_date = date.fromisoformat(old["end_date"]) if old.get("end_date") else None
            restored.append(f"kbo_seasons {change['season_id']}: dates restored")
    session.flush()
    return {"restored": restored, "conflicts": conflicts}


def format_plan(plan: RepairPlan) -> str:
    """Render a plan for operators."""
    lines = [f"== season classification repair plan: {plan.year} =="]
    lines.append(f"kbo_seasons changes: {len(plan.season_rows)}")
    lines.extend(
        f"  [{change.action}] season_id={change.season_id} code={change.league_type_code} "
        f"{change.old or '-'} -> {change.new}"
        for change in plan.season_rows
    )
    lines.append(f"game.season_id changes: {len(plan.games)}")
    groups: dict[tuple[str | None, str], list[GameChange]] = {}
    for change in plan.games:
        groups.setdefault((change.old_type, change.new_type), []).append(change)
    for (old_type, new_type), changes in sorted(groups.items(), key=lambda item: (str(item[0][0]), item[0][1])):
        first, last = changes[0].game_date, changes[-1].game_date
        lines.append(f"  {old_type or 'NULL'} -> {new_type}: {len(changes)} games ({first} ~ {last})")
        lines.extend(
            f"    {change.game_id} {change.game_date} {change.game_status or '-'} "
            f"season_id {change.old_season_id} -> {change.new_season_id}"
            for change in changes
        )
    if plan.review:
        lines.append(f"review (not changed): {len(plan.review)}")
        lines.extend(
            f"  {item['game_id']} {item['game_date']} {item['game_status']} "
            f"stored={item['current_type']} calendar={item['calendar_type']}"
            for item in plan.review
        )
    if plan.empty:
        lines.append("nothing to change")
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Repair game season classification (dry run unless --apply and KBO_ALLOW_SEASON_REPAIR=1).",
    )
    parser.add_argument("--year", type=int, default=2026, help="season year (default 2026)")
    parser.add_argument("--apply", action="store_true", help=f"write the changes (also needs {ALLOW_ENV}=1)")
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR, help="where backups are written")
    parser.add_argument("--rollback", type=Path, help="restore the values recorded in a backup file")
    parser.add_argument(
        "--include-unplayed-postseason",
        action="store_true",
        help="also reclassify cancelled/postponed games inside a postseason window",
    )
    parser.add_argument("--json", action="store_true", help="print the plan as JSON")
    return parser


def _write(text: str) -> None:
    sys.stdout.write(text + "\n")


def _guard_allows(args: argparse.Namespace) -> bool:
    return bool(args.apply) and os.getenv(ALLOW_ENV) == "1"


def _run_rollback(session: Session, args: argparse.Namespace) -> int:
    backup = json.loads(args.rollback.read_text(encoding="utf-8"))
    if not _guard_allows(args):
        games = len(backup.get("games", []))
        rows = len(backup.get("season_rows", []))
        _write(f"would roll back {games} game(s) and {rows} kbo_seasons change(s) from {args.rollback}")
        if args.apply:
            _write(f"refusing rollback: --apply requires {ALLOW_ENV}=1")
            return EXIT_GUARD_DENIED
        _write(f"dry run: pass --apply and set {ALLOW_ENV}=1 to roll back")
        return EXIT_OK
    result = rollback_backup(session, backup)
    session.commit()
    _write(json.dumps(result, ensure_ascii=False, indent=2))
    return EXIT_OK


def run(argv: Sequence[str] | None = None, *, session_factory: Callable[[], Session] | None = None) -> int:
    """Run the CLI; return the exit code."""
    args = build_arg_parser().parse_args(argv)
    if session_factory is None:
        from src.db.engine import SessionLocal

        session_factory = SessionLocal

    with session_factory() as session:
        try:
            if args.rollback:
                return _run_rollback(session, args)
            plan = build_plan(session, args.year, include_unplayed_postseason=args.include_unplayed_postseason)
            _write(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2) if args.json else format_plan(plan))
            if plan.empty:
                return EXIT_OK
            if not _guard_allows(args):
                if args.apply:
                    _write(f"refusing to write: --apply requires {ALLOW_ENV}=1")
                    return EXIT_GUARD_DENIED
                _write(f"dry run: nothing written. Re-run with --apply and {ALLOW_ENV}=1 to apply.")
                return EXIT_OK
            backup_path = write_backup(plan, args.backup_dir)
            _write(f"backup written: {backup_path}")
            apply_plan(session, plan)
            session.commit()
            _write(
                f"applied: {len(plan.games)} game(s), {len(plan.season_rows)} kbo_seasons change(s). "
                f"Roll back with: python3 -m src.cli.repair_season_classification --rollback {backup_path} --apply",
            )
        except RepairError as exc:
            session.rollback()
            _write(f"error: {exc}")
            return EXIT_ERROR
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point."""
    sys.exit(run(argv))


if __name__ == "__main__":
    main()
