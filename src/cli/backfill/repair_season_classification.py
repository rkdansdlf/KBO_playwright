"""Repair the season classification (regular / exhibition / postseason) of stored games.

Issue #40: exhibition games stored as regular season (teams over 144 games),
regular-season games stored as a postseason stage, postseason ``kbo_seasons``
spans that do not match the real calendar, and games with no ``season_id``.

The repair re-derives each game's season type from the season calendar in
:mod:`src.utils.season_classification` and points the game at the matching
``kbo_seasons`` row (creating a missing row), and aligns each ``kbo_seasons``
row's ``start_date``/``end_date`` with the calendar (a postseason stage takes
its actual first game date, and no dates until it has started).

The calendar is not trusted blindly. Rows are listed under ``review`` instead
of being changed, either as **decide** (needs an operator decision) or **info**
(the stored type is already the plausible one, nothing would change):

* decide: a stored regular game inside a postseason window with no stored game
  of that stage the same day (the crawler confirms Naver date-style ids as
  regular, so moving it would flip-flop), a stored stage *earlier* than the
  calendar stage (a delayed series, or e.g. 준PO games stored as WC), a stage
  move next to a window boundary, a stored regular game after the first
  postseason day with no postseason game that day, a stored postseason game at
  the start of the regular season or with a Naver postseason id before the
  first postseason day, a NULL ``season_id`` the repair cannot place, an
  unknown season type, and an All-Star game not stored under an all-star season;
* info: a stored postseason game outside every window on or after the first
  postseason day (rain-delayed series, e.g. 10/16 WC, 10/24 준PO), a regular
  game after the regular-season span but before the first postseason day
  (10/13), a regular make-up game beside a same-day postseason game.

A stored postseason game before the first postseason day (10/12, 10/13) is
demoted to regular (postseason games are only ever pushed later) unless its id
or a ``game_id_aliases`` row carries a Naver postseason prefix. Postseason games
stored as regular are corrected by re-crawling (the Naver id prefix).

While any **decide** row or unplaced NULL ``season_id`` game remains ("date-hold
mode"), a postseason stage with no confirmed first game never gets NULL dates
(bega_backend otherwise joins NULL-season games to every stage and labels the
postseason KS): stored dates inside the stage's calendar window are kept, NULL
or out-of-window dates are replaced by the window. As a safeguard, a stage that
any review row (decide or info) relates to is held the same way even with
nothing to decide, and a started stage never starts after a played review game
dated in its window.

After ``--apply``: recompute standings and **restart bega_backend**
(``LeagueStageResolver`` / ``LEAGUE_DATES`` cache season types and stage dates).

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
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import inspect

from src.constants import KST
from src.models.game import Game, GameIdAlias
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
    season_type_from_naver_game_id,
)
from src.validators.season_team_code import ALL_STAR_TEAM_CODES

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

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
#: A postseason-stored game this many days from a regular-season edge goes to review.
REGULAR_EDGE_DAYS = 1


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
    #: Operator warnings (e.g. postseason dates kept because review/NULL rows remain).
    warnings: list[str] = field(default_factory=list)
    #: Review rows needing a decision plus NULL season_id games left (date-hold mode when > 0).
    unresolved: int = 0

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
            "warnings": self.warnings,
            "unresolved": self.unresolved,
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


def _calendar_window(year: int, season_type: str) -> tuple[str | None, str | None]:
    for start, end, name in SEASON_DATE_RULES.get(year, []):
        if LEAGUE_NAME_TO_SEASON_TYPE.get(name) == season_type:
            return start, end
    return None, None


def _rows_by_code(session: Session, year: int) -> dict[int, list[KboSeason]]:
    by_code: dict[int, list[KboSeason]] = {}
    for row in session.query(KboSeason).filter(KboSeason.season_year == year).all():
        by_code.setdefault(int(row.league_type_code), []).append(row)
    for rows in by_code.values():
        rows.sort(key=lambda row: row.season_id)
    return by_code


def _pick_season_row(rows: list[KboSeason], season_type: str) -> KboSeason:
    """Pick the best matching row for a season type, preferring exact name match over arbitrary id."""
    expected_name = SEASON_TYPE_TO_LEAGUE_NAME.get(season_type)
    if expected_name:
        for row in rows:
            if row.league_type_name == expected_name:
                return row
    for row in rows:
        if _row_season_type(row) == season_type:
            return row
    return rows[0]


def _season_ids(session: Session, year: int) -> dict[str, int]:
    """Return ``season_type -> season_id`` (existing row, or the id a create would use)."""
    by_code = _rows_by_code(session, year)
    res: dict[str, int] = {}
    for season_type, code in SEASON_TYPE_TO_LEAGUE_CODE.items():
        if by_code.get(code):
            res[season_type] = int(_pick_season_row(by_code[code], season_type).season_id)
        else:
            res[season_type] = year * 100 + code
    return res


def _plan_season_rows(
    session: Session,
    plan: RepairPlan,
    first_played: dict[str, date],
    *,
    unresolved_rows: int = 0,
    stages: tuple[frozenset[str], frozenset[str]] = (frozenset(), frozenset()),
) -> None:
    """Plan ``kbo_seasons`` creates/updates.

    A missing postseason row is only created once its stage has started or a
    planned game moves into it; an empty not-started row would only be a target
    for a wrong stage move.

    ``unresolved_rows`` counts the review items that need a decision plus games
    left with ``season_id`` NULL. While any remain ("date-hold mode"), a
    postseason stage with no confirmed first game never gets NULL dates:
    bega_backend's ``LeagueStageResolver`` falls back to joining NULL-season
    games to every stage when a stage has no start date, and then labels the
    whole postseason as KS. Its stored dates are kept when they lie inside the
    stage's calendar window; NULL or out-of-window dates (e.g. PO 10/18, KS
    10/26) are replaced by the calendar window, as for a newly created row.

    ``stages`` is ``(played_stages, review_stages)``: stages with played games
    (for the warning wording), and stages that any review row (decision or
    informational) relates to: its stored or calendar type, or the window its
    date lies in. As a safeguard such a stage is held the same way even when
    nothing needs a decision, so its dates are never cleared under a game whose
    type is in doubt.
    """
    year = plan.year
    spans = _target_spans(year, first_played)
    by_code = _rows_by_code(session, year)
    targeted = {change.new_season_id for change in plan.games}
    played_stages, review_stages = stages

    for season_type, code in SEASON_TYPE_TO_LEAGUE_CODE.items():
        if season_type not in spans:
            continue
        start, end = spans[season_type]
        existing = by_code.get(code, [])
        unconfirmed = season_type in POSTSEASON_STAGES and start is None
        reason = _hold_reason(unresolved_rows, in_review=season_type in review_stages) if unconfirmed else None
        status = _stage_status(season_type, played=season_type in played_stages)
        if not existing:
            season_id = year * 100 + code
            if not (unconfirmed and season_id not in targeted):
                hold = f"{status}; {reason}" if reason else None
                _plan_create(session, plan, season_type, (start, end), hold)
            continue
        if reason:
            _plan_held_rows(plan, season_type, existing, f"{status}; {reason}")
            continue
        for row in existing:
            _plan_update(plan, row, (start, end))


def _hold_reason(unresolved_rows: int, *, in_review: bool) -> str | None:
    """Why a stage without a confirmed first game keeps dates (None: it does not)."""
    if unresolved_rows > 0:
        return f"{unresolved_rows} review/NULL season_id row(s) still need a decision"
    if in_review:
        return "review row(s) relate to this stage"
    return None


def _stage_status(season_type: str, *, played: bool) -> str:
    if played:
        return f"{season_type}: has played games but none with a confirmed type (they are in review)"
    return f"{season_type}: not started"


def _inside_window(row: KboSeason, window: tuple[str | None, str | None]) -> bool:
    if None in window or row.start_date is None or row.end_date is None:
        return False
    win_start, win_end = (date.fromisoformat(value) for value in window if value)
    return win_start <= row.start_date <= row.end_date <= win_end


def _plan_held_rows(
    plan: RepairPlan,
    season_type: str,
    rows: list[KboSeason],
    reason: str,
) -> None:
    """Date-hold mode for a stage without a confirmed first game (see ``_plan_season_rows``)."""
    window = _calendar_window(plan.year, season_type)
    stale = [row for row in rows if not _inside_window(row, window)]
    for row in stale:
        _plan_update(plan, row, window)
    action = (
        f"replaced NULL/out-of-window dates of {len(stale)} row(s) with the calendar window {window[0]}~{window[1]}"
        if stale
        else "kept the stored dates (inside the calendar window)"
    )
    plan.warnings.append(
        f"{reason}, so its dates are not cleared: {action}. Resolve them and re-run the repair.",
    )


def _plan_update(plan: RepairPlan, row: KboSeason, span: tuple[str | None, str | None]) -> None:
    start, end = span
    if (_iso(row.start_date), _iso(row.end_date)) == (start, end):
        return
    plan.season_rows.append(
        SeasonRowChange(
            season_id=int(row.season_id),
            league_type_code=int(row.league_type_code),
            action="update",
            old={"start_date": _iso(row.start_date), "end_date": _iso(row.end_date)},
            new={"start_date": start, "end_date": end},
        ),
    )


def _plan_create(
    session: Session,
    plan: RepairPlan,
    season_type: str,
    span: tuple[str | None, str | None],
    hold: str | None,
) -> None:
    """Plan the creation of a missing ``kbo_seasons`` row.

    ``hold`` is the warning reason in date-hold mode for a stage with no
    confirmed first game: the row takes the calendar window, not NULL dates.
    """
    year = plan.year
    code = SEASON_TYPE_TO_LEAGUE_CODE[season_type]
    season_id = year * 100 + code
    start, end = span
    if hold is not None:
        start, end = _calendar_window(year, season_type)
        plan.warnings.append(
            f"{hold}, so the new kbo_seasons row {season_id} gets the calendar window {start}~{end} "
            "instead of NULL dates",
        )
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


def _near_regular_season_start(game_date: date) -> bool:
    span = REGULAR_SEASON_SPANS.get(game_date.year)
    if span is None:
        return False
    return abs((game_date - date.fromisoformat(span[0])).days) <= REGULAR_EDGE_DAYS


def _first_postseason_day(year: int) -> date | None:
    starts = [
        date.fromisoformat(start)
        for start, _end, name in SEASON_DATE_RULES.get(year, [])
        if LEAGUE_NAME_TO_SEASON_TYPE.get(name) in POSTSEASON_STAGES
    ]
    return min(starts) if starts else None


class Verdict(NamedTuple):
    """How the repair treats one game.

    ``expected`` is the type to move to (or keep); a ``note`` means "do not
    change, list it". ``blocking`` says whether the listed row needs a decision
    (counts toward date-hold mode) or is informational: the stored type is
    already the plausible one and nothing would change.
    """

    expected: str | None
    note: str | None = None
    blocking: bool = False


def _expected_in_window(
    game_date: date,
    stored_type: str | None,
    calendar: str,
    *,
    same_day_stage: bool,
) -> Verdict:
    """``_expected_type`` for a date inside an exhibition/postseason window."""
    if calendar in POSTSEASON_STAGES and stored_type == REGULAR:
        if same_day_stage:
            return Verdict(None, f"regular make-up game beside a stored {calendar} game the same day; kept")
        return Verdict(
            None, "stored regular game inside a postseason window (make-up game?); left unchanged", blocking=True
        )
    if stored_type in POSTSEASON_STAGES and calendar in POSTSEASON_STAGES and stored_type != calendar:
        if POSTSEASON_STAGES.index(stored_type) < POSTSEASON_STAGES.index(calendar):
            # A delayed series, or a later-stage game stored as an earlier one
            # (e.g. 준PO stored as WC): the stored type must not date the stage.
            return Verdict(
                None, "stored stage earlier than the calendar stage (delayed series?); left unchanged", blocking=True
            )
        if _near_window_boundary(game_date):
            return Verdict(None, "postseason stage move next to a window boundary; left unchanged", blocking=True)
    return Verdict(calendar)


def _stored_postseason_outside_windows(game_date: date, *, postseason_id: bool) -> Verdict | None:
    """Verdict for a stored postseason game on a date in no window (None: fall through).

    ``postseason_id`` is crawler evidence: the game id carries a Naver
    postseason series prefix (``4444``/``3333``/``5555``/``7777``).
    """
    if in_regular_season_span(game_date) and _near_regular_season_start(game_date):
        return Verdict(None, "stored postseason game at the start of the regular season; left unchanged", blocking=True)
    first_postseason = _first_postseason_day(game_date.year)
    if first_postseason is None:
        return None
    if game_date < first_postseason:
        if postseason_id:
            return Verdict(None, "postseason game id before the first postseason day; left unchanged", blocking=True)
        # Postseason games are only ever pushed later, never earlier.
        return Verdict(REGULAR)
    return Verdict(None, "stored postseason game outside every window (delayed series); kept")


def _expected_type(
    game_date: date,
    stored_type: str | None,
    calendar: str | None,
    *,
    same_day_stage: bool = False,
    postseason_id: bool = False,
) -> Verdict:
    """Decide what the repair does with one game.

    The calendar alone is not evidence against a stored type:

    * rain-outs push postseason games outside their planned window (10/16 WC,
      10/24 준PO game 5) and into the next stage's window (a 준PO game on
      10/27): a stored stage at or after the first postseason day is kept
      (informational), and only moves automatically to an *earlier* stage,
      not next to a window edge;
    * postseason games are never played *earlier* than planned, so a stored
      postseason game before the first postseason day (10/12, 10/13) is
      demoted to regular, except within a day of opening day (3/28-29);
    * rained-out regular games are made up inside postseason windows, and the
      crawler confirms Naver date-style ids as regular: a stored regular game
      there goes to review (informational when a stored game of the calendar
      stage is on the same day). A stored regular game after the regular span
      is informational only before the first postseason day (10/13) or beside
      a same-day stored postseason game; otherwise it needs a decision.

    ``same_day_stage`` says a stored game of the calendar stage (outside every
    window: of any postseason stage) is on the same day.
    """
    if calendar is not None:
        return _expected_in_window(game_date, stored_type, calendar, same_day_stage=same_day_stage)
    if stored_type in POSTSEASON_STAGES:
        verdict = _stored_postseason_outside_windows(game_date, postseason_id=postseason_id)
        if verdict is not None:
            return verdict
    if in_regular_season_span(game_date) is not False:
        return Verdict(REGULAR)
    span = REGULAR_SEASON_SPANS.get(game_date.year)
    if stored_type == REGULAR and span is not None and game_date > date.fromisoformat(span[1]):
        first_postseason = _first_postseason_day(game_date.year)
        if same_day_stage or (first_postseason is not None and game_date < first_postseason):
            return Verdict(None, "regular game after the regular-season span (make-up/tiebreaker); kept")
        return Verdict(
            None,
            "stored regular game after the first postseason day with no postseason game that day; left unchanged",
            blocking=True,
        )
    return Verdict(
        None, "outside the regular season and every postseason window; type unknown, left unchanged", blocking=True
    )


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
    stored_stage_days = {
        (game.game_date, type_by_season_id.get(int(game.season_id)))
        for game in games
        if game.season_id is not None and type_by_season_id.get(int(game.season_id)) in POSTSEASON_STAGES
    }
    postseason_ids = _postseason_id_evidence(session, games)
    played_stages: set[str] = set()
    for game in games:
        old_season_id = None if game.season_id is None else int(game.season_id)
        old_type = type_by_season_id.get(old_season_id) if old_season_id is not None else None
        calendar = calendar_season_type(game.game_date)
        status = (game.game_status or "").upper() or None
        if _is_allstar_game(game, old_type, calendar):
            if old_type != ALLSTAR:
                # Stored under another type (usually regular): the repair has
                # no all-star type to move it to, so flag it instead of hiding it.
                plan.review.append(
                    {
                        "game_id": game.game_id,
                        "game_date": game.game_date.isoformat(),
                        "game_status": status,
                        "current_type": old_type,
                        "calendar_type": ALLSTAR,
                        "note": "All-Star game not stored under an all-star season; not repaired, fix by hand",
                        "blocking": True,
                    },
                )
            continue
        confirmed_type = _plan_game(
            plan,
            game,
            season_ids,
            (
                old_season_id,
                old_type,
                calendar,
                status,
                _same_day_stage(game.game_date, calendar, stored_stage_days),
                str(game.game_id) in postseason_ids,
            ),
            include_unplayed_postseason=include_unplayed_postseason,
        )
        # Only confirmed types count: a review item keeps its stored type, but
        # that type is exactly what is in doubt.
        if status in _STARTED and confirmed_type in POSTSEASON_STAGES:
            first_played.setdefault(confirmed_type, game.game_date)
        if status in _STARTED:
            # For warnings only: a stage "has played games" when a played game
            # is confirmed in it, or is listed with it as stored/calendar type.
            # A game being auto-moved away from a stage does not count for it.
            candidates = (confirmed_type,) if confirmed_type is not None else (old_type, calendar)
            played_stages.update(t for t in candidates if t in POSTSEASON_STAGES)

    _extend_back_to_review_games(first_played, plan.review)
    planned = {change.game_id for change in plan.games}
    # Only rows that need a decision hold the dates; informational review rows
    # (stored type already plausible, nothing would change) do not.
    unresolved = {str(item["game_id"]) for item in plan.review if item.get("blocking")}
    unresolved |= {str(game.game_id) for game in games if game.season_id is None and str(game.game_id) not in planned}
    plan.unresolved = len(unresolved)
    _plan_season_rows(
        session,
        plan,
        first_played,
        unresolved_rows=len(unresolved),
        stages=(frozenset(played_stages), _review_stages(plan.review)),
    )
    return plan


def _postseason_id_evidence(session: Session, games: list[Game]) -> frozenset[str]:
    """Game ids whose own id or a stored alias carries a Naver postseason series prefix.

    The Naver crawler stores its source id (e.g. ``33331012LGHT02026``) as a
    ``game_id_aliases`` row of the canonical KBO id.
    """
    evidence = {
        str(game.game_id) for game in games if season_type_from_naver_game_id(game.game_id) in POSTSEASON_STAGES
    }
    if not games or not inspect(session.get_bind()).has_table(GameIdAlias.__tablename__):
        return frozenset(evidence)
    ids = [str(game.game_id) for game in games]
    for chunk_start in range(0, len(ids), 500):
        rows = (
            session.query(GameIdAlias.alias_game_id, GameIdAlias.canonical_game_id)
            .filter(GameIdAlias.canonical_game_id.in_(ids[chunk_start : chunk_start + 500]))
            .all()
        )
        evidence.update(
            str(canonical) for alias, canonical in rows if season_type_from_naver_game_id(alias) in POSTSEASON_STAGES
        )
    return frozenset(evidence)


def _same_day_stage(game_date: date, calendar: str | None, stored_stage_days: set[tuple[date, str | None]]) -> bool:
    """Return whether a stored game of the calendar stage (outside every window: any stage) is on this day."""
    if calendar is not None:
        return (game_date, calendar) in stored_stage_days
    return any(day == game_date for day, _stage in stored_stage_days)


def _extend_back_to_review_games(first_played: dict[str, date], review: list[dict[str, Any]]) -> None:
    """Never start a confirmed stage after a played review game dated in its window.

    E.g. 준PO games 10/17-21 stored as WC go to review, 10/22 is a confirmed
    준PO game: 준PO keeps starting 10/17 rather than moving to 10/22 (the
    backend would then show the doubtful games as WC). Review games never
    *start* a stage on their own (see ``_plan_game``).
    """
    for item in review:
        stage = item.get("calendar_type")
        if stage not in first_played or item.get("game_status") not in _STARTED:
            continue
        game_date = date.fromisoformat(str(item["game_date"]))
        first_played[stage] = min(first_played[stage], game_date)


def _review_stages(review: list[dict[str, Any]]) -> frozenset[str]:
    """Postseason stages a review row relates to (stored/calendar type, or date window)."""
    stages: set[str] = set()
    for item in review:
        stages.update(t for t in (item.get("current_type"), item.get("calendar_type")) if t in POSTSEASON_STAGES)
        window = postseason_window_of(date.fromisoformat(str(item["game_date"])))
        if window is not None:
            stages.add(window[0])
    return frozenset(stages)


def _plan_game(
    plan: RepairPlan,
    game: Game,
    season_ids: dict[str, int],
    state: tuple[int | None, str | None, str | None, str | None, bool, bool],
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
    old_season_id, old_type, calendar, status, same_day_stage, postseason_id = state

    def review(note: str, calendar_type: str | None = calendar, *, blocking: bool = True) -> str | None:
        plan.review.append(
            {
                "game_id": game.game_id,
                "game_date": game.game_date.isoformat(),
                "game_status": status,
                "current_type": old_type,
                "calendar_type": calendar_type,
                "note": note,
                "blocking": blocking,
            },
        )
        return None

    if old_season_id is not None and old_type is None:
        return review("stored season_id is not a known kbo_seasons type; left unchanged")
    expected, note, blocking = _expected_type(
        game.game_date,
        old_type,
        calendar,
        same_day_stage=same_day_stage,
        postseason_id=postseason_id,
    )
    if (
        note is not None
        and include_unplayed_postseason
        and old_type == REGULAR
        and status in _UNPLAYED
        and calendar in POSTSEASON_STAGES
    ):
        # Explicit operator opt-in for cancelled/postponed placeholders.
        expected, note = calendar, None
    if note is not None:
        if old_season_id is None:
            note += "; season_id is NULL and stays NULL: clean it up by hand"
            blocking = True
        return review(note, blocking=blocking)
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
        lines.append(f"review (not changed): {len(plan.review)} (needs a decision: {plan.unresolved} incl. NULL rows)")
        lines.extend(
            f"  [{'decide' if item.get('blocking') else 'info'}] {item['game_id']} {item['game_date']} "
            f"{item['game_status']} stored={item['current_type']} calendar={item['calendar_type']}: {item['note']}"
            for item in plan.review
        )
    lines.extend(f"WARNING: {warning}" for warning in plan.warnings)
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


def _run_repair(session: Session, args: argparse.Namespace, state: dict[str, Path]) -> int:
    """Plan, and with the guard satisfied apply, one season (``state['backup']`` gets the backup path)."""
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
    state["backup"] = backup_path
    _write(f"backup written: {backup_path}")
    apply_plan(session, plan)
    session.commit()
    _write(
        f"applied: {len(plan.games)} game(s), {len(plan.season_rows)} kbo_seasons change(s). "
        f"Roll back with: python3 -m src.cli.repair_season_classification --rollback {backup_path} --apply",
    )
    _write(
        "next: recompute standings (python3 -m src.cli.calculate_standings --year "
        f"{args.year}) and restart bega_backend -- LeagueStageResolver/LEAGUE_DATES cache "
        "season types and stage start dates until restart.",
    )
    return EXIT_OK


def run(argv: Sequence[str] | None = None, *, session_factory: Callable[[], Session] | None = None) -> int:
    """Run the CLI; return the exit code."""
    args = build_arg_parser().parse_args(argv)
    if session_factory is None:
        from src.db.engine import SessionLocal

        session_factory = SessionLocal

    state: dict[str, Path] = {}
    with session_factory() as session:
        try:
            if args.rollback:
                return _run_rollback(session, args)
            return _run_repair(session, args, state)
        except RepairError as exc:
            session.rollback()
            _write(f"error: {exc}{_backup_hint(state.get('backup'))}")
            return EXIT_ERROR
        except Exception as exc:  # noqa: BLE001 - operator CLI: report cleanly, no raw traceback
            session.rollback()
            logger.debug("repair_season_classification failed: %s", type(exc).__name__)
            _write(
                f"error: unexpected {type(exc).__name__}: {_safe_error_text(exc)}; nothing was committed."
                f"{_backup_hint(state.get('backup'))}",
            )
            return EXIT_ERROR


_ERROR_TEXT_LIMIT = 300


def _safe_error_text(exc: BaseException) -> str:
    """Error text without SQL bind parameters (which can carry row data), truncated."""
    orig = getattr(exc, "orig", None)
    text = str(orig if orig is not None else exc)
    text = text.split("[parameters:", 1)[0].split("[SQL:", 1)[0].strip()
    text = " ".join(text.split())
    if len(text) > _ERROR_TEXT_LIMIT:
        text = text[: _ERROR_TEXT_LIMIT - 3] + "..."
    return text


def _backup_hint(backup_path: Path | None) -> str:
    if backup_path is None:
        return " (no backup was written)"
    return f" (backup JSON: {backup_path}; nothing from this run was committed, so no rollback is needed)"


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point."""
    sys.exit(run(argv))


if __name__ == "__main__":
    main()
