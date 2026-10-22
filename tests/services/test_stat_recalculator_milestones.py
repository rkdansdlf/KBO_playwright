"""Milestone recalculation must be idempotent and must not re-derive ``current_val``.

The old implementation added the sum of every ``game_*_stats`` row to ``current_val`` on each run, with no
season filter. Each nightly run inflated the totals (a 300-save target reached 35,417) and flagged most
rows as achieved. ``current_val`` is a source snapshot; only the derived fields are recomputed.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
from src.services.stat_recalculator import StatRecalculator


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as sess:
        yield sess


def _milestone(**overrides: object) -> PlayerMilestone:
    values: dict[str, object] = {
        "season": 2026,
        "player_id": "1001",
        "player_name": "선수",
        "team_code": "KIA",
        "milestone_category": "2000안타",
        "current_val": 1990,
        "target_val": 2000,
        "remaining_val": 10,
        "is_achieved": 0,
    }
    values.update(overrides)
    return PlayerMilestone(**values)


def test_second_run_changes_nothing(session: Session) -> None:
    session.add_all([_milestone(), _milestone(player_id="1002", current_val=2000, remaining_val=0, is_achieved=1)])
    session.commit()
    recalc = StatRecalculator(session)

    recalc.recalc_player_milestones(season=2026)
    snapshot = [(m.player_id, m.current_val, m.remaining_val, m.is_achieved) for m in session.query(PlayerMilestone)]

    assert recalc.recalc_player_milestones(season=2026) == 0
    assert snapshot == [
        (m.player_id, m.current_val, m.remaining_val, m.is_achieved) for m in session.query(PlayerMilestone)
    ]


def test_current_val_is_never_modified(session: Session) -> None:
    session.add(_milestone(current_val=1990))
    session.commit()

    StatRecalculator(session).recalc_player_milestones(season=2026)

    assert session.query(PlayerMilestone).one().current_val == 1990


def test_derived_fields_follow_current_and_target(session: Session) -> None:
    session.add_all(
        [
            _milestone(player_id="a", current_val=1990, remaining_val=5, is_achieved=0),
            _milestone(player_id="b", current_val=2000, remaining_val=3, is_achieved=0),
            _milestone(player_id="c", current_val=2100, remaining_val=0, is_achieved=0),
        ],
    )
    session.commit()

    changed = StatRecalculator(session).recalc_player_milestones(season=2026)

    rows = {m.player_id: m for m in session.query(PlayerMilestone)}
    assert changed == 3
    assert (rows["a"].remaining_val, rows["a"].is_achieved) == (10, 0)
    assert (rows["b"].remaining_val, rows["b"].is_achieved) == (0, 1)
    assert (rows["c"].remaining_val, rows["c"].is_achieved) == (0, 1)


def test_only_the_requested_season_is_touched(session: Session) -> None:
    session.add_all([_milestone(season=2026, remaining_val=99), _milestone(season=2025, remaining_val=99)])
    session.commit()

    StatRecalculator(session).recalc_player_milestones(season=2026)

    by_season = {m.season: m.remaining_val for m in session.query(PlayerMilestone)}
    assert by_season == {2026: 10, 2025: 99}


def test_game_stat_tables_are_not_queried(session: Session) -> None:
    """Re-deriving from ``game_*_stats`` is what corrupted the totals; the recalculation must not read them."""
    session.add(_milestone())
    session.commit()
    statements: list[str] = []

    def record(_conn: object, _cursor: object, statement: str, *_args: object) -> None:
        statements.append(statement.lower())

    event.listen(session.get_bind(), "before_cursor_execute", record)
    try:
        StatRecalculator(session).recalc_player_milestones(season=2026)
    finally:
        event.remove(session.get_bind(), "before_cursor_execute", record)

    assert statements
    assert not any("game_batting_stats" in s or "game_pitching_stats" in s for s in statements)


def test_target_val_zero_is_not_achieved(session: Session) -> None:
    """A row whose target_val <= 0 is not achieved and has remaining_val 0."""
    session.add_all(
        [
            _milestone(player_id="zero_1", current_val=0, target_val=0, remaining_val=0, is_achieved=0),
            _milestone(player_id="zero_2", current_val=50, target_val=0, remaining_val=10, is_achieved=0),
            _milestone(player_id="zero_3", current_val=50, target_val=0, remaining_val=0, is_achieved=1),
        ]
    )
    session.commit()

    recalc = StatRecalculator(session)
    recalc.recalc_player_milestones(season=2026)

    rows = {m.player_id: m for m in session.query(PlayerMilestone)}
    assert (rows["zero_1"].remaining_val, rows["zero_1"].is_achieved) == (0, 0)
    assert (rows["zero_2"].remaining_val, rows["zero_2"].is_achieved) == (0, 0)
    assert (rows["zero_3"].remaining_val, rows["zero_3"].is_achieved) == (0, 0)


def test_achieved_date_stamped_on_flip(session: Session) -> None:
    """When a row flips from 0 to 1 and achieved_date is None, stamp with today."""
    stamped_date = date(2026, 6, 15)
    session.add(
        _milestone(
            player_id="flip_1",
            current_val=2000,
            target_val=2000,
            remaining_val=10,
            is_achieved=0,
            achieved_date=None,
        )
    )
    session.commit()

    recalc = StatRecalculator(session)
    changed = recalc.recalc_player_milestones(season=2026, today=stamped_date)

    m = session.query(PlayerMilestone).filter_by(player_id="flip_1").one()
    assert changed == 1
    assert m.is_achieved == 1
    assert m.remaining_val == 0
    assert m.achieved_date == stamped_date


def test_existing_achieved_date_not_overwritten(session: Session) -> None:
    """An existing achieved_date must never be overwritten on a 0->1 flip or subsequent runs."""
    old_date = date(2026, 5, 1)
    new_today = date(2026, 6, 15)
    session.add_all(
        [
            _milestone(
                player_id="preserve_1",
                current_val=2000,
                target_val=2000,
                remaining_val=10,
                is_achieved=0,
                achieved_date=old_date,
            ),
            _milestone(
                player_id="preserve_2",
                current_val=2000,
                target_val=2000,
                remaining_val=0,
                is_achieved=1,
                achieved_date=old_date,
            ),
        ]
    )
    session.commit()

    recalc = StatRecalculator(session)
    recalc.recalc_player_milestones(season=2026, today=new_today)

    rows = {m.player_id: m for m in session.query(PlayerMilestone)}
    assert rows["preserve_1"].is_achieved == 1
    assert rows["preserve_1"].achieved_date == old_date
    assert rows["preserve_2"].is_achieved == 1
    assert rows["preserve_2"].achieved_date == old_date


def test_second_run_is_idempotent_with_today(session: Session) -> None:
    """Second run changes nothing when today is injected."""
    stamped_date = date(2026, 6, 15)
    session.add_all(
        [
            _milestone(
                player_id="idem_1",
                current_val=2000,
                target_val=2000,
                remaining_val=10,
                is_achieved=0,
                achieved_date=None,
            ),
            _milestone(player_id="idem_2", current_val=50, target_val=0, remaining_val=5, is_achieved=0),
            _milestone(player_id="idem_3", current_val=1990, target_val=2000, remaining_val=10, is_achieved=0),
        ]
    )
    session.commit()

    recalc = StatRecalculator(session)
    changed_first = recalc.recalc_player_milestones(season=2026, today=stamped_date)
    assert changed_first > 0

    snapshot = [
        (m.player_id, m.current_val, m.target_val, m.remaining_val, m.is_achieved, m.achieved_date)
        for m in session.query(PlayerMilestone).order_by(PlayerMilestone.player_id)
    ]

    changed_second = recalc.recalc_player_milestones(season=2026, today=stamped_date)
    assert changed_second == 0

    snapshot_second = [
        (m.player_id, m.current_val, m.target_val, m.remaining_val, m.is_achieved, m.achieved_date)
        for m in session.query(PlayerMilestone).order_by(PlayerMilestone.player_id)
    ]
    assert snapshot == snapshot_second
