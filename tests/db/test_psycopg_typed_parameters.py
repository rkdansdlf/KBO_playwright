"""Statements that PostgreSQL rejected under psycopg 3 must compile to typed SQL.

psycopg 3 sends parameters to the server untyped (``$1``), unlike psycopg2, which
interpolated them client side. Two statements relied on that and failed in the
scheduler once it moved to psycopg 3:

* ``:audit_season IS NULL OR ps.season = :audit_season`` -- ``AmbiguousParameter``
  because ``$3 IS NULL`` gives PostgreSQL nothing to infer the type from.
* ``player_milestones.is_achieved = false`` -- ``integer = boolean`` because the
  column is an integer (the DDL stores it as 0/1), so a boolean literal never matches.

Compiling against the psycopg dialect reproduces both without a database.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Integer, bindparam, create_engine, text
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
from src.repositories.milestone_repository import MilestoneRepository
from src.services.context_aggregator import ContextAggregator
from src.validators import season_team_code


def test_audit_season_parameter_is_typed_for_psycopg() -> None:
    """An untyped ``:audit_season`` is what PostgreSQL could not infer."""
    from sqlalchemy.dialects.postgresql import psycopg

    statement = text("SELECT 1 WHERE (:audit_season IS NULL OR 1 = :audit_season)").bindparams(
        bindparam("audit_season", type_=Integer),
    )
    sql = str(statement.compile(dialect=psycopg.dialect()))

    assert "::INTEGER" in sql


def test_season_team_code_audit_types_the_season_parameter() -> None:
    """The real audit query binds ``audit_season`` as an integer, scoped or not."""
    from sqlalchemy.dialects.postgresql import psycopg

    captured: dict[str, object] = {}

    class _Result:
        def fetchall(self) -> list[tuple[int, int, int]]:
            return [(0, 0, 0), (0, 0, 0)]

    class _Session:
        def execute(self, statement: object, params: dict[str, object]) -> _Result:
            captured["sql"] = str(statement.compile(dialect=psycopg.dialect()))
            captured["params"] = params
            return _Result()

    season_team_code.audit_season_team_codes(_Session(), season=None)  # type: ignore[arg-type]

    assert "::INTEGER" in str(captured["sql"])
    assert captured["params"]["audit_season"] is None  # type: ignore[index]


def _seed(session: Session) -> None:
    session.add_all(
        [
            PlayerMilestone(
                season=2026,
                player_id="1",
                player_name="미달성",
                team_code="LG",
                milestone_category="2000안타",
                current_val=1990,
                target_val=2000,
                remaining_val=10,
                is_achieved=0,
            ),
            PlayerMilestone(
                season=2026,
                player_id="2",
                player_name="달성",
                team_code="LG",
                milestone_category="2000안타",
                current_val=2000,
                target_val=2000,
                remaining_val=0,
                is_achieved=1,
            ),
        ],
    )
    session.commit()


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as sess:
        yield sess


def test_upcoming_milestones_returns_only_unachieved_rows(session: Session) -> None:
    _seed(session)

    names = [m.player_name for m in MilestoneRepository(session).get_upcoming_milestones(season=2026)]

    assert names == ["미달성"]


def test_milestone_alerts_return_only_unachieved_rows(session: Session) -> None:
    _seed(session)

    alerts = ContextAggregator(session).get_milestone_alerts("LG", "KT", season=2026)

    assert [a["player_name"] for a in alerts] == ["미달성"]


def test_milestone_filters_never_send_a_boolean_to_the_integer_column() -> None:
    """Against the psycopg dialect the predicate must not be rendered with a boolean."""
    from sqlalchemy.dialects.postgresql import psycopg

    captured: list[str] = []

    class _Scalars:
        def all(self) -> list[object]:
            return []

    class _Result:
        def scalars(self) -> _Scalars:
            return _Scalars()

    class _Session:
        def execute(self, statement: object) -> _Result:
            captured.append(str(statement.compile(dialect=psycopg.dialect())).lower())
            return _Result()

    MilestoneRepository(_Session()).get_upcoming_milestones(season=2026)  # type: ignore[arg-type]
    ContextAggregator(_Session()).get_milestone_alerts("LG", "KT", season=2026)  # type: ignore[arg-type]

    assert len(captured) == 2
    for sql in captured:
        predicate = sql.split("where", 1)[1]
        assert "is_achieved = " in predicate
        assert "is_achieved = false" not in predicate
        assert "is_achieved = true" not in predicate
