from __future__ import annotations

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from scripts.maintenance.seed_data import _seed_default_seasons
from src.models.season import KboSeason


def test_default_seasons_are_idempotent_on_sqlite() -> None:
    engine = create_engine("sqlite:///:memory:")
    KboSeason.__table__.create(engine)

    with Session(engine) as session:
        _seed_default_seasons(session)
        _seed_default_seasons(session)

        count = session.scalar(select(func.count()).select_from(KboSeason))

    assert count == 49 * 6
