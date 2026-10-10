"""ORM + repository tests for stadiums/places master tables."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.place import Place
from src.models.stadium import Stadium
from src.repositories.place_repository import PlaceRepository
from src.repositories.stadium_repository import StadiumRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


def test_stadium_orm_crud(session) -> None:
    repo = StadiumRepository(session)
    repo.save(
        {
            "stadium_id": "TST",
            "stadium_name": "테스트구장",
            "city": "서울",
            "left_fence_m": 100.0,
            "center_fence_m": 125.0,
            "turf_type": "천연",
            "homerun_park_factor": None,
        }
    )
    session.commit()

    row = repo.get_by_id("TST")
    assert row is not None
    assert row.stadium_name == "테스트구장"
    assert row.homerun_park_factor is None

    repo.update_park_factor("TST", 1.05)
    session.commit()
    assert repo.get_by_id("TST").homerun_park_factor == 1.05


def test_stadium_upsert_keeps_non_null(session) -> None:
    repo = StadiumRepository(session)
    repo.save({"stadium_id": "TST", "stadium_name": "테스트구장", "capacity": 20000})
    repo.save({"stadium_id": "TST", "stadium_name": "테스트구장", "capacity": None})
    session.commit()
    assert repo.get_by_id("TST").capacity == 20000


def test_place_upsert_and_stadium_scope(session) -> None:
    stadium_repo = StadiumRepository(session)
    place_repo = PlaceRepository(session)
    stadium_repo.save({"stadium_id": "TST", "stadium_name": "테스트구장"})
    place_repo.save(
        {"stadium_id": "TST", "category": "음식점", "name": "김밥", "description": "1층", "lat": 37.5, "lng": 127.0}
    )
    place_repo.save(
        {"stadium_id": "TST", "category": "음식점", "name": "김밥", "description": "2층", "lat": 37.5, "lng": 127.0}
    )
    session.commit()

    rows = place_repo.get_by_stadium("TST")
    assert len(rows) == 1
    assert rows[0].description == "2층"
    assert isinstance(rows[0], Place)
    assert isinstance(rows[0].to_dict()["name"], str)


def test_models_registered_in_metadata() -> None:
    assert "stadiums" in Base.metadata.tables
    assert "places" in Base.metadata.tables
    assert isinstance(Stadium(stadium_id="X", stadium_name="Y").__repr__(), str)
