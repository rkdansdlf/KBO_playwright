"""Tests for stadiums master and places API endpoints."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.api.app import app
from src.models.base import Base
from src.models.place import Place
from src.models.stadium import Stadium

client = TestClient(app)


@pytest.fixture(name="db_session_factory", scope="module")
def _db_session_factory(tmp_path_factory: pytest.TempPathFactory) -> Generator[Any]:
    """Provide a file-based temporary SQLite database for sharing connections."""
    db_dir = tmp_path_factory.mktemp("stadiums_api_test_db")
    db_file = db_dir / "test.db"

    test_engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=test_engine)
    SessionFactory = sessionmaker(bind=test_engine, autoflush=False, autocommit=False, expire_on_commit=False)

    @contextmanager
    def mock_get_db_session() -> Generator[Session]:
        session = SessionFactory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    yield mock_get_db_session


@pytest.fixture(autouse=True)
def _setup_mock_db(db_session_factory: Any) -> Generator[None]:
    """Patch get_db_session for the stadiums router and seed one stadium."""
    with (
        patch("src.api.routers.stadiums.get_db_session", db_session_factory),
    ):
        with db_session_factory() as session:
            session.query(Place).delete()
            session.query(Stadium).delete()
            session.add(
                Stadium(
                    stadium_id="JAMSIL",
                    stadium_name="잠실야구장",
                    city="서울특별시 송파구",
                    left_fence_m=100.0,
                    homerun_park_factor=0.945,
                    is_dome=False,
                    lat=37.5114,
                    lng=127.0734,
                )
            )
            session.add(
                Place(
                    stadium_id="JAMSIL",
                    category="음식점",
                    name="BHC (1층 333)",
                    description="1층 333섹션 근처",
                    lat=37.5114,
                    lng=127.0734,
                )
            )
        yield


def test_get_stadiums_master() -> None:
    response = client.get("/api/v1/stadiums/master")
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    row = rows[0]
    assert row["stadium_id"] == "JAMSIL"
    assert row["left_fence_m"] == 100.0
    assert row["homerun_park_factor"] == 0.945
    assert row["is_dome"] is False


def test_get_stadium_places() -> None:
    response = client.get("/api/v1/stadiums/JAMSIL/places")
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["name"] == "BHC (1층 333)"
    assert rows[0]["category"] == "음식점"


def test_get_stadium_places_empty_for_unknown() -> None:
    response = client.get("/api/v1/stadiums/UNKNOWN/places")
    assert response.status_code == 200
    assert response.json() == []
