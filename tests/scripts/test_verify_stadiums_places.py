"""Tests for scripts.verify_stadiums_places (read-only drift check)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.place import Place
from src.models.stadium import Stadium


def _real_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    return factory()


class TestVerifyStadiumsPlaces:
    def test_ok_on_clean_data(self) -> None:
        with _real_session() as real_session, patch("scripts.verify_stadiums_places.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = real_session
            real_session.add(Stadium(stadium_id="TST", stadium_name="테스트구장"))
            real_session.add(Place(stadium_id="TST", category="음식점", name="김밥", lat=37.5, lng=127.0))
            real_session.commit()
            from scripts.verify_stadiums_places import run

            assert run() == 0

    def test_fails_on_orphan_place(self) -> None:
        with _real_session() as real_session, patch("scripts.verify_stadiums_places.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = real_session
            # Bypass the FK with a core insert so the orphan actually exists.
            from sqlalchemy import text

            real_session.execute(
                text(
                    "INSERT INTO places (stadium_id, category, name, lat, lng) "
                    "VALUES ('GHOST', '음식점', '유령김밥', 0, 0)"
                )
            )
            real_session.commit()
            from scripts.verify_stadiums_places import run

            assert run() == 1

    def test_main_returns_run_code(self) -> None:
        with patch("scripts.verify_stadiums_places.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.all.return_value = []
            mock_session.query.return_value.filter.return_value.count.return_value = 0
            mock_session.query.return_value.count.return_value = 0
            from scripts.verify_stadiums_places import main

            assert main([]) == 0
