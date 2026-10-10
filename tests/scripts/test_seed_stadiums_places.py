"""Tests for scripts.seed_stadiums_places."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base


class TestSeedStadiumsPlaces:
    def test_run_writes_and_commits(self) -> None:
        with patch("scripts.seed_stadiums_places.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            from scripts.seed_stadiums_places import run

            run(dry_run=False)
            mock_session.commit.assert_called_once()

    def test_run_dry_run_no_commit(self) -> None:
        with patch("scripts.seed_stadiums_places.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            from scripts.seed_stadiums_places import run

            run(dry_run=True)
            mock_session.commit.assert_not_called()

    def test_run_writes_nine_stadiums_to_sqlite(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        with factory() as real_session, patch("scripts.seed_stadiums_places.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = real_session
            from scripts.seed_stadiums_places import STADIUM_DATA, run
            from src.repositories.stadium_repository import StadiumRepository

            run(dry_run=False)
            rows = StadiumRepository(real_session).get_all()
            assert len(rows) == len(STADIUM_DATA) == 9
            gochok = StadiumRepository(real_session).get_by_id("GOCHEOK")
            assert gochok is not None
            assert gochok.turf_type == "인조"
            assert gochok.bullpen_type == "지하"
            assert gochok.homerun_park_factor is None

    def test_from_food_imports_vendor_with_pending_stadiums(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        with factory() as real_session, patch("scripts.seed_stadiums_places.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = real_session
            from scripts.seed_stadiums_places import run
            from src.models.stadium_food_vendor import StadiumFoodVendor
            from src.models.stadium_info import StadiumInfo
            from src.repositories.place_repository import PlaceRepository

            real_session.add(StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장"))
            real_session.add(StadiumFoodVendor(stadium_id="JAMSIL", vendor_name="테스트김밥", location_text="1층"))
            real_session.commit()

            run(dry_run=False, from_food=True)
            rows = PlaceRepository(real_session).get_by_stadium("JAMSIL")
            assert any(r.name == "테스트김밥" and r.lat == 37.5114 for r in rows)

    def test_main_parses_flags(self) -> None:
        from scripts.seed_stadiums_places import main

        assert main(["--dry-run"]) == 0
