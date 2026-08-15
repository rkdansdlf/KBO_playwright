"""Integration tests for Stadiums and Facilities API."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import app
from src.models.parking_lot import ParkingLot
from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.models.stadium_info import StadiumInfo
from src.models.stadium_seat_section import StadiumSeatSection
from src.models.ticket_price import TicketPrice

client = TestClient(app)
AUTH_HEADERS = {"X-API-Key": os.getenv("REST_API_KEY", "")}


def test_get_stadiums_api() -> None:
    """Test GET /api/v1/stadiums."""
    mock_stadium = StadiumInfo(
        stadium_code="JAMSIL",
        name_kr="잠실종합운동장 야구장",
        home_team_id="LG,DB",
        capacity=23750,
        location="서울",
        address="서울특별시 송파구",
        is_active=True,
    )

    with patch("src.api.routers.stadiums.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_stadium]

        res = client.get("/api/v1/stadiums", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["stadium_code"] == "JAMSIL"
        assert "LG" in data[0]["home_teams"]


def test_get_stadium_facilities_api() -> None:
    """Test GET /api/v1/stadiums/{stadium_code}/facilities."""
    mock_stadium = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장", home_team_id="LG")
    mock_parking = ParkingLot(
        stadium_id="JAMSIL",
        name="탄천주차장",
        lot_type="public",
        capacity=500,
        address="서울시 송파구 탄천변",
    )
    mock_vendor = StadiumFoodVendor(
        stadium_id="JAMSIL",
        vendor_name="신철판와플",
        location_text="1루 외야",
        gate_info="Gate 1",
        order_method="onsite",
        id=1,
    )
    mock_menu = StadiumFoodMenuItem(
        vendor_id=1, menu_name="생크림와플", price=7000, category="dessert", is_signature=True
    )
    mock_seat = StadiumSeatSection(
        stadium_id="JAMSIL",
        section_name="네이비석",
        seat_grade="네이비",
        base_side="first_base",
    )
    mock_ticket_price = TicketPrice(
        team_id="LG",
        stadium_id="JAMSIL",
        season=2026,
        seat_grade="네이비",
        day_type="weekday",
        price=13000,
    )

    with patch("src.api.routers.stadiums.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = mock_stadium

        mock_session.execute.side_effect = [
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_parking])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_vendor])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_menu])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_seat])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_ticket_price])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_ticket_price])))),
        ]

        res = client.get("/api/v1/stadiums/JAMSIL/facilities", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert data["stadium_code"] == "JAMSIL"
        assert len(data["parkings"]) == 1
        assert data["parkings"][0]["name"] == "탄천주차장"
        assert len(data["food_vendors"]) == 1
        assert data["food_vendors"][0]["vendor_name"] == "신철판와플"
        assert data["food_vendors"][0]["popular_menu"] == "생크림와플 (7,000원)"
        assert len(data["seat_sections"]) == 1
        assert data["seat_sections"][0]["section_name"] == "네이비석"
        assert data["seat_sections"][0]["weekday_price"] == 13000
        assert len(data["ticket_prices"]) == 1
