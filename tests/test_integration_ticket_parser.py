"""
Integration tests for ticket_parser with realistic fixture HTML.
Tests price extraction from different team ticket page layouts.
"""

from __future__ import annotations

from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "html"

from src.parsers.ticket_parser import parse_ticket_page


def _load_fixture(name: str) -> str:
    path = FIXTURE_DIR / name
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


class TestLGTicketIntegration:
    """Test parser against LG ticket pricing HTML."""

    def test_load_fixture(self):
        html = _load_fixture("lg_ticket_prices.html")
        assert len(html) > 500

    def test_parses_weekday_prices(self):
        html = _load_fixture("lg_ticket_prices.html")
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        weekday = [r for r in result if r["day_type"] == "weekday"]
        assert len(weekday) >= 5
        prices = {r["seat_grade"]: r["price"] for r in weekday}
        assert prices.get("테이블석") == 150000
        assert prices.get("지정석") == 120000
        assert prices.get("블루석") == 50000

    def test_parses_weekend_prices(self):
        html = _load_fixture("lg_ticket_prices.html")
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        weekend = [r for r in result if r["day_type"] == "weekend"]
        assert len(weekend) >= 3
        prices = {r["seat_grade"]: r["price"] for r in weekend}
        assert prices.get("테이블석") == 180000
        assert prices.get("지정석") == 150000

    def test_team_id(self):
        html = _load_fixture("lg_ticket_prices.html")
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert all(r["team_id"] == "LG" for r in result)

    def test_stadium_id(self):
        html = _load_fixture("lg_ticket_prices.html")
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert all(r["stadium_id"] == "JAMSIL" for r in result)

    def test_output_schema(self):
        html = _load_fixture("lg_ticket_prices.html")
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert len(result) >= 8
        item = result[0]
        assert item["season"] == 2025
        assert item["currency"] == "KRW"
        assert item["audience_type"] == "general"
        assert item["effective_from"] is None
        assert item["effective_to"] is None


class TestSSGTicketIntegration:
    """Test parser against SSG ticket pricing HTML."""

    def test_parses_weekday_prices(self):
        html = _load_fixture("ssg_ticket_prices.html")
        result = parse_ticket_page(html, "ssg_landers_ticket", {"season": 2025})
        weekday = [r for r in result if r["day_type"] == "weekday"]
        prices = {r["seat_grade"]: r["price"] for r in weekday}
        assert prices.get("테이블석") == 140000
        assert prices.get("지정석") == 110000
        assert prices.get("익사이팅석") == 90000

    def test_parses_weekend_prices(self):
        html = _load_fixture("ssg_ticket_prices.html")
        result = parse_ticket_page(html, "ssg_landers_ticket", {"season": 2025})
        weekend = [r for r in result if r["day_type"] == "weekend"]
        prices = {r["seat_grade"]: r["price"] for r in weekend}
        assert prices.get("테이블석") == 170000
        assert prices.get("지정석") == 140000
        assert prices.get("익사이팅석") == 110000

    def test_team_id_sk(self):
        html = _load_fixture("ssg_ticket_prices.html")
        result = parse_ticket_page(html, "ssg_landers_ticket", {"season": 2025})
        assert all(r["team_id"] == "SK" for r in result)
