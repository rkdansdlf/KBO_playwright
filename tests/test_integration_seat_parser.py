"""
Integration tests for seat/parking/food parsers with realistic fixture HTML.
"""

from __future__ import annotations

from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "html"

from src.parsers.seat_parser import parse_seat_sections


def _load_fixture(name: str) -> str:
    path = FIXTURE_DIR / name
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


class TestSeatIntegration:
    def test_lg_seat_page(self):
        html = _load_fixture("lg_ticket_prices.html")
        result = parse_seat_sections(html, "lg_twins_seat")
        names = [s["section_name"] for s in result]
        assert "테이블석" in names
        assert "지정석" in names
        assert "블루석" in names
        assert "오렌지석" in names
        assert "파크존" in names

    def test_ssg_seat_page(self):
        html = _load_fixture("ssg_ticket_prices.html")
        result = parse_seat_sections(html, "lg_twins_seat")
        names = [s["section_name"] for s in result]
        assert "테이블석" in names
        assert "지정석" in names
        assert "익사이팅석" in names
