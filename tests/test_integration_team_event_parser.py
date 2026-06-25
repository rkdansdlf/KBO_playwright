"""Integration tests for team_event_parser with realistic fixture HTML.
Tests selector-based extraction across multiple team site layouts.
"""

from __future__ import annotations
from src.constants import KST

from datetime import datetime
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "html"

from src.parsers.team_event_parser import parse_team_events


def _load_fixture(name: str) -> str:
    path = FIXTURE_DIR / name
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


METADATA = {
    "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
    "cutoff_days": 90,
    "fetched_at": "2025-06-01T00:00:00",
}


class TestLGEventsIntegration:
    """Test parser against LG notice board HTML (selectors: a.subject, span.date)."""

    def test_load_fixture(self):
        html = _load_fixture("lg_events_notice.html")
        assert len(html) > 500

    def test_parses_event_titles(self):
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", METADATA)
        titles = [e["title"] for e in result]
        assert "2025 시즌 이벤트 안내" in titles
        assert "5월 사인회 및 팬미팅 개최 안내" in titles
        assert "할인 프로모션 안내" in titles

    def test_parses_event_types(self):
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", METADATA)
        types = {e["title"]: e["event_type"] for e in result}
        assert types.get("굿즈 증정 이벤트") == "giveaway"
        assert types.get("시구자 모집 공고") == "first_pitch"

    def test_source_urls_have_link_prefix(self):
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", METADATA)
        for event in result:
            assert event["source_url"].startswith("https://www.lgtwins.com")

    def test_parses_published_at(self):
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", METADATA)
        for event in result:
            assert isinstance(event["published_at"], datetime)

    def test_includes_notice_keyword_titles(self):
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", METADATA)
        titles = [e["title"] for e in result]
        assert "경기 일정 변경 안내" in titles

    def test_output_schema(self):
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", METADATA)
        assert len(result) >= 6
        event = result[0]
        assert event["team_id"] == "LG"
        assert event["event_scope"] == "team"
        assert event["status"] == "unknown"
        assert event["last_seen_at"] is not None

    def test_filters_by_cutoff(self):
        recent_metadata = dict(METADATA, cutoff_days=7, fetched_at="2025-03-25T00:00:00")
        html = _load_fixture("lg_events_notice.html")
        result = parse_team_events(html, "lg_twins_events", recent_metadata)
        for event in result:
            assert event["published_at"] >= datetime(2025, 3, 18, tzinfo=KST)


class TestHHEventsIntegration:
    """Test parser against Hanwha notice board HTML (selectors: td.tit a, td.date)."""

    def test_parses_events(self):
        html = _load_fixture("hh_events_notice.html")
        result = parse_team_events(html, "hanwha_eagles_events", METADATA)
        titles = [e["title"] for e in result]
        assert "2025 시즌 팬 페스티벌 개최" in titles
        assert "경기일 할인 이벤트 안내" in titles

    def test_team_code_is_hh(self):
        html = _load_fixture("hh_events_notice.html")
        result = parse_team_events(html, "hanwha_eagles_events", METADATA)
        assert all(e["team_id"] == "HH" for e in result)


class TestOBEventsIntegration:
    """Test parser against Doosan notice board HTML (selectors: td.title a, td.date)."""

    def test_parses_events(self):
        html = _load_fixture("ob_events_notice.html")
        result = parse_team_events(html, "doosan_bears_events", METADATA)
        titles = [e["title"] for e in result]
        assert "2025 두산 베어스 팬클럽 모집 안내" in titles
        assert "시구자 모집 공고" in titles

    def test_team_code_is_ob(self):
        html = _load_fixture("ob_events_notice.html")
        result = parse_team_events(html, "doosan_bears_events", METADATA)
        assert all(e["team_id"] == "OB" for e in result)
