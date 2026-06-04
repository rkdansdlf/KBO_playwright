import pytest
from datetime import datetime, timedelta

from src.parsers.team_event_parser import (
    parse_team_events,
    _classify_event,
    EVENT_KEYWORDS,
    TEAM_CODE_FROM_SOURCE_KEY,
    SOURCE_CONFIG_MAP,
)


class TestClassifyEvent:
    def test_giveaway(self):
        assert _classify_event("경품 이벤트") == "giveaway"
        assert _classify_event("굿즈 증정") == "giveaway"

    def test_first_pitch(self):
        assert _classify_event("시구 안내") == "first_pitch"

    def test_discount(self):
        assert _classify_event("할인 프로모션") == "discount"

    def test_fan_participation(self):
        assert _classify_event("사인회") == "fan_participation"

    def test_festival(self):
        assert _classify_event("페스티벌") == "festival"

    def test_promotion(self):
        assert _classify_event("모집 공고") == "promotion"

    def test_notice(self):
        assert _classify_event("개막 안내") == "notice"

    def test_notice_keyword(self):
        assert _classify_event("공지") == "notice"

    def test_default_promotion(self):
        assert _classify_event("기타 소식") == "promotion"


class TestParseTeamEvents:
    LG_HTML = """
    <html><body>
    <table>
        <tr>
            <td><a class="subject" href="/service/notice/123">2025 시즌 이벤트 안내</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/service/notice/124">사인회 및 팬미팅 개최</a></td>
            <td><span class="date">2025-03-20</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/service/notice/125">할인 프로모션 안내</a></td>
            <td><span class="date">2025-04-01</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/service/notice/126">선수단 훈련 일정입니다</a></td>
            <td><span class="date">2025-03-10</span></td>
        </tr>
    </table>
    </body></html>
    """

    def test_parse_lg_events(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        events = [e for e in result if e["source_url"]]
        assert len(events) == 3
        titles = [e["title"] for e in events]
        assert "2025 시즌 이벤트 안내" in titles
        assert "사인회 및 팬미팅 개최" in titles
        assert "할인 프로모션 안내" in titles

    def test_skips_non_event_keywords(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        titles = [e["title"] for e in result]
        assert "선수단 훈련 일정입니다" not in titles

    def test_source_url_with_link_prefix(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        for event in result:
            assert event["source_url"].startswith("https://www.lgtwins.com")

    def test_filters_by_cutoff_date(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 7,
            "fetched_at": "2025-04-10T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        assert all(e["published_at"] >= datetime(2025, 4, 3) for e in result)

    def test_unknown_source_key_returns_empty(self):
        result = parse_team_events("<html></html>", "unknown_source")
        assert result == []

    def test_output_schema(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        assert len(result) >= 3
        event = result[0]
        assert event["event_scope"] == "team"
        assert event["team_id"] == "LG"
        assert event["title"]
        assert event["event_type"] in ("giveaway", "first_pitch", "discount",
                                        "fan_participation", "festival", "promotion", "notice")
        assert isinstance(event["published_at"], datetime)
        assert event["source_url"].startswith("http")
        assert isinstance(event["last_seen_at"], datetime)
        assert event["status"] == "unknown"


class TestSourceConfigMap:
    def test_all_sources_have_selectors(self):
        for source_key, config in SOURCE_CONFIG_MAP.items():
            assert "title_sel" in config, f"{source_key} missing title_sel"
            assert "date_sel" in config, f"{source_key} missing date_sel"
            assert "link_prefix" in config, f"{source_key} missing link_prefix"

    def test_all_sources_have_mapping(self):
        for source_key in SOURCE_CONFIG_MAP:
            assert source_key in TEAM_CODE_FROM_SOURCE_KEY, f"{source_key} not in TEAM_CODE_FROM_SOURCE_KEY"
