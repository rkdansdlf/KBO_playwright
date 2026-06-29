import json
from datetime import datetime
from src.constants import KST

from src.parsers.team_event_parser import (
    CUTOFF_DAYS,
    EVENT_KEYWORDS,
    EVENT_TEAM_NAME_MAP,
    SOURCE_CONFIG_MAP,
    TEAM_CODE_FROM_SOURCE_KEY,
    _classify_event,
    _extract_published_at,
    _extract_source_url,
    _is_event_only_page,
    _is_event_title,
    _iter_json_rows,
    _parse_date_text,
    _parse_fetched_at,
    _parse_json_team_events,
    parse_team_events,
)


class TestClassifyEvent:
    def test_giveaway(self):
        assert _classify_event("경품 증정 이벤트") == "giveaway"
        assert _classify_event("굿즈 판매") == "giveaway"

    def test_first_pitch(self):
        assert _classify_event("시구 행사") == "first_pitch"

    def test_discount(self):
        assert _classify_event("할인 프로모션") == "discount"

    def test_fan_participation(self):
        assert _classify_event("사인회") == "fan_participation"
        assert _classify_event("팬미팅") == "fan_participation"

    def test_festival(self):
        assert _classify_event("페스티벌") == "festival"
        assert _classify_event("공연 안내") == "festival"

    def test_promotion_default(self):
        assert _classify_event("일반 이벤트") == "promotion"
        assert _classify_event("모집 안내") == "promotion"
        assert _classify_event("클래스 신청") == "promotion"

    def test_notice(self):
        assert _classify_event("개막 안내") == "notice"
        assert _classify_event("공지 사항") == "notice"


class TestParseFetchedAt:
    def test_valid_date(self):
        dt = _parse_fetched_at({"fetched_at": "2025-06-01T12:00:00"})
        assert dt.year == 2025
        assert dt.month == 6
        assert dt.day == 1

    def test_invalid_date(self):
        dt = _parse_fetched_at({"fetched_at": "invalid"})
        assert dt is not None

    def test_empty_metadata(self):
        dt = _parse_fetched_at(None)
        assert dt is not None

    def test_empty_dict(self):
        dt = _parse_fetched_at({})
        assert dt is not None


class TestParseDateText:
    def test_standard_format(self):
        d = _parse_date_text("2025-03-15")
        assert d is not None
        assert d.year == 2025
        assert d.month == 3
        assert d.day == 15

    def test_dot_format(self):
        d = _parse_date_text("2025.03.15")
        assert d is not None

    def test_slash_format(self):
        d = _parse_date_text("2025/03/15")
        assert d is not None

    def test_no_match(self):
        assert _parse_date_text("") is None
        assert _parse_date_text("abc") is None

    def test_invalid_date_value(self):
        d = _parse_date_text("2025-13-40")
        assert d is None


class TestIsEventOnlyPage:
    def test_event_markers(self):
        assert _is_event_only_page("/feed/events") is True
        assert _is_event_only_page("/doorun/events") is True
        assert _is_event_only_page("newstype=event") is True

    def test_non_event(self):
        assert _is_event_only_page("/news/list") is False
        assert _is_event_only_page("") is False
        assert _is_event_only_page(None) is False


class TestIsEventTitle:
    def test_event_only_page_returns_true(self):
        assert _is_event_title("일반 공지", "/feed/events") is True

    def test_keyword_match(self):
        assert _is_event_title("이벤트 안내", "/news") is True
        assert _is_event_title("팬 클래스 모집", "/news") is True

    def test_no_keyword(self):
        assert _is_event_title("일반 뉴스 기사", "/news") is False


class TestIterJsonRows:
    def test_list_input(self):
        result = _iter_json_rows([{"title": "a"}, {"title": "b"}])
        assert len(result) == 2

    def test_dict_with_content_list(self):
        result = _iter_json_rows({"content": [{"title": "a"}, {"title": "b"}]})
        assert len(result) == 2

    def test_dict_with_result_data(self):
        result = _iter_json_rows({"result": {"data": [{"title": "a"}]}})
        assert len(result) == 1

    def test_dict_with_result_content(self):
        result = _iter_json_rows({"result": {"content": [{"title": "a"}]}})
        assert len(result) == 1

    def test_dict_with_data_list(self):
        result = _iter_json_rows({"data": [{"title": "a"}]})
        assert len(result) == 1

    def test_dict_with_data_content(self):
        result = _iter_json_rows({"data": {"content": [{"title": "a"}]}})
        assert len(result) == 1

    def test_empty_or_invalid(self):
        assert _iter_json_rows([]) == []
        assert _iter_json_rows({}) == []
        assert _iter_json_rows("string") == []
        assert _iter_json_rows(None) == []


class TestExtractSourceUrl:
    def test_direct_href(self, monkeypatch):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup('<a href="/event/1">Title</a>', "html.parser")
        tag = soup.a
        config = {"link_prefix": "https://example.com"}
        url = _extract_source_url(tag, config, "https://example.com/page")
        assert url == "https://example.com/event/1"

    def test_full_url(self, monkeypatch):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup('<a href="https://other.com/event">Title</a>', "html.parser")
        url = _extract_source_url(soup.a, {"link_prefix": ""}, "https://example.com/page")
        assert url == "https://other.com/event"

    def test_parent_href(self, monkeypatch):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup('<a href="/parent-link"><span>Title</span></a>', "html.parser")
        span = soup.span
        config = {"link_prefix": "https://example.com"}
        url = _extract_source_url(span, config, "")
        assert url == "https://example.com/parent-link"

    def test_onclick_href(self, monkeypatch):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("<tr onclick=\"location.href='/onclick-link'\"><td>Title</td></tr>", "html.parser")
        td = soup.td
        config = {"link_prefix": "https://example.com"}
        url = _extract_source_url(td, config, "")
        assert url == "https://example.com/onclick-link"

    def test_javascript_href_returns_page_url(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup('<a href="javascript:void(0)">Title</a>', "html.parser")
        url = _extract_source_url(soup.a, {"link_prefix": ""}, "https://example.com/page")
        assert url == "https://example.com/page"

    def test_empty_href_returns_page_url(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("<span>Title</span>", "html.parser")
        url = _extract_source_url(soup.span, {"link_prefix": ""}, "https://example.com/page")
        assert url == "https://example.com/page"


class TestExtractPublishedAt:
    def test_date_from_date_sel(self):
        from bs4 import BeautifulSoup

        html = "<tr><td class='date'>2025-03-15</td><td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.select_one(".title")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(title_tag, ".date", cutoff)
        assert result is not None
        assert result.year == 2025

    def test_date_before_cutoff(self):
        from bs4 import BeautifulSoup

        html = "<tr><td class='date'>2024-01-01</td><td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), ".date", cutoff)
        assert result is None

    def test_fallback_to_row_text(self):
        from bs4 import BeautifulSoup

        html = "<tr>2025-06-15<td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), "", cutoff)
        assert result is not None

    def test_no_date_found(self):
        from bs4 import BeautifulSoup

        html = "<tr><td>No date here</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.td, "", cutoff)
        assert result is None


class TestParseJsonTeamEvents:
    def test_basic_json_events(self):
        payload = json.dumps(
            [
                {"TITLE": "2025 시즌 이벤트", "PUB_DATE": "2025-03-15"},
                {"TITLE": "팬 사인회", "PUB_DATE": "2025-04-01"},
            ],
        )
        metadata = {"url": "https://example.com/feed/events", "fetched_at": "2025-06-01T00:00:00"}
        events = _parse_json_team_events(
            payload,
            "lg_twins_events",
            metadata,
            datetime(2025, 1, 1, tzinfo=KST),
            datetime(2025, 6, 1, tzinfo=KST),
        )
        assert len(events) == 2
        assert events[0]["title"] == "2025 시즌 이벤트"
        assert events[0]["team_id"] == "LG"

    def test_title_too_short(self):
        payload = json.dumps([{"TITLE": "AB", "PUB_DATE": "2025-03-15"}])
        events = _parse_json_team_events(
            payload,
            "lg_twins_events",
            {},
            datetime(2025, 1, 1, tzinfo=KST),
            datetime(2025, 6, 1, tzinfo=KST),
        )
        assert len(events) == 0

    def test_deduplicates_titles(self):
        payload = json.dumps(
            [
                {"TITLE": "이벤트 안내", "PUB_DATE": "2025-03-15"},
                {"TITLE": "이벤트 안내", "PUB_DATE": "2025-03-16"},
            ],
        )
        events = _parse_json_team_events(
            payload,
            "lg_twins_events",
            {},
            datetime(2025, 1, 1, tzinfo=KST),
            datetime(2025, 6, 1, tzinfo=KST),
        )
        assert len(events) == 1

    def test_unparsable_json(self):
        events = _parse_json_team_events(
            "not json",
            "lg_twins_events",
            {},
            datetime(2025, 1, 1, tzinfo=KST),
            datetime(2025, 6, 1, tzinfo=KST),
        )
        assert events == []

    def test_unknown_team_returns_empty(self):
        payload = json.dumps([{"TITLE": "Event", "PUB_DATE": "2025-03-15"}])
        events = _parse_json_team_events(
            payload,
            "unknown_key",
            {},
            datetime(2025, 1, 1, tzinfo=KST),
            datetime(2025, 6, 1, tzinfo=KST),
        )
        assert events == []


class TestParseTeamEvents:
    def test_html_events_lg(self):
        html = """
        <html><body>
        <table>
        <tr>
        <td><a class="subject" href="/notice/1">2025 시즌 이벤트 안내</a></td>
        <td><span class="date">2025-03-15</span></td>
        </tr>
        <tr>
        <td><a class="subject" href="/notice/2">팬 사인회 모집</a></td>
        <td><span class="date">2025-04-01</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00", "url": "https://www.lgtwins.com"},
        )
        assert len(events) >= 2
        assert events[0]["team_id"] == "LG"
        assert events[0]["event_scope"] == "team"
        assert len(events[0]["title"]) > 4

    def test_unknown_source_key(self):
        result = parse_team_events("<html></html>", "unknown_key")
        assert result == []

    def test_empty_html(self):
        result = parse_team_events("", "lg_twins_events")
        assert result == []

    def test_json_events_preferred_over_html(self):
        payload = json.dumps(
            [
                {"TITLE": "2025 시즌 이벤트", "PUB_DATE": "2025-03-15"},
            ],
        )
        events = parse_team_events(
            payload,
            "lg_twins_events",
            {"url": "/feed/events", "cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 1

    def test_no_event_titles_returns_empty(self):
        html = "<html><body><a href='/news'>일반 뉴스</a></body></html>"
        events = parse_team_events(html, "lg_twins_events", {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"})
        assert events == []

    def test_fallback_to_all_links_when_no_selector_match(self):
        html = (
            "<html><body><table><tr>"
            "<td><a href='/event/1'>2025 이벤트 안내</a></td>"
            "<td class='date'>2025-03-15</td>"
            "</tr></table></body></html>"
        )
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00", "url": "https://www.lgtwins.com"},
        )
        assert len(events) >= 1


class TestConstants:
    def test_event_keywords_not_empty(self):
        assert len(EVENT_KEYWORDS) > 10

    def test_all_team_mappings_present(self):
        assert len(TEAM_CODE_FROM_SOURCE_KEY) == 10
        assert len(SOURCE_CONFIG_MAP) == 10

    def test_event_team_name_map(self):
        assert EVENT_TEAM_NAME_MAP["LG"] == "LG"
        assert EVENT_TEAM_NAME_MAP["SK"] == "SK"

    def test_cutoff_days(self):
        assert CUTOFF_DAYS == 60
