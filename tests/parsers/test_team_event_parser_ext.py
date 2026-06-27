from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from src.constants import KST
from src.parsers.team_event_parser import (
    DATE_PATTERN,
    ONCLICK_HREF_PATTERN,
    TEAM_CODE_FROM_SOURCE_KEY,
    _classify_event,
    _extract_onclick_href,
    _extract_published_at,
    _extract_rows_from_dict_payload,
    _extract_source_url,
    _filter_dict_rows,
    _is_event_only_page,
    _is_event_title,
    _iter_json_rows,
    _match_onclick_href,
    _parse_date_text,
    _parse_fetched_at,
    _parse_json_team_events,
    parse_team_events,
)


class TestClassifyEventExtended:
    def test_all_event_types(self):
        assert _classify_event("증정 이벤트") == "giveaway"
        assert _classify_event("시구") == "first_pitch"
        assert _classify_event("할인") == "discount"
        assert _classify_event("프로모션") == "discount"
        assert _classify_event("사인회") == "fan_participation"
        assert _classify_event("페스티벌") == "festival"
        assert _classify_event("공연") == "festival"
        assert _classify_event("신청") == "promotion"
        assert _classify_event("모집") == "promotion"
        assert _classify_event("개막") == "notice"
        assert _classify_event("안내") == "notice"
        assert _classify_event("공지") == "notice"
        assert _classify_event("이벤트") == "promotion"
        assert _classify_event("팬") == "fan_participation"
        assert _classify_event("클래스") == "promotion"
        assert _classify_event("데이") == "promotion"
        assert _classify_event("투어") == "promotion"
        assert _classify_event("스페셜") == "promotion"
        assert _classify_event("매치") == "promotion"

    def test_unknown_title_defaults_to_promotion(self):
        assert _classify_event("잡담") == "promotion"
        assert _classify_event("일반 글") == "promotion"
        assert _classify_event("") == "promotion"


class TestMatchOnclickHref:
    def test_simple_href(self):
        result = _match_onclick_href("location.href='/event/1'")
        assert result == "/event/1"

    def test_double_quoted_href(self):
        result = _match_onclick_href('location.href="/event/2"')
        assert result == "/event/2"

    def test_no_match(self):
        assert _match_onclick_href("") == ""
        assert _match_onclick_href("no href here") == ""

    def test_whitespace_stripped(self):
        result = _match_onclick_href("location.href='  /event/3  '")
        assert result == "/event/3"


class TestExtractOnclickHref:
    def test_onclick_in_tag(self):
        from bs4 import BeautifulSoup

        html = "<tr onclick=\"location.href='/onclick'\"><td>Title</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        td = soup.td
        result = _extract_onclick_href(td)
        assert result == "/onclick"

    def test_onclick_in_ancestor(self):
        from bs4 import BeautifulSoup

        html = '<tr onclick="location.href=\'/ancestor\'"><td class="title">Title</td></tr>'
        soup = BeautifulSoup(html, "html.parser")
        td = soup.select_one(".title")
        result = _extract_onclick_href(td)
        assert result == "/ancestor"

    def test_no_onclick(self):
        from bs4 import BeautifulSoup

        html = "<tr><td><span>Title</span></td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        result = _extract_onclick_href(soup.span)
        assert result == ""


class TestFilterDictRows:
    def test_list_of_dicts(self):
        result = _filter_dict_rows([{"a": 1}, {"b": 2}])
        assert len(result) == 2

    def test_filters_non_dicts(self):
        result = _filter_dict_rows([{"a": 1}, "string", 42, {"b": 2}])
        assert len(result) == 2

    def test_non_list_input(self):
        assert _filter_dict_rows("string") == []
        assert _filter_dict_rows(None) == []
        assert _filter_dict_rows(42) == []


class TestExtractRowsFromDictPayload:
    def test_result_data(self):
        payload = {"result": {"data": [{"title": "a"}]}}
        result = _extract_rows_from_dict_payload(payload)
        assert len(result) == 1

    def test_result_content(self):
        payload = {"result": {"content": [{"title": "a"}]}}
        result = _extract_rows_from_dict_payload(payload)
        assert len(result) == 1

    def test_data_list(self):
        payload = {"data": [{"title": "a"}]}
        result = _extract_rows_from_dict_payload(payload)
        assert len(result) == 1

    def test_data_content(self):
        payload = {"data": {"content": [{"title": "a"}]}}
        result = _extract_rows_from_dict_payload(payload)
        assert len(result) == 1

    def test_empty_payload(self):
        assert _extract_rows_from_dict_payload({}) == []
        assert _extract_rows_from_dict_payload({"unknown": "value"}) == []


class TestParseDateTextExtended:
    def test_various_separators(self):
        assert _parse_date_text("2025.01.15") is not None
        assert _parse_date_text("2025-01-15") is not None
        assert _parse_date_text("2025/01/15") is not None

    def test_single_digit_month_day(self):
        result = _parse_date_text("2025.1.5")
        assert result is not None
        assert result.month == 1
        assert result.day == 5

    def test_datetime_string(self):
        result = _parse_date_text("2025-03-15 18:30")
        assert result is not None
        assert result.year == 2025

    def test_invalid_month(self):
        assert _parse_date_text("2025-13-01") is None

    def test_invalid_day(self):
        assert _parse_date_text("2025-01-32") is None

    def test_empty_string(self):
        assert _parse_date_text("") is None

    def test_none_input(self):
        assert _parse_date_text(None) is None


class TestIsEventTitleExtended:
    def test_all_keywords(self):
        for keyword in ["이벤트", "시구", "증정", "팬", "클래스", "신청", "모집", "프로모션", "할인"]:
            assert _is_event_title(f"{keyword} 제목", "/page") is True

    def test_non_event_title(self):
        assert _is_event_title("일반 기사", "/page") is False
        assert _is_event_title("", "/page") is False

    def test_event_only_page_overrides_title(self):
        assert _is_event_title("아무 제목", "/feed/events") is True
        assert _is_event_title("아무 제목", "/doorun/events") is True


class TestParseFetchedAtExtended:
    def test_iso_format_with_tz(self):
        dt = _parse_fetched_at({"fetched_at": "2025-06-01T12:00:00+09:00"})
        assert dt is not None
        assert dt.year == 2025

    def test_iso_format_without_tz(self):
        dt = _parse_fetched_at({"fetched_at": "2025-06-01T12:00:00"})
        assert dt is not None
        assert dt.tzinfo is not None

    def test_invalid_string_uses_now(self):
        dt = _parse_fetched_at({"fetched_at": "not-a-date"})
        assert dt is not None

    def test_none_metadata(self):
        dt = _parse_fetched_at(None)
        assert dt is not None

    def test_empty_string_fetched_at(self):
        dt = _parse_fetched_at({"fetched_at": ""})
        assert dt is not None


class TestExtractSourceUrlExtended:
    def test_javascript_href(self):
        soup = BeautifulSoup('<a href="javascript:void(0)">Title</a>', "html.parser")
        result = _extract_source_url(soup.a, {"link_prefix": ""}, "https://example.com/page")
        assert result == "https://example.com/page"

    def test_anchor_href(self):
        soup = BeautifulSoup('<a href="#section">Title</a>', "html.parser")
        result = _extract_source_url(soup.a, {"link_prefix": ""}, "https://example.com/page")
        assert result == "https://example.com/page"

    def test_relative_href_with_prefix(self):
        soup = BeautifulSoup('<a href="/event/123">Title</a>', "html.parser")
        result = _extract_source_url(soup.a, {"link_prefix": "https://www.lgtwins.com"}, "")
        assert result == "https://www.lgtwins.com/event/123"

    def test_nested_tag_in_link(self):
        soup = BeautifulSoup('<a href="/parent"><span class="title">Title</span></a>', "html.parser")
        result = _extract_source_url(soup.span, {"link_prefix": "https://example.com"}, "")
        assert result == "https://example.com/parent"

    def test_empty_tag_no_href(self):
        soup = BeautifulSoup("<span>Title</span>", "html.parser")
        result = _extract_source_url(soup.span, {"link_prefix": "https://example.com"}, "https://example.com/page")
        assert result == "https://example.com/page"


class TestExtractPublishedAtExtended:
    def test_date_within_cutoff(self):
        html = "<tr><td class='date'>2025-06-15</td><td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), ".date", cutoff)
        assert result is not None
        assert result.month == 6

    def test_date_before_cutoff_returns_none(self):
        html = "<tr><td class='date'>2024-06-15</td><td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), ".date", cutoff)
        assert result is None

    def test_no_date_sel_fallback_to_row(self):
        html = "<tr><td>2025-06-15</td><td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), "", cutoff)
        assert result is not None

    def test_no_date_in_row(self):
        html = "<tr><td>No date</td><td class='title'>Event</td></tr>"
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), "", cutoff)
        assert result is None

    def test_multiple_ancestors_checked(self):
        html = """
        <table>
            <tr class="row">
                <td class="date">2025-07-01</td>
                <td class="title"><a href="/event">Event</a></td>
            </tr>
        </table>
        """
        soup = BeautifulSoup(html, "html.parser")
        cutoff = datetime(2025, 1, 1, tzinfo=KST)
        result = _extract_published_at(soup.select_one(".title"), ".date", cutoff)
        assert result is not None


class TestParseJsonTeamEventsExtended:
    def test_lowercase_title_key(self):
        payload = json.dumps([{"title": "이벤트 안내", "pubDate": "2025-06-15"}])
        metadata = {"url": "https://www.lgtwins.com/feed/events", "fetched_at": "2025-06-01T00:00:00"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert len(events) == 1

    def test_created_date_key_with_url(self):
        payload = json.dumps([{"TITLE": "이벤트 안내", "createdDate": "2025-06-15"}])
        metadata = {"url": "https://www.lgtwins.com/feed/events", "fetched_at": "2025-06-01T00:00:00"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert len(events) == 1

    def test_show_date_key_with_url(self):
        payload = json.dumps([{"TITLE": "이벤트 안내", "showDate": "2025-06-15"}])
        metadata = {"url": "https://www.lgtwins.com/feed/events", "fetched_at": "2025-06-01T00:00:00"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert len(events) == 1

    def test_date_before_cutoff_excluded(self):
        payload = json.dumps([{"TITLE": "이벤트 안내", "PUB_DATE": "2024-01-01"}])
        metadata = {"url": "https://www.lgtwins.com/feed/events"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert len(events) == 0

    def test_non_event_title_excluded(self):
        payload = json.dumps([{"TITLE": "일반 기사", "PUB_DATE": "2025-06-15"}])
        metadata = {"url": "https://www.lgtwins.com/news"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert len(events) == 0

    def test_with_url_metadata(self):
        payload = json.dumps([{"TITLE": "이벤트 안내", "PUB_DATE": "2025-06-15", "ID": 123}])
        metadata = {"url": "https://www.lgtwins.com/feed/events", "fetched_at": "2025-06-01T00:00:00"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert len(events) == 1
        assert "#id=123" in events[0]["source_url"]

    def test_all_team_codes(self):
        payload = json.dumps([{"TITLE": "이벤트 안내", "PUB_DATE": "2025-06-15"}])
        for source_key, team_code in TEAM_CODE_FROM_SOURCE_KEY.items():
            metadata = {"url": "https://example.com/feed/events"}
            events = _parse_json_team_events(
                payload, source_key, metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
            )
            assert len(events) == 1
            assert events[0]["team_id"] == team_code

    def test_event_type_classification(self):
        payload = json.dumps(
            [
                {"TITLE": "증정 이벤트", "PUB_DATE": "2025-06-15"},
                {"TITLE": "시구 행사", "PUB_DATE": "2025-06-16"},
                {"TITLE": "할인 프로모션", "PUB_DATE": "2025-06-17"},
            ]
        )
        metadata = {"url": "https://www.lgtwins.com/feed/events", "fetched_at": "2025-06-01T00:00:00"}
        events = _parse_json_team_events(
            payload, "lg_twins_events", metadata, datetime(2025, 1, 1, tzinfo=KST), datetime(2025, 6, 1, tzinfo=KST)
        )
        assert events[0]["event_type"] == "giveaway"
        assert events[1]["event_type"] == "first_pitch"
        assert events[2]["event_type"] == "discount"


class TestParseTeamEventsExtended:
    def test_html_with_multiple_events(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">2025 시즌 이벤트</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/event/2">팬 사인회</a></td>
            <td><span class="date">2025-04-01</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/event/3">굿즈 판매</a></td>
            <td><span class="date">2025-05-10</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 365, "fetched_at": "2025-06-01T00:00:00", "url": "https://www.lgtwins.com"},
        )
        assert len(events) == 3
        assert events[0]["event_type"] == "promotion"
        assert events[1]["event_type"] == "fan_participation"
        assert events[2]["event_type"] == "giveaway"

    def test_html_with_no_matching_selector_falls_back(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a href="/event/1">이벤트 안내</a></td>
            <td class="date">2025-03-15</td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "hanwha_eagles_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) >= 1

    def test_duplicate_titles_deduplicated(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">이벤트 안내</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/event/2">이벤트 안내</a></td>
            <td><span class="date">2025-04-01</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 1

    def test_short_title_excluded(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">AB</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 0

    def test_non_event_title_excluded(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">일반 뉴스</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 0

    def test_event_only_page_accepts_all_titles(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">아무 제목</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00", "url": "https://www.lgtwins.com/feed/events"},
        )
        assert len(events) == 1

    def test_title_truncated_to_300_chars(self):
        long_title = "이벤트" * 200
        html = f"""
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">{long_title}</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 1
        assert len(events[0]["title"]) == 300

    def test_all_source_keys(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">이벤트 안내</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        </table>
        </body></html>
        """
        for source_key in TEAM_CODE_FROM_SOURCE_KEY:
            events = parse_team_events(
                html,
                source_key,
                {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00", "url": "https://example.com/feed/events"},
            )
            assert len(events) >= 1

    def test_empty_html(self):
        assert parse_team_events("", "lg_twins_events") == []

    def test_malformed_html(self):
        html = "<html><body><table><tr><td>Broken"
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert isinstance(events, list)

    def test_json_payload_preferred(self):
        payload = json.dumps(
            [
                {"TITLE": "JSON 이벤트", "PUB_DATE": "2025-03-15"},
            ]
        )
        events = parse_team_events(
            payload,
            "lg_twins_events",
            {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00", "url": "/feed/events"},
        )
        assert len(events) == 1
        assert events[0]["title"] == "JSON 이벤트"

    def test_custom_cutoff_days(self):
        html = """
        <html><body>
        <table>
        <tr>
            <td><a class="subject" href="/event/1">이벤트 안내</a></td>
            <td><span class="date">2024-06-01</span></td>
        </tr>
        </table>
        </body></html>
        """
        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 365, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 1

        events = parse_team_events(
            html,
            "lg_twins_events",
            {"cutoff_days": 30, "fetched_at": "2025-06-01T00:00:00"},
        )
        assert len(events) == 0


class TestRegexPatterns:
    def test_date_pattern_variants(self):
        assert DATE_PATTERN.search("2025-01-15")
        assert DATE_PATTERN.search("2025.01.15")
        assert DATE_PATTERN.search("2025/01/15")
        assert DATE_PATTERN.search("Event on 2025-03-15")
        assert not DATE_PATTERN.search("No date here")

    def test_onclick_pattern_variants(self):
        assert ONCLICK_HREF_PATTERN.search("location.href='/event'")
        assert ONCLICK_HREF_PATTERN.search('location.href="/event"')
        assert not ONCLICK_HREF_PATTERN.search("no href")
