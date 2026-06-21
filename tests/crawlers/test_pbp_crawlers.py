from __future__ import annotations

from src.crawlers.pbp_bs4_crawler import PBPBS4Crawler
from src.crawlers.pbp_crawler import PBPCrawler


class FakePBPPage:
    def __init__(self, spans: list[dict[str, str]]) -> None:
        self.spans = spans

    async def evaluate(self, _script: str) -> list[dict[str, str]]:
        return self.spans


class TestPBPBS4ParseInningHeader:
    def setup_method(self):
        self.crawler = PBPBS4Crawler()

    def test_top_of_inning(self):
        result = self.crawler._parse_inning_header("1회초 삼성 공격", 0)
        assert result == {"inning": 1, "half": "top"}

    def test_bottom_of_inning(self):
        result = self.crawler._parse_inning_header("3회말 LG 공격", 2)
        assert result == {"inning": 3, "half": "bottom"}

    def test_fallback_to_index(self):
        result = self.crawler._parse_inning_header("Some header text", 5)
        assert result == {"inning": 6, "half": "unknown"}

    def test_extra_inning(self):
        result = self.crawler._parse_inning_header("10회초 두산 공격", 0)
        assert result == {"inning": 10, "half": "top"}


class TestPBPBS4FormatBaseString:
    def setup_method(self):
        self.crawler = PBPBS4Crawler()

    def test_no_runners(self):
        assert self.crawler._format_base_string(0) == "---"

    def test_first_base(self):
        assert self.crawler._format_base_string(1) == "1--"

    def test_second_base(self):
        assert self.crawler._format_base_string(2) == "-2-"

    def test_third_base(self):
        assert self.crawler._format_base_string(4) == "--3"

    def test_first_and_second(self):
        assert self.crawler._format_base_string(3) == "12-"

    def test_first_and_third(self):
        assert self.crawler._format_base_string(5) == "1-3"

    def test_loaded_bases(self):
        assert self.crawler._format_base_string(7) == "123"


class TestPBPCrawlerParseInningHeader:
    def setup_method(self):
        self.crawler = PBPCrawler()

    def test_top_of_inning(self):
        result = self.crawler._parse_inning_header("1회초", 0)
        assert result == {"inning": 1, "half": "top"}

    def test_bottom_of_inning(self):
        result = self.crawler._parse_inning_header("5회말", 4)
        assert result == {"inning": 5, "half": "bottom"}

    def test_fallback_to_index(self):
        result = self.crawler._parse_inning_header("No inning text", 3)
        assert result == {"inning": 4, "half": "unknown"}


class TestPBPCrawlerFormatBaseString:
    def setup_method(self):
        self.crawler = PBPCrawler()

    def test_no_runners(self):
        assert self.crawler._format_base_string(0) == "---"

    def test_first_base(self):
        assert self.crawler._format_base_string(1) == "1--"

    def test_second_base(self):
        assert self.crawler._format_base_string(2) == "-2-"

    def test_third_base(self):
        assert self.crawler._format_base_string(4) == "--3"

    def test_loaded_bases(self):
        assert self.crawler._format_base_string(7) == "123"


class TestPBPCrawlerExtractFlatEventsLegacy:
    def setup_method(self):
        self.crawler = PBPCrawler()

    async def test_extracts_reverse_chronological_spans_forward(self):
        page = FakePBPPage(
            [
                {"text": "타자 이타자 : 병살 아웃", "class": "normaiflTxt"},
                {"text": "타자 김현수 : 솔로 홈런", "class": "red"},
                {"text": "경기 시작", "class": "normaiflTxt"},
                {"text": "1회초 삼성 공격", "class": "blue"},
            ]
        )

        events = await self.crawler._extract_flat_events_legacy(page)  # type: ignore[arg-type]

        assert [event["description"] for event in events] == [
            "타자 김현수 : 솔로 홈런",
            "타자 이타자 : 병살 아웃",
        ]
        assert events[0]["event_seq"] == 1
        assert events[0]["inning"] == 1
        assert events[0]["inning_half"] == "top"
        assert events[0]["away_score"] == 1
        assert events[0]["batter"] == "김현수"
        assert events[0]["result"] == "솔로 홈런"

    async def test_returns_empty_when_no_spans(self):
        page = FakePBPPage([])

        events = await self.crawler._extract_flat_events_legacy(page)  # type: ignore[arg-type]

        assert events == []
