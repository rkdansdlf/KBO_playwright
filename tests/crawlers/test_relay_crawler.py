
from src.crawlers.relay_crawler import (
    RelayCrawler,
)


class TestMapToNaverId:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_converts_kbo_id(self):
        assert self.crawler._map_to_naver_id("20260412SKLG0") == "20260412SKLG02026"

    def test_different_year(self):
        assert self.crawler._map_to_naver_id("20251013SSHH0") == "20251013SSHH02025"


class TestNaverTeamCode:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_known_conversions(self):
        assert self.crawler._naver_team_code("DB") == "DO"
        assert self.crawler._naver_team_code("KH") == "WO"
        assert self.crawler._naver_team_code("SK") == "SSG"
        assert self.crawler._naver_team_code("HT") == "KIA"

    def test_unknown_passthrough(self):
        assert self.crawler._naver_team_code("XYZ") == "XYZ"

    def test_none_returns_empty(self):
        assert self.crawler._naver_team_code(None) == ""


class TestScheduleGameHasTeamMatch:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_exact_match(self):
        game = {"awayTeamCode": "SSG", "homeTeamCode": "LG"}
        assert self.crawler._schedule_game_has_team_match(game, "SSG", "LG") is True

    def test_mismatch(self):
        game = {"awayTeamCode": "SSG", "homeTeamCode": "LG"}
        assert self.crawler._schedule_game_has_team_match(game, "KT", "LG") is False

    def test_empty_fields_match_anything(self):
        game = {"awayTeamCode": "", "homeTeamCode": ""}
        assert self.crawler._schedule_game_has_team_match(game, "SSG", "LG") is True


class TestComputePayloadHash:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_consistent_hash(self):
        data = [{"title": "1회초", "textOptions": []}]
        h1 = self.crawler._compute_payload_hash(data)
        h2 = self.crawler._compute_payload_hash(data)
        assert h1 == h2
        assert len(h1) == 12


class TestProviderLogId:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_generates_id(self):
        log_id = self.crawler._provider_log_id(
            payload_hash="abc123def456",
            inning=1,
            half="top",
            segment_index=0,
            log_index=1,
            text="삼진: 헛스윙",
        )
        assert log_id.startswith("naver:abc123def456:1t:0:1:")


class TestParseSegmentInningHalf:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_korean_title(self):
        segment = {"title": "1회초"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 1
        assert half == "top"

    def test_home_or_away_field(self):
        segment = {"inn": 3, "homeOrAway": "0"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert inning == 3
        assert half == "top"

    def test_no_match_returns_none_half(self):
        segment = {"title": "unknown"}
        inning, half = self.crawler._parse_segment_inning_half(segment)
        assert half is None


class TestExpectedMatchValues:
    def setup_method(self):
        self.crawler = RelayCrawler()

    def test_standard_game_id(self):
        gdate, away, home, dh, year = self.crawler._expected_match_values("20260412SKLG0")
        assert gdate == "20260412"
        assert away == "SSG"
        assert home == "LG"
        assert dh == "0"
        assert year == "2026"
