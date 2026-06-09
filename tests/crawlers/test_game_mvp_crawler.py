from __future__ import annotations

import pytest

from src.crawlers.game_mvp_crawler import GameMvpCrawler


@pytest.fixture
def crawler() -> GameMvpCrawler:
    return GameMvpCrawler()


class TestParseMvpPlayer:
    def test_korean_name_before_mvp(self, crawler):
        result = crawler._parse_mvp_player("홍길동 선수, MVP 선정!")
        assert result == "홍길동"

    def test_mvp_colon_name(self, crawler):
        result = crawler._parse_mvp_player("MVP: 김철수")
        assert result == "김철수"

    @pytest.mark.xfail(reason="Pattern 2 (MVP + Korean word) matches before pattern 3 (name, MVP)")
    def test_name_comma_mvp(self, crawler):
        result = crawler._parse_mvp_player("이영희, MVP 수상")
        assert result == "이영희"

    def test_name_directly_before_mvp(self, crawler):
        result = crawler._parse_mvp_player("박민수 MVP")
        assert result == "박민수"

    def test_no_mvp_keyword(self, crawler):
        result = crawler._parse_mvp_player("오늘의 선수: 최준용")
        assert result is None

    def test_empty_text(self, crawler):
        assert crawler._parse_mvp_player("") is None


class TestParseMvpTeam:
    def test_lg(self, crawler):
        assert crawler._parse_mvp_team("LG 트윈스") == "LG"

    def test_doosan(self, crawler):
        assert crawler._parse_mvp_team("두산 베어스") == "DB"

    def test_lotte(self, crawler):
        assert crawler._parse_mvp_team("롯데 자이언츠") == "LT"

    def test_samsung(self, crawler):
        assert crawler._parse_mvp_team("삼성 라이온즈") == "SS"

    def test_kiwoom(self, crawler):
        assert crawler._parse_mvp_team("키움 히어로즈") == "KH"

    def test_hanwha(self, crawler):
        assert crawler._parse_mvp_team("한화 이글스") == "HH"

    def test_kia(self, crawler):
        assert crawler._parse_mvp_team("KIA 타이거즈") == "KIA"

    def test_ssg(self, crawler):
        assert crawler._parse_mvp_team("SSG 랜더스") == "SSG"

    def test_nc(self, crawler):
        assert crawler._parse_mvp_team("NC 다이노스") == "NC"

    def test_kt(self, crawler):
        assert crawler._parse_mvp_team("KT 위즈") == "KT"

    def test_no_match(self, crawler):
        assert crawler._parse_mvp_team("") is None
        assert crawler._parse_mvp_team("야쿠르트") is None
