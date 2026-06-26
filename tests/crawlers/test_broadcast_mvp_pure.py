from __future__ import annotations

from src.crawlers.broadcast_crawler import BroadcastCrawler
from src.crawlers.game_mvp_crawler import GameMvpCrawler


class TestBroadcastNormalizeGameIds:
    def test_basic(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            },
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert len(result) == 1
        assert "game_id" in result[0]
        assert result[0]["broadcaster"] == "SPOTV"

    def test_empty_list(self):
        result = BroadcastCrawler._normalize_game_ids([], 2023)
        assert result == []

    def test_multiple_entries(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            },
            {
                "game_date": "20230625",
                "away_team_code": "KT",
                "home_team_code": "NC",
                "broadcaster": "KBS",
                "channel_name": "KBS",
                "source": "KBO",
            },
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert len(result) == 2

    def test_missing_game_date_skipped(self):
        data = [
            {
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            },
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert len(result) == 0

    def test_source_default(self):
        data = [
            {
                "game_date": "20230625",
                "away_team_code": "LG",
                "home_team_code": "SS",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
            },
        ]
        result = BroadcastCrawler._normalize_game_ids(data, 2023)
        assert result[0]["source"] == "KBO"


class TestGameMvpParsePlayer:
    def test_pattern_1(self):
        text = "홍길동 선수, MVP 수상"
        assert GameMvpCrawler._parse_mvp_player(text) == "홍길동"

    def test_pattern_2(self):
        text = "MVP: 김철수"
        assert GameMvpCrawler._parse_mvp_player(text) == "김철수"

    def test_pattern_3(self):
        text = "이영희, MVP"
        assert GameMvpCrawler._parse_mvp_player(text) == "이영희"

    def test_pattern_4(self):
        text = "박민수 MVP"
        assert GameMvpCrawler._parse_mvp_player(text) == "박민수"

    def test_no_match(self):
        text = "경기 결과 발표"
        assert GameMvpCrawler._parse_mvp_player(text) is None

    def test_empty_string(self):
        assert GameMvpCrawler._parse_mvp_player("") is None

    def test_two_char_name(self):
        text = "MVP 이상"
        assert GameMvpCrawler._parse_mvp_player(text) == "이상"

    def test_four_char_name(self):
        text = "MVP: 사나이"
        assert GameMvpCrawler._parse_mvp_player(text) == "사나이"


class TestGameMvpParseTeam:
    def test_lg(self):
        assert GameMvpCrawler._parse_mvp_team("LG 선수 MVP") == "LG"

    def test_kt(self):
        assert GameMvpCrawler._parse_mvp_team("KT 선수 MVP") == "KT"

    def test_nc(self):
        assert GameMvpCrawler._parse_mvp_team("NC 선수 MVP") == "NC"

    def test_doosan(self):
        assert GameMvpCrawler._parse_mvp_team("두산 선수 MVP") == "DB"

    def test_lotte(self):
        assert GameMvpCrawler._parse_mvp_team("롯데 선수 MVP") == "LT"

    def test_samsung(self):
        assert GameMvpCrawler._parse_mvp_team("삼성 선수 MVP") == "SS"

    def test_kiwoom(self):
        assert GameMvpCrawler._parse_mvp_team("키움 선수 MVP") == "KH"

    def test_hanwha(self):
        assert GameMvpCrawler._parse_mvp_team("한화 선수 MVP") == "HH"

    def test_kia(self):
        assert GameMvpCrawler._parse_mvp_team("KIA 선수 MVP") == "KIA"

    def test_ssg(self):
        assert GameMvpCrawler._parse_mvp_team("SSG 선수 MVP") == "SSG"

    def test_no_team(self):
        assert GameMvpCrawler._parse_mvp_team("MVP 수상") is None

    def test_empty_string(self):
        assert GameMvpCrawler._parse_mvp_team("") is None
