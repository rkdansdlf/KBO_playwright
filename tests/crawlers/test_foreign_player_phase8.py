from __future__ import annotations

from src.crawlers.foreign_player_crawler import ForeignPlayerCrawler


class TestExtractForeignPlayerNameExtended:
    def test_foreign_hitter_korean(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "새 외국인 타자 김철수 계약"
        result = crawler._extract_foreign_player_name(text)
        assert result == "김철수"

    def test_foreign_pitcher_signing(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 투수 이영호 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result == "이영호"

    def test_new_foreign_player(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "새 외국인 선수 박민재 합류"
        result = crawler._extract_foreign_player_name(text)
        assert result == "박민재"

    def test_release_name_extraction(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 타자 John Smith 방출"
        result = crawler._extract_foreign_player_name(text)
        assert result == "John Smith"

    def test_renewal_name_extraction(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 투수 Mike Johnson 재계약"
        result = crawler._extract_foreign_player_name(text)
        assert result == "Mike Johnson"

    def test_dot_in_name(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 타자 A.B. Martinez 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result == "Martinez"

    def test_korean_surname_only(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 투수 이영호 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result == "이영호"

    def test_three_letter_korean_name(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 선수 홍길동 계약"
        result = crawler._extract_foreign_player_name(text)
        assert result == "홍길동"

    def test_four_letter_korean_name(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 선수 김철수희 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result is not None

    def test_invalid_name_부상(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 부상 교체"
        result = crawler._extract_foreign_player_name(text)
        assert result is None

    def test_invalid_name_대체(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 대체 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result is None

    def test_english_name_long_filtered(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 선수 AB 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result is None

    def test_kbo_filtered(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 선수 KBO 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result is None

    def test_two_word_english_name(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 David Wright 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result == "David Wright"

    def test_three_word_english_name(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 선수 John Jacob Smith 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result is not None


class TestDetectChangeTypeExtended:
    def test_퇴출_is_released(self):
        assert ForeignPlayerCrawler._detect_change_type("외국인 선수 퇴출") == "RELEASED"

    def test_방출_is_released(self):
        assert ForeignPlayerCrawler._detect_change_type("선수 방출") == "RELEASED"

    def test_웨이버_is_released(self):
        assert ForeignPlayerCrawler._detect_change_type("웨이버 공시") == "RELEASED"

    def test_교체_is_replaced(self):
        assert ForeignPlayerCrawler._detect_change_type("외국인 교체") == "REPLACED"

    def test_대체_is_replaced(self):
        assert ForeignPlayerCrawler._detect_change_type("대체 선수") == "REPLACED"

    def test_재계약_is_renewed(self):
        assert ForeignPlayerCrawler._detect_change_type("재계약 체결") == "RENEWED"

    def test_영입_is_signed(self):
        assert ForeignPlayerCrawler._detect_change_type("영입 발표") == "SIGNED"

    def test_빈_문자열_is_signed(self):
        assert ForeignPlayerCrawler._detect_change_type("") == "SIGNED"


class TestDetectReasonExtended:
    def test_부상_is_injury(self):
        assert ForeignPlayerCrawler._detect_reason("부상으로 교체") == "INJURY"

    def test_성적_부진_is_performance(self):
        assert ForeignPlayerCrawler._detect_reason("성적 부진") == "PERFORMANCE"

    def test_부진_is_performance(self):
        assert ForeignPlayerCrawler._detect_reason("실력 부진") == "PERFORMANCE"

    def test_이유없음_none(self):
        assert ForeignPlayerCrawler._detect_reason("새 영입") is None

    def test_빈_문자열_none(self):
        assert ForeignPlayerCrawler._detect_reason("") is None


class TestExtractTeamIdExtended:
    def test_lg_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("LG 트윈스 외국인 영입")
        assert result == "LG"

    def test_kt_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("KT 위즈 외국인 영입")
        assert result == "KT"

    def test_nc_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("NC 다이노스 외국인 영입")
        assert result == "NC"

    def test_doosan_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("두산 베어스 외국인 영입")
        assert result == "DB"

    def test_samsung_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("삼성 라이온즈 외국인 영입")
        assert result == "SS"

    def test_lotte_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("롯데 자이언츠 외국인 영입")
        assert result == "LT"

    def test_hanwha_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("한화 이글스 외국인 영입")
        assert result == "HH"

    def test_kia_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("KIA 타이거즈 외국인 영입")
        assert result == "KIA"

    def test_ssg_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("SSG 랜더스 외국인 영입")
        assert result == "SSG"

    def test_kiwoom_team(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("키움 히어로즈 외국인 영입")
        assert result == "KH"

    def test_unknown_team_returns_none(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("외국인 영입 발표")
        assert result is None


class TestParseArticleExtended:
    def test_release_article(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "LG 외국인 Mike Johnson 방출",
            "subContent": "2026-06-25 공식 발표",
            "dateTime": "2026-06-25 14:00",
            "oid": "111",
            "aid": "222",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert result["player_name"] == "Mike Johnson"
        assert result["change_type"] == "RELEASED"
        assert result["replacement_reason"] is None

    def test_renewal_article(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "두산 외국인 투수 David Lee 재계약",
            "subContent": "",
            "dateTime": "2025-12-01 10:00",
            "oid": "333",
            "aid": "444",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert result["change_type"] == "RENEWED"
        assert result["season"] == 2025

    def test_injury_reason_article(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "SSG 외국인 교체",
            "subContent": "부상으로 교체 발표",
            "dateTime": "2026-06-25 14:00",
            "oid": "555",
            "aid": "666",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert result["replacement_reason"] == "INJURY"
        assert result["change_type"] == "REPLACED"

    def test_performance_reason_article(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "KT 외국인 대체",
            "subContent": "성적 부진으로 교체",
            "dateTime": "2026-06-25 14:00",
            "oid": "777",
            "aid": "888",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert result["replacement_reason"] == "PERFORMANCE"
        assert result["change_type"] == "REPLACED"

    def test_article_without_name_returns_none(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "감독 교체 발표",
            "subContent": "성적 부진으로 경질",
            "dateTime": "2026-06-25 14:00",
        }
        result = crawler._parse_article(article)
        assert result is None

    def test_note_truncated_to_500_chars(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        long_title = "외국인 타자 John " + "x" * 600
        article = {
            "title": long_title,
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
            "oid": "999",
            "aid": "000",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert len(result["note"]) == 500

    def test_season_from_date(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "외국인 선수 John 영입",
            "subContent": "",
            "dateTime": "2024-03-15 10:00",
            "oid": "123",
            "aid": "456",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert result["season"] == 2024

    def test_source_url_construction(self):
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "외국인 타자 홍길동 영입",
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
            "oid": "42",
            "aid": "17",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert "42" in result["source_url"]
        assert "17" in result["source_url"]
