from datetime import date

from src.parsers.roster_parser import (
    _map_team_name,
    _parse_alternate_mobile,
    parse_mobile_roster,
)


class TestMapTeamName:
    def test_korean_names(self):
        assert _map_team_name("LG") == "LG"
        assert _map_team_name("엘지") == "LG"
        assert _map_team_name("한화") == "HH"
        assert _map_team_name("삼성") == "SS"
        assert _map_team_name("두산") == "OB"
        assert _map_team_name("롯데") == "LT"
        assert _map_team_name("기아") == "HT"
        assert _map_team_name("키움") == "WO"

    def test_english_names(self):
        assert _map_team_name("LG") == "LG"
        assert _map_team_name("HH") == "HH"
        assert _map_team_name("SS") == "SS"
        assert _map_team_name("KT") == "KT"
        assert _map_team_name("OB") == "OB"
        assert _map_team_name("NC") == "NC"
        assert _map_team_name("SK") == "SK"
        assert _map_team_name("SSG") == "SK"
        assert _map_team_name("WO") == "WO"

    def test_unknown_name(self):
        assert _map_team_name("UNKNOWN") is None
        assert _map_team_name("") is None
        assert _map_team_name("야구팀") is None


class TestParseMobileRoster:
    def test_parse_registration_html(self):
        html = """
        <div>
        오늘자 선수 등록현황
        <strong class="team">LG</strong>
        <ul>
        <li><a href="?playerId=12345">홍길동</a></li>
        <li><a href="?playerId=67890">김철수</a></li>
        </ul>
        오늘자 선수 말소현황
        <strong class="team">LG</strong>
        <ul>
        <li><a href="?playerId=11111">이영희</a></li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        assert len(result) == 3
        registered = [t for t in result if t["action"] == "registered"]
        deregistered = [t for t in result if t["action"] == "deregistered"]
        assert len(registered) == 2
        assert len(deregistered) == 1
        assert registered[0]["player_name"] == "홍길동"
        assert registered[0]["player_id"] == 12345
        assert deregistered[0]["player_name"] == "이영희"

    def test_empty_html_returns_empty(self):
        result = parse_mobile_roster("", "kbo_today_roster")
        assert result is not None

    def test_no_sections_triggers_alternate_parse(self):
        html = "<html><body><p>No roster data here</p></body></html>"
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        assert result is not None

    def test_output_schema(self):
        html = """
        <div>
        오늘자 선수 등록현황
        <strong class="team">LG</strong>
        <ul>
        <li><a href="?playerId=12345">홍길동</a></li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        item = result[0]
        assert item["transaction_date"] == date(2025, 6, 1)
        assert item["team_id"] == "LG"
        assert item["roster_level"] == "first_team"
        assert item["source_type"] == "kbo_today_page"
        assert item["confidence"] == "high"
        assert item["dedupe_key"] == "2025-06-01_LG_홍길동_registered"

    def test_alternate_parse_basic(self):
        html = "\n".join([
            '<div class="team">LG</div>',
            '<div>오늘자 선수 등록현황</div>',
            '<a href="?playerId=12345">홍길동</a>',
            '<div>오늘자 선수 말소현황</div>',
            '<a href="?playerId=67890">김철수</a>',
        ])
        result = _parse_alternate_mobile(html, date(2025, 6, 1))
        assert len(result) >= 1

    def test_player_without_link_uses_li_text(self):
        html = """
        <div>
        오늘자 선수 등록현황
        <strong class="team">LG</strong>
        <ul>
        <li>홍길동</li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        assert len(result) == 1
        assert result[0]["player_name"] == "홍길동"
        assert result[0]["player_id"] is None

    def test_malformed_date_falls_back_to_today(self):
        html = """
        <div>
        오늘자 선수 등록현황
        <strong class="team">LG</strong>
        <ul>
        <li><a href="?playerId=1">선수</a></li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "invalid-date"})
        assert result[0]["transaction_date"] == date.today()

    def test_unknown_team_skipped(self):
        html = """
        <div>
        오늘자 선수 등록현황
        <strong class="team">UNKNOWN_TEAM</strong>
        <ul>
        <li><a href="?playerId=1">선수</a></li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        assert len(result) == 0

    def test_deregistered_inferred_level(self):
        html = """
        <div>
        오늘자 선수 말소현황
        <strong class="team">LG</strong>
        <ul>
        <li><a href="?playerId=1">선수</a></li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        assert result[0]["inferred_to_level"] == "second_team"

    def test_registered_no_inferred_level(self):
        html = """
        <div>
        오늘자 선수 등록현황
        <strong class="team">LG</strong>
        <ul>
        <li><a href="?playerId=1">선수</a></li>
        </ul>
        </div>
        """
        result = parse_mobile_roster(html, "kbo_today_roster", {"fetched_at": "2025-06-01T00:00:00"})
        assert result[0]["inferred_to_level"] is None


class TestParseAlternateMobile:
    def test_alternate_parse_with_all_fields(self):
        html = """
        <div class="team">LG</div>
        등록현황
        <a href="?playerId=12345">홍길동</a>
        말소현황
        <a href="?playerId=67890">김철수</a>
        """
        result = _parse_alternate_mobile(html, date(2025, 6, 1))
        assert len(result) == 2
        assert result[0]["action"] == "registered"
        assert result[1]["action"] == "deregistered"

    def test_alternate_parse_empty(self):
        assert _parse_alternate_mobile("", date.today()) == []
