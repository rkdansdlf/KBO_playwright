from src.parsers.schedule_parser import LINK_PATTERN, parse_schedule_html


class TestParseScheduleHtml:
    def test_parse_basic_schedule(self):
        html = """
        <html><body>
        <a href="https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20250325LGSS0">Game 1</a>
        <a href="https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId=20250325HTOB0">Game 2</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 2
        assert games[0]["game_id"] == "20250325LGSS0"
        assert games[0]["season_year"] == 2025
        assert games[0]["game_date"] == "20250325"
        assert games[0]["doubleheader_no"] == 0
        assert games[0]["season_type"] == "regular"

    def test_doubleheader_detection(self):
        html = """
        <html><body>
        <a href="?gameId=20250401LGSS1">Game DH1</a>
        <a href="?gameId=20250401LGSS2">Game DH2</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 2
        dhs = sorted(g["doubleheader_no"] for g in games)
        assert dhs == [1, 2]

    def test_empty_html_returns_empty_list(self):
        assert parse_schedule_html("", default_year=2025) == []
        assert parse_schedule_html("<html></html>", default_year=2025) == []

    def test_no_game_links_returns_empty(self):
        html = "<html><body><a href='/notice'>공지사항</a></body></html>"
        assert parse_schedule_html(html, default_year=2025) == []

    def test_deduplicates_duplicate_game_ids(self):
        html = """
        <html><body>
        <a href="?gameId=20250325LGSS0">Link 1</a>
        <a href="?gameId=20250325LGSS0">Link 2</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 1

    def test_year_from_game_id_when_no_default(self):
        html = """
        <html><body>
        <a href="?gameId=20240325LGSS0">Game</a>
        </body></html>
        """
        games = parse_schedule_html(html)
        assert games[0]["season_year"] == 2024

    def test_year_default_overrides_game_id(self):
        html = """
        <html><body>
        <a href="?gameId=20240325LGSS0">Game</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert games[0]["season_year"] == 2025

    def test_team_codes_resolved(self):
        html = """
        <html><body>
        <a href="?gameId=20250325LGSS0">Game</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert games[0]["away_team_code"] is not None
        assert games[0]["home_team_code"] is not None

    def test_output_schema(self):
        html = """
        <html><body>
        <a href="?gameId=20250325LGSS0">Game</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        g = games[0]
        assert "game_id" in g
        assert "season_year" in g
        assert "season_type" in g
        assert "game_date" in g
        assert "away_team_code" in g
        assert "home_team_code" in g
        assert "doubleheader_no" in g
        assert "game_status" in g
        assert "crawl_status" in g
        assert "stadium" in g

    def test_season_type_passthrough(self):
        html = """
        <html><body>
        <a href="?gameId=20251025LGSS0">Postseason</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025, season_type="postseason")
        assert games[0]["season_type"] == "postseason"

    def test_html_with_extra_whitespace(self):
        html = """
        <html><body>
        <a   href   =   "?gameId=20250325LGSS0"   >Game</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 1

    def test_multiple_games_same_date(self):
        html = """
        <html><body>
        <a href="?gameId=20250325LGSS0">Game 1</a>
        <a href="?gameId=20250325HTOB0">Game 2</a>
        <a href="?gameId=20250325NCSK0">Game 3</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 3


class TestParseScheduleHtmlFallback:
    def test_game_id_failing_split_uses_fallback(self):
        html = """
        <html><body>
        <a href="?gameId=20250325ZZYY0">Game with non-standard team codes</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 1
        g = games[0]
        assert g["game_id"] == "20250325ZZYY0"
        assert g["away_team_code"] is not None
        assert g["home_team_code"] is not None

    def test_game_id_without_digit_suffix_gets_doubleheader_zero(self):
        html = """
        <html><body>
        <a href="?gameId=20250325LGXX">Game</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 1
        assert games[0]["doubleheader_no"] == 0

    def test_game_id_with_digit_suffix_gets_doubleheader_from_last_char(self):
        html = """
        <html><body>
        <a href="?gameId=20250325LGXX5">Game</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 1
        assert games[0]["doubleheader_no"] == 5

    def test_href_without_gameId_skipped(self):
        html = """
        <html><body>
        <a href="https://example.com/notice">No gameId in href</a>
        <a href="/schedule?date=20250325">Also no gameId</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 0

    def test_gameId_param_without_value_skipped(self):
        html = """
        <html><body>
        <a href="?gameId=">Empty gameId</a>
        </body></html>
        """
        games = parse_schedule_html(html, default_year=2025)
        assert len(games) == 0

    def test_year_falls_back_to_game_id_prefix(self):
        html = """
        <html><body>
        <a href="?gameId=20241015LGSS0">Old Game</a>
        </body></html>
        """
        games = parse_schedule_html(html)
        assert games[0]["season_year"] == 2024


class TestLinkPattern:
    def test_extracts_game_id(self):
        m = LINK_PATTERN.search("gameId=20250325LGSS0")
        assert m
        assert m.group(1) == "20250325LGSS0"

    def test_url_with_query_params(self):
        url = "https://example.com?foo=bar&gameId=20250401HTOB1&baz=qux"
        m = LINK_PATTERN.search(url)
        assert m and m.group(1) == "20250401HTOB1"

    def test_no_match(self):
        assert LINK_PATTERN.search("no game id here") is None
        assert LINK_PATTERN.search("gameId=") is None
