
from src.crawlers.ticket_crawler import TicketCrawler


class TestAltToTeamCode:
    def setup_method(self):
        self.crawler = TicketCrawler()

    def test_known_team_alts(self):
        assert self.crawler._alt_to_team_code("LG") == "LG"
        assert self.crawler._alt_to_team_code("kt") == "KT"
        assert self.crawler._alt_to_team_code("두산") == "OB"
        assert self.crawler._alt_to_team_code("롯데") == "LT"
        assert self.crawler._alt_to_team_code("키움") == "WO"
        assert self.crawler._alt_to_team_code("ssg") == "SK"

    def test_no_match_returns_none(self):
        assert self.crawler._alt_to_team_code("없는팀") is None

    def test_case_insensitive(self):
        assert self.crawler._alt_to_team_code("LG") == "LG"
        assert self.crawler._alt_to_team_code("lg") == "LG"


class TestTeamCodeToKr:
    def setup_method(self):
        self.crawler = TicketCrawler()

    def test_known_codes(self):
        assert self.crawler._team_code_to_kr("LG") == "LG"
        assert self.crawler._team_code_to_kr("HH") == "한화"
        assert self.crawler._team_code_to_kr("HT") == "KIA"

    def test_unknown_returns_none(self):
        assert self.crawler._team_code_to_kr("ZZ") is None


class TestBuildOpenRules:
    def setup_method(self):
        self.crawler = TicketCrawler()

    def test_returns_all_teams(self):
        rules = self.crawler._build_open_rules()
        assert len(rules) == 10

    def test_rule_structure(self):
        rules = self.crawler._build_open_rules()
        lg = [r for r in rules if r["team_id"] == "LG"][0]
        assert lg["platform"] == "Ticketlink"
        assert lg["open_offset_days"] == 7

    def test_all_have_required_keys(self):
        rules = self.crawler._build_open_rules()
        for r in rules:
            assert "team_id" in r
            assert "platform" in r
            assert "open_offset_days" in r
            assert "open_time" in r
