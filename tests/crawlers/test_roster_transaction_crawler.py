from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler


class TestMapTeamName:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()

    def test_known_team_codes(self):
        assert self.crawler._map_team_name("LG") == "LG"
        assert self.crawler._map_team_name("한화") == "HH"
        assert self.crawler._map_team_name("삼성") == "SS"
        assert self.crawler._map_team_name("두산") == "OB"
        assert self.crawler._map_team_name("롯데") == "LT"

    def test_unknown_returns_none(self):
        assert self.crawler._map_team_name("없음") is None


class TestDedupeTransactions:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()

    def test_deduplicates_by_dedupe_key(self):
        data = [
            {"dedupe_key": "a", "value": 1},
            {"dedupe_key": "a", "value": 2},
            {"dedupe_key": "b", "value": 3},
        ]
        result = self.crawler._dedupe_transactions(data)
        assert len(result) == 2

    def test_no_dedupe_key_preserved(self):
        data = [
            {"value": 1},
            {"value": 2},
        ]
        result = self.crawler._dedupe_transactions(data)
        assert len(result) == 2

    def test_empty_input(self):
        assert self.crawler._dedupe_transactions([]) == []
