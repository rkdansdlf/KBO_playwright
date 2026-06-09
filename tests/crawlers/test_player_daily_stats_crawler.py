import pytest

from src.crawlers.player_daily_stats_crawler import PlayerDailyStatsCrawler


class TestParseHitterRow:
    def setup_method(self):
        self.crawler = PlayerDailyStatsCrawler()

    def test_full_hitter_row(self):
        row = ["4.15", "LG", ".300", "5", "4", "1", "2", "0", "0", "0", "1", "0", "0", "1", "0", "1", "0", ".300"]
        result = self.crawler._parse_hitter_row(row, 2025)
        assert result is not None
        assert result["game_date"] == "2025-4-15"
        assert result["stats"]["plate_appearances"] == 5
        assert result["stats"]["hits"] == 2
        assert result["stats"]["home_runs"] == 0
        assert result["stats"]["rbi"] == 1

    def test_too_few_cells_returns_none(self):
        assert self.crawler._parse_hitter_row(["a", "b"], 2025) is None


class TestParsePitcherRow:
    def setup_method(self):
        self.crawler = PlayerDailyStatsCrawler()

    def test_full_pitcher_row(self):
        row = ["5.01", "SS", "선발", "승", "0.00", "27", "7.0", "3", "0", "1", "0", "8", "0", "0", "0.00"]
        result = self.crawler._parse_pitcher_row(row, 2025)
        assert result is not None
        assert result["stats"]["decision"] == "W"
        assert result["stats"]["batters_faced"] == 27
        assert result["stats"]["strikeouts"] == 8

    def test_loss_decision(self):
        row = ["5.01", "SS", "선발", "패", "6.00", "20", "5.0", "6", "1", "2", "0", "3", "3", "3", "6.00"]
        result = self.crawler._parse_pitcher_row(row, 2025)
        assert result["stats"]["decision"] == "L"
        assert result["stats"]["losses"] == 1

    def test_too_few_cells_returns_none(self):
        assert self.crawler._parse_pitcher_row(["a"], 2025) is None


class TestParseInningsToOuts:
    def setup_method(self):
        self.crawler = PlayerDailyStatsCrawler()

    def test_whole_innings(self):
        assert self.crawler._parse_innings_to_outs("7") == 21

    def test_fractional_innings(self):
        assert self.crawler._parse_innings_to_outs("5 2/3") == 17

    def test_only_fraction(self):
        assert self.crawler._parse_innings_to_outs("1/3") == 1

    def test_empty_or_dash(self):
        assert self.crawler._parse_innings_to_outs("") == 0
        assert self.crawler._parse_innings_to_outs("-") == 0
