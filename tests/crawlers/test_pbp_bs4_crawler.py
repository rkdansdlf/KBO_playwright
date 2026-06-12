from src.crawlers.pbp_bs4_crawler import PBPBS4Crawler


class TestParseInningHeader:
    def setup_method(self):
        self.crawler = PBPBS4Crawler()

    def test_top_of_inning(self):
        result = self.crawler._parse_inning_header("1회초 LG 공격", 0)
        assert result["inning"] == 1
        assert result["half"] == "top"

    def test_bottom_of_inning(self):
        result = self.crawler._parse_inning_header("3회말 두산 공격", 2)
        assert result["inning"] == 3
        assert result["half"] == "bottom"

    def test_no_match_falls_back_to_index(self):
        result = self.crawler._parse_inning_header("경기 시작", 5)
        assert result["inning"] == 6
        assert result["half"] == "unknown"


class TestFormatBaseString:
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

    def test_loaded(self):
        assert self.crawler._format_base_string(7) == "123"
