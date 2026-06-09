"""Tests for KBOTextParser — relay text parsing."""


from src.utils.text_parser import KBOTextParser


class TestParseRunners:
    def test_manru_full(self):
        assert KBOTextParser.parse_runners("만루") == 7

    def test_1ru_single(self):
        assert KBOTextParser.parse_runners("1루") == 1

    def test_2ru_single(self):
        assert KBOTextParser.parse_runners("2루") == 2

    def test_3ru_single(self):
        assert KBOTextParser.parse_runners("3루") == 4

    def test_12ru_combined(self):
        assert KBOTextParser.parse_runners("1,2루") == 3

    def test_13ru_combined(self):
        assert KBOTextParser.parse_runners("1,3루") == 5

    def test_23ru_combined(self):
        assert KBOTextParser.parse_runners("2,3루") == 6

    def test_full_text_middle(self):
        assert KBOTextParser.parse_runners("1사 1,2루") == 3

    def test_no_runners(self):
        assert KBOTextParser.parse_runners("1사") == 0

    def test_empty_text(self):
        assert KBOTextParser.parse_runners("") == 0


class TestParseOuts:
    def test_2sa(self):
        assert KBOTextParser.parse_outs("2사") == 2

    def test_two_out(self):
        assert KBOTextParser.parse_outs("투아웃") == 2

    def test_1sa(self):
        assert KBOTextParser.parse_outs("1사") == 1

    def test_one_out(self):
        assert KBOTextParser.parse_outs("원아웃") == 1

    def test_musa(self):
        assert KBOTextParser.parse_outs("무사") == 0

    def test_no_out(self):
        assert KBOTextParser.parse_outs("노아웃") == 0

    def test_full_text(self):
        assert KBOTextParser.parse_outs("1사 1,2루") == 1

    def test_empty_fallback(self):
        assert KBOTextParser.parse_outs("") == 0


class TestParseScoreChange:
    def test_explicit_score(self):
        assert KBOTextParser.parse_score_change("1점 홈런") == 1

    def test_explicit_score_two(self):
        assert KBOTextParser.parse_score_change("2점 득점") == 2

    def test_solo_hr(self):
        assert KBOTextParser.parse_score_change("솔로 홈런") == 1

    def test_two_run_hr(self):
        assert KBOTextParser.parse_score_change("투런 홈런") == 2

    def test_three_run_hr(self):
        assert KBOTextParser.parse_score_change("쓰리런 홈런") == 3

    def test_grand_slam(self):
        assert KBOTextParser.parse_score_change("만루 홈런") == 4

    def test_rbi_hit_no_score(self):
        assert KBOTextParser.parse_score_change("1타점 적시타") == 0

    def test_no_score(self):
        assert KBOTextParser.parse_score_change("2루수 땅볼") == 0

    def test_empty_text(self):
        assert KBOTextParser.parse_score_change("") == 0
