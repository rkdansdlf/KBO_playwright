from __future__ import annotations

from src.utils.text_parser import KBOTextParser


class TestParseRunners:
    def test_empty(self):
        assert KBOTextParser.parse_runners("무사 아웃") == 0

    def test_first_base(self):
        assert KBOTextParser.parse_runners("1사 1루") == 1

    def test_second_base(self):
        assert KBOTextParser.parse_runners("1사 2루") == 2

    def test_third_base(self):
        assert KBOTextParser.parse_runners("1사 3루") == 4

    def test_loaded_bases(self):
        assert KBOTextParser.parse_runners("1사 만루") == 7

    def test_first_and_second(self):
        assert KBOTextParser.parse_runners("1사 1,2루") == 3

    def test_first_and_third(self):
        assert KBOTextParser.parse_runners("1사 1,3루") == 5

    def test_second_and_third(self):
        assert KBOTextParser.parse_runners("1사 2,3루") == 6

    def test_comma_normalization(self):
        assert KBOTextParser.parse_runners("1,2,3루") == 7


class TestParseOuts:
    def test_zero_outs(self):
        assert KBOTextParser.parse_outs("무사 아웃") == 0

    def test_zero_outs_nodae(self):
        assert KBOTextParser.parse_outs("노아웃") == 0

    def test_one_out(self):
        assert KBOTextParser.parse_outs("1사 아웃") == 1

    def test_one_out_won(self):
        assert KBOTextParser.parse_outs("원아웃") == 1

    def test_two_outs(self):
        assert KBOTextParser.parse_outs("2사 아웃") == 2

    def test_two_outs_tu(self):
        assert KBOTextParser.parse_outs("투아웃") == 2

    def test_default(self):
        assert KBOTextParser.parse_outs("안타") == 0


class TestParseScoreChange:
    def test_explicit_score(self):
        assert KBOTextParser.parse_score_change("좌월 1점 홈런") == 1

    def test_explicit_2_score(self):
        assert KBOTextParser.parse_score_change("좌월 2점 홈런") == 2

    def test_solo_hr(self):
        assert KBOTextParser.parse_score_change("김하성 솔로 홈런") == 1

    def test_two_run_hr(self):
        assert KBOTextParser.parse_score_change("박병호 투런 홈런") == 2

    def test_three_run_hr(self):
        assert KBOTextParser.parse_score_change("이정후 쓰리런 홈런") == 3

    def test_grand_slam(self):
        assert KBOTextParser.parse_score_change("김하성 만루 홈런") == 4

    def test_two_run_explicit(self):
        assert KBOTextParser.parse_score_change("2점 홈런") == 2

    def test_three_run_explicit(self):
        assert KBOTextParser.parse_score_change("3점 득점") == 3

    def test_no_score(self):
        assert KBOTextParser.parse_score_change("삼진 아웃") == 0

    def test_walk(self):
        assert KBOTextParser.parse_score_change("볼넷") == 0
