from __future__ import annotations

from src.utils.text_parser import KBOTextParser


class TestParseRunnersEdgeCases:
    def test_only_outs(self):
        assert KBOTextParser.parse_runners("2사 아웃") == 0

    def test_no_base_mention(self):
        assert KBOTextParser.parse_runners("삼진 아웃") == 0

    def test_single_base_no_comma(self):
        assert KBOTextParser.parse_runners("1사 1루") == 1

    def test_all_bases_comma(self):
        assert KBOTextParser.parse_runners("1,2,3루") == 7

    def test_duplicate_base(self):
        assert KBOTextParser.parse_runners("1,1루") == 1

    def test_full_base_trumps_individual(self):
        assert KBOTextParser.parse_runners("1사 만루") == 7

    def test_empty_string(self):
        assert KBOTextParser.parse_runners("") == 0

    def test_only_numbers(self):
        assert KBOTextParser.parse_runners("123") == 0


class TestParseOutsEdgeCases:
    def test_three_outs_defaults_to_zero(self):
        assert KBOTextParser.parse_outs("3사 아웃") == 0

    def test_outs_with_runners(self):
        assert KBOTextParser.parse_outs("2사 1,2루") == 2

    def test_default_for_unknown(self):
        assert KBOTextParser.parse_outs("볼넷") == 0

    def test_mixed_text_with_outs(self):
        assert KBOTextParser.parse_outs("1사 안타") == 1


class TestParseScoreChangeEdgeCases:
    def test_explicit_4_score(self):
        assert KBOTextParser.parse_score_change("좌월 4점 홈런") == 4

    def test_explicit_5_score(self):
        assert KBOTextParser.parse_score_change("5점 득점") == 5

    def test_solo_hr_english(self):
        assert KBOTextParser.parse_score_change("sol로 홈런") == 0

    def test_no_number_no_keyword(self):
        assert KBOTextParser.parse_score_change("삼진") == 0

    def test_empty_string(self):
        assert KBOTextParser.parse_score_change("") == 0

    def test_only_number_no_keyword(self):
        assert KBOTextParser.parse_score_change("3") == 0

    def test_score_without_hr_or_deukjeom(self):
        assert KBOTextParser.parse_score_change("3타점") == 0

    def test_multiple_scores_takes_first(self):
        assert KBOTextParser.parse_score_change("1점 득점 2점 추가") == 1
