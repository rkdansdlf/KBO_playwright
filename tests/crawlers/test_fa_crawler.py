import pytest

from src.crawlers.fa_crawler import parse_amount_krw


class TestParseAmountKrw:
    def test_billion_only(self):
        assert parse_amount_krw("75억원") == 750000

    def test_billion_with_ten_thousand(self):
        assert parse_amount_krw("6억 5천만원") == 65000

    def test_ten_thousand_only(self):
        assert parse_amount_krw("5000만원") == 5000

    def test_none_or_empty(self):
        assert parse_amount_krw(None) is None
        assert parse_amount_krw("") is None

    def test_with_commas_and_spaces(self):
        assert parse_amount_krw("10억 5,000만원") == 105000

    def test_no_match_returns_none(self):
        assert parse_amount_krw("비공개") is None
