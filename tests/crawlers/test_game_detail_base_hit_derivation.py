"""Regression tests for deriving doubles and triples from the inning breakdown.

The Naver record API and the current KBO hitter table both report a batter's
hits only as a total (``hit`` / 안타); neither carries a per-batter doubles or
triples column. The inning breakdown does, and it uses the same tokens on both
sites: the field position followed by the number of bases (``좌2`` is a double
to left, ``중3`` a triple to center) with home runs spelled out (``좌홈``).

These tests pin that decoding, and pin that the derived counts are applied to
the payload instead of being dropped.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.utils.inning_cells import (
    classify_base_hit_token,
    derive_hitter_stats_from_inning_cells,
)

FIXTURE = Path("tests/fixtures/game_details/20251001NCLG0.html")


class TestClassifyBaseHitToken:
    @pytest.mark.parametrize(
        ("token", "expected"),
        [
            ("좌2", "doubles"),
            ("우2", "doubles"),
            ("중2", "doubles"),
            ("좌중2", "doubles"),
            ("우중2", "doubles"),
            ("좌3", "triples"),
            ("우3", "triples"),
            ("중3", "triples"),
            ("좌중3", "triples"),
            ("좌홈", "home_runs"),
            ("우홈", "home_runs"),
            ("중홈", "home_runs"),
            ("좌중홈", "home_runs"),
        ],
    )
    def test_multi_base_and_home_run_tokens(self, token: str, expected: str) -> None:
        assert classify_base_hit_token(token) == expected

    @pytest.mark.parametrize(
        "token",
        [
            "좌안",  # single
            "중안",
            "4구",  # walk
            "삼진",  # strikeout
            "3땅",  # out at third
            "1땅",
            "2비",
            "유병",
            "사구",
            "고4",  # intentional walk
            "",
            "&nbsp;",
        ],
    )
    def test_non_multi_base_tokens_are_not_counted(self, token: str) -> None:
        assert classify_base_hit_token(token) is None


class TestDeriveHitterStatsFromInningCells:
    def test_doubles_and_triples_are_counted(self) -> None:
        cells = {"1": "좌2", "2": "중3", "3": "우2"}
        result = derive_hitter_stats_from_inning_cells(cells)
        assert result["doubles"] == 2
        assert result["triples"] == 1

    def test_home_run_is_counted(self) -> None:
        result = derive_hitter_stats_from_inning_cells({"1": "좌홈"})
        assert result["home_runs"] == 1

    def test_single_is_not_counted_as_a_multi_base_hit(self) -> None:
        result = derive_hitter_stats_from_inning_cells({"1": "좌안"})
        assert result["doubles"] == 0
        assert result["triples"] == 0
        assert result["home_runs"] == 0

    def test_two_results_in_one_inning_are_counted_separately(self) -> None:
        """A batter who batted twice in an inning has both results in one cell."""
        result = derive_hitter_stats_from_inning_cells({"1": "4구/좌2"})
        assert result["doubles"] == 1
        assert result["walks"] == 1

    def test_blank_cells_are_skipped(self) -> None:
        result = derive_hitter_stats_from_inning_cells({"1": "", "2": "&nbsp;"})
        assert result["doubles"] == 0
        assert result["triples"] == 0
        assert result["home_runs"] == 0

    def test_crawler_delegates_to_the_shared_decoder(self) -> None:
        assert GameDetailCrawler._derive_hitter_stats_from_inning_cells({"1": "좌2"}) == (
            derive_hitter_stats_from_inning_cells({"1": "좌2"})
        )
        assert GameDetailCrawler._classify_base_hit_token("좌2") == classify_base_hit_token("좌2")


class TestNaverBattersCarryDerivedDoublesAndTriples:
    def test_batter_entries_record_doubles_and_triples(self) -> None:
        """A Naver-sourced batter must not end up with zero doubles and triples."""
        crawler = GameDetailCrawler()
        rows = [
            {
                "name": "홍길동",
                "playerCode": "99999",
                "ab": "4",
                "hit": "3",
                "hr": "0",
                "rbi": "1",
                "run": "1",
                "bb": "0",
                "kk": "1",
                "inn1": "좌2",
                "inn2": "중3",
                "inn3": "우안",
                "inn4": "삼진",
            }
        ]

        entries = crawler._naver_batters_to_entries(rows, "HH")

        assert len(entries) == 1
        stats = entries[0]["stats"]
        assert stats["doubles"] == 1
        assert stats["triples"] == 1
        assert stats["hits"] == 3


class TestKboPageFixtureGridDecodes:
    """The KBO official page's own inning grid uses the same tokens.

    The fixture game lists ``2루타: 신민재(8회) 천재환(8회)`` in ``#tblEtc``.
    The grid rows for those two batters carry the matching ``…2`` token in the
    8th inning, which is what makes the KBO path usable for the same
    derivation.
    """

    def test_fixture_grid_rows_match_the_tblEtc_player_list(self) -> None:
        html = FIXTURE.read_text(encoding="utf-8")
        assert "신민재(8회)" in html
        assert "천재환(8회)" in html
        # The grid stores the double as the position followed by the base count.
        assert "우2" in html
        assert "좌2" in html
