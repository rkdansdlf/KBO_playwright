"""A drawn game gets no result summary, on any path.

``_naver_summary_rows`` reads the summary table from the page. When it comes
back empty it falls back to deriving "who won" from the scores, which is what
makes the detail crawl usable against a page that stopped rendering summaries.

The fallback picks a winner by comparing scores. A draw has no winner, so
``winner`` is None and the fallback appends nothing -- leaving the same empty
result it was called to fix. And the fallback is only reached when the table is
empty, so a drawn game that *does* carry summaries (umpire calls, hits, errors)
never reaches it either.

That leaves no path that produces a result line for a draw. 493 of the stored
games are draws.

This is written as a table of cases rather than one assertion because the shape
of the defect is that two different empty-ish states both produce nothing, and a
single test would pass as soon as either one started working.
"""

from __future__ import annotations

import pytest

from src.crawlers.game_detail_crawler import GameDetailCrawler


def _team_info(home_score: int | None, away_score: int | None) -> dict:
    return {
        "away": {"code": "HH", "name": "한화", "score": away_score},
        "home": {"code": "SK", "name": "SSG", "score": home_score},
    }


@pytest.fixture
def crawler() -> GameDetailCrawler:
    """Only the summary extractor; the page it takes is never used here."""
    return GameDetailCrawler.__new__(GameDetailCrawler)


class TestTheResultLineIsDerivedFromScores:
    def test_a_decided_game_gets_a_result_line(self, crawler) -> None:
        """The case the fallback was written for, pinned so the other two mean something."""
        rows = crawler._naver_summary_rows({}, _team_info(5, 3))

        assert rows == [{"summary_type": "경기결과", "detail_text": "SK 승"}]

    def test_the_losing_side_is_not_named(self, crawler) -> None:
        """A result line that names the loser as the winner is worse than none."""
        rows = crawler._naver_summary_rows({}, _team_info(3, 5))

        assert rows == [{"summary_type": "경기결과", "detail_text": "HH 승"}]


class TestADrawIsStated:
    def test_a_draw_with_no_other_summaries_is_recorded_as_one(self, crawler) -> None:
        """Empty table, equal scores: the result line says so.

        It used to append nothing. Nothing here is indistinguishable from a crawl
        that failed to read the page, and 493 of the stored games are draws, so
        the gap was not hypothetical.
        """
        rows = crawler._naver_summary_rows({}, _team_info(3, 3))

        assert rows == [{"summary_type": "경기결과", "detail_text": "무승부"}]

    def test_a_draw_keeps_the_result_alongside_the_summaries_it_already_has(self, crawler) -> None:
        """The second empty-ish state: the table is not empty, so no fallback ran.

        This is the case that hid the first one. A drawn game carrying a save
        has rows, so the code that would have derived a result never executed --
        and the result was missing the same way. Deriving it unconditionally is
        what makes both states produce the same answer.
        """
        entries = [{"wls": "S", "name": "박세Whoever"}]

        rows = crawler._naver_summary_rows({"pitchingResult": entries}, _team_info(3, 3))

        assert {"summary_type": "경기결과", "detail_text": "무승부"} in rows
        assert {"summary_type": "세이브투수", "detail_text": "박세Whoever"} in rows

    def test_a_partial_score_pair_is_not_treated_as_a_result(self, crawler) -> None:
        """One side missing is an unfinished crawl, not a 0-0 game.

        Recording a result from half a score line would write a claim the source
        did not make, into a column later reports read as fact.
        """
        rows = crawler._naver_summary_rows({}, _team_info(None, 3))

        assert rows == [], "a result was derived from an incomplete score"

    def test_a_decided_game_still_names_the_winner_when_summaries_exist(self, crawler) -> None:
        """The summary table describes the pitching, not the outcome.

        Deriving the result unconditionally must not double it up or overwrite
        the rows that were read from the page.
        """
        entries = [{"wls": "W", "name": "박세Whoever"}]

        rows = crawler._naver_summary_rows({"pitchingResult": entries}, _team_info(3, 5))

        result_rows = [row for row in rows if row["summary_type"] == "경기결과"]
        assert result_rows == [{"summary_type": "경기결과", "detail_text": "HH 승"}]
