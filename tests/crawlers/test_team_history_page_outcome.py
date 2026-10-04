"""A history table that is still there and no longer means anything.

``crawl`` returns a list, and a list cannot say whether an empty result came
from a blank page or from a page whose selectors stopped matching. On this
crawler that was not a cosmetic gap: a year row whose ``th`` is gone is skipped
one at a time, so a redesign that broke every row produced the same ``[]`` as a
page that genuinely had nothing, and the run recorded ``success`` with nothing
queued. The table in the database then keeps whatever it last held while every
report says it is current.

These tests use the real captured page, so "healthy" is not a hand-written
approximation of the table -- it is the table, including the seasons that are
legitimately sparse. That matters for the verdict: on the live page 1983 has six
rank cells and no team names at all, so a rule that read "rows found but nothing
parsed" as drift would fire on correct data.
"""

from __future__ import annotations

import pathlib

from bs4 import BeautifulSoup

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.team_history_outcome import (
    NO_ROWS_REASON,
    TeamHistoryStatus,
    classify_history_failure,
    read_team_history,
)

FIXTURE = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "html" / "kbo_team_history.html"

SLOT_COUNT = 12


def _soup() -> BeautifulSoup:
    return BeautifulSoup(FIXTURE.read_text(encoding="utf-8"), "html.parser")


def _counts() -> tuple[int, int, int]:
    """Return (rows, years parsed, entries) under the crawler's own rules."""
    rows = 0
    years = 0
    entries = 0
    slots: list[dict[str, str | None]] = [{"name": None, "logo": None} for _ in range(SLOT_COUNT)]

    for row in _soup().select("table.tData.tbd02 tbody tr"):
        rows += 1
        header = row.select_one("th")
        if header is None:
            continue
        try:
            int(header.get_text(strip=True))
        except ValueError:
            continue
        years += 1
        cells = row.select("td")
        for index, cell in enumerate(cells[:SLOT_COUNT]):
            rank_el = cell.select_one("span.nums")
            rank = None
            if rank_el is not None:
                try:
                    rank = int(rank_el.get_text(strip=True))
                except ValueError:
                    rank = None
            image = cell.select_one("img")
            name_span = cell.select_one("span:not(.nums)")
            if image is not None:
                if image.get("alt"):
                    slots[index]["name"] = image.get("alt")
                if image.get("src"):
                    slots[index]["logo"] = image.get("src")
            elif name_span is not None:
                slots[index]["name"] = name_span.get_text(strip=True)
            if rank is not None and slots[index]["name"]:
                entries += 1
    return rows, years, entries


class TestTheRealPageIsRead:
    def test_the_fixture_holds_the_table_it_claims_to(self) -> None:
        rows, years, entries = _counts()
        assert rows > 0
        assert years == rows, "every captured row should carry a readable year"
        assert entries > 0

    def test_the_crawlers_own_rules_produce_a_healthy_read(self) -> None:
        rows, years, entries = _counts()
        read = read_team_history(rows_found=rows, years_parsed=years)

        assert read.status is TeamHistoryStatus.SUCCESS
        assert read.reason is None

    def test_a_healthy_read_is_terminal_free_and_not_a_failure(self) -> None:
        read = read_team_history(rows_found=43, years_parsed=43)
        assert read.is_terminal is False


class TestSparseIsNotBroken:
    """The live page has seasons with ranks and no names.

    Those are real seasons, so any verdict that treats "found rows but parsed
    nothing" as drift would page an operator about correct data. This is the
    reason ``EMPTY`` and ``SCHEMA_CHANGED`` are separate outcomes rather than one
    rule with a threshold.
    """

    def test_some_seasons_carry_no_team_names(self) -> None:
        soup = _soup()
        bare = 0
        for row in soup.select("table.tData.tbd02 tbody tr"):
            cells = row.select("td")
            named = any(c.select_one("img") or c.select_one("span:not(.nums)") for c in cells)
            if not named:
                bare += 1
        assert bare > 0, "the captured page was expected to include sparse seasons"

    def test_parsing_one_row_is_enough_to_call_the_page_a_success(self) -> None:
        """A page is not disqualified because most of its rows are blank."""
        read = read_team_history(rows_found=43, years_parsed=1)
        assert read.status is TeamHistoryStatus.SUCCESS

    def test_rows_skipped_counts_what_the_parser_could_not_use(self) -> None:
        read = read_team_history(rows_found=43, years_parsed=40)
        assert read.rows_skipped == 3

    def test_nothing_read_is_not_negative(self) -> None:
        assert read_team_history(rows_found=0, years_parsed=0).rows_skipped == 0


class TestAnUnreadablePageIsDrift:
    def test_no_rows_at_all_is_drift_not_an_empty_page(self) -> None:
        """The table is the page. Without it there was nothing to read."""
        read = read_team_history(rows_found=0, years_parsed=0)

        assert read.status is TeamHistoryStatus.SCHEMA_CHANGED
        assert read.reason == NO_ROWS_REASON

    def test_a_page_without_the_table_selects_as_empty(self) -> None:
        """What the crawler would really see after the markup moved."""
        soup = BeautifulSoup("<html><body><div class='notice'>under maintenance</div></body></html>", "html.parser")
        assert soup.select("table.tData.tbd02 tbody tr") == []

    def test_rows_found_and_none_read_is_reported_as_empty_with_a_reason(self) -> None:
        """Rows that exist but carry no readable year is its own claim.

        Neither success (the page answered) nor drift (the table is there), and
        the reason says which, so an operator can tell "the table is blank" from
        "the table is gone" without reading the source.
        """
        read = read_team_history(rows_found=43, years_parsed=0)

        assert read.status is TeamHistoryStatus.EMPTY
        assert read.reason is not None
        assert read.rows_found == 43
        assert read.rows_skipped == 43


class TestTheVocabularyIsTotal:
    def test_every_reason_maps_to_a_code(self) -> None:
        from src.crawlers.team_history_outcome import NO_YEARS_REASON

        for reason in (NO_ROWS_REASON, NO_YEARS_REASON):
            code, _terminal = classify_history_failure(reason)
            assert code == FailureCode.PARSE_SELECTOR_MISSING.value, reason

    def test_drift_is_terminal(self) -> None:
        """Retrying returns the same redesigned page."""
        assert classify_history_failure(NO_ROWS_REASON)[1] is True

    def test_an_unknown_reason_is_not_guessed_at(self) -> None:
        code, terminal = classify_history_failure("something_else")

        assert code == FailureCode.UNKNOWN.value
        assert terminal is False

    def test_the_statuses_are_exactly_these(self) -> None:
        assert {status.value for status in TeamHistoryStatus} == {
            "success",
            "empty",
            "schema_changed",
            "fetch_failed",
        }


def test_the_read_is_immutable() -> None:
    """A verdict that can be edited after the fact is not a verdict."""
    read = read_team_history(rows_found=1, years_parsed=1)
    try:
        read.status = TeamHistoryStatus.FETCH_FAILED  # type: ignore[misc]
    except AttributeError:
        return
    raise AssertionError("TeamHistoryRead should not be assignable")


class TestWhatTheseTestsDoNotCover:
    """Where the verdict is produced, and where it is not exercised here.

    The parser reaches its verdict through a Playwright locator, so nothing in
    this file runs the code that decides ``rows_found``. The crawler-level tests
    stub ``crawl`` outright and hand it a verdict instead. That leaves exactly
    one link unverified: a selector that stops matching must actually produce
    ``rows_found == 0``.

    A mutation that made a missing table look like a blank one passes every
    crawler-level test here and fails only in this module, which is why the rule
    is pinned where it is written rather than where it is wired.
    """

    def test_the_verdict_is_computed_from_counts_the_parser_supplies(self) -> None:
        read = read_team_history(rows_found=0, years_parsed=0)
        assert read.rows_found == 0
        assert read.reason == NO_ROWS_REASON

    def test_a_single_unreadable_row_does_not_make_a_broken_page(self) -> None:
        """Blank slots are normal here, so the threshold is one, not a majority."""
        assert read_team_history(rows_found=43, years_parsed=42).status is TeamHistoryStatus.SUCCESS
