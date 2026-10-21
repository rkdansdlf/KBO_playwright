"""Schedule crawler season-type classification (issue #40)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.utils.season_classification import season_payload_type


def _raw(game_id: str, game_date: str, away: str, home: str) -> dict:
    return {
        "gameId": game_id,
        "gameDate": game_date,
        "gameDateTime": f"{game_date}T18:30:00",
        "awayTeamCode": away,
        "homeTeamCode": home,
        "statusCode": "RESULT",
        "cancel": False,
        "suspended": False,
    }


@pytest.fixture
def crawler() -> ScheduleCrawler:
    return ScheduleCrawler()


@pytest.mark.parametrize(
    ("raw", "month", "expected", "confirmed"),
    [
        # Calendar / default answers are not confirmed ...
        (_raw("20260320HTHH02026", "2026-03-20", "HT", "HH"), 3, "exhibition", False),
        (_raw("20260328KTLG02026", "2026-03-28", "KT", "LG"), 3, "regular", False),
        (_raw("20261006KTWO02026", "2026-10-06", "KT", "WO"), 10, "regular", False),
        (_raw("20261012LGHT02026", "2026-10-12", "LG", "HT"), 10, "regular", False),
        # ... a series prefix is.
        (_raw("44441014OBLG02026", "2026-10-14", "OB", "LG"), 10, "wildcard", True),
        (_raw("33331017LGHT02026", "2026-10-17", "LG", "HT"), 10, "semi_playoff", True),
    ],
)
def test_naver_payload_carries_the_real_season_type(
    crawler: ScheduleCrawler,
    raw: dict,
    month: int,
    expected: str,
    confirmed: bool,
) -> None:
    game = crawler._naver_game_to_payload(raw, 2026, month, {})
    assert game is not None
    assert game["season_type"] == expected
    assert game["season_type_confirmed"] is confirmed
    # Whatever the flag, the stored type is the same.
    assert season_payload_type(game) == expected


def test_naver_round_code_answer_is_confirmed(crawler: ScheduleCrawler) -> None:
    raw = _raw("20310315HTHH02031", "2031-03-15", "HT", "HH")
    game = crawler._naver_game_to_payload(raw, 2031, 3, {}, day_season_type="regular")
    assert game["season_type"] == "regular"
    assert game["season_type_confirmed"] is True


def test_naver_unprefixed_id_on_a_postseason_date_is_confirmed_regular(crawler: ScheduleCrawler) -> None:
    """The missing series prefix is the id telling us "not postseason"; the calendar must not override it."""
    game = crawler._naver_game_to_payload(_raw("20261014KTSS02026", "2026-10-14", "KT", "SS"), 2026, 10, {})
    assert game["season_type_confirmed"] is True
    assert season_payload_type(game) == "regular"


def test_naver_postseason_game_id_is_normalized_to_date_form(crawler: ScheduleCrawler) -> None:
    game = crawler._naver_game_to_payload(_raw("33331017LGHT02026", "2026-10-17", "LG", "HT"), 2026, 10, {})
    assert game["game_id"] == "20261017LGHT0"


def test_naver_regular_game_in_a_stale_postseason_window_stays_regular(crawler: ScheduleCrawler) -> None:
    # A date-prefixed id proves "not postseason" even inside a calendar window.
    game = crawler._naver_game_to_payload(_raw("20261014KTSS02026", "2026-10-14", "KT", "SS"), 2026, 10, {})
    assert game["season_type"] == "regular"


async def test_day_round_lookup_used_only_without_a_calendar(crawler: ScheduleCrawler) -> None:
    crawler._http.fetch_json = AsyncMock(
        return_value=CrawlResult.success({"result": {"game": {"roundCode": "kbo_e"}}}),
    )
    raws = [_raw("20310315HTHH02031", "2031-03-15", "HT", "HH")]
    assert await crawler._naver_day_round_type(2031, 3, raws) == "exhibition"
    crawler._http.fetch_json.assert_awaited_once()
    game = crawler._naver_game_to_payload(raws[0], 2031, 3, {}, day_season_type="exhibition")
    assert game["season_type"] == "exhibition"

    crawler._http.fetch_json.reset_mock()
    assert await crawler._naver_day_round_type(2026, 3, raws) is None  # calendar known
    assert await crawler._naver_day_round_type(2031, 7, raws) is None  # not a pre-season month
    crawler._http.fetch_json.assert_not_awaited()


async def test_day_round_lookup_failure_falls_back(crawler: ScheduleCrawler) -> None:
    crawler._http.fetch_json = AsyncMock(
        return_value=CrawlResult.failure(CrawlOutcome.PERMANENT_ERROR, error="boom", error_code="FETCH_HTTP_ERROR"),
    )
    raws = [_raw("20310315HTHH02031", "2031-03-15", "HT", "HH")]
    assert await crawler._naver_day_round_type(2031, 3, raws) is None


async def test_day_round_lookup_skips_postseason_only_days(crawler: ScheduleCrawler) -> None:
    crawler._http.fetch_json = AsyncMock()
    assert await crawler._naver_day_round_type(2031, 3, [_raw("77770330HHLG02031", "2031-03-30", "HH", "LG")]) is None
    crawler._http.fetch_json.assert_not_awaited()


@pytest.mark.parametrize(
    ("option", "expected"),
    [("1", "exhibition"), ("0,9,6", "regular"), ("3,4,5,7", "postseason"), ("99", None)],
)
def test_kbo_series_options(option: str, expected: str | None) -> None:
    assert ScheduleCrawler._kbo_series_season_type(option) == expected


def test_kbo_page_rows_get_stage_from_calendar() -> None:
    assert ScheduleCrawler._kbo_page_season_type("postseason", "20261017") == ("semi_playoff", False)
    assert ScheduleCrawler._kbo_page_season_type("postseason", "20261016") == ("postseason", False)
    assert ScheduleCrawler._kbo_page_season_type("regular", "20261006") == ("regular", True)
    assert ScheduleCrawler._kbo_page_season_type(None, "20260320") == (None, False)
