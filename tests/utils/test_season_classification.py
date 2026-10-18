"""Season-type classification rules (issue #40)."""

from __future__ import annotations

from datetime import date

import pytest

from src.utils.season_classification import (
    EXHIBITION,
    KOREAN_SERIES,
    PLAYOFF,
    POSTSEASON,
    REGULAR,
    SEMI_PLAYOFF,
    WILDCARD,
    calendar_season_type,
    normalize_season_type,
    resolve_effective_season_type,
    season_payload_type,
    season_type_from_kbo_series,
    season_type_from_naver_game_id,
    season_type_from_naver_round_code,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("regular", REGULAR),
        ("Exhibition", EXHIBITION),
        ("semi-playoff", SEMI_PLAYOFF),
        ("한국시리즈", KOREAN_SERIES),
        (5, KOREAN_SERIES),
        ("2", WILDCARD),
        ("postseason", POSTSEASON),
        ("", None),
        (None, None),
        ("nonsense", None),
        (True, None),
    ],
)
def test_normalize_season_type(value: object, expected: str | None) -> None:
    assert normalize_season_type(value) == expected


@pytest.mark.parametrize(
    ("game_id", "expected"),
    [
        ("44441014OBLG02026", WILDCARD),
        ("33331017LGHT02026", SEMI_PLAYOFF),
        ("55551018SSHH02025", PLAYOFF),
        ("77771026HHLG02025", KOREAN_SERIES),
        ("20261006KTWO02026", None),
        ("20260320HTHH02026", None),
        (None, None),
    ],
)
def test_naver_game_id_prefix(game_id: str | None, expected: str | None) -> None:
    assert season_type_from_naver_game_id(game_id) == expected


def test_naver_round_codes() -> None:
    assert season_type_from_naver_round_code("kbo_e") == EXHIBITION
    assert season_type_from_naver_round_code("kbo_r") == REGULAR
    assert season_type_from_naver_round_code("kbo_ps_sp") == SEMI_PLAYOFF
    assert season_type_from_naver_round_code("kbo_ps_ks") == KOREAN_SERIES
    assert season_type_from_naver_round_code(None) is None


def test_kbo_series_options_match_the_live_page() -> None:
    # The live #ddlSeries offers these three grouped values only.
    assert season_type_from_kbo_series("1") == EXHIBITION
    assert season_type_from_kbo_series("0,9,6") == REGULAR
    assert season_type_from_kbo_series("3,4,5,7") == POSTSEASON
    assert season_type_from_kbo_series("8") is None


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 3, 12), EXHIBITION),
        (date(2026, 3, 19), EXHIBITION),
        (date(2026, 3, 24), EXHIBITION),
        (date(2026, 3, 28), None),  # opening day
        (date(2026, 8, 16), None),
        (date(2026, 10, 6), None),  # regular season still running
        (date(2026, 10, 7), None),
        (date(2026, 10, 12), None),  # last regular-season day
        (date(2026, 10, 14), WILDCARD),
        (date(2026, 10, 15), WILDCARD),
        (date(2026, 10, 17), SEMI_PLAYOFF),
        (date(2026, 10, 25), PLAYOFF),
        (date(2026, 11, 2), KOREAN_SERIES),
        ("20261017", SEMI_PLAYOFF),
        ("2026-03-20", EXHIBITION),
        (date(2031, 3, 20), None),  # no calendar
    ],
)
def test_2026_calendar(day: object, expected: str | None) -> None:
    assert calendar_season_type(day) == expected


class TestResolveEffectiveSeasonType:
    def test_confirmed_regular_beats_calendar(self) -> None:
        # A date-prefixed Naver id is never a postseason game, whatever the
        # calendar says.
        assert resolve_effective_season_type(REGULAR, date(2026, 10, 14), confirmed=True) == REGULAR

    def test_unconfirmed_regular_is_only_a_default(self) -> None:
        assert resolve_effective_season_type(REGULAR, date(2026, 3, 20)) == EXHIBITION
        assert resolve_effective_season_type(None, date(2026, 10, 17)) == SEMI_PLAYOFF
        assert resolve_effective_season_type(None, date(2026, 10, 6)) == REGULAR

    def test_specific_source_type_wins(self) -> None:
        assert resolve_effective_season_type(KOREAN_SERIES, date(2026, 10, 24)) == KOREAN_SERIES
        assert resolve_effective_season_type(SEMI_PLAYOFF, date(2026, 10, 24)) == SEMI_PLAYOFF

    def test_generic_postseason_takes_the_calendar_stage(self) -> None:
        assert resolve_effective_season_type(POSTSEASON, date(2026, 10, 18)) == SEMI_PLAYOFF

    def test_generic_postseason_without_a_stage_is_unknown(self) -> None:
        assert resolve_effective_season_type(POSTSEASON, date(2031, 10, 18)) is None
        assert resolve_effective_season_type(POSTSEASON, date(2026, 10, 16)) is None

    def test_payload_helper_reads_the_confirmed_flag(self) -> None:
        payload = {"season_type": "regular", "game_date": "2026-03-20", "season_type_confirmed": True}
        assert season_payload_type(payload) == REGULAR
        payload["season_type_confirmed"] = False
        assert season_payload_type(payload) == EXHIBITION
