from __future__ import annotations

from http import HTTPStatus
from zoneinfo import ZoneInfo

from src.constants import (
    BASE_STATE_FIRST,
    BASE_STATE_SECOND,
    BASE_STATE_THIRD,
    DATE_STR_LEN,
    GAME_ID_FULL_LEN,
    GAME_ID_MIN_LEN,
    GAME_ID_YEAR_END,
    GAME_ID_YEAR_LEN,
    GAME_ID_YEAR_START,
    HTTP_OK,
    IP_FRAC_THIRD,
    IP_FRAC_TWO_THIRDS,
    KBO_2000S_ERA_END,
    KBO_90S_ERA_END,
    KBO_EARLY_ERA_END,
    KBO_FOUNDING_YEAR,
    KBO_MID_80S_ERA_END,
    KBO_PLAYOFF_ERA_END,
    KBO_SEMI_PLAYOFF_ERA_END,
    KST,
    MAX_INNINGS,
    MAX_OUTS,
    SURROGATE_PLAYER_ID_BOUNDARY,
)


class TestHttpConstants:
    def test_http_ok(self):
        assert HTTP_OK == HTTPStatus.OK
        assert HTTP_OK == 200


class TestTimezone:
    def test_kst_timezone(self):
        assert ZoneInfo("Asia/Seoul") == KST
        assert str(KST) == "Asia/Seoul"


class TestDateFormat:
    def test_date_str_len(self):
        assert DATE_STR_LEN == 8

    def test_game_id_year_positions(self):
        assert GAME_ID_YEAR_START == 0
        assert GAME_ID_YEAR_END == 4
        assert GAME_ID_YEAR_LEN == 4

    def test_game_id_lengths(self):
        assert GAME_ID_MIN_LEN == 10
        assert GAME_ID_FULL_LEN == 12


class TestKBOHistory:
    def test_founding_year(self):
        assert KBO_FOUNDING_YEAR == 1982

    def test_era_boundaries(self):
        assert KBO_EARLY_ERA_END == 1985
        assert KBO_MID_80S_ERA_END == 1988
        assert KBO_90S_ERA_END == 1999
        assert KBO_2000S_ERA_END == 2001
        assert KBO_PLAYOFF_ERA_END == 2006
        assert KBO_SEMI_PLAYOFF_ERA_END == 2014


class TestGameRules:
    def test_max_innings(self):
        assert MAX_INNINGS == 9

    def test_max_outs(self):
        assert MAX_OUTS == 3


class TestPlayerId:
    def test_surrogate_boundary(self):
        assert SURROGATE_PLAYER_ID_BOUNDARY == 900000


class TestBaseState:
    def test_base_state_values(self):
        assert BASE_STATE_FIRST == 1
        assert BASE_STATE_SECOND == 2
        assert BASE_STATE_THIRD == 4

    def test_base_state_combinations(self):
        loaded = BASE_STATE_FIRST | BASE_STATE_SECOND | BASE_STATE_THIRD
        assert loaded == 7


class TestIPFractions:
    def test_ip_frac_third(self):
        assert IP_FRAC_THIRD == 33

    def test_ip_frac_two_thirds(self):
        assert IP_FRAC_TWO_THIRDS == 66

    def test_ip_frac_sum(self):
        assert IP_FRAC_THIRD + IP_FRAC_TWO_THIRDS == 99
