from src.utils.player_season_stat_validation import (
    _has_core_stats,
    _is_number_like,
    _number_or_none,
    filter_valid_season_stat_payloads,
    normalize_season_stat_payload,
    validate_season_stat_payload,
)


def _batting_row(**overrides):
    row = {"player_id": 1001, "player_name": "홍길동", "season": 2025, "league": "REGULAR", "team_code": "LG", "games": 10, "hits": 5}
    row.update(overrides)
    return row


def _pitching_row(**overrides):
    row = {"player_id": 2001, "player_name": "원태인", "season": 2025, "league": "REGULAR", "team_code": "SS", "games": 10, "innings_outs": 90}
    row.update(overrides)
    return row


def _fielding_row(**overrides):
    row = {"player_id": 3001, "player_name": "오지환", "year": 2025, "team_id": "LG", "position_id": "SS", "games": 10, "errors": 1}
    row.update(overrides)
    return row


class TestIsNumberLike:
    def test_none_is_number_like(self):
        assert _is_number_like(None) is True

    def test_int_is_number_like(self):
        assert _is_number_like(42) is True

    def test_float_is_number_like(self):
        assert _is_number_like(3.14) is True

    def test_empty_string_is_number_like(self):
        assert _is_number_like("") is True

    def test_dash_is_number_like(self):
        assert _is_number_like("-") is True

    def test_numeric_string_is_number_like(self):
        assert _is_number_like("100") is True

    def test_non_numeric_string_is_not_number_like(self):
        assert _is_number_like("abc") is False


class TestNumberOrNone:
    def test_none_returns_none(self):
        assert _number_or_none(None) is None

    def test_int_converted(self):
        assert _number_or_none(5) == 5.0

    def test_empty_string_returns_none(self):
        assert _number_or_none("") is None

    def test_numeric_string(self):
        assert _number_or_none("42") == 42.0


class TestHasCoreStats:
    def test_batting_has_core(self):
        assert _has_core_stats({"games": 10, "hits": 5}, "batting") is True

    def test_batting_no_core(self):
        assert _has_core_stats({}, "batting") is False

    def test_pitching_has_core(self):
        assert _has_core_stats({"innings_outs": 90}, "pitching") is True

    def test_fielding_has_core(self):
        assert _has_core_stats({"errors": 1}, "fielding") is True


class TestValidateSeasonStatPayload:
    def test_rejects_missing_player_id(self):
        assert validate_season_stat_payload(_batting_row(player_id=None), stat_type="batting") == (False, "invalid_player_id")

    def test_rejects_invalid_season(self):
        assert validate_season_stat_payload(_batting_row(season=None), stat_type="batting") == (False, "missing_season")

    def test_rejects_missing_team_code(self):
        assert validate_season_stat_payload(_batting_row(team_code=""), stat_type="batting") == (False, "missing_team_code")

    def test_rejects_empty_core_stats(self):
        assert validate_season_stat_payload(_batting_row(games=None, hits=None), stat_type="batting") == (False, "empty_core_stats")

    def test_rejects_hits_gt_at_bats(self):
        assert validate_season_stat_payload(_batting_row(hits=10, at_bats=5), stat_type="batting") == (False, "hits_gt_at_bats")

    def test_rejects_at_bats_gt_plate_appearances(self):
        assert validate_season_stat_payload(_batting_row(at_bats=10, plate_appearances=5), stat_type="batting") == (False, "at_bats_gt_plate_appearances")

    def test_rejects_earned_runs_gt_runs_allowed(self):
        assert validate_season_stat_payload(_pitching_row(earned_runs=5, runs_allowed=3), stat_type="pitching") == (False, "earned_runs_gt_runs_allowed")

    def test_valid_batting_passes(self):
        assert validate_season_stat_payload(_batting_row(), stat_type="batting") == (True, None)

    def test_valid_pitching_passes(self):
        assert validate_season_stat_payload(_pitching_row(), stat_type="pitching") == (True, None)

    def test_fielding_valid(self):
        assert validate_season_stat_payload(_fielding_row(), stat_type="fielding") == (True, None)


class TestNormalizeSeasonStatPayload:
    def test_normalizes_player_id(self):
        result = normalize_season_stat_payload(_batting_row(player_id="1001", player_name=" 홍길동 "))
        assert result["player_id"] == 1001
        assert result["player_name"] == "홍길동"

    def test_normalizes_season(self):
        result = normalize_season_stat_payload(_batting_row(season="2025"))
        assert result["season"] == 2025


class TestFilterValidSeasonStatPayloads:
    def test_filters_and_counts(self):
        rows, reasons = filter_valid_season_stat_payloads(
            [_batting_row(), _batting_row(player_id="bad")],
            stat_type="batting",
        )
        assert len(rows) == 1
        assert dict(reasons) == {"invalid_player_id": 1}
