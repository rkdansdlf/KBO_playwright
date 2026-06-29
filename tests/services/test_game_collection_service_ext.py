from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services.game_collection_service import (
    ExistingGameData,
    GameCollectionTarget,
    _format_game_date,
    _get_failure_reason,
    _get_value,
    _has_required_detail_rows,
    _maybe_pause,
    build_game_id_range,
    inspect_existing_game_data,
    normalize_game_targets,
)


class TestBuildGameIdRange:
    def test_with_month(self):
        start, end = build_game_id_range(2024, 3)
        assert start == "20240301"
        assert end == "20240401"

    def test_december_month_rolls_year(self):
        start, end = build_game_id_range(2024, 12)
        assert start == "20241201"
        assert end == "20250101"

    def test_without_month(self):
        start, end = build_game_id_range(2024, None)
        assert start == "20240101"
        assert end == "20250101"


class TestGetValue:
    def test_dict_access(self):
        assert _get_value({"a": 1}, "a") == 1

    def test_dict_missing(self):
        assert _get_value({"a": 1}, "b") is None

    def test_object_attr(self):
        class Obj:
            x = 42

        assert _get_value(Obj(), "x") == 42

    def test_none_value(self):
        assert _get_value(None, "x") is None


class TestFormatGameDate:
    def test_datetime_object(self):
        d = datetime(2024, 3, 15, 12, 0)
        assert _format_game_date(d, fallback_game_id="20240315LG0") == "20240315"

    def test_date_object(self):
        d = date(2024, 3, 15)
        assert _format_game_date(d, fallback_game_id="20240315LG0") == "20240315"

    def test_yyyymmdd_string(self):
        assert _format_game_date("20240315", fallback_game_id="x") == "20240315"

    def test_yyyy_mm_dd_string(self):
        assert _format_game_date("2024-03-15", fallback_game_id="x") == "20240315"

    def test_invalid_falls_back(self):
        assert _format_game_date("", fallback_game_id="20240315LG0") == "20240315"

    def test_none_falls_back(self):
        assert _format_game_date(None, fallback_game_id="20240315LG0") == "20240315"


class TestNormalizeGameTargets:
    def test_empty_input(self):
        assert normalize_game_targets([]) == []

    def test_skipped_missing_game_id(self):
        assert normalize_game_targets([{}]) == []

    def test_creates_target_with_yyyymmdd(self):
        with patch("src.services.game_collection_service.normalize_kbo_game_id", return_value="20240315LGSS0"):
            result = normalize_game_targets([{"game_id": "20240315LGSS0", "game_date": "2024-03-15"}])
            assert len(result) == 1
            assert result[0].game_id == "20240315LGSS0"
            assert result[0].game_date == "20240315"

    def test_deduplicates_by_game_id(self):
        with patch("src.services.game_collection_service.normalize_kbo_game_id", return_value="20240315LGSS0"):
            games = [
                {"game_id": "20240315LGSS0", "game_date": "2024-03-15"},
                {"game_id": "20240315LGSS0", "game_date": "2024-03-15"},
            ]
            result = normalize_game_targets(games)
            assert len(result) == 1


class TestExistingGameData:
    def test_defaults(self):
        d = ExistingGameData()
        assert d.has_detail is False
        assert d.has_relay is False

    def test_custom_values(self):
        d = ExistingGameData(has_detail=True, has_relay=False)
        assert d.has_detail is True
        assert d.has_relay is False


class TestInspectExistingGameData:
    def test_empty_targets(self):
        assert inspect_existing_game_data([]) == {}

    def test_returns_data_for_targets(self):
        targets = [GameCollectionTarget(game_id="20240315LGSS0", game_date="20240315")]
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [
                ("20240315LGSS0",),
            ]
            result = inspect_existing_game_data(targets)
            assert "20240315LGSS0" in result
            assert result["20240315LGSS0"].has_detail is True


class TestHasRequiredDetailRows:
    def test_full_box_returns_true(self):
        payload = {
            "hitters": {"away": [{"name": "a"}], "home": [{"name": "b"}]},
            "pitchers": {"away": [{"name": "c"}], "home": [{"name": "d"}]},
        }
        assert _has_required_detail_rows(payload) is True

    def test_missing_hitters_returns_false(self):
        payload = {"hitters": {}, "pitchers": {}}
        assert _has_required_detail_rows(payload) is False

    def test_partial_recovery_with_teams_and_score(self):
        payload = {
            "teams": {"away": {"code": "LG", "line_score": [1]}, "home": {"code": "SS"}},
            "metadata": {},
            "hitters": {},
            "pitchers": {},
        }
        assert _has_required_detail_rows(payload) is True

    def test_partial_recovery_with_metadata(self):
        payload = {
            "teams": {"away": {"code": "LG"}, "home": {"code": "SS"}},
            "metadata": {"stadium": "Jamsil"},
            "hitters": {},
            "pitchers": {},
        }
        assert _has_required_detail_rows(payload) is True

    def test_requires_teams_or_scores(self):
        payload = {"teams": {}, "metadata": {}, "hitters": {}, "pitchers": {}}
        assert _has_required_detail_rows(payload) is False


class TestGetFailureReason:
    def test_crawler_without_getter(self):
        assert _get_failure_reason(MagicMock(spec=[]), "g1") is None

    def test_crawler_with_getter(self):
        crawler = MagicMock()
        crawler.get_last_failure_reason.return_value = "no_data"
        assert _get_failure_reason(crawler, "g1") == "no_data"

    def test_crawler_getter_exception(self):
        crawler = MagicMock()
        crawler.get_last_failure_reason.side_effect = TypeError("bad")
        assert _get_failure_reason(crawler, "g1") is None


@pytest.mark.asyncio
class TestMaybePause:
    async def test_no_pause_when_disabled(self):
        log = MagicMock()
        await _maybe_pause(5, None, 1.0, log)
        log.assert_not_called()

    async def test_no_pause_when_not_divisible(self):
        log = MagicMock()
        await _maybe_pause(3, 5, 1.0, log)
        log.assert_not_called()

    async def test_pauses_at_interval(self):
        log = MagicMock()
        await _maybe_pause(5, 5, 0.001, log)
        log.assert_called_once()
