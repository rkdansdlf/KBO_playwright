from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.game_collection_service import (
    DETAIL_COLLECTION_FAILURE_REASONS_NON_RETRYABLE,
    DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE,
    DetailProcessingContext,
    ExistingGameData,
    GameCollectionConfig,
    GameCollectionItemResult,
    GameCollectionResult,
    GameCollectionTarget,
    GameWriteContract,
    GameWriteSource,
    _collect_detail_phase,
    _collect_relay_phase,
    _derive_sh_sf_for_results,
    _detail_payload_failure_reason,
    _format_game_date,
    _get_failure_reason,
    _has_required_detail_rows,
    _ids_with_rows,
    _mark_detail_failed,
    _mark_skipped_detail_targets,
    _maybe_pause,
    _normalize_detail_failure_reason,
    _process_detail_target,
    _save_detail_payload,
    build_game_id_range,
    crawl_and_save_game_details,
    inspect_existing_game_data,
    load_game_targets_by_ids,
    load_game_targets_from_db,
    normalize_game_targets,
)


class TestLoadGameTargetsFromDb:
    def test_returns_targets(self):
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
                ("20240315LGSS0", date(2024, 3, 15)),
            ]
            with patch("src.services.game_collection_service.normalize_kbo_game_id", return_value="20240315LGSS0"):
                result = load_game_targets_from_db(2024, 3)
                assert len(result) == 1
                assert result[0].game_id == "20240315LGSS0"

    def test_empty_result(self):
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = load_game_targets_from_db(2024)
            assert result == []


class TestLoadGameTargetsByIds:
    def test_returns_targets(self):
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
                ("20240315LGSS0", date(2024, 3, 15)),
            ]
            with patch("src.services.game_collection_service.normalize_kbo_game_id", return_value="20240315LGSS0"):
                result = load_game_targets_by_ids(["20240315LGSS0"])
                assert len(result) == 1

    def test_empty_ids(self):
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = load_game_targets_by_ids([])
            assert result == []


class TestIdsWithRows:
    def test_returns_game_ids(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [
            ("20240315LGSS0",),
            ("20240316LGSS0",),
        ]
        result = _ids_with_rows(session, MagicMock(), ["g1", "g2"])
        assert result == {"20240315LGSS0", "20240316LGSS0"}


class TestDetailPayloadFailureReason:
    def test_no_payload(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = _detail_payload_failure_reason(target, None, MagicMock(), None)
        assert result[2] == "no_detail_payload"

    def test_incomplete_detail(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = _detail_payload_failure_reason(target, {"hitters": {}, "pitchers": {}}, MagicMock(), None)
        assert result[2] == "incomplete_detail"

    def test_filtered_by_predicate(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = _detail_payload_failure_reason(
            target,
            {
                "hitters": {"away": [{"name": "a"}], "home": [{"name": "b"}]},
                "pitchers": {"away": [{"name": "c"}], "home": [{"name": "d"}]},
            },
            MagicMock(),
            lambda x: False,
        )
        assert result[2] == "filtered"

    def test_valid_payload(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = _detail_payload_failure_reason(
            target,
            {
                "hitters": {"away": [{"name": "a"}], "home": [{"name": "b"}]},
                "pitchers": {"away": [{"name": "c"}], "home": [{"name": "d"}]},
            },
            MagicMock(),
            None,
        )
        assert result is None


class TestMarkDetailFailed:
    def test_increments_failed(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        _mark_detail_failed(target, ("crawl_failed", None, "no_detail_payload"), result, MagicMock())
        assert result.detail_failed == 1
        assert result.items["g1"].detail_status == "crawl_failed"

    def test_incomplete_detail(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        _mark_detail_failed(target, ("filtered", None, "incomplete_detail"), result, MagicMock())
        assert result.items["g1"].detail_status == "filtered"


class TestSaveDetailPayload:
    def test_save_success(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx = MagicMock()
        ctx.result = result
        ctx.detail_ready = set()
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        with patch("src.services.game_collection_service.save_game_detail", return_value=True):
            assert _save_detail_payload(target, {"data": "test"}, ctx) is True
            assert result.detail_saved == 1
            assert "g1" in ctx.detail_ready

    def test_save_failed(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx = MagicMock()
        ctx.result = result
        ctx.detail_ready = set()
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        with patch("src.services.game_collection_service.save_game_detail", return_value=False):
            assert _save_detail_payload(target, {"data": "test"}, ctx) is False
            assert result.detail_failed == 1


class TestMarkSkippedDetailTargets:
    def test_no_skipped(self):
        result = GameCollectionResult()
        _mark_skipped_detail_targets([], {}, force=False, result=result, log=MagicMock())

    def test_marks_skipped(self):
        result = GameCollectionResult()
        result.detail_skipped_existing = 1
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        exist_map = {"g1": ExistingGameData(has_detail=True)}
        _mark_skipped_detail_targets([target], exist_map, force=False, result=result, log=MagicMock())
        assert result.items["g1"].detail_status == "skipped_existing"


class TestNormalizeDetailFailureReason:
    def test_empty_uses_default(self):
        assert _normalize_detail_failure_reason(None, default="crawl_failed") == "crawl_failed"
        assert _normalize_detail_failure_reason("", default="crawl_failed") == "crawl_failed"

    def test_non_retryable_filtered(self):
        assert _normalize_detail_failure_reason("filtered", default="other") == "filtered"
        assert _normalize_detail_failure_reason("detail_payload_filtered", default="other") == "filtered"

    def test_non_retryable_save_failed(self):
        assert _normalize_detail_failure_reason("save_failed", default="other") == "save_failed"
        assert _normalize_detail_failure_reason("detail_save_failed", default="other") == "save_failed"

    def test_retryable_preserved(self):
        assert _normalize_detail_failure_reason("timeout", default="other") == "timeout"
        assert _normalize_detail_failure_reason("exception", default="other") == "exception"

    def test_unknown_uses_default(self):
        assert _normalize_detail_failure_reason("unknown_reason", default="crawl_failed") == "crawl_failed"


class TestDeriveShSfForResults:
    def test_no_success_games(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315", detail_status="failed")}
        _derive_sh_sf_for_results(result, log=MagicMock())

    def test_no_games_to_process(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315", detail_status="failed")}
        mock_session = MagicMock()
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.return_value = mock_session
            _derive_sh_sf_for_results(result, log=MagicMock())
            mock_session.commit.assert_not_called()

    def test_exception_handled(self):
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315", detail_status="success")}
        with patch("src.services.game_collection_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            with patch(
                "src.services.game_collection_service.apply_sh_sf_to_batting_stats",
                side_effect=RuntimeError("DB error"),
            ):
                _derive_sh_sf_for_results(result, log=MagicMock())


class TestCrawlAndSaveGameDetails:
    def test_empty_games(self):
        async def _test():
            return await crawl_and_save_game_details([], detail_crawler=MagicMock(), config=GameCollectionConfig())

        import asyncio

        result = asyncio.run(_test())
        assert result.total_targets == 0


class TestCollectDetailPhase:
    @pytest.mark.asyncio
    async def test_skips_existing(self):
        targets = [
            GameCollectionTarget(game_id="g1", game_date="20240315"),
            GameCollectionTarget(game_id="g2", game_date="20240316"),
        ]
        exist_map = {
            "g1": ExistingGameData(has_detail=True),
            "g2": ExistingGameData(has_detail=False),
        }
        ctx = MagicMock()
        ctx.cfg.force = False
        ctx.cfg.pause_every = None
        ctx.result = GameCollectionResult()
        ctx.result.items = {
            t.game_id: GameCollectionItemResult(game_id=t.game_date, game_date=t.game_date) for t in targets
        }
        ctx.cfg.log = MagicMock()
        ctx.detail_crawler = MagicMock()
        ctx.detail_crawler.crawl_games = AsyncMock(return_value=[])
        ctx.detail_crawler.close = AsyncMock()
        ctx.detail_crawler.get_last_failure_reason = MagicMock(return_value=None)
        await _collect_detail_phase(targets, exist_map, ctx)
        assert ctx.result.detail_targets == 1

    @pytest.mark.asyncio
    async def test_force_includes_all(self):
        targets = [GameCollectionTarget(game_id="g1", game_date="20240315")]
        exist_map = {"g1": ExistingGameData(has_detail=True)}
        ctx = MagicMock()
        ctx.cfg.force = True
        ctx.cfg.pause_every = None
        ctx.result = GameCollectionResult()
        ctx.result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx.cfg.log = MagicMock()
        ctx.detail_crawler = MagicMock()
        ctx.detail_crawler.crawl_games = AsyncMock(return_value=[])
        ctx.detail_crawler.close = AsyncMock()
        ctx.detail_crawler.get_last_failure_reason = MagicMock(return_value=None)
        await _collect_detail_phase(targets, exist_map, ctx)
        assert ctx.result.detail_targets == 1


class TestCollectRelayPhase:
    @pytest.mark.asyncio
    async def test_skips_existing_relay(self):
        targets = [GameCollectionTarget(game_id="g1", game_date="20240315")]
        exist_map = {"g1": ExistingGameData(has_relay=True)}
        ctx = MagicMock()
        ctx.cfg.force = False
        ctx.cfg.relay_requires_detail = True
        ctx.result = GameCollectionResult()
        ctx.result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx.relay_crawler = MagicMock()
        await _collect_relay_phase(targets, exist_map, set(), ctx)
        assert ctx.result.relay_skipped_existing == 1

    @pytest.mark.asyncio
    async def test_skips_no_detail(self):
        targets = [GameCollectionTarget(game_id="g1", game_date="20240315")]
        exist_map = {"g1": ExistingGameData(has_relay=False)}
        ctx = MagicMock()
        ctx.cfg.force = False
        ctx.cfg.relay_requires_detail = True
        ctx.result = GameCollectionResult()
        ctx.result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx.relay_crawler = MagicMock()
        await _collect_relay_phase(targets, exist_map, set(), ctx)
        assert ctx.result.items["g1"].relay_status == "skipped_no_detail"

    @pytest.mark.asyncio
    async def test_saves_relay_data(self):
        targets = [GameCollectionTarget(game_id="g1", game_date="20240315")]
        exist_map = {"g1": ExistingGameData(has_relay=False)}
        ctx = MagicMock()
        ctx.cfg.force = False
        ctx.cfg.relay_requires_detail = False
        ctx.cfg.pause_every = None
        ctx.result = GameCollectionResult()
        ctx.result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx.contract = MagicMock()
        ctx.relay_crawler = MagicMock()
        ctx.relay_crawler.__class__.__name__ = "TestRelayCrawler"
        ctx.relay_crawler.crawl_game_events = AsyncMock(
            return_value={
                "events": [{"inning": 1}],
                "raw_pbp_rows": [],
            },
        )
        with patch("src.services.game_collection_service.save_relay_data", return_value=1):
            await _collect_relay_phase(targets, exist_map, set(), ctx)
            assert ctx.result.relay_saved_games == 1

    @pytest.mark.asyncio
    async def test_no_relay_data(self):
        targets = [GameCollectionTarget(game_id="g1", game_date="20240315")]
        exist_map = {"g1": ExistingGameData(has_relay=False)}
        ctx = MagicMock()
        ctx.cfg.force = False
        ctx.cfg.relay_requires_detail = False
        ctx.cfg.pause_every = None
        ctx.result = GameCollectionResult()
        ctx.result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx.contract = MagicMock()
        ctx.relay_crawler = MagicMock()
        ctx.relay_crawler.crawl_game_events = AsyncMock(return_value=None)
        await _collect_relay_phase(targets, exist_map, set(), ctx)
        assert ctx.result.relay_missing == 1
        assert ctx.result.items["g1"].relay_status == "missing"


class TestMaybePause:
    @pytest.mark.asyncio
    async def test_disabled_when_no_interval(self):
        log = MagicMock()
        await _maybe_pause(10, None, 5.0, log)
        log.assert_not_called()

    @pytest.mark.asyncio
    async def test_disabled_when_zero_seconds(self):
        log = MagicMock()
        await _maybe_pause(10, 5, 0.0, log)
        log.assert_not_called()

    @pytest.mark.asyncio
    async def test_pauses_at_interval(self):
        log = MagicMock()
        await _maybe_pause(10, 5, 0.001, log)
        log.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_pause_between_intervals(self):
        log = MagicMock()
        await _maybe_pause(7, 5, 0.001, log)
        log.assert_not_called()


class TestGameWriteContract:
    def test_claim_game(self):
        contract = GameWriteContract(run_label="test", log=MagicMock())
        source = GameWriteSource("detail", "TestCrawler", "test_reason")
        contract.claim_game("g1", source)
        assert "g1" in contract.claimed_games

    def test_summary(self):
        contract = GameWriteContract(run_label="test", log=MagicMock())
        contract.claim_game("g1", GameWriteSource("detail", "Test", "test"))
        summary = contract.summary()
        assert "test" in summary


class TestGameCollectionItemResult:
    def test_defaults(self):
        item = GameCollectionItemResult(game_id="g1", game_date="20240315")
        assert item.detail_status == "pending"
        assert item.relay_status == "not_requested"
        assert item.detail_saved is False
        assert item.relay_rows_saved == 0

    def test_with_failure(self):
        item = GameCollectionItemResult(game_id="g1", game_date="20240315", failure_reason="timeout")
        assert item.failure_reason == "timeout"


class TestGameCollectionTarget:
    def test_as_crawler_input(self):
        target = GameCollectionTarget(game_id="20240315LGSS0", game_date="20240315")
        result = target.as_crawler_input()
        assert result == {"game_id": "20240315LGSS0", "game_date": "20240315"}


class TestBuildGameIdRange:
    def test_january(self):
        start, end = build_game_id_range(2024, 1)
        assert start == "20240101"
        assert end == "20240201"

    def test_december_rolls(self):
        start, end = build_game_id_range(2024, 12)
        assert start == "20241201"
        assert end == "20250101"

    def test_no_month(self):
        start, end = build_game_id_range(2024, None)
        assert start == "20240101"
        assert end == "20250101"


class TestFormatGameDate:
    def test_datetime(self):
        assert _format_game_date(datetime(2024, 3, 15), fallback_game_id="x") == "20240315"

    def test_date(self):
        assert _format_game_date(date(2024, 3, 15), fallback_game_id="x") == "20240315"

    def test_yyyymmdd_string(self):
        assert _format_game_date("20240315", fallback_game_id="x") == "20240315"

    def test_dashed_string(self):
        assert _format_game_date("2024-03-15", fallback_game_id="x") == "20240315"

    def test_invalid_falls_back(self):
        assert _format_game_date("invalid", fallback_game_id="20240315LG0") == "20240315"

    def test_none_falls_back(self):
        assert _format_game_date(None, fallback_game_id="20240315LG0") == "20240315"


class TestGetFailureReason:
    def test_no_getter(self):
        crawler = MagicMock(spec=[])
        assert _get_failure_reason(crawler, "g1") is None

    def test_with_getter(self):
        crawler = MagicMock()
        crawler.get_last_failure_reason.return_value = "timeout"
        assert _get_failure_reason(crawler, "g1") == "timeout"

    def test_exception_returns_none(self):
        crawler = MagicMock()
        crawler.get_last_failure_reason.side_effect = RuntimeError("bad")
        assert _get_failure_reason(crawler, "g1") is None


class TestHasRequiredDetailRows:
    def test_full_box(self):
        payload = {
            "hitters": {"away": [{"name": "a"}], "home": [{"name": "b"}]},
            "pitchers": {"away": [{"name": "c"}], "home": [{"name": "d"}]},
        }
        assert _has_required_detail_rows(payload) is True

    def test_partial_with_teams_and_scores(self):
        payload = {
            "teams": {
                "away": {"code": "LG", "line_score": [1, 2, 3]},
                "home": {"code": "SS"},
            },
            "hitters": {},
            "pitchers": {},
        }
        assert _has_required_detail_rows(payload) is True

    def test_partial_with_metadata(self):
        payload = {
            "teams": {"away": {"code": "LG"}, "home": {"code": "SS"}},
            "metadata": {"stadium": "Jamsil", "attendance": 20000},
            "hitters": {},
            "pitchers": {},
        }
        assert _has_required_detail_rows(payload) is True

    def test_missing_teams(self):
        payload = {
            "teams": {"away": {}, "home": {}},
            "metadata": {},
            "hitters": {},
            "pitchers": {},
        }
        assert _has_required_detail_rows(payload) is False

    def test_teams_without_scores_or_metadata(self):
        payload = {
            "teams": {"away": {"code": "LG"}, "home": {"code": "SS"}},
            "metadata": {},
            "hitters": {},
            "pitchers": {},
        }
        assert _has_required_detail_rows(payload) is False


class TestExistingGameData:
    def test_defaults(self):
        d = ExistingGameData()
        assert d.has_detail is False
        assert d.has_relay is False

    def test_custom(self):
        d = ExistingGameData(has_detail=True, has_relay=True)
        assert d.has_detail is True
        assert d.has_relay is True
