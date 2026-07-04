from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.game_collection_service import (
    DetailProcessingContext,
    GameCollectionConfig,
    GameCollectionItemResult,
    GameCollectionResult,
    GameCollectionTarget,
    GameWriteContract,
    GameWriteSource,
    _derive_sh_sf_for_results,
    _pause_between_detail_batches,
    _process_detail_target,
    _save_detail_payload,
)


class TestPauseBetweenDetailBatches:
    @pytest.mark.asyncio
    async def test_first_batch_no_pause(self):
        log = MagicMock()
        crawler = AsyncMock()
        await _pause_between_detail_batches(0, 1.0, crawler, log)
        log.assert_not_called()

    @pytest.mark.asyncio
    async def test_pause_between_batches(self):
        log = MagicMock()
        crawler = AsyncMock()
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await _pause_between_detail_batches(20, 1.0, crawler, log)
            log.assert_called()
            crawler.close.assert_awaited_once()


class TestProcessDetailTarget:
    def test_failed_payload(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx = MagicMock()
        ctx.cfg.log = MagicMock()
        ctx.detail_crawler = MagicMock()
        ctx.cfg.should_save_detail = None
        ctx.detail_ready = set()
        ctx.result = result
        with patch(
            "src.services.game_collection_service._detail_payload_failure_reason",
            return_value=("crawl_failed", "timeout", "no_detail_payload"),
        ):
            _process_detail_target(target, None, ctx, global_index=1, total_targets=1)
            assert result.items["g1"].detail_status == "crawl_failed"

    def test_success_save(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx = MagicMock()
        ctx.cfg.log = MagicMock()
        ctx.detail_crawler = MagicMock()
        ctx.cfg.should_save_detail = None
        ctx.detail_ready = set()
        ctx.result = result
        with (
            patch(
                "src.services.game_collection_service._detail_payload_failure_reason",
                return_value=None,
            ),
            patch("src.services.game_collection_service._save_detail_payload", return_value=True),
        ):
            _process_detail_target(target, {"data": 1}, ctx, global_index=1, total_targets=1)

    def test_failed_save(self):
        target = GameCollectionTarget(game_id="g1", game_date="20240315")
        result = GameCollectionResult()
        result.items = {"g1": GameCollectionItemResult(game_id="g1", game_date="20240315")}
        ctx = MagicMock()
        ctx.cfg.log = MagicMock()
        ctx.detail_crawler = MagicMock()
        ctx.cfg.should_save_detail = None
        ctx.detail_ready = set()
        ctx.result = result
        with (
            patch(
                "src.services.game_collection_service._detail_payload_failure_reason",
                return_value=None,
            ),
            patch("src.services.game_collection_service._save_detail_payload", return_value=False),
        ):
            _process_detail_target(target, {"data": 1}, ctx, global_index=1, total_targets=1)
            ctx.cfg.log.assert_called()


class TestDeriveShSfForResultsWithUpdates:
    def test_updates_committed(self):
        result = GameCollectionResult()
        result.items = {
            "g1": GameCollectionItemResult(game_id="g1", game_date="20240315", detail_status="success"),
        }
        mock_session = MagicMock()
        log = MagicMock()
        with (
            patch("src.services.game_collection_service.SessionLocal") as mock_sl,
            patch(
                "src.services.game_collection_service.apply_sh_sf_to_batting_stats",
                return_value=3,
            ),
        ):
            mock_sl.return_value.__enter__.return_value = mock_session
            _derive_sh_sf_for_results(result, log=log)
            mock_session.commit.assert_called_once()
            log.assert_called()
