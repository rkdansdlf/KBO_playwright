"""Tests for transit_time_crawler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.transit_time_crawler import JAMSIL_ORIGINS, STADIUM_CODE, TransitTimeCrawler


class TestTransitTimeCrawlerInit:
    def test_default_init(self):
        crawler = TransitTimeCrawler()
        assert crawler.stadium_code == STADIUM_CODE
        assert crawler.origins == JAMSIL_ORIGINS

    def test_custom_init(self):
        custom_origins = [{"label": "test", "lat": 1.0, "lng": 2.0, "mode": "walk"}]
        crawler = TransitTimeCrawler(stadium_code="TEST", origins=custom_origins)
        assert crawler.stadium_code == "TEST"
        assert crawler.origins == custom_origins


class TestJamsilOrigins:
    def test_has_expected_count(self):
        assert len(JAMSIL_ORIGINS) >= 5

    def test_all_have_required_keys(self):
        for origin in JAMSIL_ORIGINS:
            assert "label" in origin
            assert "lat" in origin
            assert "lng" in origin
            assert "mode" in origin

    def test_modes_are_valid(self):
        valid_modes = {"walk", "bus", "car", "subway"}
        for origin in JAMSIL_ORIGINS:
            assert origin["mode"] in valid_modes


def _make_result(label: str, mode: str, duration: int, distance: int, source: str) -> SimpleNamespace:
    return SimpleNamespace(
        origin_label=label,
        transport_mode=mode,
        duration_minutes=duration,
        distance_meters=distance,
        source_api=source,
        raw_response={"ok": True},
    )


class TestRun:
    @pytest.mark.asyncio
    async def test_run_returns_records_without_save(self):
        crawler = TransitTimeCrawler()
        walk_results = [_make_result("잠실역_2호선_7번출구", "walk", 12, 800, "naver")]
        bus_results = [_make_result("잠실역_환승센터_버스정류장", "mixed", 8, 600, "kakao")]
        car_results = [_make_result("잠실야구장_공영주차장", "car", 5, 300, "naver")]

        async def _side_effect(*args, **kwargs):
            mode = kwargs.get("mode")
            if mode == "walk":
                return walk_results
            if mode == "mixed":
                return bus_results
            return car_results

        with patch(
            "src.crawlers.transit_time_crawler.get_transit_times_batch",
            new_callable=AsyncMock,
            side_effect=_side_effect,
        ) as mock_batch:
            records = await crawler.run(game_date=date(2025, 6, 15), save=False)

        assert mock_batch.call_count == 3
        assert len(records) == 3
        assert records[0]["stadium_code"] == STADIUM_CODE
        assert records[0]["origin_label"] == "잠실역_2호선_7번출구"
        assert records[0]["duration_minutes"] == 12
        assert records[0]["transport_mode"] == "walk"

    @pytest.mark.asyncio
    async def test_run_with_empty_results(self):
        crawler = TransitTimeCrawler()
        with patch(
            "src.crawlers.transit_time_crawler.get_transit_times_batch",
            new_callable=AsyncMock,
            return_value=[],
        ):
            records = await crawler.run(game_date=date(2025, 6, 15), save=False)
        assert records == []

    @pytest.mark.asyncio
    async def test_run_with_save_calls_repo(self):
        crawler = TransitTimeCrawler()
        walk_results = [_make_result("test", "walk", 10, 500, "naver")]

        async def _side_effect(*args, **kwargs):
            if kwargs.get("mode") == "walk":
                return walk_results
            return []

        with patch(
            "src.crawlers.transit_time_crawler.get_transit_times_batch",
            new_callable=AsyncMock,
            side_effect=_side_effect,
        ):
            with patch.object(crawler, "_save_to_db") as mock_save:
                records = await crawler.run(game_date=date(2025, 6, 15), save=True)

        mock_save.assert_called_once()
        assert len(records) == 1

    @pytest.mark.asyncio
    async def test_run_without_save_skips_db(self):
        crawler = TransitTimeCrawler()

        async def _side_effect(*args, **kwargs):
            return []

        with patch(
            "src.crawlers.transit_time_crawler.get_transit_times_batch",
            new_callable=AsyncMock,
            side_effect=_side_effect,
        ):
            with patch.object(crawler, "_save_to_db") as mock_save:
                await crawler.run(game_date=date(2025, 6, 15), save=False)

        mock_save.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_groups_origins_by_mode(self):
        crawler = TransitTimeCrawler()

        async def _side_effect(*args, **kwargs):
            return []

        with patch(
            "src.crawlers.transit_time_crawler.get_transit_times_batch",
            new_callable=AsyncMock,
            side_effect=_side_effect,
        ) as mock_batch:
            await crawler.run(game_date=date(2025, 6, 15), save=False)

        calls = mock_batch.call_args_list
        modes_called = [call.kwargs.get("mode") for call in calls]
        assert "walk" in modes_called
        assert "mixed" in modes_called
        assert "car" in modes_called

    @pytest.mark.asyncio
    async def test_run_record_has_all_fields(self):
        crawler = TransitTimeCrawler()
        walk_results = [_make_result("잠실역_2호선_7번출구", "walk", 15, 900, "kakao")]

        async def _side_effect(*args, **kwargs):
            if kwargs.get("mode") == "walk":
                return walk_results
            return []

        with patch(
            "src.crawlers.transit_time_crawler.get_transit_times_batch",
            new_callable=AsyncMock,
            side_effect=_side_effect,
        ):
            records = await crawler.run(game_date=date(2025, 6, 15), save=False)

        rec = records[0]
        assert rec["stadium_code"] == STADIUM_CODE
        assert rec["origin_label"] == "잠실역_2호선_7번출구"
        assert rec["origin_lat"] == 37.5133
        assert rec["origin_lng"] == 127.0999
        assert rec["transport_mode"] == "walk"
        assert rec["duration_minutes"] == 15
        assert rec["distance_meters"] == 900
        assert rec["congestion_factor"] is None
        assert rec["source_api"] == "kakao"
        assert rec["raw_response"] == {"ok": True}
        assert isinstance(rec["measured_at"], datetime)
        assert rec["game_date"] == date(2025, 6, 15)


class TestSaveToDb:
    def test_save_commits_on_success(self):
        crawler = TransitTimeCrawler()
        records = [{"origin_label": "test", "duration_minutes": 10}]

        mock_session = MagicMock()
        mock_repo = MagicMock()
        mock_repo.bulk_upsert.return_value = (1, 0)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.crawlers.transit_time_crawler.SessionLocal", return_value=mock_session):
            with patch(
                "src.crawlers.transit_time_crawler.TransitTimeRepository",
                return_value=mock_repo,
            ):
                crawler._save_to_db(records)

        mock_repo.bulk_upsert.assert_called_once_with(records)
        mock_session.commit.assert_called_once()

    def test_save_rolls_back_on_error(self):
        crawler = TransitTimeCrawler()
        records = [{"origin_label": "test"}]

        mock_session = MagicMock()
        mock_repo = MagicMock()
        mock_repo.bulk_upsert.side_effect = RuntimeError("DB error")
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.crawlers.transit_time_crawler.SessionLocal", return_value=mock_session):
            with patch(
                "src.crawlers.transit_time_crawler.TransitTimeRepository",
                return_value=mock_repo,
            ):
                crawler._save_to_db(records)

        mock_session.rollback.assert_called_once()
