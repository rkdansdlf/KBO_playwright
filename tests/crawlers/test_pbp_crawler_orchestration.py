from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import src.crawlers.pbp_crawler as pbp_crawler
from src.crawlers.pbp_crawler import GameEventsContext, PBPCrawler


class TestPrepareAndWait:
    def test_prepare_stops_when_compliance_rejects_url(self, monkeypatch):
        crawler = PBPCrawler()
        page = MagicMock()
        monkeypatch.setattr(pbp_crawler.compliance, "is_allowed", AsyncMock(return_value=False))

        result = asyncio.run(crawler._prepare_live_text_page(page, "20260101", "https://example.test/live"))

        assert result is False

    def test_prepare_warms_session_before_relay_navigation(self, monkeypatch):
        crawler = PBPCrawler()
        crawler.policy.delay_async = AsyncMock()
        page = MagicMock()
        page.goto = AsyncMock()
        monkeypatch.setattr(pbp_crawler.compliance, "is_allowed", AsyncMock(return_value=True))
        monkeypatch.setattr(pbp_crawler.asyncio, "sleep", AsyncMock())

        result = asyncio.run(crawler._prepare_live_text_page(page, "20260101", "https://example.test/live"))

        assert result is True
        assert page.goto.await_count == 2
        assert page.goto.await_args_list[1].kwargs["referer"].endswith("gameDate=20260101")

    def test_wait_marks_empty_page_as_empty_failure(self):
        crawler = PBPCrawler()
        page = MagicMock()
        page.wait_for_selector = AsyncMock(side_effect=TimeoutError())
        page.content = AsyncMock(return_value="데이터가 없습니다")

        result = asyncio.run(crawler._wait_for_pbp_container(page, "game"))

        assert result is False
        assert crawler.last_failure_reason == "empty"

    def test_wait_allows_non_empty_page_after_selector_timeout(self):
        crawler = PBPCrawler()
        page = MagicMock()
        page.wait_for_selector = AsyncMock(side_effect=TimeoutError())
        page.content = AsyncMock(return_value="temporary markup")

        assert asyncio.run(crawler._wait_for_pbp_container(page, "game")) is True


class TestLegacyExtraction:
    def test_build_legacy_event_updates_top_team_score_and_wpa(self):
        crawler = PBPCrawler()
        crawler.wpa_calc = MagicMock()
        crawler.wpa_calc.get_win_probability.side_effect = [0.4, 0.5]
        state = crawler._initial_legacy_state()
        state.update({"current_inning": 3, "current_half": "top"})

        event = crawler._build_legacy_event(state, "타자 홍길동: 솔로 홈런", 1, 0, 0)

        assert event["event_seq"] == 1
        assert event["away_score"] == 1
        assert event["home_score"] == 0
        assert event["batter"] == "홍길동"
        assert event["wpa"] == -0.1

    def test_extracts_legacy_events_in_chronological_order(self):
        crawler = PBPCrawler()
        crawler.wpa_calc = MagicMock()
        crawler.wpa_calc.get_win_probability.return_value = 0.5
        page = MagicMock()
        page.evaluate = AsyncMock(
            return_value=[
                {"text": "타자 홍길동: 안타", "class": "normaiflTxt"},
                {"text": "1회초", "class": "blue"},
            ],
        )

        events = asyncio.run(crawler._extract_flat_events_legacy(page))

        assert len(events) == 1
        assert events[0]["inning"] == 1
        assert events[0]["description"] == "타자 홍길동: 안타"

    def test_returns_empty_events_when_page_evaluation_fails(self):
        crawler = PBPCrawler()
        page = MagicMock()
        page.evaluate = AsyncMock(side_effect=RuntimeError("page closed"))

        assert asyncio.run(crawler._extract_flat_events_legacy(page)) == []


class TestPoolLifecycle:
    def test_crawl_owns_and_closes_created_pool(self, monkeypatch):
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.close = AsyncMock()
        crawler = PBPCrawler()
        crawl = AsyncMock(return_value={"game_id": "20260101AABB0", "events": []})
        monkeypatch.setattr(pbp_crawler, "AsyncPlaywrightPool", MagicMock(return_value=pool))
        monkeypatch.setattr(crawler, "_crawl_game_events_with_pool", crawl)

        result = asyncio.run(crawler.crawl_game_events("20260101AABB0"))

        assert result == {"game_id": "20260101AABB0", "events": []}
        pool.start.assert_awaited_once()
        pool.close.assert_awaited_once()
        assert crawl.await_args.args[1:3] == ("20260101AABB0", "20260101")

    def test_crawl_uses_injected_pool_without_closing_it(self, monkeypatch):
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.close = AsyncMock()
        crawler = PBPCrawler(pool=pool)
        monkeypatch.setattr(crawler, "_crawl_game_events_with_pool", AsyncMock(return_value=None))

        assert asyncio.run(crawler.crawl_game_events("20260101AABB0")) is None
        pool.start.assert_not_awaited()
        pool.close.assert_not_awaited()

    def test_pool_wrapper_releases_page_after_success(self, monkeypatch):
        pool = MagicMock()
        page = MagicMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        crawler = PBPCrawler(pool=pool)
        monkeypatch.setattr(crawler, "_crawl_game_events_page", AsyncMock(return_value={"events": ["event"]}))

        result = asyncio.run(crawler._crawl_game_events_with_pool(pool, "game", "20260101", "url"))

        assert result == {"events": ["event"]}
        pool.release.assert_awaited_once_with(page)

    def test_pool_wrapper_records_error_when_acquire_fails(self):
        pool = MagicMock()
        pool.acquire = AsyncMock(side_effect=RuntimeError("pool unavailable"))
        crawler = PBPCrawler(pool=pool)

        assert asyncio.run(crawler._crawl_game_events_with_pool(pool, "game", "20260101", "url")) is None
        assert crawler.last_failure_reason == "error"


class TestPageProcessing:
    def test_page_processing_returns_events(self, monkeypatch):
        crawler = PBPCrawler()
        page = MagicMock()
        crawler._prepare_live_text_page = AsyncMock(return_value=True)
        crawler._wait_for_pbp_container = AsyncMock(return_value=True)
        crawler._extract_flat_events_legacy = AsyncMock(return_value=[{"event_seq": 1}])
        monkeypatch.setattr(crawler, "_is_auth_redirect", MagicMock(return_value=False))

        result = asyncio.run(
            crawler._crawl_game_events_page(GameEventsContext(MagicMock(), page, "game", "20260101", "url", 0))
        )

        assert result == {"game_id": "game", "game_date": "20260101", "events": [{"event_seq": 1}]}

    def test_page_processing_marks_empty_extraction(self, monkeypatch):
        crawler = PBPCrawler()
        crawler._prepare_live_text_page = AsyncMock(return_value=True)
        crawler._wait_for_pbp_container = AsyncMock(return_value=True)
        crawler._extract_flat_events_legacy = AsyncMock(return_value=[])
        monkeypatch.setattr(crawler, "_is_auth_redirect", MagicMock(return_value=False))

        assert (
            asyncio.run(
                crawler._crawl_game_events_page(
                    GameEventsContext(MagicMock(), MagicMock(), "game", "20260101", "url", 0)
                )
            )
            is None
        )
        assert crawler.last_failure_reason == "empty"

    def test_page_processing_retries_auth_redirect(self, monkeypatch):
        crawler = PBPCrawler()
        crawler._prepare_live_text_page = AsyncMock(return_value=True)
        crawler._retry_after_auth_redirect = AsyncMock(return_value={"events": ["retry"]})
        monkeypatch.setattr(crawler, "_is_auth_redirect", MagicMock(return_value=True))

        result = asyncio.run(
            crawler._crawl_game_events_page(GameEventsContext(MagicMock(), MagicMock(), "game", "20260101", "url", 0))
        )

        assert result == {"events": ["retry"]}

    def test_auth_retry_stops_after_second_redirect(self):
        crawler = PBPCrawler()
        pool = MagicMock()
        page = MagicMock()
        page.url = "https://example.test/Login.aspx"

        result = asyncio.run(
            crawler._retry_after_auth_redirect(GameEventsContext(pool, page, "game", "20260101", "url", 1))
        )

        assert result is None
        assert crawler.last_failure_reason == "auth_required"

    def test_auth_retry_restarts_pool_before_second_crawl(self, monkeypatch):
        crawler = PBPCrawler()
        pool = MagicMock()
        pool.close = AsyncMock()
        pool.start = AsyncMock()
        page = MagicMock()
        page.url = "https://example.test/Login.aspx"
        retry_crawl = AsyncMock(return_value={"events": ["retry"]})
        monkeypatch.setattr(crawler, "_crawl_game_events_with_pool", retry_crawl)

        result = asyncio.run(
            crawler._retry_after_auth_redirect(GameEventsContext(pool, page, "game", "20260101", "url", 0))
        )

        assert result == {"events": ["retry"]}
        pool.close.assert_awaited_once()
        pool.start.assert_awaited_once()
        assert retry_crawl.await_args.kwargs["retry_count"] == 1
