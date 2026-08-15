"""Tests for playwright_blocking — resource blocking helpers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils.playwright_blocking import (
    DEFAULT_BLOCKED_DOMAINS,
    DEFAULT_BLOCKED_RESOURCE_TYPES,
    install_async_resource_blocking,
    install_sync_resource_blocking,
    should_block_request,
)


class TestConstants:
    def test_default_blocked_types(self):
        assert "image" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "media" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "font" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "beacon" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "ping" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "texttrack" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "eventsource" in DEFAULT_BLOCKED_RESOURCE_TYPES

    def test_default_blocked_domains(self):
        assert "google-analytics.com" in DEFAULT_BLOCKED_DOMAINS
        assert "wcs.naver.net" in DEFAULT_BLOCKED_DOMAINS
        assert "ad.naver.com" in DEFAULT_BLOCKED_DOMAINS
        assert "ad.daum.net" in DEFAULT_BLOCKED_DOMAINS


class TestShouldBlockRequest:
    def test_blocks_by_resource_type(self):
        assert should_block_request("image", "https://example.com/photo.jpg") is True
        assert should_block_request("font", "https://example.com/font.woff2") is True
        assert should_block_request("media", "https://example.com/video.mp4") is True
        assert should_block_request("beacon", "https://example.com/log") is True

    def test_allows_safe_script_and_document(self):
        assert should_block_request("document", "https://www.koreabaseball.com/") is False
        assert should_block_request("script", "https://www.koreabaseball.com/js/common.js") is False
        assert should_block_request("xhr", "https://www.koreabaseball.com/api/data") is False
        assert should_block_request("fetch", "https://sports.news.naver.com/api/schedule") is False

    def test_blocks_tracker_and_ad_scripts_by_domain(self):
        assert (
            should_block_request(
                "script",
                "https://www.google-analytics.com/analytics.js",
            )
            is True
        )
        assert (
            should_block_request(
                "script",
                "https://wcs.naver.net/wcslog.js",
            )
            is True
        )
        assert (
            should_block_request(
                "xhr",
                "https://ad.daum.net/realtime/ad",
            )
            is True
        )

    def test_custom_blocked_types_and_domains(self):
        assert (
            should_block_request(
                "stylesheet",
                "https://example.com/style.css",
                blocked_types={"stylesheet"},
            )
            is True
        )
        assert (
            should_block_request(
                "script",
                "https://bad-tracker.com/track.js",
                blocked_domains=("bad-tracker.com",),
            )
            is True
        )


class TestInstallSyncResourceBlocking:
    def test_registers_route(self):
        target = MagicMock()
        install_sync_resource_blocking(target)
        target.route.assert_called_once()

    def test_blocks_image_resources(self):
        target = MagicMock()
        install_sync_resource_blocking(target, blocked_types={"image"})

        handler = target.route.call_args[0][1]
        route = MagicMock()
        route.request.resource_type = "image"
        route.request.url = "https://www.koreabaseball.com/images/logo.png"
        handler(route)
        route.abort.assert_called_once()

    def test_blocks_ad_scripts(self):
        target = MagicMock()
        install_sync_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = MagicMock()
        route.request.resource_type = "script"
        route.request.url = "https://www.google-analytics.com/analytics.js"
        handler(route)
        route.abort.assert_called_once()

    def test_allows_safe_resources(self):
        target = MagicMock()
        install_sync_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = MagicMock()
        route.request.resource_type = "script"
        route.request.url = "https://www.koreabaseball.com/Resource/js/common.js"
        handler(route)
        route.continue_.assert_called_once()

    def test_suppresses_exceptions(self):
        target = MagicMock()
        install_sync_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = MagicMock()
        route.request.resource_type = "image"
        route.request.url = "https://example.com/pic.jpg"
        route.abort.side_effect = RuntimeError("Route already handled")
        # Should not raise
        handler(route)

    def test_env_disable(self, monkeypatch):
        monkeypatch.setenv("KBO_BLOCK_RESOURCES", "false")
        target = MagicMock()
        install_sync_resource_blocking(target)
        target.route.assert_not_called()


class TestInstallAsyncResourceBlocking:
    @pytest.mark.asyncio
    async def test_registers_route(self):
        target = AsyncMock()
        await install_async_resource_blocking(target)
        target.route.assert_called_once()

    @pytest.mark.asyncio
    async def test_blocks_image_resources(self):
        target = AsyncMock()
        await install_async_resource_blocking(target, blocked_types={"image"})

        handler = target.route.call_args[0][1]
        route = AsyncMock()
        route.request.resource_type = "image"
        route.request.url = "https://www.koreabaseball.com/images/logo.png"
        await handler(route)
        route.abort.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_blocks_tracker_domains(self):
        target = AsyncMock()
        await install_async_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = AsyncMock()
        route.request.resource_type = "script"
        route.request.url = "https://ssl.pstatic.net/ad/banner.js"
        await handler(route)
        route.abort.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_allows_document_resources(self):
        target = AsyncMock()
        await install_async_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = AsyncMock()
        route.request.resource_type = "document"
        route.request.url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        await handler(route)
        route.continue_.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_suppresses_exceptions(self):
        target = AsyncMock()
        await install_async_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = AsyncMock()
        route.request.resource_type = "image"
        route.request.url = "https://example.com/pic.jpg"
        route.abort.side_effect = RuntimeError("Target closed")
        # Should not raise
        await handler(route)

    @pytest.mark.asyncio
    async def test_env_disable(self, monkeypatch):
        monkeypatch.setenv("KBO_BLOCK_RESOURCES", "0")
        target = AsyncMock()
        await install_async_resource_blocking(target)
        target.route.assert_not_called()
