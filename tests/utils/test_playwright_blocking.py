"""Tests for playwright_blocking — resource blocking helpers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils.playwright_blocking import (
    DEFAULT_BLOCKED_RESOURCE_TYPES,
    install_async_resource_blocking,
    install_sync_resource_blocking,
)


class TestConstants:
    def test_default_blocked_types(self):
        assert "image" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "media" in DEFAULT_BLOCKED_RESOURCE_TYPES
        assert "font" in DEFAULT_BLOCKED_RESOURCE_TYPES


class TestInstallSyncResourceBlocking:
    def test_registers_route(self):
        target = MagicMock()
        install_sync_resource_blocking(target)
        target.route.assert_called_once()

    def test_blocks_image_resources(self):
        target = MagicMock()
        install_sync_resource_blocking(target, blocked_types={"image"})

        # Get the handler and test it
        handler = target.route.call_args[0][1]
        route = MagicMock()
        route.request.resource_type = "image"
        handler(route)
        route.abort.assert_called_once()

    def test_allows_script_resources(self):
        target = MagicMock()
        install_sync_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = MagicMock()
        route.request.resource_type = "script"
        handler(route)
        route.continue_.assert_called_once()

    def test_custom_blocked_types(self):
        target = MagicMock()
        install_sync_resource_blocking(target, blocked_types={"stylesheet"})

        handler = target.route.call_args[0][1]
        route_block = MagicMock()
        route_block.request.resource_type = "stylesheet"
        handler(route_block)
        route_block.abort.assert_called_once()

        route_allow = MagicMock()
        route_allow.request.resource_type = "image"
        handler(route_allow)
        route_allow.continue_.assert_called_once()


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
        await handler(route)
        route.abort.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_allows_document_resources(self):
        target = AsyncMock()
        await install_async_resource_blocking(target)

        handler = target.route.call_args[0][1]
        route = AsyncMock()
        route.request.resource_type = "document"
        await handler(route)
        route.continue_.assert_awaited_once()
