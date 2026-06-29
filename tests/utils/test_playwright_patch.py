from __future__ import annotations

import os
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from playwright.async_api import BrowserType as AsyncBrowserType
from playwright.sync_api import BrowserType as SyncBrowserType

from src import _apply_playwright_patch, _original_async_launch, _original_sync_launch


@pytest.fixture(autouse=True)
def _restore_playwright_launches() -> Generator[None, None, None]:
    """Ensure Playwright launch methods are restored after each test."""
    yield
    AsyncBrowserType.launch = _original_async_launch
    SyncBrowserType.launch = _original_sync_launch


def test_playwright_async_patch_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that async launch is redirected to connect when PLAYWRIGHT_WS_ENDPOINT is set."""
    endpoint = "ws://localhost:9999"
    monkeypatch.setenv("PLAYWRIGHT_WS_ENDPOINT", endpoint)

    # Re-apply the patch to ensure it runs with the set env var
    _apply_playwright_patch()

    mock_connect = AsyncMock()
    # Mock the connect method on AsyncBrowserType
    with patch.object(AsyncBrowserType, "connect", mock_connect):
        browser_type_mock = MagicMock(spec=AsyncBrowserType)
        browser_type_mock.name = "chromium"
        browser_type_mock.connect = mock_connect

        import asyncio

        asyncio.run(AsyncBrowserType.launch(browser_type_mock, headless=True, timeout=5000, invalid_arg="val"))

        # Verify that connect was called with the endpoint and connection-compatible kwargs only
        mock_connect.assert_called_once_with(endpoint, timeout=5000)


def test_playwright_sync_patch_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that sync launch is redirected to connect when PLAYWRIGHT_WS_ENDPOINT is set."""
    endpoint = "ws://localhost:9999"
    monkeypatch.setenv("PLAYWRIGHT_WS_ENDPOINT", endpoint)

    # Re-apply the patch
    _apply_playwright_patch()

    mock_connect = MagicMock()
    with patch.object(SyncBrowserType, "connect", mock_connect):
        browser_type_mock = MagicMock(spec=SyncBrowserType)
        browser_type_mock.name = "chromium"
        browser_type_mock.connect = mock_connect

        SyncBrowserType.launch(browser_type_mock, headless=True, timeout=5000, invalid_arg="val")

        # Verify that connect was called
        mock_connect.assert_called_once_with(endpoint, timeout=5000)


def test_playwright_patch_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that launch falls back to the original launch method when PLAYWRIGHT_WS_ENDPOINT is unset."""
    monkeypatch.delenv("PLAYWRIGHT_WS_ENDPOINT", raising=False)

    # Ensure methods are clean
    AsyncBrowserType.launch = _original_async_launch
    SyncBrowserType.launch = _original_sync_launch

    # Re-apply patch (should do nothing since env is cleared)
    _apply_playwright_patch()

    assert AsyncBrowserType.launch == _original_async_launch
    assert SyncBrowserType.launch == _original_sync_launch
