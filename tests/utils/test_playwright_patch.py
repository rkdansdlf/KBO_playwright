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


def test_multi_endpoint_load_balancing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test round-robin endpoint selection with multiple endpoints."""
    endpoints = "ws://ep1:3000/chromium/playwright, ws://ep2:3000/chromium/playwright"
    monkeypatch.setenv("PLAYWRIGHT_WS_ENDPOINT", endpoints)
    monkeypatch.setenv("BROWSERLESS_MAX_RETRIES", "1")
    monkeypatch.setenv("BROWSERLESS_RETRY_BACKOFF_SEC", "0.01")

    _apply_playwright_patch()

    called_endpoints = []

    async def fake_connect(endpoint: str, **kwargs):
        called_endpoints.append(endpoint)
        return MagicMock()

    mock_connect = AsyncMock(side_effect=fake_connect)
    with patch.object(AsyncBrowserType, "connect", mock_connect):
        browser_type_mock = MagicMock(spec=AsyncBrowserType)
        browser_type_mock.name = "chromium"
        browser_type_mock.connect = mock_connect

        import asyncio

        asyncio.run(AsyncBrowserType.launch(browser_type_mock, headless=True))
        asyncio.run(AsyncBrowserType.launch(browser_type_mock, headless=True))

    assert len(called_endpoints) == 2
    assert "ws://ep1:3000/chromium/playwright" in called_endpoints
    assert "ws://ep2:3000/chromium/playwright" in called_endpoints


def test_browserless_async_retry_and_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test async retry attempts and graceful fallback when all retries fail."""
    endpoint = "ws://localhost:9999"
    monkeypatch.setenv("PLAYWRIGHT_WS_ENDPOINT", endpoint)
    monkeypatch.setenv("BROWSERLESS_MAX_RETRIES", "3")
    monkeypatch.setenv("BROWSERLESS_RETRY_BACKOFF_SEC", "0.01")
    monkeypatch.setenv("BROWSERLESS_ALLOW_LOCAL_FALLBACK", "true")

    _apply_playwright_patch()

    mock_connect = AsyncMock(side_effect=RuntimeError("WS connection error"))
    mock_orig_launch = AsyncMock(return_value="local_browser")

    with (
        patch.object(AsyncBrowserType, "connect", mock_connect),
        patch("src._original_async_launch", mock_orig_launch),
    ):
        browser_type_mock = MagicMock(spec=AsyncBrowserType)
        browser_type_mock.name = "chromium"
        browser_type_mock.connect = mock_connect

        import asyncio

        res = asyncio.run(AsyncBrowserType.launch(browser_type_mock, headless=True))

        # Verify 3 connection attempts were made
        assert mock_connect.call_count == 3
        # Verify fallback to local launch
        assert res == "local_browser"
        assert mock_orig_launch.call_count == 1
