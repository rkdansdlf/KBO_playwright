"""__init__.py 패키지."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from threading import Lock

from playwright.async_api import BrowserType as AsyncBrowserType
from playwright.sync_api import BrowserType as SyncBrowserType

logger = logging.getLogger(__name__)

# Keep references to original launch methods for fallback and test restoration
_original_async_launch = AsyncBrowserType.launch
_original_sync_launch = SyncBrowserType.launch

_endpoint_cursor_lock = Lock()
_endpoint_cursor_container = [0]


def _get_browserless_endpoints(raw_endpoints: str) -> list[str]:
    """Parse comma-separated WebSocket endpoints for load balancing."""
    if not raw_endpoints:
        return []
    return [ep.strip() for ep in raw_endpoints.split(",") if ep.strip()]


def _get_next_browserless_endpoint(endpoints: list[str]) -> str:
    """Return the next endpoint in a round-robin order."""
    if not endpoints:
        return ""
    with _endpoint_cursor_lock:
        idx = _endpoint_cursor_container[0] % len(endpoints)
        _endpoint_cursor_container[0] += 1
    return endpoints[idx]


def _apply_playwright_patch() -> None:  # noqa: C901, PLR0915
    """Apply global monkeypatch to Playwright browser launches to support Browserless Chrome."""
    ws_endpoint = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
    if not ws_endpoint:
        return

    try:

        async def _patched_async_launch(self: object, **kwargs: object) -> object:
            raw_endpoints = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
            endpoints = _get_browserless_endpoints(raw_endpoints or "")
            if not endpoints:
                return await _original_async_launch(self, **kwargs)  # type: ignore[arg-type]

            timeout_ms = int(os.getenv("BROWSERLESS_CONNECT_TIMEOUT_MS", "15000"))
            max_retries = int(os.getenv("BROWSERLESS_MAX_RETRIES", "3"))
            backoff_sec = float(os.getenv("BROWSERLESS_RETRY_BACKOFF_SEC", "1.0"))
            allow_fallback = os.getenv("BROWSERLESS_ALLOW_LOCAL_FALLBACK", "true").lower() in ("true", "1", "yes")

            connect_keys = {"ws_endpoint", "headers", "timeout", "slow_mo"}
            connect_kwargs = {k: v for k, v in kwargs.items() if k in connect_keys}
            if "timeout" not in connect_kwargs and timeout_ms > 0:
                connect_kwargs["timeout"] = timeout_ms

            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                endpoint = _get_next_browserless_endpoint(endpoints)
                logger.info(
                    "[PLAYWRIGHT-PATCH] Async connect attempt %d/%d for %s to %s (timeout=%s)",
                    attempt,
                    max_retries,
                    getattr(self, "name", "browser"),
                    endpoint,
                    connect_kwargs.get("timeout"),
                )
                try:
                    return await self.connect(endpoint, **connect_kwargs)  # type: ignore[attr-defined]
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.warning(
                        "[PLAYWRIGHT-PATCH] Remote connect failed attempt %d/%d (%s): %s",
                        attempt,
                        max_retries,
                        endpoint,
                        exc,
                    )
                    if attempt < max_retries:
                        sleep_time = backoff_sec * (2 ** (attempt - 1))
                        await asyncio.sleep(sleep_time)

            if allow_fallback:
                logger.warning(
                    "[PLAYWRIGHT-PATCH] All %d remote attempts failed. Fallback to local launch. Last: %s",
                    max_retries,
                    last_exc,
                )
                return await _original_async_launch(self, **kwargs)  # type: ignore[arg-type]

            if last_exc:
                raise last_exc  # noqa: TRY301
            return await _original_async_launch(self, **kwargs)  # type: ignore[arg-type]

        def _patched_sync_launch(self: object, **kwargs: object) -> object:
            raw_endpoints = os.getenv("PLAYWRIGHT_WS_ENDPOINT")
            endpoints = _get_browserless_endpoints(raw_endpoints or "")
            if not endpoints:
                return _original_sync_launch(self, **kwargs)  # type: ignore[arg-type]

            timeout_ms = int(os.getenv("BROWSERLESS_CONNECT_TIMEOUT_MS", "15000"))
            max_retries = int(os.getenv("BROWSERLESS_MAX_RETRIES", "3"))
            backoff_sec = float(os.getenv("BROWSERLESS_RETRY_BACKOFF_SEC", "1.0"))
            allow_fallback = os.getenv("BROWSERLESS_ALLOW_LOCAL_FALLBACK", "true").lower() in ("true", "1", "yes")

            connect_keys = {"ws_endpoint", "headers", "timeout", "slow_mo"}
            connect_kwargs = {k: v for k, v in kwargs.items() if k in connect_keys}
            if "timeout" not in connect_kwargs and timeout_ms > 0:
                connect_kwargs["timeout"] = timeout_ms

            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                endpoint = _get_next_browserless_endpoint(endpoints)
                logger.info(
                    "[PLAYWRIGHT-PATCH] Sync connect attempt %d/%d for %s to %s (timeout=%s)",
                    attempt,
                    max_retries,
                    getattr(self, "name", "browser"),
                    endpoint,
                    connect_kwargs.get("timeout"),
                )
                try:
                    return self.connect(endpoint, **connect_kwargs)  # type: ignore[attr-defined]
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.warning(
                        "[PLAYWRIGHT-PATCH] Remote connect failed attempt %d/%d (%s): %s",
                        attempt,
                        max_retries,
                        endpoint,
                        exc,
                    )
                    if attempt < max_retries:
                        sleep_time = backoff_sec * (2 ** (attempt - 1))
                        time.sleep(sleep_time)

            if allow_fallback:
                logger.warning(
                    "[PLAYWRIGHT-PATCH] All %d remote attempts failed. Fallback to local launch. Last: %s",
                    max_retries,
                    last_exc,
                )
                return _original_sync_launch(self, **kwargs)  # type: ignore[arg-type]

            if last_exc:
                raise last_exc  # noqa: TRY301
            return _original_sync_launch(self, **kwargs)  # type: ignore[arg-type]

        # Apply monkeypatches
        AsyncBrowserType.launch = _patched_async_launch  # type: ignore[assignment,method-assign]
        SyncBrowserType.launch = _patched_sync_launch  # type: ignore[assignment,method-assign]
        logger.info("[PLAYWRIGHT-PATCH] Playwright launch methods globally patched.")

        # Additional debug patch for Connection.dispatch
        from playwright._impl._connection import Connection

        _original_dispatch = Connection.dispatch

        def _patched_dispatch(self: Connection, msg: dict) -> None:
            try:
                _original_dispatch(self, msg)
            except KeyError:
                logger.exception(
                    "[PLAYWRIGHT-PATCH-DEBUG] KeyError in dispatch! msg=%s",
                    msg,
                )
                raise

        Connection.dispatch = _patched_dispatch  # type: ignore[assignment]
        logger.info("[PLAYWRIGHT-PATCH] Connection.dispatch successfully patched for debugging.")
    except Exception as e:  # noqa: BLE001
        logger.warning("[PLAYWRIGHT-PATCH] Failed to apply playwright patches: %s", e)


_apply_playwright_patch()
