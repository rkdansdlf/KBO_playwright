"""Resource blocking helpers for Playwright (sync + async)."""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from playwright.async_api import BrowserContext as AsyncBrowserContext
    from playwright.async_api import Page as AsyncPage
    from playwright.async_api import Route as AsyncRoute
    from playwright.sync_api import BrowserContext as SyncBrowserContext
    from playwright.sync_api import Page as SyncPage
    from playwright.sync_api import Route as SyncRoute

DEFAULT_BLOCKED_RESOURCE_TYPES: set[str] = {
    "beacon",
    "eventsource",
    "font",
    "image",
    "media",
    "ping",
    "texttrack",
}

DEFAULT_BLOCKED_DOMAINS: tuple[str, ...] = (
    "google-analytics.com",
    "googletagmanager.com",
    "doubleclick.net",
    "adservice.google",
    "pagead2.googlesyndication.com",
    "wcs.naver.net",
    "ad.naver.com",
    "g.naver.com",
    "ssl.pstatic.net/ad",
    "log.sports.naver.com",
    "ad.cr.naver.com",
    "siape.veta.naver.com",
    "t1.daumcdn.net/kas/",
    "ad.daum.net",
    "display.ad.daum.net",
    "dapi.kakao.com",
    "clarity.ms",
    "connect.facebook.net",
    "dpm.demdex.net",
    "analytics.tiktok.com",
    "sentry.io",
    "fonts.googleapis.com",
    "fonts.gstatic.com",
)


def should_block_request(
    resource_type: str,
    url: str,
    blocked_types: set[str] | None = None,
    blocked_domains: Iterable[str] | None = None,
) -> bool:
    """Determine whether a network request should be blocked.

    Args:
        resource_type: Playwright resource type (e.g. 'image', 'script').
        url: Target request URL.
        blocked_types: Set of blocked resource types.
        blocked_domains: Iterable of domain or URL substrings to block.

    Returns:
        True if the request should be blocked, False otherwise.

    """
    types = DEFAULT_BLOCKED_RESOURCE_TYPES if blocked_types is None else blocked_types
    if resource_type in types:
        return True

    domains = DEFAULT_BLOCKED_DOMAINS if blocked_domains is None else blocked_domains
    return any(domain in url for domain in domains)


async def install_async_resource_blocking(
    target: AsyncBrowserContext | AsyncPage,
    blocked_types: Iterable[str] | None = None,
    blocked_domains: Iterable[str] | None = None,
) -> None:
    """Install async network route handler to block unwanted resources and trackers.

    Args:
        target: AsyncBrowserContext or AsyncPage instance.
        blocked_types: Optional custom iterable of resource types to block.
        blocked_domains: Optional custom iterable of domains/patterns to block.

    """
    if os.getenv("KBO_BLOCK_RESOURCES", "true").lower() in ("false", "0", "no"):
        return

    types = set(blocked_types) if blocked_types is not None else DEFAULT_BLOCKED_RESOURCE_TYPES
    domains = tuple(blocked_domains) if blocked_domains is not None else DEFAULT_BLOCKED_DOMAINS

    async def handler(route: AsyncRoute) -> None:
        with contextlib.suppress(Exception):
            req = route.request
            if should_block_request(req.resource_type, req.url, types, domains):
                await route.abort()
            else:
                await route.continue_()

    await target.route("**/*", handler)


def install_sync_resource_blocking(
    target: SyncBrowserContext | SyncPage,
    blocked_types: Iterable[str] | None = None,
    blocked_domains: Iterable[str] | None = None,
) -> None:
    """Install sync network route handler to block unwanted resources and trackers.

    Args:
        target: SyncBrowserContext or SyncPage instance.
        blocked_types: Optional custom iterable of resource types to block.
        blocked_domains: Optional custom iterable of domains/patterns to block.

    """
    if os.getenv("KBO_BLOCK_RESOURCES", "true").lower() in ("false", "0", "no"):
        return

    types = set(blocked_types) if blocked_types is not None else DEFAULT_BLOCKED_RESOURCE_TYPES
    domains = tuple(blocked_domains) if blocked_domains is not None else DEFAULT_BLOCKED_DOMAINS

    def handler(route: SyncRoute) -> None:
        with contextlib.suppress(Exception):
            req = route.request
            if should_block_request(req.resource_type, req.url, types, domains):
                route.abort()
            else:
                route.continue_()

    target.route("**/*", handler)


__all__ = [
    "DEFAULT_BLOCKED_DOMAINS",
    "DEFAULT_BLOCKED_RESOURCE_TYPES",
    "install_async_resource_blocking",
    "install_sync_resource_blocking",
    "should_block_request",
]
