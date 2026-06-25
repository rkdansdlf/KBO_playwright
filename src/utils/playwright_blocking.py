"""Resource blocking helpers for Playwright (sync + async)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from playwright.async_api import BrowserContext as AsyncBrowserContext
    from playwright.async_api import Page as AsyncPage
    from playwright.async_api import Route as AsyncRoute
    from playwright.sync_api import BrowserContext as SyncBrowserContext
    from playwright.sync_api import Page as SyncPage
    from playwright.sync_api import Route as SyncRoute

DEFAULT_BLOCKED_RESOURCE_TYPES: set[str] = {"image", "media", "font"}


async def install_async_resource_blocking(
    target: AsyncBrowserContext | AsyncPage,
    blocked_types: Iterable[str] | None = None,
) -> None:
    """Handles the install async resource blocking operation.

    Args:
        target: Target.
        blocked_types: Blocked Types.

    """
    types = set(blocked_types or DEFAULT_BLOCKED_RESOURCE_TYPES)

    async def handler(route: AsyncRoute) -> None:
        """Handles the handler operation.

        Args:
            route: Route.

        """
        if route.request.resource_type in types:
            await route.abort()
        else:
            await route.continue_()

    await target.route("**/*", handler)


def install_sync_resource_blocking(
    target: SyncBrowserContext | SyncPage,
    blocked_types: Iterable[str] | None = None,
) -> None:
    """Syncs install resource blocking.

    Args:
        target: Target.
        blocked_types: Blocked Types.

    """
    types = set(blocked_types or DEFAULT_BLOCKED_RESOURCE_TYPES)

    def handler(route: SyncRoute) -> None:
        """Handles the handler operation.

        Args:
            route: Route.

        """
        if route.request.resource_type in types:
            route.abort()
        else:
            route.continue_()

    target.route("**/*", handler)


__all__ = [
    "DEFAULT_BLOCKED_RESOURCE_TYPES",
    "install_async_resource_blocking",
    "install_sync_resource_blocking",
]
