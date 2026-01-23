"""
Resource blocking helpers for Playwright (sync + async).
"""
from __future__ import annotations

from typing import Iterable, Set


DEFAULT_BLOCKED_RESOURCE_TYPES: Set[str] = {"image", "media", "font"}


async def install_async_resource_blocking(target, blocked_types: Iterable[str] | None = None) -> None:
    types = set(blocked_types or DEFAULT_BLOCKED_RESOURCE_TYPES)

    async def handler(route):
        if route.request.resource_type in types:
            await route.abort()
        else:
            await route.continue_()

    await target.route("**/*", handler)


def install_sync_resource_blocking(target, blocked_types: Iterable[str] | None = None) -> None:
    types = set(blocked_types or DEFAULT_BLOCKED_RESOURCE_TYPES)

    def handler(route):
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
