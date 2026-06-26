"""Refresh DataSource raw snapshots and last-success timestamps."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository, save_raw_snapshots
from src.utils.http_client import DEFAULT_HEADERS
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.throttle import throttle

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.source_registry import DataSource

logger = logging.getLogger(__name__)

FETCH_OK_STATUSES = range(200, 400)
PLAYWRIGHT_FALLBACK_STATUSES = {403, 429, 500, 502, 503, 504}


@dataclass(slots=True)
class FetchedPage:
    """FetchedPage class."""

    source_key: str
    url: str
    html: str
    status_code: int
    method: str


@dataclass(slots=True)
class RefreshResult:
    """RefreshResult class."""

    source_key: str
    status: str
    method: str | None = None
    status_code: int | None = None
    url: str | None = None
    snapshots_saved: int = 0
    error: str | None = None


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Builds arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="Refresh DataSource raw snapshots")
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--all", action="store_true", help="Refresh all active DataSources")
    selector.add_argument("--domain", type=str, help="Refresh active DataSources for one target domain")
    selector.add_argument("--source-key", type=str, help="Refresh one DataSource by source_key")
    parser.add_argument("--max-hours", type=float, default=None, help="Only refresh sources stale for at least N hours")
    parser.add_argument("--dry-run", action="store_true", help="Preview target sources without fetching or writing")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    parser.add_argument("--delay", type=float, default=None, help="Override request delay between hosts")
    parser.add_argument(
        "--no-playwright-fallback",
        action="store_true",
        help="Disable Playwright fallback for blocked HTTP requests",
    )
    return parser


def _select_sources(repo: DataSourceRepository, args: argparse.Namespace) -> list[DataSource]:
    if args.source_key:
        source = repo.get_by_key(args.source_key)
        return [source] if source else []
    if args.domain:
        return repo.get_active_by_domain(args.domain)
    return repo.get_all_active()


def _is_stale(source: DataSource, max_hours: float | None) -> bool:
    if max_hours is None:
        return True
    if source.last_success_at is None:
        return True
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=max_hours)
    return source.last_success_at < cutoff


def _filter_sources(sources: Sequence[DataSource], max_hours: float | None) -> list[DataSource]:
    return [source for source in sources if source.is_active and _is_stale(source, max_hours)]


def _host_for_url(url: str) -> str:
    return urlparse(url).hostname or "unknown"


async def _fetch_with_httpx(source: DataSource, client: httpx.AsyncClient) -> FetchedPage:
    if not source.base_url:
        msg = "base_url is empty"
        raise ValueError(msg)
    host = _host_for_url(source.base_url)
    await throttle.wait(host)
    response = await client.get(source.base_url)
    return FetchedPage(
        source_key=source.source_key,
        url=str(response.url),
        html=response.text,
        status_code=response.status_code,
        method="httpx",
    )


async def _fetch_with_playwright(source: DataSource) -> FetchedPage:
    if not source.base_url:
        msg = "base_url is empty"
        raise ValueError(msg)
    pool = AsyncPlaywrightPool(
        max_pages=1,
        context_kwargs={
            "locale": "ko-KR",
            "timezone_id": "Asia/Seoul",
            "viewport": {"width": 1920, "height": 1080},
        },
    )
    await pool.start()
    page = await pool.acquire()
    try:
        response = await page.goto(source.base_url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(1500)
        return FetchedPage(
            source_key=source.source_key,
            url=page.url,
            html=await page.content(),
            status_code=response.status if response else 200,
            method="playwright",
        )
    finally:
        await pool.release(page)
        await pool.close()


async def _fetch_source(
    source: DataSource,
    client: httpx.AsyncClient,
    *,
    use_playwright_fallback: bool,
) -> FetchedPage:
    try:
        page = await _fetch_with_httpx(source, client)
    except (httpx.HTTPError, RuntimeError, ValueError, OSError) as exc:
        if not use_playwright_fallback:
            raise
        logger.info("[SOURCE] httpx failed for %s: %s. Trying Playwright fallback.", source.source_key, exc)
        return await _fetch_with_playwright(source)

    if page.status_code in FETCH_OK_STATUSES:
        return page
    if use_playwright_fallback and page.status_code in PLAYWRIGHT_FALLBACK_STATUSES:
        logger.info("[SOURCE] %s returned %s. Trying Playwright fallback.", source.source_key, page.status_code)
        return await _fetch_with_playwright(source)
    return page


async def refresh_sources(args: argparse.Namespace) -> list[RefreshResult]:
    """
    Handles the refresh sources operation.

    Args:
        args: Args.

    Returns:
        List of results.

    """
    if args.delay is not None:
        throttle.default_delay = args.delay

    results: list[RefreshResult] = []
    with SessionLocal() as session:
        repo = DataSourceRepository(session)
        sources = _filter_sources(_select_sources(repo, args), args.max_hours)
        if args.dry_run:
            return [
                RefreshResult(source_key=source.source_key, status="dry_run", url=source.base_url) for source in sources
            ]

        async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True, timeout=30) as client:
            for source in sources:
                try:
                    page = await _fetch_source(
                        source,
                        client,
                        use_playwright_fallback=not args.no_playwright_fallback,
                    )
                    if page.status_code not in FETCH_OK_STATUSES:
                        results.append(
                            RefreshResult(
                                source_key=source.source_key,
                                status="failed",
                                method=page.method,
                                status_code=page.status_code,
                                url=page.url,
                                error=f"bad status: {page.status_code}",
                            ),
                        )
                        continue
                    saved = save_raw_snapshots(session, [asdict(page)])
                    session.commit()
                    results.append(
                        RefreshResult(
                            source_key=source.source_key,
                            status="saved",
                            method=page.method,
                            status_code=page.status_code,
                            url=page.url,
                            snapshots_saved=saved,
                        ),
                    )
                except (
                    httpx.HTTPError,
                    PlaywrightError,
                    PlaywrightTimeoutError,
                    RuntimeError,
                    ValueError,
                    OSError,
                ) as exc:
                    session.rollback()
                    results.append(RefreshResult(source_key=source.source_key, status="failed", error=str(exc)))
    return results


def _write_results(results: Sequence[RefreshResult], *, json_output: bool) -> None:
    if json_output:
        sys.stdout.write(json.dumps([asdict(result) for result in results], ensure_ascii=False, indent=2) + "\n")
        return
    for result in results:
        if result.status == "saved":
            logger.info(
                "[SOURCE] %s saved via %s status=%s snapshots=%s",
                result.source_key,
                result.method,
                result.status_code,
                result.snapshots_saved,
            )
        elif result.status == "dry_run":
            logger.info("[SOURCE] %s would refresh %s", result.source_key, result.url)
        else:
            logger.error("[SOURCE] %s failed: %s", result.source_key, result.error)


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    results = asyncio.run(refresh_sources(args))
    _write_results(results, json_output=args.json)
    return 1 if any(result.status == "failed" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
