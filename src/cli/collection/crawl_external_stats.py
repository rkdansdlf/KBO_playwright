"""CLI for crawling external stats."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass

from src.crawlers.external_stats_crawler import (
    EXTERNAL_PROVIDER_ADAPTERS,
    ExternalCrawlResult,
    ExternalStatsCrawler,
)

logger = logging.getLogger(__name__)

DEFAULT_PROVIDERS = tuple(EXTERNAL_PROVIDER_ADAPTERS)


async def _crawl(season: int, providers: list[str]) -> ExternalCrawlResult:
    """Crawl the requested providers and close the owned HTTP client."""
    crawler = ExternalStatsCrawler()
    try:
        selected_providers = tuple(providers) or DEFAULT_PROVIDERS
        return await crawler.crawl(season, providers=selected_providers)
    finally:
        await crawler.close()


@dataclass(frozen=True)
class Options:
    """Options for persisting external crawl results."""

    providers: list[str]
    season: int = 0
    save: bool = False
    project: bool = False
    rebuild_rankings: bool = False
    capture_raw: bool = False


def _persist_result(result: ExternalCrawlResult, options: Options) -> None:
    """Persist successful provider results and optional derived outputs."""
    if options.save:
        from src.db.engine import SessionLocal
        from src.repositories.external_season_stats_repository import ExternalSeasonStatsRepository
        from src.repositories.source_registry_repository import save_raw_snapshots

        with SessionLocal() as session:
            repository = ExternalSeasonStatsRepository(session)
            content_hashes = {page.source_key: page.content_hash for page in result.pages}
            save_report = repository.save_records(result.records, content_hashes=content_hashes)

            providers = sorted({record.provider for record in result.records} or set(options.providers))
            projection_reports = []
            if options.project:
                projection_reports = [
                    repository.project(provider=provider, season=options.season) for provider in providers
                ]

            raw_saved = 0
            if options.capture_raw:
                raw_pages = [
                    {
                        "source_key": page.source_key,
                        "body": page.body,
                        "url": page.url,
                        "status_code": page.status_code,
                        "content_type": page.content_type,
                        "parse_status": "done",
                        "parser_version": "external-stats-v1",
                    }
                    for page in result.pages
                ]
                raw_saved = save_raw_snapshots(session, raw_pages)

            session.commit()
            logger.info(
                "External stats saved: attempted=%s saved=%s resolved=%s raw_snapshots=%s projections=%s",
                save_report.attempted,
                save_report.saved,
                save_report.resolved,
                raw_saved,
                sum(report.projected for report in projection_reports),
            )

    if options.rebuild_rankings:
        from src.cli.calculate_rankings import rebuild_rankings

        ranking_count = rebuild_rankings(options.season, external_provider=options.providers[0])
        logger.info("External stats ranking rebuild: provider=%s rows=%s", options.providers[0], ranking_count)


def main(argv: list[str] | None = None) -> int:
    """Run external stats crawling CLI."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--provider", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", action="store_true")
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--rebuild-rankings", action="store_true")
    parser.add_argument("--capture-raw", action="store_true", help="Archive successful provider responses")

    args = parser.parse_args(argv)

    if args.project and not args.save:
        sys.exit("--project requires --save")

    if args.rebuild_rankings and len(args.provider) != 1:
        sys.exit("exactly one provider required")
    if args.capture_raw and not args.save:
        sys.exit("--capture-raw requires --save")

    options = Options(
        providers=args.provider,
        season=args.season,
        save=(args.save and not args.dry_run),
        project=args.project and not args.dry_run,
        rebuild_rankings=args.rebuild_rankings and not args.dry_run,
        capture_raw=args.capture_raw and not args.dry_run,
    )

    result = asyncio.run(_crawl(args.season, args.provider))
    _persist_result(result, options)

    for failure in result.failures:
        logger.warning("External stats failure: %s", failure)
    return 1 if result.failures else 0


if __name__ == "__main__":
    sys.exit(main())
