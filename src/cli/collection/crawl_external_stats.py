"""CLI for crawling external stats."""

from __future__ import annotations

import argparse
import asyncio
import sys

from src.crawlers.external_stats_crawler import ExternalCrawlResult


async def _crawl(_season: int, _providers: list[str]) -> ExternalCrawlResult:
    return ExternalCrawlResult(records=[], pages=[], failures=[])


class Options:
    """Options for persisting external crawl results."""

    def __init__(self, providers: list[str], *, save: bool = False) -> None:
        """Initialize Options."""
        self.save = save
        self.providers = providers


def _persist_result(_result: ExternalCrawlResult, _options: Options) -> None:
    pass


def main(argv: list[str] | None = None) -> int:
    """Run external stats crawling CLI."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--provider", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", action="store_true")
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--rebuild-rankings", action="store_true")

    args = parser.parse_args(argv)

    if args.project and not args.save:
        sys.exit("--project requires --save")

    if args.rebuild_rankings and len(args.provider) != 1:
        sys.exit("exactly one provider required")

    options = Options(providers=args.provider, save=(args.save and not args.dry_run))

    result = asyncio.run(_crawl(args.season, args.provider))
    _persist_result(result, options)

    return 0


if __name__ == "__main__":
    sys.exit(main())
