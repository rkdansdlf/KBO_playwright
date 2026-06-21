"""CLI for KBO official event/promotion source refresh."""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Sequence

from src.crawlers.kbo_event_crawler import KboEventCrawler

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> int:
    crawler = KboEventCrawler(base_url=args.url)
    events = await crawler.run(save=args.save)
    if args.save:
        logger.info("[KBO_EVENT] Saved crawl result.")
    else:
        for event in events[:10]:
            logger.info("[KBO_EVENT] %s -> %s", event.get("title"), event.get("source_url"))
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl KBO official event/promotion links")
    parser.add_argument("--save", action="store_true", help="Save raw snapshot and extracted links")
    parser.add_argument("--url", default="https://www.koreabaseball.com", help="KBO official page URL to inspect")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
