"""
문자중계(Text Relay) 수집 CLI 모듈.

단일 또는 여러 경기의 문자중계 데이터를 수집하여 CSV로 저장합니다.

사용 예시:
    # 단일 경기 수집
    python -m src.cli.crawl_text_relay --game-id 20260412SKLG0 --save

    # 시즌 전체 수집
    python -m src.cli.crawl_text_relay --season 2026 --save

    # 특정 월 수집
    python -m src.cli.crawl_text_relay --season 2026 --month 4 --save

"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "data"


async def run_single_game(
    *,
    game_id: str,
    save: bool = False,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> int:
    """
    Run single game.

    Args:
        game_id: Game ID.
        save: Whether to persist the results.
        output_dir: Output Dir.

    Returns:
        Integer result.

    """
    from src.crawlers.text_relay_crawler import TextRelayCrawler

    crawler = TextRelayCrawler(output_dir=output_dir)
    result = await crawler.crawl_game_relay(game_id, save=save)
    logger.info("Result: %s (%d rows)", result.status, len(result.rows))
    return len(result.rows)


async def run_season(
    *,
    season: int,
    month: int | None = None,
    save: bool = False,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> dict[str, int]:
    """
    Run season.

    Args:
        season: Season year.
        month: Month.
        save: Whether to persist the results.
        output_dir: Output Dir.

    Returns:
        Dictionary result.

    """
    from src.crawlers.schedule_crawler import ScheduleCrawler
    from src.crawlers.text_relay_crawler import TextRelayCrawler

    schedule_crawler = ScheduleCrawler()
    games = await schedule_crawler.crawl_schedule(year=season, month=month or 0)

    game_ids = [g["game_id"] for g in games if g.get("game_id")]
    logger.info("Found %d games for %d/%s", len(game_ids), season, month or "*")

    crawler = TextRelayCrawler(output_dir=output_dir)
    results = await crawler.crawl_games(game_ids, save=save)

    success = sum(1 for r in results if r.status == "success")
    failed = len(results) - success
    return {"total": len(results), "success": success, "failed": failed}


async def run_from_args(args: argparse.Namespace) -> dict[str, int]:
    """
    Run from args.

    Args:
        args: Positional arguments to pass through.
        args: Args.

    Returns:
        Dictionary result.

    """
    if args.game_id:
        rows = await run_single_game(
            game_id=args.game_id,
            save=args.save,
            output_dir=args.output_dir,
        )
        return {"game_id": args.game_id, "rows": rows}

    season = args.season or datetime.now(KST).year
    return await run_season(
        season=season,
        month=args.month,
        save=args.save,
        output_dir=args.output_dir,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(
        description="KBO 문자중계(Text Relay) 수집",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --game-id 20260412SKLG0 --save
  %(prog)s --season 2026 --save
  %(prog)s --season 2026 --month 4 --save --output-dir ./relay_data
        """,
    )
    parser.add_argument(
        "--game-id",
        type=str,
        default=None,
        help="수집할 단일 경기 ID (예: 20260412SKLG0)",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="수집할 시즌 연도 (기본값: 현재 연도)",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="수집할 월 (1-12, 미지정 시 전체 시즌)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="수집 결과를 CSV 파일로 저장",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"CSV 출력 디렉토리 (기본값: {DEFAULT_OUTPUT_DIR})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> dict[str, int]:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)
    return asyncio.run(run_from_args(args))


if __name__ == "__main__":
    main()
