"""문자중계(Text Relay) 수집 CLI 모듈.

단일 또는 여러 경기의 문자중계 데이터를 수집하여 CSV로 저장합니다.

Naver 스포츠 relay API를 사용합니다 (KBO 공식 사이트 robots.txt의 전체
크롤링 차단에 대응하여 2026-08 전환). 경기 목록은 로컬 DB(game 테이블)에서
조회하며, 데이터가 없으면 Naver 스포츠 스케줄 API로 폴백합니다.

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
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import httpx

from src.constants import DATE_STR_LEN, KST

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "data"

NAVER_SCHEDULE_API = "https://api-gw.sports.naver.com/schedule/today-games"
_DECEMBER = 12
NAVER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://m.sports.naver.com",
}


def _load_game_ids_from_db(season: int, month: int | None) -> list[str]:
    """Load KBO game ids from the local game table.

    Args:
        season: Season year.
        month: Month (1-12) or None for the whole season.

    Returns:
        List of game ids.

    """
    try:
        from sqlalchemy import extract, select

        from src.db.engine import create_engine_for_url
        from src.models import Game

        db_url = os.environ.get("SOURCE_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if not db_url:
            logger.warning("[DB] No SOURCE_DATABASE_URL/DATABASE_URL set; skipping DB schedule lookup")
            return []
        engine = create_engine_for_url(db_url)
        stmt = select(Game.game_id).where(extract("year", Game.game_date) == season)
        if month is not None:
            stmt = stmt.where(extract("month", Game.game_date) == month)
        with engine.connect() as conn:
            return [row[0] for row in conn.execute(stmt).all() if row[0]]
    except Exception:
        logger.exception("[DB] Failed to load game ids from database")
        return []


async def _fetch_naver_schedule_games(
    client: httpx.AsyncClient,
    season: int,
    date_str: str,
) -> list[dict[str, Any]]:
    """Fetch the Naver schedule payload for a single date.

    Args:
        client: Http client.
        season: Season year.
        date_str: ISO date string (YYYY-MM-DD).

    Returns:
        List of game objects.

    """
    params = {
        "sectionId": "kbaseball",
        "categoryId": "kbo",
        "seasonYear": str(season),
        "date": date_str,
    }
    try:
        response = await client.get(NAVER_SCHEDULE_API, params=params, timeout=10.0)
        if response.status_code != HTTPStatus.OK:
            logger.warning("[NAVER] Schedule fetch failed with status %s for %s", response.status_code, date_str)
            return []
        payload = response.json()
        return list((payload.get("result") or {}).get("games") or [])
    except (httpx.HTTPError, ValueError, TypeError, KeyError):
        logger.exception("[NAVER] Schedule fetch/parse failed for %s", date_str)
        return []


def _kbo_game_id_from_naver(game: dict[str, Any]) -> str | None:
    """Build a canonical KBO game id from a Naver schedule game object.

    Args:
        game: Naver schedule game object.

    Returns:
        KBO game id (e.g. 20260819HTHH0) or None when fields are missing.

    """
    if game.get("cancel") or game.get("suspended"):
        return None
    game_date = str(game.get("gameDate") or "").replace("-", "")
    away = str(game.get("awayTeamCode") or "").strip()
    home = str(game.get("homeTeamCode") or "").strip()
    if len(game_date) != DATE_STR_LEN or not away or not home:
        return None
    return f"{game_date}{away}{home}0"


async def _fetch_schedule_game_ids_from_naver(season: int, month: int | None) -> list[str]:
    """Fetch KBO game ids from the Naver schedule API for a season/month.

    Args:
        season: Season year.
        month: Month (1-12) or None for the regular season months.

    Returns:
        Sorted unique KBO game ids.

    """
    months = [month] if month else list(range(3, 11))
    game_ids: set[str] = set()
    async with httpx.AsyncClient(headers=NAVER_HEADERS) as client:
        for m in months:
            end = date(season, _DECEMBER, 31) if m == _DECEMBER else date(season, m + 1, 1) - timedelta(days=1)
            cursor = date(season, m, 1)
            while cursor <= end:
                games = await _fetch_naver_schedule_games(client, season, cursor.isoformat())
                for game in games:
                    game_id = _kbo_game_id_from_naver(game)
                    if game_id:
                        game_ids.add(game_id)
                cursor += timedelta(days=1)
    return sorted(game_ids)


def _payload_to_relay_rows(payload: dict[str, Any]) -> list[Any]:
    """Convert a Naver relay payload into CSV-ready relay rows.

    Args:
        payload: Relay payload from RelayCrawler.crawl_game_relay.

    Returns:
        List of RelayRow instances.

    """
    from src.crawlers.text_relay_crawler import RelayRow

    return [
        RelayRow(
            inning=int(row.get("inning") or 0),
            inning_half=str(row.get("inning_half") or ""),
            pitcher_name=str(row.get("pitcher_name") or ""),
            batter_name=str(row.get("batter_name") or ""),
            result=str(row.get("result") or ""),
            description=str(row.get("play_description") or ""),
        )
        for row in payload.get("raw_pbp_rows") or []
    ]


def _save_relay_csv(
    game_id: str,
    rows: list[Any],
    output_dir: str,
) -> None:
    """Save relay rows as a CSV file.

    Args:
        game_id: KBO game id.
        rows: RelayRow instances.
        output_dir: Output directory.

    """
    from src.crawlers.text_relay_crawler import RelayCrawlResult

    RelayCrawlResult(
        game_id=game_id,
        game_date=game_id[:8],
        rows=rows,
        status="success",
    ).save_csv(output_dir)


async def run_single_game(
    *,
    game_id: str,
    save: bool = False,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> int:
    """Run single game.

    Args:
        game_id: Game ID.
        save: Whether to persist the results.
        output_dir: Output Dir.

    Returns:
        Integer row count.

    """
    from src.crawlers.relay_crawler import RelayCrawler

    crawler = RelayCrawler()
    try:
        payload = await crawler.crawl_game_relay(game_id)
    finally:
        await crawler.close()
    if not payload:
        logger.warning("No relay data for %s (%s)", game_id, crawler.get_last_failure_reason(game_id))
        return 0
    rows = _payload_to_relay_rows(payload)
    if save:
        _save_relay_csv(game_id, rows, output_dir)
    logger.info("Result: completed (%d rows)", len(rows))
    return len(rows)


async def run_season(
    *,
    season: int,
    month: int | None = None,
    save: bool = False,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> dict[str, int]:
    """Run season.

    Args:
        season: Season year.
        month: Month.
        save: Whether to persist the results.
        output_dir: Output Dir.

    Returns:
        Dictionary result.

    """
    from src.crawlers.relay_crawler import RelayCrawler

    game_ids = _load_game_ids_from_db(season, month)
    if not game_ids:
        game_ids = await _fetch_schedule_game_ids_from_naver(season, month)
    logger.info("Found %d games for %d/%s", len(game_ids), season, month or "*")

    crawler = RelayCrawler()
    success = 0
    failed = 0
    try:
        for game_id in game_ids:
            payload = await crawler.crawl_game_relay(game_id)
            if not payload:
                failed += 1
                logger.warning("[PROGRESS] %s: failed", game_id)
                continue
            rows = _payload_to_relay_rows(payload)
            if save:
                _save_relay_csv(game_id, rows, output_dir)
            success += 1
            logger.info("[PROGRESS] %s: success (%d rows)", game_id, len(rows))
    finally:
        await crawler.close()
    return {"total": len(game_ids), "success": success, "failed": failed}


async def run_from_args(args: argparse.Namespace) -> dict[str, int]:
    """Run from args.

    Args:
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
    """Build arg parser.

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


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    Returns:
        Exit code (0).

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)
    result = asyncio.run(run_from_args(args))
    sys.stdout.write(f"{json.dumps(result, ensure_ascii=False)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
