"""오프라인 HTML fixture를 사용하여 전체 데이터 파이프라인을 시연하는 스크립트.

이 스크립트는 실제 웹 크롤링 없이, 로컬에 저장된 HTML 파일을 사용하여
다음과 같은 전체 데이터 처리 과정을 보여줍니다:
1. 경기 일정(schedule) HTML을 읽어와 데이터베이스에 저장합니다.
2. 경기 상세(game detail) HTML을 읽어와 데이터베이스에 저장합니다.
3. (선택 사항) 실제 퓨처스리그 크롤러를 실행합니다.
4. 처리된 데이터의 요약 정보를 출력합니다.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import func

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.parsers.game_detail_parser import parse_game_detail_html
from src.parsers.schedule_parser import parse_schedule_html
from src.repositories.game_repository import save_game_detail
from src.services.schedule_collection_service import save_schedule_games

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def ingest_schedule_fixtures(fixtures_dir: Path, season_type: str, default_year: int | None) -> int:
    """경기 일정 fixture 파일들을 데이터베이스로 가져옵니다."""
    total = 0
    for html_file in sorted(fixtures_dir.glob("*.html")):
        html = html_file.read_text(encoding="utf-8")
        rows = parse_schedule_html(html, default_year=default_year, season_type=season_type)
        if not rows:
            continue
        result = save_schedule_games(rows)
        total += result.saved
        logger.info("✅ Schedule ingest: %s (%s saved, %s failed)", html_file.name, result.saved, result.failed)
    return total


def ingest_game_fixtures(fixtures_dir: Path) -> int:
    """경기 상세 정보 fixture 파일들을 데이터베이스로 가져옵니다."""
    count = 0
    for html_file in sorted(fixtures_dir.glob("*.html")):
        game_id = html_file.stem
        html = html_file.read_text(encoding="utf-8")
        payload = parse_game_detail_html(html, game_id, game_id[:8])
        if save_game_detail(payload):
            count += 1
            logger.info("✅ Game ingest: %s", game_id)
    return count


async def run_futures(limit: int | None, season: int, delay: float, concurrency: int) -> None:
    """퓨처스리그 크롤러를 실행하는 래퍼(wrapper) 함수."""
    from src.cli.crawl_futures import crawl_futures

    args = argparse.Namespace(
        season=season,
        concurrency=concurrency,
        delay=delay,
        limit=limit,
    )
    await crawl_futures(args)


def _count_games_by_season_id() -> dict[str, int]:
    """Return stored game counts grouped by season_id."""
    with SessionLocal() as session:
        rows = (
            session.query(Game.season_id, func.count(Game.game_id))
            .group_by(Game.season_id)
            .order_by(Game.season_id)
            .all()
        )
    return {str(season_id if season_id is not None else "unknown"): count for season_id, count in rows}


def show_schedule_totals() -> None:
    """현재 저장된 경기 수 요약을 출력합니다."""
    counts = _count_games_by_season_id()
    logger.info("\n📊 Schedule totals:")
    for season_id, count in sorted(counts.items()):
        logger.info("  - season_id %s: %s", season_id, count)


def show_summary(game_ids: list[str]) -> None:
    """처리된 게임 데이터의 요약 정보를 출력합니다."""
    show_schedule_totals()

    with SessionLocal() as session:
        for game_id in game_ids:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            batting_rows = session.query(GameBattingStat).filter(GameBattingStat.game_id == game_id).count()
            pitching_rows = session.query(GamePitchingStat).filter(GamePitchingStat.game_id == game_id).count()

            logger.info("\n🎯 Game summary: %s", game_id)
            if game:
                logger.info("  Game date:  %s", game.game_date)
                logger.info("  Season ID:  %s", game.season_id)
                logger.info("  Stored scores: away %s / home %s", game.away_score, game.home_score)
            else:
                logger.info("  Game: not found")
            logger.info("  Batting rows:  %s", batting_rows)
            logger.info("  Pitching rows: %s", pitching_rows)


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="End-to-end pipeline demo using saved fixtures.")
    parser.add_argument("--schedule-fixtures", type=str, default=None, help="경기 일정 HTML fixture가 있는 디렉터리")
    parser.add_argument(
        "--schedule-season-type",
        type=str,
        default="regular",
        choices=["preseason", "regular", "postseason"],
        help="적용할 시즌 유형",
    )
    parser.add_argument("--schedule-year", type=int, default=None, help="적용할 시즌 연도")
    parser.add_argument("--game-fixtures", type=str, default=None, help="경기 상세 HTML fixture가 있는 디렉터리")
    parser.add_argument("--report-game-id", action="append", default=[], help="요약 보고서를 출력할 게임 ID")
    parser.add_argument("--run-futures", action="store_true", help="(선택) 퓨처스리그 크롤러 실행")
    parser.add_argument("--futures-limit", type=int, default=None, help="퓨처스 크롤러가 처리할 최대 선수 수")
    parser.add_argument("--futures-season", type=int, default=None, help="퓨처스 크롤러의 기준 시즌")
    parser.add_argument("--futures-delay", type=float, default=1.5, help="퓨처스 크롤러의 요청 간 지연 시간")
    parser.add_argument("--futures-concurrency", type=int, default=3, help="퓨처스 크롤러의 동시 요청 수")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """스크립트의 메인 실행 함수."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.schedule_fixtures:
        fixtures_dir = Path(args.schedule_fixtures)
        if not fixtures_dir.exists():
            msg = f"Schedule fixtures directory not found: {fixtures_dir}"
            raise SystemExit(msg)
        total = ingest_schedule_fixtures(fixtures_dir, args.schedule_season_type, args.schedule_year)
        logger.info("\n✅ Schedule ingest complete (%s rows processed)", total)

    game_ids = list(args.report_game_id)
    if args.game_fixtures:
        game_dir = Path(args.game_fixtures)
        if not game_dir.exists():
            msg = f"Game fixtures directory not found: {game_dir}"
            raise SystemExit(msg)
        ingested = ingest_game_fixtures(game_dir)
        logger.info("\n✅ Game detail ingest complete (%s files)", ingested)
        if ingested and not game_ids:
            game_ids = [path.stem for path in sorted(game_dir.glob("*.html"))]

    if args.run_futures:
        season = args.futures_season
        if season is None:
            from datetime import datetime

            season = datetime.now(KST).year
        asyncio.run(run_futures(args.futures_limit, season, args.futures_delay, args.futures_concurrency))

    if game_ids:
        show_summary(game_ids)
    else:
        show_schedule_totals()


if __name__ == "__main__":  # pragma: no cover
    main()
