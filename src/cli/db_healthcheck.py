"""
데이터베이스 상태를 점검하는 CLI 스크립트.

환경 변수에 설정된 `DATABASE_URL`을 사용하여 데이터베이스에 연결하고,
연결 성공 여부, 테이블 목록, 주요 테이블의 레코드 수를 출력하여
데이터베이스의 현재 상태를 빠르게 진단할 수 있도록 돕습니다.

사용법:
  python -m src.cli.db_healthcheck
"""

from __future__ import annotations

import logging

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import DATABASE_URL, Engine

logger = logging.getLogger(__name__)


def main(_argv: list[str] | None = None) -> None:
    """데이터베이스 상태 점검을 수행하는 메인 함수."""
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    url = DATABASE_URL
    dialect = Engine.url.get_backend_name()

    logger.info("\n=== DB Healthcheck ===")
    logger.info("URL: %s", url)
    logger.info("Dialect: %s", dialect)

    # 1. 데이터베이스 연결 테스트
    try:
        with Engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connectivity: OK")
    except SQLAlchemyError:
        logger.exception("Connectivity: FAILED")
        return

    # 2. 테이블 목록 조회
    try:
        insp = inspect(Engine)
        tables = insp.get_table_names()
        logger.info("Tables: %s found", len(tables))
        if tables:
            # 최대 10개의 테이블 이름 출력
            for t in tables[:10]:
                logger.info("  - %s", t)
    except SQLAlchemyError:
        logger.exception("Introspection failed")

    # 3. 주요 테이블의 레코드 수 집계
    for table in [
        "players",
        "teams",
        "game_schedules",
        "player_season_batting",
        "player_game_batting",
        "player_game_pitching",
        "game_events",
        "game_summary",
        "game_play_by_play",
    ]:
        try:
            with Engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))  # noqa: S608
                count = result.scalar_one()
                logger.info("%s: %s", table, count)
        except SQLAlchemyError:
            # 테이블이 존재하지 않으면 조용히 넘어감
            continue

    logger.info("\nReview/WPA focus:")
    logger.info("  - game_events: required raw event source for Coach review and WPA summaries")
    logger.info("  - game_play_by_play: optional legacy text feed")

    logger.info("\nHealthcheck complete.\n")


if __name__ == "__main__":
    main()
