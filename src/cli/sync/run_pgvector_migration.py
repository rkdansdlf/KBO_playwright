"""pgvector DB 마이그레이션 CLI.

migrations/pgvector/ 디렉토리의 SQL 파일을 순서대로 실행하여
로컬 Docker pgvector 데이터베이스를 초기화합니다.

사용법:
    python -m src.cli.run_pgvector_migration
    python -m src.cli.run_pgvector_migration --reset  # 테이블 초기화 후 재생성
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from src.db.vector_engine import VectorBase, VectorEngine, init_vector_db

logger = logging.getLogger(__name__)

_MIGRATION_DIR = Path(__file__).resolve().parents[3] / "migrations" / "pgvector"


def _apply_sql_files(engine: object) -> None:  # type: ignore[type-arg]
    """migrations/pgvector/*.sql 파일을 번호 순서대로 실행합니다."""
    sql_files = sorted(_MIGRATION_DIR.glob("*.sql"))
    if not sql_files:
        logger.warning("No SQL files found in %s", _MIGRATION_DIR)
        return

    for sql_file in sql_files:
        logger.info("Applying migration: %s", sql_file.name)
        sql_content = sql_file.read_text(encoding="utf-8")
        try:
            with engine.begin() as conn:  # type: ignore[attr-defined]
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                cursor.execute(sql_content)
                cursor.close()
            logger.info("  ✅ %s applied", sql_file.name)
        except SQLAlchemyError:
            logger.exception("  ❌ Failed to apply %s", sql_file.name)
            raise


def main(argv: list[str] | None = None) -> None:
    """Pgvector 마이그레이션을 실행합니다."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="로컬 Docker pgvector DB 마이그레이션 실행")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="기존 테이블을 모두 삭제하고 재생성합니다 (데이터 손실 주의)",
    )
    args = parser.parse_args(argv)

    pgvector_url = os.getenv("PGVECTOR_URL")
    if not pgvector_url:
        logger.error("PGVECTOR_URL 환경변수가 설정되지 않았습니다. .env 파일에 PGVECTOR_URL을 추가하세요.")
        sys.exit(1)

    if VectorEngine is None:
        logger.error(
            "pgvector DB(%s)에 연결할 수 없습니다. docker-compose up pgvector -d 로 서비스를 먼저 기동하세요.",
            pgvector_url[:40],
        )
        sys.exit(1)

    if args.reset:
        logger.warning("⚠️  모든 vector 테이블을 삭제합니다...")
        VectorBase.metadata.drop_all(bind=VectorEngine)
        logger.info("테이블 삭제 완료")

    logger.info("migrations/pgvector/ SQL 파일 적용 중...")
    _apply_sql_files(VectorEngine)

    logger.info("ORM 메타데이터 기반 테이블 생성 중...")
    init_vector_db()

    logger.info("✅ pgvector 마이그레이션 완료")


if __name__ == "__main__":
    main()
