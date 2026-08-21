"""RAG vector database helpers.

PostgreSQL/pgvector remains available for isolated local acceptance tests.
Oracle Autonomous Database uses the primary RAG session and its native VECTOR
column instead of a second database.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PGVECTOR_URL = os.getenv("PGVECTOR_TEST_URL") or os.getenv("PGVECTOR_URL", "")

_VECTOR_ENGINE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, OSError)


class VectorBase(DeclarativeBase):
    """pgvector ORM 모델 전용 Base 클래스 (메인 SQLite Base와 분리)."""


def _create_vector_engine() -> Engine | None:
    """PGVECTOR_URL 환경변수로 pgvector 엔진을 생성합니다."""
    if not PGVECTOR_URL:
        logger.debug("PGVECTOR_URL not set — pgvector engine disabled")
        return None
    try:
        return create_engine(
            PGVECTOR_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            echo=False,
        )
    except _VECTOR_ENGINE_EXCEPTIONS:
        logger.exception("Failed to create pgvector engine from PGVECTOR_URL=%r", PGVECTOR_URL[:30])
        return None


VectorEngine: Engine | None = _create_vector_engine()

VectorSessionLocal = (
    sessionmaker(
        bind=VectorEngine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )
    if VectorEngine is not None
    else None
)


def is_pgvector_available() -> bool:
    """Pgvector DB가 설정되어 있고 연결 가능한지 확인합니다."""
    if VectorEngine is None:
        return False
    try:
        with VectorEngine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except _VECTOR_ENGINE_EXCEPTIONS:
        return False
    else:
        return True


def is_oracle_vector_backend() -> bool:
    """Return whether the primary database is the configured Oracle vector backend."""
    if PGVECTOR_URL:
        return False
    try:
        from src.db.engine import Engine

        return getattr(getattr(Engine, "dialect", None), "name", None) == "oracle"
    except (ImportError, AttributeError):
        return False


def is_oracle_vector_schema_available() -> bool:
    """Return whether the Oracle RAG vector column has been migrated."""
    if not is_oracle_vector_backend():
        return False
    try:
        from src.db.engine import Engine

        with Engine.connect() as connection:
            result = connection.execute(
                text(
                    "SELECT COUNT(*) FROM user_tab_columns "
                    "WHERE table_name = 'RAG_CHUNKS' AND column_name = 'EMBEDDING_VECTOR'"
                ),
            )
            return int(result.scalar() or 0) == 1
    except _VECTOR_ENGINE_EXCEPTIONS:
        return False


def is_vector_search_available() -> bool:
    """Return whether either the Oracle or PostgreSQL vector backend is available."""
    if is_pgvector_available():
        return True
    if not is_oracle_vector_schema_available():
        return False
    try:
        from src.db.engine import Engine

        with Engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM dual"))
    except _VECTOR_ENGINE_EXCEPTIONS:
        return False
    else:
        return True


@contextmanager
def get_vector_session() -> Iterator[Session]:
    """Pgvector 데이터베이스 세션을 컨텍스트 매니저로 제공합니다.

    Raises:
        RuntimeError: PGVECTOR_URL이 설정되지 않은 경우.

    """
    if is_oracle_vector_backend():
        from src.db.engine import get_rag_index_session

        with get_rag_index_session() as session:
            yield session
        return

    if VectorSessionLocal is None:
        message = (
            "pgvector DB를 사용할 수 없습니다. .env에 PGVECTOR_URL을 설정하고 Docker pgvector 서비스를 기동하세요."
        )
        raise RuntimeError(message)
    session: Session = VectorSessionLocal()
    try:
        yield session
        session.commit()
    except _VECTOR_ENGINE_EXCEPTIONS:
        session.rollback()
        raise
    finally:
        session.close()


def init_vector_db() -> None:
    """Pgvector extension 활성화 및 VectorBase 메타데이터 기반 테이블 생성."""
    if VectorEngine is None:
        logger.warning("PGVECTOR_URL not configured — skipping vector DB init")
        return
    try:
        with VectorEngine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.commit()
        VectorBase.metadata.create_all(bind=VectorEngine)
        logger.info("pgvector database initialized successfully")
    except _VECTOR_ENGINE_EXCEPTIONS:
        logger.exception("Failed to initialize pgvector database")
