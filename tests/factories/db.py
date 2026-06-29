"""Shared in-memory SQLite engine for tests.

The module builds a single process-wide engine with all 59 ORM tables
created once (~500ms). Each call to ``build_session`` reuses that engine
and clears all table rows between tests with batched DELETEs (~2ms),
giving per-test data isolation without paying the schema-build cost.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base


_SHARED_ENGINE = None
_SHARED_FACTORY = None
_DELETE_STMTS: list = []


def _get_shared_engine():
    global _SHARED_ENGINE, _DELETE_STMTS
    if _SHARED_ENGINE is None:
        engine = create_engine(
            "sqlite:///:memory:",
            echo=False,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )

        @event.listens_for(engine, "connect")
        def _set_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        Base.metadata.create_all(bind=engine)
        _SHARED_ENGINE = engine
        _DELETE_STMTS = [text(f"DELETE FROM {t.name}") for t in reversed(Base.metadata.sorted_tables)]
    return _SHARED_ENGINE


def _get_shared_factory():
    global _SHARED_FACTORY
    if _SHARED_FACTORY is None:
        engine = _get_shared_engine()
        _SHARED_FACTORY = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return _SHARED_FACTORY


def _reset_schema() -> None:
    """Delete all rows from every ORM table."""
    factory = _get_shared_factory()
    session = factory()
    try:
        for stmt in _DELETE_STMTS:
            session.execute(stmt)
        session.commit()
    finally:
        session.close()


def build_engine():
    """Return the shared in-memory engine (schema already created)."""
    return _get_shared_engine()


def create_tables(engine, tables):
    """Create a subset of tables on the given engine."""
    for table in tables:
        table.create(bind=engine, checkfirst=True)


def init_tables(engine):
    """Create all ORM-registered tables on the given engine (idempotent on shared)."""
    Base.metadata.create_all(bind=engine)


def build_session():
    """Return ``(engine, session)`` bound to the shared engine with empty tables."""
    _reset_schema()
    engine = _get_shared_engine()
    session = _get_shared_factory()()
    return engine, session


def resetting_factory():
    """Return a sessionmaker whose first call clears all tables.

    Use this in place of an inline ``_build_session_factory`` so each test
    starts clean without paying the schema-build cost.
    """
    _reset_schema()
    return _get_shared_factory()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """Yield a shared-engine session with empty tables; leaves schema clean on exit."""
    _reset_schema()
    session = _get_shared_factory()()
    try:
        yield session
    finally:
        try:
            session.rollback()
        finally:
            session.close()
