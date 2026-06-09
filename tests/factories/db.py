from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.models.base import Base


def build_engine():
    engine = create_engine("sqlite:///:memory:", echo=False)

    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def create_tables(engine, tables):
    """Create a subset of tables on the given engine."""
    for table in tables:
        table.create(bind=engine, checkfirst=True)


def init_tables(engine):
    """Create all ORM-registered tables on the given engine."""
    Base.metadata.create_all(bind=engine)


def build_session():
    """Create an in-memory SQLite engine + session with all tables."""
    engine = build_engine()
    init_tables(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    session = Session()
    return engine, session


@contextmanager
def session_scope() -> Generator:
    """Context manager that yields a session and rolls back on error."""
    engine = build_engine()
    init_tables(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
