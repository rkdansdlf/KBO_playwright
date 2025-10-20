"""
Database engine configuration
Supports both SQLite (dev) and MySQL (production)
"""
import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
DISABLE_SQLITE_WAL = os.getenv("DISABLE_SQLITE_WAL", "0") == "1"


def _is_sqlite(url: str) -> bool:
    """Check if database URL is SQLite"""
    return url.startswith("sqlite:")


# Shared engine factory -----------------------------------------------------

def create_engine_for_url(url: str, *, disable_sqlite_wal: bool = False):
    """Create SQLAlchemy engine for the given URL."""
    if _is_sqlite(url):
        engine = create_engine(
            url,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
            echo=False,
        )

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_con, _):  # pragma: no cover - driver specific
            try:
                cursor = dbapi_con.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                if not disable_sqlite_wal:
                    cursor.execute("PRAGMA journal_mode = WAL;")
                cursor.execute("PRAGMA synchronous = NORMAL;")
                cursor.close()
            except Exception:
                try:
                    cursor.close()
                except Exception:
                    pass

        return engine

    # MySQL / Postgres / other SQLAlchemy-supported URLs
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        echo=False,
    )


def get_engine():
    """Return the primary engine based on `DATABASE_URL`."""
    return create_engine_for_url(
        DATABASE_URL,
        disable_sqlite_wal=DISABLE_SQLITE_WAL,
    )


def get_database_type() -> str:
    """Return database type from current DATABASE_URL"""
    url = DATABASE_URL.lower()
    if url.startswith("sqlite:"):
        return "sqlite"
    elif url.startswith("mysql:"):
        return "mysql"
    elif url.startswith("postgresql:"):
        return "postgresql"
    else:
        return "unknown"


# Global engine and session factory
Engine = get_engine()
SessionLocal = sessionmaker(
    bind=Engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False
)


def get_session():
    """
    Get database session (context manager)

    Usage:
        with get_session() as session:
            # use session
            pass
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """Initialize database (create all tables)"""
    from src.models.base import Base
    # Import all models to register them with Base

    from src.models import team  # noqa: F401
    from src.models import player  # noqa: F401
    from src.models import season  # noqa: F401

    Base.metadata.create_all(bind=Engine)
    print(f"✅ Database initialized: {DATABASE_URL}")