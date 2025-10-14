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


def _is_sqlite(url: str) -> bool:
    """Check if database URL is SQLite"""
    return url.startswith("sqlite:")


def get_engine():
    """
    Create database engine with appropriate settings for SQLite or MySQL

    Returns:
        SQLAlchemy Engine instance
    """
    if _is_sqlite(DATABASE_URL):
        # SQLite-specific configuration
        engine = create_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
            echo=False  # Set to True for SQL debugging
        )

        # Enable SQLite optimizations
        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_con, _):
            cursor = dbapi_con.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")  # Enable foreign keys
            cursor.execute("PRAGMA journal_mode = WAL;")  # Write-Ahead Logging
            cursor.execute("PRAGMA synchronous = NORMAL;")  # Balance safety/speed
            cursor.close()

        return engine
    else:
        # MySQL-specific configuration
        return create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            echo=False
        )


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
    from src.models import game  # noqa: F401
    from src.models import team  # noqa: F401
    from src.models import player  # noqa: F401

    Base.metadata.create_all(bind=Engine)
    print(f"✅ Database initialized: {DATABASE_URL}")
