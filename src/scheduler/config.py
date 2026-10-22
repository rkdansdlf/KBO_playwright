"""Configuration and shared constants for the KBO scheduler."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

from requests import RequestException
from sqlalchemy.exc import SQLAlchemyError

from src.config.env_loader import load_project_env
from src.db.engine import DATABASE_URL
from src.utils.lock import LockAcquisitionError

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_project_env(PROJECT_ROOT / ".env")

# Configure logging
log_path = Path("logs/scheduler.log")
log_path.parent.mkdir(exist_ok=True)

handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[handler, logging.StreamHandler()],
)
logger = logging.getLogger("src.scheduler")

KST = ZoneInfo("Asia/Seoul")
FALSE_ENV_VALUES = {"0", "false", "no", "off"}

ALERT_EXCEPTIONS = (OSError, RuntimeError, ValueError, RequestException)
SCHEDULER_JOB_EXCEPTIONS = (
    RuntimeError,
    OSError,
    ValueError,
    TypeError,
    LookupError,
    SQLAlchemyError,
    RequestException,
    asyncio.TimeoutError,
    json.JSONDecodeError,
    LockAcquisitionError,
)

# Maximum time a tier-locked (daily/maintenance) job waits for the SQLite writer lock
SQLITE_WRITE_LOCK_TIMEOUT_SECONDS = 60.0

# Alert threshold: number of lock-skips per monitor interval that triggers a warning
LOCK_SKIP_ALERT_THRESHOLD = float(os.getenv("LOCK_SKIP_ALERT_THRESHOLD", "5"))

MILESTONE_CRAWL_ENABLED_ENV = "MILESTONE_CRAWL_ENABLED"


def _scheduler_uses_sqlite_database() -> bool:
    database_url = os.getenv("DATABASE_URL", DATABASE_URL)
    return database_url.startswith("sqlite:")


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def milestone_crawl_enabled() -> bool:
    """Return whether scheduled milestone crawling is enabled.

    MilestoneCrawler reads Record/History/Top/Hitter.aspx and Pitcher.aspx, which
    are KBO's 'all-time top 10' single-season record pages (no playerId links).
    A scheduled run would upsert ~20 junk rows daily (e.g. player_id name_백인천 /
    통산 안타 / 0->10) that recalc_milestones_and_rag (07:15 KST) then indexes into
    RAG. Milestone crawling is therefore disabled by default until the crawler has
    a real milestone source.

    Returns:
        True if MILESTONE_CRAWL_ENABLED is truthy, False otherwise (default: False).

    """
    return _env_enabled(MILESTONE_CRAWL_ENABLED_ENV, "0")


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val.strip())
    except ValueError, TypeError:
        return default


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return float(val.strip())
    except ValueError, TypeError:
        return default
