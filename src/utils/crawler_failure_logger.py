"""Crawler failure logging utility for tracking failed games and players."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from src.constants import KST

logger = logging.getLogger(__name__)

DEFAULT_FAILURE_LOG_PATH = Path("logs/crawl_failures.log")


def record_crawl_failure(  # noqa: PLR0913
    target_type: str,
    identifier: str,
    url: str | None = None,
    reason: str = "unknown_error",
    *,
    retry_count: int = 3,
    error: Exception | None = None,
    log_path: Path | str = DEFAULT_FAILURE_LOG_PATH,
) -> None:
    """Record a permanent crawl failure to console and a dedicated log file.

    Args:
        target_type: Target category e.g. 'game', 'player', 'schedule'.
        identifier: Unique target id e.g. game_id '20260505SSLG0' or player_id.
        url: URL of the failed page.
        reason: Short reason code or descriptive failure message.
        retry_count: Total retry attempts made before final failure.
        error: Underlying exception if available.
        log_path: Path to the persistent failure log file.

    """
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    err_str = f" | Exception: {type(error).__name__}: {error}" if error else ""
    url_str = url or "N/A"

    msg = (
        f"❌ [CRAWL FAILURE] [{now_str}] Type: {target_type} | ID: {identifier} | "
        f"URL: {url_str} | Retries: {retry_count} | Reason: {reason}{err_str}"
    )

    logger.error(msg)

    # 2. Append to dedicated failure log file for subsequent retry / reconciliation
    try:
        path = Path(log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{now_str}\t{target_type}\t{identifier}\t{url_str}\t{retry_count}\t{reason}{err_str}\n")
    except OSError:
        logger.warning("Failed to append to crawl failure log file: %s", log_path, exc_info=True)
