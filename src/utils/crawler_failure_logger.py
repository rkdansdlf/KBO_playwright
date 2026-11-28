"""Crawler failure logging utility for tracking failed games and players."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_FAILURE_LOG_PATH = Path("logs/crawl_failures.log")
KBO_GAME_ID_LEN = 13
KBO_YEAR_LEN = 4


def record_crawl_failure(  # noqa: PLR0913
    target_type: str,
    identifier: str,
    url: str | None = None,
    reason: str = "unknown_error",
    *,
    retry_count: int = 3,
    error: Exception | None = None,
    log_path: Path | str = DEFAULT_FAILURE_LOG_PATH,
    crawler: str | None = None,
    run_id: str | None = None,
    session: Session | None = None,
    enqueue_dlq: bool = True,
) -> None:
    """Record a permanent crawl failure to console and a dedicated log file, and enqueue to DLQ.

    Args:
        target_type: Target category e.g. 'game', 'player', 'schedule'.
        identifier: Unique target id e.g. game_id '20260505SSLG0' or player_id.
        url: URL of the failed page.
        reason: Short reason code or descriptive failure message.
        retry_count: Total retry attempts made before final failure.
        error: Underlying exception if available.
        log_path: Path to the persistent failure log file.
        crawler: Crawler class name for dead letter tracking.
        run_id: Crawl run id for traceability.
        session: Optional SQLAlchemy DB session for transactional enqueuing.
        enqueue_dlq: Whether to record to CrawlDeadLetter DB queue.

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

    # 3. Enqueue to Dead Letter Queue (crawl_dead_letters) for automated retry & kbo dlq CLI operations
    if enqueue_dlq:
        try:
            from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
            from src.services.crawl_dead_letter_service import enqueue_failure

            err_code = type(error).__name__ if error else "CRAWL_FAILED"
            clean_id = str(identifier)
            is_game_code = target_type == "game" or (len(clean_id) == KBO_GAME_ID_LEN and clean_id[:4].isdigit())
            game_id = clean_id if is_game_code else None
            season = int(clean_id[:KBO_YEAR_LEN]) if clean_id[:4].isdigit() and len(clean_id) >= KBO_YEAR_LEN else None

            spec = DeadLetterSpec(
                original_run_id=run_id or "adhoc_run",
                crawler=crawler or f"{target_type.capitalize()}Crawler",
                target_type=target_type,
                target_id=clean_id,
                game_id=game_id,
                season=season,
                source_url=url,
                failure_stage="crawl_extraction",
                error_code=err_code,
                error_message=f"{reason}{err_str}",
                error_type=type(error).__name__ if error else "CrawlError",
                max_retries=retry_count,
            )
            enqueue_failure(spec, session=session)
            logger.info("📬 Dead letter enqueued for %s (%s)", target_type, identifier)
        except Exception as dlq_err:  # noqa: BLE001
            logger.warning("DLQ enqueue skipped or failed: %s", dlq_err)
