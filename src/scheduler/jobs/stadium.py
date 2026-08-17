"""Stadium real-time jobs: transit time, congestion, and operation notices."""

from __future__ import annotations

import asyncio
import logging
import sys

from tenacity import retry, stop_after_attempt, wait_exponential

from src.scheduler.alerting import alert_failure
from src.scheduler.config import SCHEDULER_JOB_EXCEPTIONS
from src.scheduler.locks import DAILY_LOCK, LIVE_LOCK, _scheduler_job_lock, _sqlite_writer_lock, _with_lock_skip_guard

logger = logging.getLogger("src.scheduler.jobs.stadium")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=10, max=60),
    retry_error_callback=alert_failure,
)
def crawl_transit_time_job() -> None:
    """Collect transit time to stadiums (Jamsil etc.) via Kakao Mobility."""
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    live_lock = getattr(mod, "LIVE_LOCK", LIVE_LOCK) if mod else LIVE_LOCK

    if not live_lock.acquire(blocking=False):
        logger.info("Skipping transit time crawl because LIVE_LOCK is held")
        return
    try:
        logger.info("=== Starting Transit Time Crawl ===")
        from src.crawlers.transit_time_crawler import TransitTimeCrawler

        crawler = TransitTimeCrawler()
        with _sqlite_writer_lock(blocking=False, job_id="crawl_transit_time") as acquired:
            if not acquired:
                logger.info("Skipping transit time crawl: sqlite_writer lock is held")
                return
            asyncio.run(crawler.run(save=True))
        logger.info("=== Transit Time Crawl Completed Successfully ===")
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("Transit time crawl failed")
    finally:
        live_lock.release()


def crawl_congestion_job() -> None:
    """Collect real-time stadium area congestion via Seoul Open Data."""
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    live_lock = getattr(mod, "LIVE_LOCK", LIVE_LOCK) if mod else LIVE_LOCK

    if not live_lock.acquire(blocking=False):
        logger.info("Skipping congestion crawl because LIVE_LOCK is held")
        return
    try:
        logger.info("[Congestion] Starting congestion data collection")
        from src.crawlers.congestion_crawler import CongestionCrawler

        crawler = CongestionCrawler()
        with _sqlite_writer_lock(blocking=False, job_id="crawl_congestion") as acquired:
            if not acquired:
                logger.info("Skipping congestion crawl: sqlite_writer lock is held")
                return
            asyncio.run(crawler.run(save=True))
        logger.info("[Congestion] Congestion data collection completed")
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("[Congestion] Congestion data collection failed")
    finally:
        live_lock.release()


@_with_lock_skip_guard
def crawl_operation_notices_job() -> None:
    """Operation Notices: stadium gate opening times, event guidelines, rain checks."""
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    lock_fn = getattr(mod, "_scheduler_job_lock", _scheduler_job_lock) if mod else _scheduler_job_lock
    daily_lock = getattr(mod, "DAILY_LOCK", DAILY_LOCK) if mod else DAILY_LOCK

    with lock_fn(daily_lock):
        logger.info("=== Starting Operation Notices Crawl (Club Sites) ===")
        try:
            from src.cli.crawl_operation_notices import main as notices_main

            notices_main(["--save"])
            logger.info("=== Operation Notices Crawl Completed Successfully ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Operation notices crawl failed")


@_with_lock_skip_guard
def crawl_operation_notices_naver_job() -> None:
    """Operation Notices: Naver News/Blog search for last-minute stadium operational updates."""
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    lock_fn = getattr(mod, "_scheduler_job_lock", _scheduler_job_lock) if mod else _scheduler_job_lock
    daily_lock = getattr(mod, "DAILY_LOCK", DAILY_LOCK) if mod else DAILY_LOCK

    with lock_fn(daily_lock):
        logger.info("=== Starting Operation Notices Crawl (Naver) ===")
        try:
            from src.cli.crawl_operation_notices import main as notices_main

            notices_main(["--naver", "--save"])
            logger.info("=== Operation Notices Naver Crawl Completed Successfully ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Operation notices Naver crawl failed")
