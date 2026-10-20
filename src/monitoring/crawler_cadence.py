"""The period each crawler is expected to complete a run in.

`KboCrawlerNoRecentSuccess` needs a period per crawler, and one fixed window
cannot supply one. Six hours was the previous choice, and it is shorter than one
cycle of every scheduled crawler: a daily collector that failed reported for the
six hours after its run and then went quiet for the other eighteen. Measured on
2026-10-19 in the run ledger, `food` and `seat` each ran once a day and were
therefore *observable for a quarter of every day* -- and the daily on/off made
the alert resolve and reopen rather than stay open. Silence in the other eighteen
hours read as health.

The period is not a new fact. Every crawler is started by a scheduler job, and
that job's trigger already states how often it fires. ``src.scheduler.cadence``
reads the longest gap out of the trigger, including the quiet overnight stretch,
so a job that genuinely pauses is not counted as late. What cannot be derived is
*which* job starts a crawler: the chain is job -> CLI entrypoint -> crawler, two
hops deep, so matching crawler names against job bodies finds nothing -- the
source inventory hit the same wall and declared the mapping too. The mapping is
therefore declared here, and tests hold it against both the registry and the
ledger vocabulary.

A crawler with no periodic collection has no period, and inventing one would
page on a correct silence: award history is announced once a season and runs
from CI, and the snapshot replayer follows a recording rather than a calendar.
Those are named in ``NOT_SCHEDULED`` with the reason. The rules skip a crawler
whose interval was never published rather than treat the absence as zero; zero
would mean "always late", which is the loudest possible reading of "unknown".
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.monitoring.crawler_metrics import CRAWL_EXPECTED_INTERVAL_SECONDS

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

#: The scheduler job that starts each crawler, keyed by the crawler's name in the
#: run ledger (`CrawlExecutionRun.crawler`), not by its class name.
#:
#: A job id rather than an interval, because the interval belongs to the trigger
#: and restating it here would let the two drift: moving the job's cron would
#: leave the alert window behind. `publish_crawler_cadence` reads the trigger the
#: scheduler actually registered, including the jobs registered outside
#: `job_specs()`.
CRAWLER_JOBS: dict[str, str] = {
    # crawl_daily_games (03:00) -> run_daily_update: schedule, then game details
    # and the relay for each collected game, then player movements. Four crawlers
    # behind one job, so they share its period. `relay` also runs during live
    # polling, which only makes its series fresher than this window expects.
    "game_detail": "crawl_daily_games",
    "schedule": "crawl_daily_games",
    "relay": "crawl_daily_games",
    "player_movement": "crawl_daily_games",
    # heal_pbp (04:30) -> heal_unverified_pbp_job -> PBPCrawler.
    "pbp": "heal_pbp",
    # crawl_p0_non_game (06:20) -> roster movements.
    "roster_transactions": "crawl_p0_non_game",
    # crawl_p1p2_data (06:45) -> the three P1/P2 crawlers, each reported
    # separately since the 2026-10-19 outcome plumbing.
    "seat": "crawl_p1p2_data",
    "parking": "crawl_p1p2_data",
    "food": "crawl_p1p2_data",
    # Registered in `_start_scheduler` rather than `job_specs()`: the pregame
    # refresh ticks every 15 minutes between 10:00 and 23:00, so its longest gap
    # is the overnight one. Resolved from the live trigger, never assumed.
    "preview": "crawl_pregame_refresh",
    # Weekly, Sunday 06:00, also registered outside `job_specs()`.
    "team_history": "crawl_team_info_history",
}

#: Crawlers that reach the run ledger but have no periodic local collection,
#: with the reason they are not judged by a cadence rule.
#:
#: Publishing an interval for any of these would be a claim about a schedule
#: that does not exist. Leaving them undeclared makes the cadence rules skip
#: them, which is the honest reading: these series advance in processes the
#: scheduler does not own, so no window here can describe them.
NOT_SCHEDULED: dict[str, str] = {
    "awards": (
        "runs from .github/workflows/periodic_extras.yml (monthly) in a CI process that is never "
        "scraped; its runs cannot reach these gauges, so a cadence rule here would page about a "
        "crawler this process does not run"
    ),
    "kbo_event": (
        "run through the crawl_kbo_official_events CLI from CI/manual dispatch, not from a "
        "scheduler job; no zero-argument job owns its period"
    ),
    "realtime_issue": (
        "run through the run_all_crawlers CLI (manual/CI). It has no scheduler job, so the only "
        "period that could be published would be one this file invented"
    ),
    "player_batting_all_series": (
        "weekly maintenance runs it from CI (run_weekly_maintenance), outside the scheduler "
        "process that exports these series"
    ),
    "player_pitching_all_series": (
        "weekly maintenance runs it from CI (run_weekly_maintenance), outside the scheduler "
        "process that exports these series"
    ),
    "snapshot_replay": (
        "replays a recorded snapshot rather than collecting on a period; its runs follow whatever "
        "the snapshot recorded, and the nightly drift check owns that question"
    ),
}


def _triggers_by_job(scheduler: object) -> dict[str, object]:
    """Return the registered triggers keyed by job id.

    Read from the scheduler rather than from `job_specs()` so the jobs registered
    directly in `_start_scheduler` (pregame refresh, the tier-2 list) resolve
    too. A table that could only name `job_specs()` entries would have to exempt
    `preview`, which is the crawler whose six-hour silence started this work.
    """
    get_jobs = getattr(scheduler, "get_jobs", None)
    jobs = get_jobs() if callable(get_jobs) else ()
    return {
        str(getattr(job, "id", "") or ""): getattr(job, "trigger", None)
        for job in jobs
        if str(getattr(job, "id", "") or "")
    }


def publish_crawler_cadence(scheduler: object) -> dict[str, int]:
    """Publish each crawler's expected interval from its job's live trigger.

    Called once at scheduler start, next to `_publish_job_cadence_contract`.
    Only the interval is published: unlike a job's last-run timestamp, a
    crawler's cannot be seeded with "now" at boot, because that would claim a
    collection that did not happen and would reset the very staleness the rule
    is watching.

    Args:
        scheduler: The scheduler whose registered jobs carry the triggers.

    Returns:
        The published intervals keyed by crawler name. A crawler declared in
        `CRAWLER_JOBS` but missing from the result has no published interval,
        and the cadence-based rules deliberately do not judge it.

    """
    # Imported here rather than at module scope: `src.scheduler.__init__` imports
    # the registry, and the registry imports this module to publish the table. A
    # module-level import of `src.scheduler.cadence` would therefore re-enter a
    # partially initialised package. `cadence` itself is a leaf module -- it
    # imports nothing from `src.scheduler` -- so reading it inside the call is
    # the whole cycle.
    from src.scheduler.cadence import expected_interval_seconds

    triggers = _triggers_by_job(scheduler)
    published: dict[str, int] = {}
    unresolved: list[str] = []

    for crawler, job_id in sorted(CRAWLER_JOBS.items()):
        interval = expected_interval_seconds(triggers.get(job_id))
        if interval <= 0:
            unresolved.append(f"{crawler}->{job_id}")
            continue
        CRAWL_EXPECTED_INTERVAL_SECONDS.labels(crawler=crawler).set(interval)
        published[crawler] = interval

    if unresolved:
        # A declared crawler whose job is missing is unjudged by the cadence
        # rules -- the same silence those rules exist to remove. Saying so at
        # start is the only place it can be noticed.
        logger.warning(
            "No usable cadence for %s; cadence-based crawler alerts will not judge them "
            "(the job is not registered, or its trigger is not a cron trigger)",
            ", ".join(unresolved),
        )

    return published


def declared_crawlers() -> Iterable[str]:
    """Return every crawler this module accounts for, scheduled or not."""
    return (*CRAWLER_JOBS, *NOT_SCHEDULED)


__all__ = [
    "CRAWLER_JOBS",
    "NOT_SCHEDULED",
    "declared_crawlers",
    "publish_crawler_cadence",
]
