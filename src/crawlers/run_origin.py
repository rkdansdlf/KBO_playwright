"""Caller identity for a recorded crawl execution.

The run ledger records *what* ran and *whether* it worked, but not *who asked
for it*. That gap is invisible until something has to be attributed: a day of
``schedule`` runs at a two-minute cadence cannot be told apart from the daily
pipeline's single run without it, and the difference matters because the ledger
is the projection every crawl alert reads.

``checkpoint`` was the tempting place for this and is the wrong one twice over.
It is written only on the terminal paths, so a run left in ``running`` by a dead
process -- exactly the case worth attributing -- would carry nothing. And it is
a crawler's own progress channel, so a later ``{"page": 3}`` would overwrite it.

So the caller is its own nullable column instead. Rows written before this
existed stay ``NULL``, which is the honest value for them: the caller was never
recorded, rather than guessed at from the timing.
"""

from __future__ import annotations

from enum import StrEnum


class CrawlRunOrigin(StrEnum):
    """Which subsystem started a crawl execution.

    The value set is closed on purpose. An open string here would reintroduce
    exactly the problem this column solves: unbounded caller labels make the
    field useless for grouping, which is the only reason to record it.
    """

    #: The live polling loop, which re-reads schedule data as a dependency.
    LIVE_CRAWLER = "live_crawler"
    #: The daily orchestrating pipeline.
    DAILY_UPDATE = "daily_update"
    #: A dead letter retry or recovery job.
    DLQ_RETRY = "dlq_retry"
    #: A scheduled maintenance or backfill job.
    SCHEDULER = "scheduler"
    #: A replay dispatched for an existing dead letter.
    REPLAY = "replay"
    #: Direct operator or script invocation.
    CLI = "cli"


#: Every origin a recorded run may carry. Used by the contract test so a new
#: member has to be a deliberate addition rather than a stray string.
CRAWL_RUN_ORIGINS = frozenset(origin.value for origin in CrawlRunOrigin)

__all__ = ["CRAWL_RUN_ORIGINS", "CrawlRunOrigin"]
