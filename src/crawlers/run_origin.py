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

import contextvars
import os
from contextlib import contextmanager
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


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

#: Environment override for callers that cannot share the context object: the
#: daily pipeline runs many of its steps as child processes (``python -m src.cli.*``),
#: and a context variable does not cross that boundary. The value is validated
#: against the closed set, so a typo in a deployment script is ignored rather
#: than recorded as a new label.
RUN_ORIGIN_ENV_VAR = "KBO_RUN_ORIGIN"

_current_origin: contextvars.ContextVar[str | None] = contextvars.ContextVar("kbo_crawl_run_origin", default=None)


def current_run_origin() -> str | None:
    """Return the caller identity in force, or ``None`` when nobody claimed one.

    The context value is set by the entry points that know what they are
    (:func:`use_run_origin`); the environment override exists for child processes,
    which cannot inherit a context variable. An unset or unparseable value returns
    ``None`` -- the same honest "not recorded" as a row written before this field
    existed, rather than a guess.
    """
    claimed = _current_origin.get()
    if claimed is not None:
        return claimed
    from_env = os.environ.get(RUN_ORIGIN_ENV_VAR)
    return from_env if from_env in CRAWL_RUN_ORIGINS else None


@contextmanager
def use_run_origin(origin: CrawlRunOrigin | str) -> Iterator[None]:
    """Claim ``origin`` for every recorded run inside the block.

    The claim is reset on the way out, exception or not: APScheduler reuses its
    worker threads, so a value that outlived its job would mislabel the next job
    scheduled onto the same thread.

    Args:
        origin: The caller identity to claim.

    Yields:
        Nothing; the claim is ambient for the duration of the block.

    Raises:
        ValueError: when ``origin`` is not one of :data:`CRAWL_RUN_ORIGINS`.
            The set is closed; an unknown label is a programming error, not a
            new kind of caller.

    """
    value = origin.value if isinstance(origin, CrawlRunOrigin) else str(origin)
    if value not in CRAWL_RUN_ORIGINS:
        message = f"unknown crawl run origin: {value!r}"
        raise ValueError(message)
    token = _current_origin.set(value)
    try:
        yield
    finally:
        _current_origin.reset(token)


__all__ = [
    "CRAWL_RUN_ORIGINS",
    "RUN_ORIGIN_ENV_VAR",
    "CrawlRunOrigin",
    "current_run_origin",
    "use_run_origin",
]
