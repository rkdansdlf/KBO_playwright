"""Policy that decides which target dates may rewrite ``game_status``.

The 2026-10-12 incident showed that a date-ordered sweep over 1982~2000 rewrote
``game_status`` on thousands of rows whose boxscore bodies were never published.
The write path had no opinion about *which* dates it was allowed to touch, so a
crawl that could not find data silently rewrote the status that recorded that
fact. This module is the missing opinion.

Three things are deliberate here:

- The default is protective. With no policy file present the current season and
  the one before it may still write, and everything older is refused. A missing
  configuration file must not be the reason a historical row changes.
- Refusal is not an error. The caller is expected to report the projected
  transitions and carry on, because a past-date run that raises would take the
  review and story batches down with it.
- Approval is a data change, not a flag. ``approved_dates`` lives in a committed
  file, so the approval that let a historical date be rewritten is reviewable.
  There is deliberately no command-line override.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)

#: Committed policy file. Absent means "built-in defaults", not "allow anything".
DEFAULT_POLICY_PATH = Path(__file__).resolve().parents[2] / "Docs" / "config" / "historical_backfill_dates.yaml"

#: Seasons that may write without an explicit approval, counting the current one.
DEFAULT_GRACE_SEASONS = 1

#: Ceiling on approved dates consumed by one run.
DEFAULT_MAX_DATES_PER_RUN = 10

#: Reported when a date is refused because its season is not approved.
REASON_SEASON_NOT_APPROVED = "historical_season_not_approved"

#: Reported when a run asked for more approved dates than the policy allows.
REASON_RUN_CAP_EXCEEDED = "approved_dates_run_cap_exceeded"


@dataclass(frozen=True)
class HistoricalBackfillPolicy:
    """Describe which dates may rewrite game status."""

    grace_seasons: int = DEFAULT_GRACE_SEASONS
    approved_dates: frozenset[str] = frozenset()
    max_dates_per_run: int = DEFAULT_MAX_DATES_PER_RUN

    def season_floor(self, today: date) -> int:
        """Return the oldest season year that may write without approval."""
        return today.year - self.grace_seasons

    def is_approved(self, target: date) -> bool:
        """Return whether the date appears in the committed approval list."""
        return target.strftime("%Y%m%d") in self.approved_dates

    def status_write_allowed(self, target: date, today: date) -> tuple[bool, str | None]:
        """Return whether ``target`` may rewrite game status.

        Args:
            target: Date the caller wants to rewrite.
            today: Today, supplied by the caller so runs stay reproducible.

        Returns:
            A ``(allowed, reason)`` pair. ``reason`` is set only when refused.

        """
        if target >= today:
            return True, None
        if target.year >= self.season_floor(today):
            return True, None
        if self.is_approved(target):
            return True, None
        return False, f"{REASON_SEASON_NOT_APPROVED}:{target:%Y%m%d}"

    def approve_for_run(self, dates: list[str], *, requested: int | None = None) -> tuple[list[str], str | None]:
        """Return the approved dates one run may consume and why it was capped.

        Args:
            dates: Approved dates, in whatever order the caller has them.
            requested: How many of them the caller wanted, for the report.

        Returns:
            A ``(allowed, reason)`` pair; ``reason`` is set when the cap bit.

        """
        wanted = requested if requested is not None else len(dates)
        if wanted <= self.max_dates_per_run:
            return list(dates), None
        return list(dates[: self.max_dates_per_run]), (
            f"{REASON_RUN_CAP_EXCEEDED}:requested={wanted},allowed={self.max_dates_per_run}"
        )


def load_historical_backfill_policy(path: Path | str | None = None) -> HistoricalBackfillPolicy:
    """Load the committed policy, falling back to protective defaults.

    A missing file is the expected state on a host that has never opted into
    historical writes, so it is not logged as a warning. A malformed file is:
    silently falling back to defaults there would look like a configured policy
    while granting a different scope than the operator wrote.

    Args:
        path: Override for the policy file location.

    Returns:
        The loaded policy.

    """
    target = Path(path) if path is not None else DEFAULT_POLICY_PATH
    if not target.exists():
        return HistoricalBackfillPolicy()
    try:
        raw = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    except OSError, yaml.YAMLError:
        logger.exception("Historical backfill policy at %s is unreadable; using defaults", target)
        return HistoricalBackfillPolicy()
    if not isinstance(raw, dict):
        logger.error("Historical backfill policy at %s is not a mapping; using defaults", target)
        return HistoricalBackfillPolicy()

    approved_raw = raw.get("approved_dates") or []
    if not isinstance(approved_raw, list):
        logger.error("approved_dates in %s is not a list; treating it as empty", target)
        approved_raw = []

    return HistoricalBackfillPolicy(
        grace_seasons=int(raw.get("current_season_grace_seasons", DEFAULT_GRACE_SEASONS)),
        approved_dates=frozenset(str(value).replace("-", "") for value in approved_raw),
        max_dates_per_run=int(raw.get("max_dates_per_run", DEFAULT_MAX_DATES_PER_RUN)),
    )
