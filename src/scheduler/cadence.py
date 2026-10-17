"""How often a scheduled job is expected to fire, read from its own trigger.

The scheduler staleness rule needs a cadence per job, and the only honest source
is the trigger that actually schedules it. A table of intervals kept beside the
schedule is a second copy of the schedule, and it goes stale the next time the
schedule changes -- the same reason the inventory reads `job_specs()` rather than
restating it.

The interval is deliberately the **longest** gap a trigger can leave, not the
average. A job that runs every ten seconds between 12:00 and 22:00 has two gaps
(the ten seconds, and the fourteen hours overnight), and a rule built on the
short one would alert every night on a job that is behaving exactly as
scheduled. Counting the quiet stretch is what makes one rule serve a monthly job
and an every-ten-minute job without an exemption list.
"""

from __future__ import annotations

DAY_SECONDS = 86400
WEEK_SECONDS = 7 * DAY_SECONDS
MONTH_SECONDS = 31 * DAY_SECONDS

_HOURS_PER_DAY = 24
_SECONDS_PER_MINUTE = 60
_MINUTES_PER_HOUR = 60


def _cron_fields(trigger: object) -> dict[str, str]:
    """Return the trigger's cron fields as plain strings, keyed by field name."""
    return {str(getattr(field, "name", "")): str(field) for field in getattr(trigger, "fields", ())}


def _count_part(part: str, *, low: int, high: int) -> int:
    """Count the values one comma-separated piece of a cron field selects."""
    if not part:
        return 0
    body, _, raw_step = part.partition("/")
    try:
        step = max(1, int(raw_step)) if raw_step else 1
    except ValueError:
        step = 1
    if body in ("*", "?"):
        return (high - low) // step + 1
    if "-" in body:
        start_text, _, stop_text = body.partition("-")
        try:
            start, stop = int(start_text), int(stop_text)
        except ValueError:
            return 0
        return (stop - start) // step + 1 if stop >= start else 0
    return 1


def _count_values(expression: str, *, low: int, high: int) -> int:
    """Count the values a cron field expression selects, or 0 when unreadable."""
    return sum(_count_part(part.strip(), low=low, high=high) for part in str(expression).split(","))


def expected_interval_seconds(trigger: object) -> int:
    """Return the longest expected gap, in seconds, between two firings.

    Returns ``0`` when the trigger cannot be read. The caller treats that as "no
    cadence known" rather than "fires constantly": a missing cadence must not
    silently become the tightest window in the alert rule, which would turn a job
    nobody understands into an alert that fires all day.
    """
    fields = _cron_fields(trigger)
    if not fields:
        return 0
    if fields.get("day_of_week", "*") != "*":
        return WEEK_SECONDS
    if fields.get("day", "*") != "*":
        return MONTH_SECONDS

    hours = _count_values(fields.get("hour", "*"), low=0, high=_HOURS_PER_DAY - 1)
    minutes = _count_values(fields.get("minute", "*"), low=0, high=_SECONDS_PER_MINUTE - 1)
    seconds = _count_values(fields.get("second", "*"), low=0, high=_SECONDS_PER_MINUTE - 1)
    if hours <= 0 or minutes <= 0 or seconds <= 0:
        return 0

    within_active = _MINUTES_PER_HOUR * _SECONDS_PER_MINUTE / (minutes * seconds)
    quiet_hours = max(0, _HOURS_PER_DAY - min(hours, _HOURS_PER_DAY))
    return int(quiet_hours * _MINUTES_PER_HOUR * _SECONDS_PER_MINUTE + max(within_active, 1.0))
