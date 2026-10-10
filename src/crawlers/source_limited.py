"""The checkpoint a crawler writes when policy stopped it from asking.

Three parties need to agree on this vocabulary and they live in different
layers, which is why it is its own module rather than a constant beside one of
them:

* crawlers write ``checkpoint = {"outcome": "source_limited", "reason": ...}``
  instead of reporting a failure, because nothing failed;
* the metrics projection reads it to keep a skipped run from refreshing the
  freshness gauge, which is what BUG-014 was about;
* the replay verdict reads it so a retry that was refused is not mistaken for a
  recovered incident, which is BUG-016.

Before this existed the string was written by five crawlers and read by one
projection, and the replay path -- the one that decides whether an incident
closes -- did not know it at all.
"""

from __future__ import annotations

#: The checkpoint outcome meaning "the source was never consulted".
SOURCE_LIMITED_OUTCOME = "source_limited"

#: Reasons a run may be source-limited, as a closed set for the metric label.
#:
#: Same argument as the crawler-label pattern: a free-form reason would let a
#: caller mistake multiply series. Anything outside this set is bucketed rather
#: than dropped, so an unknown reason is still counted -- it just cannot invent
#: a label.
SOURCE_LIMITED_REASONS = frozenset({"compliance_blocked", "kbo_robots_blocked"})
UNKNOWN_SOURCE_LIMITED_REASON = "other"


def source_limited_reason(checkpoint: object) -> str | None:
    """Return the reason when a checkpoint records a policy skip, else None.

    `checkpoint` is a crawler-owned JSON column, so nothing here may assume its
    shape: a run that wrote a list, a string, or a nested document reads as "not
    source-limited" rather than raising. Callers include a metrics projection
    and a replay verdict -- a raise in either would lose every record after it,
    which is worse than the ambiguity being resolved.

    Args:
        checkpoint: The raw `checkpoint` value from a crawl execution run.

    Returns:
        A bounded reason label, or None when the source was consulted.

    """
    if not isinstance(checkpoint, dict):
        return None
    if checkpoint.get("outcome") != SOURCE_LIMITED_OUTCOME:
        return None

    reason = checkpoint.get("reason")
    if isinstance(reason, str) and reason in SOURCE_LIMITED_REASONS:
        return reason
    return UNKNOWN_SOURCE_LIMITED_REASON


__all__ = [
    "SOURCE_LIMITED_OUTCOME",
    "SOURCE_LIMITED_REASONS",
    "UNKNOWN_SOURCE_LIMITED_REASON",
    "source_limited_reason",
]
