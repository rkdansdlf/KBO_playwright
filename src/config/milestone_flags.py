"""Configuration and feature flags for milestone data collection."""

from __future__ import annotations

import os

MILESTONE_CRAWL_ENABLED_ENV = "MILESTONE_CRAWL_ENABLED"
_FALSE_ENV_VALUES = frozenset({"0", "false", "no", "off"})


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
    val = os.getenv(MILESTONE_CRAWL_ENABLED_ENV, "0")
    return val.strip().lower() not in _FALSE_ENV_VALUES
