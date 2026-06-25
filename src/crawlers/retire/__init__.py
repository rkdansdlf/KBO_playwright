"""crawlers.retire 패키지."""

from __future__ import annotations

"""
Retired/inactive player crawlers.
"""

from .detail import RetiredPlayerDetailCrawler
from .listing import RetiredPlayerListingCrawler

__all__ = ["RetiredPlayerDetailCrawler", "RetiredPlayerListingCrawler"]
