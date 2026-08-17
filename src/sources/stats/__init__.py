"""Third-party season-stat source adapters."""

from __future__ import annotations

from .base import ExternalStatRecord as ExternalStatRecord
from .base import ExternalStatsAccessError as ExternalStatsAccessError
from .base import ExternalStatsAdapter as ExternalStatsAdapter
from .base import ExternalStatsParseError as ExternalStatsParseError
from .base import StatTableParseConfig as StatTableParseConfig
from .fangraphs import FanGraphsKboAdapter as FanGraphsKboAdapter
from .statiz import StatizKboAdapter as StatizKboAdapter

__all__ = [
    "ExternalStatRecord",
    "ExternalStatsAccessError",
    "ExternalStatsAdapter",
    "ExternalStatsParseError",
    "FanGraphsKboAdapter",
    "StatTableParseConfig",
    "StatizKboAdapter",
]
