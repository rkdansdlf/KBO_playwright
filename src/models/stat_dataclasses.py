"""데이터 모델: stat dataclasses."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BattingStats:
    """BattingStats class."""

    hits: int = 0
    at_bats: int = 0
    walks: int = 0
    hbp: int = 0
    sf: int = 0
    strikeouts: int = 0
    doubles: int = 0
    triples: int = 0
    home_runs: int = 0


@dataclass
class PitchingStats:
    """PitchingStats class."""

    total_outs: int = 0
    hits: int = 0
    bb: int = 0
    er: int = 0
    k: int = 0
    hr: int = 0
