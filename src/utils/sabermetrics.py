"""Shared sabermetric formula utilities."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PitchingStats:
    """PitchingStats class."""

    home_runs: int = 0
    walks: int = 0
    hit_batters: int = 0
    strikeouts: int = 0


def calculate_fip(stats: PitchingStats, ip: float, fip_constant: float) -> float:
    """
    Calculates fip.

    Args:
        stats: Stats.
        ip: Ip.
        fip_constant: Fip Constant.

    Returns:
        float instance.

    """
    if ip <= 0:
        return 0.0
    return round(
        ((13 * stats.home_runs) + (3 * (stats.walks + stats.hit_batters)) - (2 * stats.strikeouts)) / ip + fip_constant,
        2,
    )


def calculate_era(earned_runs: int, ip: float) -> float:
    """
    Calculates era.

    Args:
        earned_runs: Earned Runs.
        ip: Ip.

    Returns:
        float instance.

    """
    if ip <= 0:
        return 0.0
    return round((earned_runs / ip) * 9, 2)
