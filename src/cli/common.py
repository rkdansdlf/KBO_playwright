"""Shared CLI configuration dataclasses and argument types."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path


def non_negative_int(value: str) -> int:
    """Parse a non-negative integer for arguments such as ``--limit``.

    A plain ``int`` accepts ``-1``, which becomes an *unbounded* ``LIMIT`` on
    SQLite (and an error on PostgreSQL), so the bound must never go negative.

    Args:
        value: Raw command line token.

    Returns:
        The parsed non-negative integer.

    Raises:
        argparse.ArgumentTypeError: If the value is not a non-negative integer.

    """
    try:
        parsed = int(value)
    except ValueError as exc:
        msg = f"invalid integer value: {value!r}"
        raise argparse.ArgumentTypeError(msg) from exc
    if parsed < 0:
        msg = f"must be >= 0, got {parsed}"
        raise argparse.ArgumentTypeError(msg)
    return parsed


@dataclass(frozen=True)
class RegenerationConfig:
    """Shared configuration for regenerate_* CLI commands."""

    game_ids: Sequence[str] | None = None
    dates: Sequence[str] | None = None
    seasons: Sequence[int] | None = None
    apply: bool = False
    report_out: Path | None = None
    backup_out: Path | None = None
    log: Callable[[str], object] = print
