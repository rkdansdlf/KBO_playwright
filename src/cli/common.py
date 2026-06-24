"""Shared CLI configuration dataclasses for regeneration commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path


@dataclass(frozen=True)
class RegenerationConfig:
    """Shared configuration for regenerate_* CLI commands."""

    game_ids: Sequence[str] | None = None
    dates: Sequence[str] | None = None
    seasons: Sequence[int] | None = None
    apply: bool = False
    sync_oci: bool = False
    oci_url: str | None = None
    report_out: Path | None = None
    backup_out: Path | None = None
    log: Callable[[str], object] = print
